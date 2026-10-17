import uuid
import logging
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Response, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.config import settings
from app.db.session import get_db
from app.db.models.user import User, UserRole
from app.core.security import hash_password, verify_password, create_access_token
from app.core.dependencies import get_current_user
from app.core.audit import log_audit_event
from app.raft.node import raft_node_instance, NodeRole
from app.raft.engine import raft_election_engine
from app.services.commands import FileOperationCommand, FileOperationType

logger = logging.getLogger("secure-raft-dropbox.api.auth")

router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])


class RegisterRequest(BaseModel):
    email: EmailStr
    username: str
    password: str
    admin_secret: str = None  # Optional secret to bootstrap initial admin account in dev


class LoginRequest(BaseModel):
    username_or_email: str
    password: str


class UserResponse(BaseModel):
    id: str
    email: str
    username: str
    role: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(request_data: RegisterRequest, req: Request, db: Session = Depends(get_db)):
    # 1. Non-Leader Write Interception
    if raft_node_instance.role != NodeRole.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "NOT_LEADER",
                "message": "Write operations must be sent to the current Raft leader.",
                "leader_id": raft_node_instance.leader_id
            }
        )

    # 2. Check duplicate email/username
    existing_user = db.query(User).filter(
        (User.email == request_data.email) | (User.username == request_data.username)
    ).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email or username already exists"
        )

    # Determine role (Bootstrap ADMIN if secret matches or if first user)
    user_role = UserRole.USER.value
    if request_data.admin_secret == "bootstrap-admin-secret-dev" or db.query(User).count() == 0:
        user_role = UserRole.ADMIN.value

    # Close initial read transaction snapshot so session db does not hold a stale transaction across async Raft consensus
    db.commit()

    # Argon2id password hashing
    hashed_pwd = hash_password(request_data.password)
    user_id = str(uuid.uuid4())

    # Create Raft command (Note: ONLY password_hash is stored in command, never plaintext password)
    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id=user_id,
        owner_id=user_id,
        filename=request_data.username,
        email=request_data.email,
        username=request_data.username,
        password_hash=hashed_pwd,
        role=user_role
    )

    # Append entry to Raft log
    entry = await raft_node_instance.append_log_entry(cmd)

    # Replicate across cluster & await Majority Quorum Commit AND State-Machine Application
    committed = await raft_election_engine.replicate_to_peers_and_await_applied(entry.index, timeout=5.0)

    if not committed:
        logger.error(f"Consensus failed for user registration user_id={user_id}.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "CONSENSUS_FAILED",
                "message": "Failed to achieve majority consensus for user registration."
            }
        )

    # Refresh session read snapshot following async Raft state-machine commit
    db.rollback()
    db.expire_all()

    created_user = db.query(User).filter(User.id == user_id).first()
    if not created_user:
        logger.error(f"State machine failed to persist user_id={user_id} in PostgreSQL after Raft consensus.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "REGISTRATION_PERSISTENCE_FAILED",
                "message": "User registration was committed by Raft consensus but state machine failed to persist user record."
            }
        )

    return UserResponse(
        id=created_user.id,
        email=created_user.email,
        username=created_user.username,
        role=created_user.role
    )


@router.post("/login", response_model=TokenResponse)
def login(request_data: LoginRequest, req: Request, response: Response, db: Session = Depends(get_db)):
    # Find user by username or email
    user = db.query(User).filter(
        (User.username == request_data.username_or_email) | (User.email == request_data.username_or_email)
    ).first()

    if not user or not verify_password(request_data.password, user.password_hash):
        log_audit_event(
            db=db,
            event_type="LOGIN_FAILED",
            ip_address=req.client.host if req.client else None,
            details={"attempted_user": request_data.username_or_email}
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username/email or password"
        )

    # Generate JWT token
    access_token = create_access_token(data={"sub": user.id, "role": user.role})

    # Set Secure HttpOnly Cookie
    response.set_cookie(
        key=settings.COOKIE_NAME,
        value=access_token,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/"
    )

    log_audit_event(
        db=db,
        event_type="LOGIN_SUCCESS",
        user_id=user.id,
        ip_address=req.client.host if req.client else None
    )

    user_resp = UserResponse(id=user.id, email=user.email, username=user.username, role=user.role)
    return TokenResponse(access_token=access_token, user=user_resp)


@router.post("/logout")
def logout(response: Response, req: Request, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    response.delete_cookie(key=settings.COOKIE_NAME, path="/")
    log_audit_event(
        db=db,
        event_type="LOGOUT",
        user_id=current_user.id,
        ip_address=req.client.host if req.client else None
    )
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)):
    return UserResponse(
        id=current_user.id,
        email=current_user.email,
        username=current_user.username,
        role=current_user.role
    )
