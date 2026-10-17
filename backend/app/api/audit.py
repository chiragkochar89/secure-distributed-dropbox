from typing import List
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models.user import User
from app.db.models.audit import AuditLog
from app.core.dependencies import get_current_admin_user

router = APIRouter(prefix="/api/v1/audit", tags=["audit-logging"])


class AuditLogResponse(BaseModel):
    id: str
    user_id: str | None
    event_type: str
    resource_id: str | None
    ip_address: str | None
    details_json: str | None
    created_at: str


@router.get("/logs", response_model=List[AuditLogResponse])
def get_audit_logs(
    admin_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    logs = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(100).all()
    return [
        AuditLogResponse(
            id=l.id,
            user_id=l.user_id,
            event_type=l.event_type,
            resource_id=l.resource_id,
            ip_address=l.ip_address,
            details_json=l.details_json,
            created_at=l.created_at.isoformat() if l.created_at else ""
        )
        for l in logs
    ]
