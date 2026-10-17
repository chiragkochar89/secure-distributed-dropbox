import json
import logging
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from app.db.models.audit import AuditLog

logger = logging.getLogger(__name__)


def log_audit_event(
    db: Session,
    event_type: str,
    user_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
) -> AuditLog:
    """Record a security audit event in the database."""
    details_str = json.dumps(details) if details else None
    
    audit_entry = AuditLog(
        user_id=user_id,
        event_type=event_type,
        resource_id=resource_id,
        ip_address=ip_address,
        details_json=details_str
    )
    db.add(audit_entry)
    db.commit()
    db.refresh(audit_entry)
    
    logger.info(f"AUDIT EVENT [{event_type}] User: {user_id or 'Anonymous'} Resource: {resource_id or 'N/A'}")
    return audit_entry
