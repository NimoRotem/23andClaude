"""Admin API — list / create / update / delete user accounts.

All endpoints in this router require the caller to have role == 'admin'.
"""

import logging
import shutil
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from backend.api.auth import hash_password
from backend.config import user_data_dir, user_scratch_dir
from backend.database import get_db
from backend.models.schemas import User
from backend.utils.auth import get_current_user, require_admin

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class UserOut(BaseModel):
    id: str
    email: str
    display_name: str
    role: str
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    display_name: str = Field(..., min_length=1, max_length=120)
    role: str = Field("user", pattern="^(admin|user)$")


class UserUpdate(BaseModel):
    display_name: Optional[str] = Field(None, min_length=1, max_length=120)
    role: Optional[str] = Field(None, pattern="^(admin|user)$")
    password: Optional[str] = Field(None, min_length=6)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_admins(db: Session) -> int:
    return db.query(User).filter(User.role == "admin").count()


def _ensure_user_workspace(user_id: str) -> None:
    """Pre-create the per-user data + scratch directories so a brand-new user
    can immediately upload data without having to trigger lazy creation."""
    try:
        user_data_dir(user_id)
        user_scratch_dir(user_id)
    except OSError as exc:
        logger.warning("Failed to create workspace for user %s: %s", user_id, exc)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/users", response_model=list[UserOut])
def list_users(
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """List every registered user account, newest first."""
    users = db.query(User).order_by(User.created_at.desc().nullslast()).all()
    return users


@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(
    body: UserCreate,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Create a new user account (admin only)."""
    existing = db.query(User).filter(User.email == body.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A user with this email already exists",
        )
    user = User(
        email=body.email,
        password_hash=hash_password(body.password),
        display_name=body.display_name,
        role=body.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    _ensure_user_workspace(user.id)
    return user


@router.get("/users/{user_id}", response_model=UserOut)
def get_user(
    user_id: str,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.patch("/users/{user_id}", response_model=UserOut)
def update_user(
    user_id: str,
    body: UserUpdate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent demoting the last remaining admin
    if body.role is not None and user.role == "admin" and body.role != "admin":
        if _count_admins(db) <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot demote the only remaining admin",
            )
        if user.id == admin.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You cannot demote yourself",
            )

    if body.display_name is not None:
        user.display_name = body.display_name
    if body.role is not None:
        user.role = body.role
    if body.password is not None:
        user.password_hash = hash_password(body.password)

    db.commit()
    db.refresh(user)
    return user


@router.delete("/users/{user_id}", status_code=status.HTTP_200_OK)
def delete_user(
    user_id: str,
    purge_files: bool = False,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Delete a user. Their owned runs/vcfs/files have user_id set NULL but the
    rows are kept so historical data is not silently lost. Pass ?purge_files=true
    to also delete the user's per-user data directory from disk."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot delete your own account while logged in as it",
        )

    if user.role == "admin" and _count_admins(db) <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the last remaining admin",
        )

    # Detach owned rows so we don't violate FKs / silently lose history
    from backend.models.schemas import ScoringRun, VCF, GenomicFile
    db.query(ScoringRun).filter(ScoringRun.user_id == user.id).update(
        {ScoringRun.user_id: None}, synchronize_session=False
    )
    db.query(VCF).filter(VCF.created_by_user_id == user.id).update(
        {VCF.created_by_user_id: None}, synchronize_session=False
    )
    db.query(GenomicFile).filter(GenomicFile.user_id == user.id).update(
        {GenomicFile.user_id: None}, synchronize_session=False
    )

    db.delete(user)
    db.commit()

    purged = False
    if purge_files:
        try:
            from backend.config import USERS_ROOT, SCRATCH_USERS_ROOT
            for root in (USERS_ROOT, SCRATCH_USERS_ROOT):
                target = root / user_id
                if target.exists() and target.is_dir():
                    shutil.rmtree(target, ignore_errors=True)
                    purged = True
        except Exception:
            logger.exception("Failed to purge user files for %s", user_id)

    return {"deleted": user_id, "files_purged": purged}


@router.post("/users/{user_id}/workspace", response_model=UserOut)
def ensure_workspace(
    user_id: str,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Force-create the per-user data + scratch directories for a user."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    _ensure_user_workspace(user.id)
    return user
