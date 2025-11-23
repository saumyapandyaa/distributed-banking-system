from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import uuid

from app.db import get_db
from app import models
from app.auth.dependencies import get_current_admin

router = APIRouter(prefix="/accounts", tags=["Accounts"])


# ----------------------------------------------------
# Get all accounts of a user
# ----------------------------------------------------
@router.get("/{user_id}")
def get_user_accounts(
    user_id: str,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    user = db.query(models.User).filter(
        models.User.user_id == user_id,
        models.User.bank_id == bank_id
    ).first()

    if not user:
        return []

    accounts = db.query(models.Account).filter(
        models.Account.user_id == user.id,
        models.Account.bank_id == bank_id
    ).all()

    return accounts
