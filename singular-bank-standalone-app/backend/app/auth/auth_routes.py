# app/auth/auth_routes.py
import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from passlib.context import CryptContext

from app.db import get_db
from app.models import BankAdmin
from app.schemas import AdminLogin, TokenResponse
from app.auth.jwt_handler import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])
pwd = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

env_bank_id = os.getenv("BANK_ID", "BANK1")  # 👈 add this once


@router.post("/login", response_model=TokenResponse)
def login(payload: AdminLogin, db: Session = Depends(get_db)):

    admin = db.query(BankAdmin).filter(
        BankAdmin.admin_id == payload.admin_id,
        BankAdmin.bank_id == env_bank_id    # 👈 enforce bank match
    ).first()


    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found")

    if not pwd.verify(payload.password, admin.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect password")

    token = create_access_token({
        "admin_id": admin.admin_id,
        "bank_id": admin.bank_id,   # 👈 comes from DB
        "role": "BANK_ADMIN"
    })

    return TokenResponse(access_token=token)

@router.post("/create-test-admin")
def create_test_admin(db: Session = Depends(get_db)):
    # env_bank_id is available from above
    admin_id = f"ADMIN_{env_bank_id}"   # e.g. ADMIN_BANK1, ADMIN_BANK2

    existing = db.query(BankAdmin).filter(
        BankAdmin.admin_id == admin_id,
        BankAdmin.bank_id == env_bank_id
    ).first()

    if existing:
        return {
            "message": "Admin already exists",
            "admin_id": existing.admin_id,
            "bank_id": existing.bank_id
        }

    pwd_hash = pwd.hash("pass123")

    admin = BankAdmin(
        admin_id=admin_id,
        name=f"Default Admin {env_bank_id}",
        password_hash=pwd_hash,
        bank_id=env_bank_id,
    )

    db.add(admin)
    db.commit()

    return {
        "message": "Admin created",
        "admin_id": admin_id,
        "password": "pass123",
        "bank_id": env_bank_id,
    }
