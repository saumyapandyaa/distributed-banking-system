from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from passlib.context import CryptContext

from app.db import get_db
from app.models import BankAdmin
from app.schemas import AdminLogin, TokenResponse
from app.auth.jwt_handler import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])
pwd = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


@router.post("/login", response_model=TokenResponse)
def login(payload: AdminLogin, db: Session = Depends(get_db)):

    admin = db.query(BankAdmin).filter(BankAdmin.admin_id == payload.admin_id).first()

    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found")

    if not pwd.verify(payload.password, admin.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect password")

    token = create_access_token({
        "admin_id": admin.admin_id,
        "bank_id": admin.bank_id,
        "role": "BANK_ADMIN"
    })

    return TokenResponse(access_token=token)


# TEMPORARY: Create a test bank admin
@router.post("/create-test-admin")
def create_test_admin(db: Session = Depends(get_db)):

    existing = db.query(BankAdmin).filter(BankAdmin.admin_id == "ADMIN1").first()
    if existing:
        return {"message": "Admin already exists"}

    pwd_hash = pwd.hash("pass123")

    admin = BankAdmin(
        admin_id="ADMIN1",
        name="Default Bank Admin",
        password_hash=pwd_hash,
        bank_id="BANK1"
    )

    db.add(admin)
    db.commit()

    return {
        "message": "Admin created",
        "admin_id": "ADMIN1",
        "password": "pass123",
        "bank_id": "BANK1"
    }
