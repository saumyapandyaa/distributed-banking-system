from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import uuid
import random

from app.db import get_db
from app.models import User, Account, Transaction, Card
from app.schemas import UserCreate, UserResponse, UserUpdate
from app.auth.dependencies import get_current_admin

router = APIRouter(prefix="/users", tags=["Users"])


# ----------------------------------------------------------
# List all users for this bank
# ----------------------------------------------------------
@router.get("/", response_model=list[UserResponse])
def list_users(current=Depends(get_current_admin), db: Session = Depends(get_db)):
    bank_id = current["bank_id"]
    return db.query(User).filter(User.bank_id == bank_id).all()


# ----------------------------------------------------------
# Create user + checking + savings + debit card
# ----------------------------------------------------------
@router.post("/create")
def create_user(
    payload: UserCreate,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    # Unique phone per bank
    if db.query(User).filter(User.phone == payload.phone, User.bank_id == bank_id).first():
        raise HTTPException(status_code=400, detail="Phone already in use")

    user = User(
        user_id=f"U{uuid.uuid4().hex[:6].upper()}",
        name=payload.name,
        phone=payload.phone,
        bank_id=bank_id
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    # Create checking + savings
    checking = Account(
        account_number=f"CHK-{uuid.uuid4().hex[:6].upper()}",
        account_type="checking",
        balance=payload.initial_deposit,
        bank_id=bank_id,
        owner=user
    )
    savings = Account(
        account_number=f"SVG-{uuid.uuid4().hex[:6].upper()}",
        account_type="savings",
        balance=0.0,
        bank_id=bank_id,
        owner=user
    )

    db.add_all([checking, savings])
    db.commit()

    # Auto-create debit card
    debit_card = Card(
        card_number=" ".join(str(random.randint(1000, 9999)) for _ in range(4)),
        card_type="debit",
        expiry=f"{random.randint(1,12):02d}/30",
        cvv=str(random.randint(100, 999)),
        user_id=user.id
    )

    db.add(debit_card)
    db.commit()

    return {
        "message": "User created with checking, savings, and debit card",
        "user_id": user.user_id
    }


# ----------------------------------------------------------
# Update user
# ----------------------------------------------------------
@router.put("/update/{user_id}")
def update_user(
    user_id: str,
    payload: UserUpdate,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    user = db.query(User).filter(User.user_id == user_id, User.bank_id == bank_id).first()
    if not user:
        raise HTTPException(404, "User not found")

    # Unique phone per bank
    if payload.phone:
        exists = db.query(User).filter(
            User.phone == payload.phone,
            User.user_id != user_id,
            User.bank_id == bank_id
        ).first()
        if exists:
            raise HTTPException(400, "Phone already in use")

    if payload.name:
        user.name = payload.name
    if payload.phone:
        user.phone = payload.phone

    db.commit()
    db.refresh(user)

    return {"message": "User updated", "user_id": user.user_id}


# ----------------------------------------------------------
# Delete user + accounts + cards + transactions
# ----------------------------------------------------------
@router.delete("/{user_id}")
def delete_user(user_id: str, current=Depends(get_current_admin), db: Session = Depends(get_db)):
    bank_id = current["bank_id"]

    user = db.query(User).filter(User.user_id == user_id, User.bank_id == bank_id).first()
    if not user:
        raise HTTPException(404, "User not found")

    # Delete transactions + accounts
    for acc in user.accounts:
        db.query(Transaction).filter(Transaction.account_id == acc.id).delete()
        db.delete(acc)

    # Delete cards
    db.query(Card).filter(Card.user_id == user.id).delete()

    # Delete user
    db.delete(user)
    db.commit()

    return {"message": "User + accounts + cards deleted"}
