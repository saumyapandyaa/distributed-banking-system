from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import random

from app.db import get_db
from app import models
from app.auth.dependencies import get_current_admin

router = APIRouter(prefix="/cards", tags=["Cards"])


def generate_card_number():
    return " ".join(str(random.randint(1000, 9999)) for _ in range(4))

def generate_expiry():
    return f"{random.randint(1,12):02d}/30"

def generate_cvv():
    return f"{random.randint(100, 999)}"


# ---------------------------------------
# Get all cards for a user
# ---------------------------------------
@router.get("/{user_id}")
def get_cards(user_id: str, current=Depends(get_current_admin), db: Session = Depends(get_db)):
    bank_id = current["bank_id"]

    user = db.query(models.User).filter(
        models.User.user_id == user_id,
        models.User.bank_id == bank_id
    ).first()

    if not user:
        raise HTTPException(404, "User not found")

    return db.query(models.Card).filter(models.Card.user_id == user.id).all()


# ---------------------------------------
# Create card for user
# ---------------------------------------
@router.post("/create")
def create_card(
    user_id: str,
    card_type: str,  # debit / credit / prepaid
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    if card_type not in ["debit", "credit", "prepaid"]:
        raise HTTPException(400, "Invalid card type")

    user = db.query(models.User).filter(
        models.User.user_id == user_id,
        models.User.bank_id == bank_id
    ).first()

    if not user:
        raise HTTPException(404, "User not found")

    # Only 1 card per type allowed
    exists = db.query(models.Card).filter(
        models.Card.user_id == user.id,
        models.Card.card_type == card_type
    ).first()
    if exists:
        raise HTTPException(400, f"{card_type.capitalize()} card already exists")

    # Debit cards linked to checking
    if card_type == "debit":
        checking = next((a for a in user.accounts if a.account_type == "checking"), None)
        if not checking:
            raise HTTPException(400, "User has no checking account")

    card = models.Card(
        card_number=generate_card_number(),
        card_type=card_type,
        expiry=generate_expiry(),
        cvv=generate_cvv(),
        user_id=user.id
    )

    db.add(card)
    db.commit()
    db.refresh(card)

    return {
        "message": f"{card_type.capitalize()} card created",
        "card_number": card.card_number,
        "expiry": card.expiry
    }


# ---------------------------------------
# Delete card
# ---------------------------------------
@router.delete("/delete/{card_number}")
def delete_card(card_number: str, current=Depends(get_current_admin), db: Session = Depends(get_db)):
    card = db.query(models.Card).filter(models.Card.card_number == card_number).first()

    if not card:
        raise HTTPException(404, "Card not found")

    db.delete(card)
    db.commit()

    return {"message": "Card deleted"}
