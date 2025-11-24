from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import uuid

from app.db import get_db
from app import models, schemas
from app.kafka_producer import publish_event
from app.auth.dependencies import get_current_admin

router = APIRouter(prefix="/transactions", tags=["Transactions"])


# ----------------------------------------------------------
# Deposit
# ----------------------------------------------------------
@router.post("/deposit")
def deposit(
    request: schemas.DepositRequest,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    account = db.query(models.Account).filter(
        models.Account.account_number == request.account_number,
        models.Account.bank_id == bank_id
    ).first()

    if not account:
        raise HTTPException(404, "Account not found")

    account.balance += request.amount

    tx = models.Transaction(
        tx_id=str(uuid.uuid4()),
        tx_type="deposit",
        amount=request.amount,
        origin_bank=bank_id,
        destination_bank=bank_id,
        account_id=account.id
    )

    db.add(tx)
    db.commit()
    db.refresh(account)

    return {"message": "Deposit successful", "balance": account.balance}


# ----------------------------------------------------------
# Withdraw
# ----------------------------------------------------------
@router.post("/withdraw")
def withdraw(
    request: schemas.WithdrawRequest,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    account = db.query(models.Account).filter(
        models.Account.account_number == request.account_number,
        models.Account.bank_id == bank_id
    ).first()

    if not account:
        raise HTTPException(404, "Account not found")

    if account.balance < request.amount:
        raise HTTPException(400, "Insufficient funds")

    account.balance -= request.amount

    tx = models.Transaction(
        tx_id=str(uuid.uuid4()),
        tx_type="withdraw",
        amount=request.amount,
        origin_bank=bank_id,
        destination_bank=bank_id,
        account_id=account.id
    )

    db.add(tx)
    db.commit()
    db.refresh(account)

    return {"message": "Withdrawal successful", "balance": account.balance}


# ----------------------------------------------------------
# Internal transfer (checking ↔ savings)
# ----------------------------------------------------------
@router.post("/transfer-internal")
def transfer_internal(
    request: schemas.InternalAccountTransferRequest,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    user = db.query(models.User).filter(
        models.User.user_id == request.user_id,
        models.User.bank_id == bank_id
    ).first()

    if not user:
        raise HTTPException(404, "User not found")

    from_acc = next((a for a in user.accounts if a.account_type == request.from_account_type), None)
    to_acc = next((a for a in user.accounts if a.account_type == request.to_account_type), None)

    if not from_acc or not to_acc:
        raise HTTPException(404, "Account type missing")

    if from_acc.balance < request.amount:
        raise HTTPException(400, "Insufficient funds")

    from_acc.balance -= request.amount
    to_acc.balance += request.amount

    tx = models.Transaction(
        tx_id=str(uuid.uuid4()),
        tx_type="internal_transfer",
        amount=request.amount,
        origin_bank=bank_id,
        destination_bank=bank_id,
        account_id=from_acc.id
    )

    db.add(tx)
    db.commit()

    return {"message": "Internal transfer successful"}


# ----------------------------------------------------------
# Same bank transfer (user → user)
# ----------------------------------------------------------
@router.post("/transfer-same-bank")
def transfer_same_bank(
    request: schemas.SameBankUserTransferRequest,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    bank_id = current["bank_id"]

    from_acc = db.query(models.Account).filter(
        models.Account.account_number == request.from_account,
        models.Account.bank_id == bank_id
    ).first()

    to_acc = db.query(models.Account).filter(
        models.Account.account_number == request.to_account,
        models.Account.bank_id == bank_id
    ).first()

    if not from_acc or not to_acc:
        raise HTTPException(404, "Account not found")

    if from_acc.balance < request.amount:
        raise HTTPException(400, "Insufficient funds")

    from_acc.balance -= request.amount
    to_acc.balance += request.amount

    tx = models.Transaction(
        tx_id=str(uuid.uuid4()),
        tx_type="same_bank_transfer",
        amount=request.amount,
        origin_bank=bank_id,
        destination_bank=bank_id,
        account_id=from_acc.id
    )

    db.add(tx)
    db.commit()

    return {"message": "Transfer successful"}


# ----------------------------------------------------------
# Cross-bank external transfer
# ----------------------------------------------------------
# app/routes/transactions.py (external_transfer section only)

@router.post("/external-transfer")
def external_transfer(
    request: schemas.ExternalTransferRequest,
    current=Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    origin_bank = current["bank_id"]

    from_acc = db.query(models.Account).filter(
        models.Account.account_number == request.from_account,
        models.Account.bank_id == origin_bank
    ).first()

    if not from_acc:
        raise HTTPException(404, "Account not found")

    if from_acc.balance < request.amount:
        raise HTTPException(400, "Insufficient funds")

    from_acc.balance -= request.amount

    tx = models.Transaction(
        tx_id=str(uuid.uuid4()),
        tx_type="external_transfer_sent",
        amount=request.amount,
        origin_bank=origin_bank,
        destination_bank=request.destination_bank,
        account_id=from_acc.id
    )

    db.add(tx)
    db.commit()

    event = {
        "tx_id": tx.tx_id,
        "from_bank": origin_bank,
        "destination_bank": request.destination_bank,  # e.g. "BANK2"
        "to_account": request.to_account,
        "amount": request.amount,
    }

    try:
        publish_event(
            key=f"{origin_bank}->{request.destination_bank}",
            value=event,
        )
    except Exception as e:
        # Withdraw already committed – for lab, we just log the Kafka failure
        print(f"[KAFKA] Failed to publish event: {e}")

    return {"message": "External transfer sent", "tx_id": tx.tx_id}


# ----------------------------------------------------------
# History for one account
# ----------------------------------------------------------
@router.get("/history/{account_number}")
def get_history(account_number: str, current=Depends(get_current_admin), db: Session = Depends(get_db)):
    bank_id = current["bank_id"]

    account = db.query(models.Account).filter(
        models.Account.account_number == account_number,
        models.Account.bank_id == bank_id
    ).first()

    if not account:
        raise HTTPException(404, "Account not found")

    tx = db.query(models.Transaction).filter(
        models.Transaction.account_id == account.id
    ).order_by(models.Transaction.timestamp.desc()).all()

    return tx
