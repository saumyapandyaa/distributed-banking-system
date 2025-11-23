from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from .db import Base

# ----------------------------
# Bank Admin (authentication)
# ----------------------------
class BankAdmin(Base):
    __tablename__ = "bank_admins"

    id = Column(Integer, primary_key=True, index=True)
    admin_id = Column(String, unique=True, index=True)
    name = Column(String)
    password_hash = Column(String)
    bank_id = Column(String, index=True)  # each bank has its own ID


# ----------------------------
# User (bank customer)
# ----------------------------
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    name = Column(String)
    phone = Column(String)           # phone is unique PER BANK, not globally
    bank_id = Column(String, index=True)

    accounts = relationship("Account", back_populates="owner")
    cards = relationship("Card", back_populates="user")


# ----------------------------
# Account
# ----------------------------
class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    account_number = Column(String, unique=True, index=True)
    account_type = Column(String)               # checking / savings
    balance = Column(Float, default=0.0)
    bank_id = Column(String)

    user_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="accounts")

    transactions = relationship("Transaction", back_populates="account")


# ----------------------------
# Transactions
# ----------------------------
class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    tx_id = Column(String, unique=True)
    tx_type = Column(String)
    amount = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    origin_bank = Column(String)
    destination_bank = Column(String)

    account_id = Column(Integer, ForeignKey("accounts.id"))
    account = relationship("Account", back_populates="transactions")


# ----------------------------
# Cards
# ----------------------------
class Card(Base):
    __tablename__ = "cards"

    id = Column(Integer, primary_key=True, index=True)
    card_number = Column(String, unique=True)
    card_type = Column(String)        # debit / credit / prepaid
    expiry = Column(String)
    cvv = Column(String)

    user_id = Column(Integer, ForeignKey("users.id"))
    user = relationship("User", back_populates="cards")
