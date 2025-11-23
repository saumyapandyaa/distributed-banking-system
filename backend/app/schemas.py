from pydantic import BaseModel
from typing import Optional

# AUTH
class AdminLogin(BaseModel):
    admin_id: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# USERS
class UserCreate(BaseModel):
    name: str
    phone: str
    initial_deposit: float = 0.0

class UserUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None

class UserResponse(BaseModel):
    user_id: str
    name: str
    phone: str

    class Config:
        orm_mode = True


# ACCOUNTS
class DepositRequest(BaseModel):
    account_number: str
    amount: float

class WithdrawRequest(BaseModel):
    account_number: str
    amount: float

class InternalAccountTransferRequest(BaseModel):
    user_id: str
    from_account_type: str
    to_account_type: str
    amount: float

class SameBankUserTransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: float

class ExternalTransferRequest(BaseModel):
    from_account: str
    destination_bank: str
    to_account: str
    amount: float
