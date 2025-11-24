from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime

class BankBase(BaseModel):
    bank_id: str
    name: Optional[str] = None
    base_url: Optional[HttpUrl] = None

class BankCreate(BankBase):
    pass

class BankRead(BankBase):
    id: int
    status: str
    created_at: datetime

    class Config:
        from_attributes = True
