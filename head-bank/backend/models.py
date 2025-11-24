from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func

from database import Base   # 👈 remove the leading dot

class Bank(Base):
    __tablename__ = "banks"

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(100), nullable=True)
    base_url = Column(String(200), nullable=True)
    status = Column(String(20), default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
