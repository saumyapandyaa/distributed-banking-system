from fastapi import FastAPI, Depends, HTTPException, status 
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List

from orchestrator import compute_ports, start_standalone_bank  # 👈 updated import

from database import Base, engine, get_db
import models
import schemas
import crud

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Head Bank Service", version="1.0.0")

origins = [
    "http://localhost:3000",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", tags=["system"])
def health_check():
    return {"status": "ok"}


@app.post("/banks", response_model=schemas.BankRead,
          status_code=status.HTTP_201_CREATED, tags=["banks"])
def create_bank(bank: schemas.BankCreate, db: Session = Depends(get_db)):
    """
    Register a new bank node in the Head Bank, compute its base_url,
    and start a standalone bank instance.
    """
    existing = crud.get_bank_by_bank_id(db, bank.bank_id)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Bank with bank_id '{bank.bank_id}' already exists.",
        )

    # 1) Compute dynamic ports from bank_id
    backend_port, frontend_port = compute_ports(bank.bank_id)

    # 2) Compute base_url and store it in the incoming object
    base_url = f"http://localhost:{backend_port}"
    bank.base_url = base_url   # 👈 this is what will be persisted

    # 3) Save metadata in head-bank SQLite registry
    db_bank = crud.create_bank(db, bank)

    # 4) Start the actual standalone bank via Docker with these ports
    start_standalone_bank(bank.bank_id, backend_port, frontend_port)

    return db_bank


@app.get("/banks", response_model=List[schemas.BankRead], tags=["banks"])
def list_banks(db: Session = Depends(get_db)):
    banks = crud.get_banks(db)
    return banks


@app.get("/banks/{bank_id}", response_model=schemas.BankRead, tags=["banks"])
def get_bank(bank_id: str, db: Session = Depends(get_db)):
    bank = crud.get_bank_by_bank_id(db, bank_id)
    if not bank:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bank '{bank_id}' not found.",
        )
    return bank


@app.delete("/banks/{bank_id}", response_model=schemas.BankRead, tags=["banks"])
def delete_bank(bank_id: str, db: Session = Depends(get_db)):
    bank = crud.soft_delete_bank(db, bank_id)
    if not bank:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bank '{bank_id}' not found.",
        )
    return bank
