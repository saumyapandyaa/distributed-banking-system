from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import threading

from app.db import Base, engine
from app import models

from app.auth.auth_routes import router as auth_router
from app.routes.users import router as users_router
from app.routes.accounts import router as accounts_router
from app.routes.transactions import router as transactions_router
from app.routes.cards import router as cards_router

from app.kafka_consumer import start_consumer  # <- we’ll define this properly

BANK_ID = os.getenv("BANK_ID", "BANK1")

app = FastAPI(title=f"Bank Backend - {BANK_ID}")


@app.get("/")
def home():
    return {"message": "Bank backend running", "bank_id": BANK_ID}


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB INIT
Base.metadata.create_all(bind=engine)
print(f"✅ Database initialized for {BANK_ID}")

# ROUTES
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(accounts_router)
app.include_router(transactions_router)
app.include_router(cards_router)


# START KAFKA CONSUMER ON STARTUP
@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=start_consumer, args=(BANK_ID,), daemon=True)
    t.start()
    print(f"🔄 Kafka consumer started for {BANK_ID}")
