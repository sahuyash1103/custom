from contextlib import asynccontextmanager
from fastapi import FastAPI
from .api import actions
from .actions import run_action
from fastapi import  Depends
from sqlalchemy.orm import Session
from .database import get_db
from .kafka_consumer import start_kafka_listener
import logging

@asynccontextmanager
async def lifespan(_):
    logging.info("Starting Kafka listener...")
    start_kafka_listener()
    yield

app = FastAPI(lifespan=lifespan)

app.include_router(actions.router)

@app.get("/")
async def root():
    return {"message": "Hello, World!"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/run/{action_name}")
async def run(action_name: str, db: Session = Depends(get_db)):
    return run_action(action_name, db)
