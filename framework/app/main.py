from contextlib import asynccontextmanager
from fastapi import FastAPI
from .api import actions
from .actions import run_action
from fastapi import  Depends
from sqlalchemy.orm import Session
from .database import get_db
from .kafka_consumer import subscribe_and_listen
import threading

app = FastAPI()

# Start Kafka consumer in a separate thread when FastAPI starts
def start_kafka_listener():
    kafka_thread = threading.Thread(target=subscribe_and_listen, daemon=True)
    kafka_thread.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    start_kafka_listener()
    yield

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
