from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .. import models
from ..database import get_db
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
from ..kafka_consumer import KafkaActionHandler
import logging

router = APIRouter()

class ActionBase(BaseModel):
    name: str
    type: str  # Either 'script' or 'docker_image'
    docker_image: str = None
    script: str = None

class ScriptAction(ActionBase):
    script: str

class DockerAction(ActionBase):
    docker_image: str


class ActionTriggerResponse(BaseModel):
    id: int
    action_id: int
    trigger_type: str  # Add this field
    created_at: str  # Change type to str
    updated_at: str  # Change type to str

    class Config:
        from_attributes = True

class ActionResponse(BaseModel):
    id: int
    name: str
    type: str
    triggers: List[ActionTriggerResponse]
    script: Optional[str] = None  # Use Optional
    docker_image: Optional[str] = None  # Use Optional
    created_at: str  # Change type to str
    updated_at: str  # Change type to str

    class Config:
        from_attributes = True


@router.post("/actions/")
def create_action(action: ActionBase, db: Session = Depends(get_db)):
    # Handle action creation based on its type
    new_action = None
    if action.type == "script":
        if not hasattr(action, "script") or action.script is None:
            raise HTTPException(status_code=400, detail="Script is required for script actions.")
        new_action = models.Action(
            name=action.name,
            type=action.type,
            script=action.script,
            docker_image=None,
        )
    elif action.type == "docker_image":
        if not hasattr(action, "docker_image") or action.docker_image is None:
            raise HTTPException(status_code=400, detail="Docker image is required for docker_image actions.")
        new_action = models.Action(
            name=action.name,
            type=action.type,
            docker_image=action.docker_image,
            script=None,
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid action type. Must be 'script' or 'docker_image'.")

    if new_action is None:
        raise HTTPException(status_code=500, detail="Failed to create action.")
    db.add(new_action)
    db.commit()
    db.refresh(new_action)
    return new_action


@router.get("/actions/")
def get_actions(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    actions = db.query(models.Action).offset(skip).limit(limit).all()
    return actions

@router.get("/actions/{id}", response_model=ActionResponse)
def get_actions(id: str, db: Session = Depends(get_db)):
    action = db.query(models.Action).filter(models.Action.id == id).first()
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    
    # Convert to response model
    return ActionResponse(
        id=action.id,
        name=action.name,
        type=action.type,
        triggers=[ActionTriggerResponse(
            id=trigger.id,
            action_id=trigger.action_id,
            trigger_type="kafka",  # Set this according to your logic
            created_at=trigger.created_at.isoformat(),  # Convert to string
            updated_at=trigger.updated_at.isoformat()   # Convert to string
        ) for trigger in action.triggers],
        script=action.script,
        docker_image=action.docker_image,
        created_at=action.created_at.isoformat(),  # Convert to string
        updated_at=action.updated_at.isoformat()   # Convert to string
    )

# ---------------------------------------------TRIGGERS------
class ActionTriggerBase(BaseModel):
    kafka_topic: str = None
    data_pattern: str = None

@router.post("/actions/{id}/triggers/")
def create_trigger(id: int, trigger: ActionTriggerBase, db: Session = Depends(get_db)):
    action = db.query(models.Action).filter(models.Action.id == id).first()
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    new_trigger = models.ActionTrigger(
        action_id=id,
        kafka_topic=trigger.kafka_topic,
        data_pattern=trigger.data_pattern
    )

    action.triggers.append(new_trigger)

    db.add(new_trigger)
    db.commit()
    db.refresh(new_trigger)

    kafka = KafkaActionHandler()
    kafka.create_topic(trigger.kafka_topic)
    kafka.create_topic(f"{action.name}-logs")
    
    return new_trigger