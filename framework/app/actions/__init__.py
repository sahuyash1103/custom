from sqlalchemy.orm import Session
from .. import models
from .docker_runner import run_docker_image
from .script_runner import run_script

def run_action(name: str, db: Session):
    action = db.query(models.Action).filter(models.Action.name == name).first()
    if not action:
        return f"Action {name} not found"
    if action.type == "script":
        return run_script(action.script)
    elif action.type == "docker_image":
        return run_docker_image(action.docker_image)
    else:
        return f"Invalid action type for action {name}"
    
    