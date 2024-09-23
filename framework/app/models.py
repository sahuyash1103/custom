from sqlalchemy import Column, Integer, String, Enum, DateTime, ForeignKey
from sqlalchemy.sql import func
from .database import Base, engine
from sqlalchemy.orm import relationship

class Action(Base):
    __tablename__ = "actions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, index=True, nullable=False)
    type = Column(Enum("script", "docker_image", name="action_type"), nullable=False)
    script = Column(String, nullable=True)
    docker_image = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    triggers = relationship("ActionTrigger", back_populates="action")


class ActionTrigger(Base):
    __tablename__ = "action_triggers"

    id = Column(Integer, primary_key=True, index=True)
    action_id = Column(Integer, ForeignKey('actions.id'))
    kafka_topic = Column(String, nullable=True)  # Only relevant if Kafka trigger
    data_pattern = Column(String, nullable=True)  # Only relevant if data-based trigger
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    action = relationship("Action", back_populates="triggers")


Base.metadata.create_all(bind=engine)