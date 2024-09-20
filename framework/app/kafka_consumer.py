import os
from confluent_kafka import Consumer, KafkaError
from sqlalchemy.orm import Session
from app.models import ActionTrigger
from app.database import SessionLocal
from .actions import run_action


kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),  # Default value for local development
    'group.id': os.getenv('KAFKA_GROUP_ID', 'action-consumer-group'),              # Default consumer group ID
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')          # Default offset reset policy
}

def create_consumer():
    return Consumer(kafka_config)

def handle_kafka_event(topic, message):
    db: Session = SessionLocal()
        
    # Find any actions triggered by this Kafka topic
    triggers = db.query(ActionTrigger).filter(ActionTrigger.kafka_topic == topic).all()
    
    for trigger in triggers:
        run_action(trigger.action.name, db)

    db.close()

def subscribe_and_listen():
    consumer = create_consumer()

    # Fetch all unique Kafka topics for subscribed actions
    db: Session = SessionLocal()
    topics = db.query(ActionTrigger.kafka_topic).distinct().all()
    db.close()

    topic_list = [topic.kafka_topic for topic in topics]
    print(f"Subscribing to topics: {topic_list}")
    consumer.subscribe(topic_list)

    # Listen for messages on subscribed topics
    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {message.error()}")
                break

        topic = message.topic()
        message_value = message.value().decode('utf-8')
        print(f"Received message from topic '{topic}': {message_value}")
        
        # Handle the event for the corresponding topic
        handle_kafka_event(topic, message_value)

    consumer.close()
