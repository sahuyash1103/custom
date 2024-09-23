import os
from confluent_kafka import Consumer, KafkaError, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy.orm import Session
from app.models import ActionTrigger
from app.database import SessionLocal
from .actions import run_action


class KafkaActionHandler:
    def __init__(self):
        # Kafka configuration with environment defaults
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'group.id': os.getenv('KAFKA_GROUP_ID', 'action-consumer-group'),
            'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        }

        # Initialize Kafka Admin Client and Producer globally
        self.admin_client = AdminClient({'bootstrap.servers': self.kafka_config['bootstrap.servers']})
        self.producer = Producer(self.kafka_config)

    # Create a Kafka consumer
    def create_consumer(self):
        return Consumer(self.kafka_config)

    # Check if a Kafka topic exists
    def topic_exists(self, topic_name):
        return topic_name in self.admin_client.list_topics().topics

    # Create a Kafka topic if it doesn't exist
    def create_topic(self, topic_name):
        if not self.topic_exists(topic_name):
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            self.admin_client.create_topics([new_topic])

    # Send logs to the specified Kafka topic
    def send_logs_to_kafka(self, log_topic, log_message):
        self.create_topic(log_topic)  # Ensure the topic exists or create it
        self.producer.produce(log_topic, value=log_message)

    # Handle the event for a Kafka topic
    def handle_kafka_event(self, topic, message):
        with SessionLocal() as db:
            # Find any actions triggered by this Kafka topic
            triggers = db.query(ActionTrigger).filter(ActionTrigger.kafka_topic == topic).all()

            for trigger in triggers:
                log = run_action(trigger.action.name, db)
                print(f"Action logs: {log}")

                # Send log to Kafka
                log_topic = f"{trigger.action.name}-logs"
                log_message = str(log)
                self.send_logs_to_kafka(log_topic, log_message)

        # Flush the producer after processing all logs
        self.producer.flush()

    # Subscribe to topics and listen for Kafka messages
    def subscribe_and_listen(self):
        consumer = self.create_consumer()

        # Fetch unique Kafka topics from the database
        with SessionLocal() as db:
            topics = db.query(ActionTrigger.kafka_topic).distinct().all()

        topic_list = [topic.kafka_topic for topic in topics]
        print(f"Subscribing to topics: {topic_list}")
        consumer.subscribe(topic_list)

        try:
            while True:
                message = consumer.poll(timeout=1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue  # End of partition event
                    else:
                        raise KafkaException(message.error())

                topic = message.topic()
                message_value = message.value().decode('utf-8')
                print(f"Received message from topic '{topic}': {message_value}")

                # Handle the event for the corresponding topic
                self.handle_kafka_event(topic, message_value)

        except Exception as e:
            print(f"Error in Kafka consumer: {e}")
        finally:
            # Ensure consumer is closed on exit
            consumer.close()
