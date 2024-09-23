import os
from confluent_kafka import Consumer, KafkaError, KafkaException

# Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'terminal-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start from the earliest messages
}

print(f"Kafka configuration: {kafka_config}")

def create_consumer():
    return Consumer(kafka_config)

def main():
    # Get the topic to consume from user input
    topic = input("Enter the Kafka topic name (or type 'exit' to quit): ").strip().lower()
    if topic == "exit":
        print("Exiting Kafka consumer...")
        return

    # Create a Kafka consumer
    consumer = create_consumer()

    # Subscribe to the topic
    consumer.subscribe([topic])

    print(f"Subscribed to topic: {topic}")

    try:
        while True:
            # Poll for a message from the Kafka topic
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, not an actual error
                    continue
                else:
                    raise KafkaException(message.error())

            # Print the received message
            print(f"Received message: {message.value().decode('utf-8')} from topic {message.topic()}")
    except KeyboardInterrupt:
        print("Consumer interrupted.")
    
    finally:
        # Close down the consumer
        consumer.close()

if __name__ == "__main__":
    main()
