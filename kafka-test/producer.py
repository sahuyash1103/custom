import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
kafka_config = {
    'bootstrap.server': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
}
admin_client = AdminClient({'bootstrap.servers': kafka_config['bootstrap.servers']})

print(f"Kafka configuration: {kafka_config}")
print(f"bootstrap.servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")

def create_producer():
    return Producer(kafka_config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def topic_exists(topic_name):
    return topic_name in admin_client.list_topics().topics

    # Create a Kafka topic if it doesn't exist
def create_topic( topic_name):
    if not topic_exists(topic_name):
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])


def main():
    # Create a Kafka producer
    producer = create_producer()

    while True:
        # Take topic name from user input
        topic = input("Enter the Kafka topic name (or type 'exit' to quit): ").strip().lower()
        if topic.lower() == "exit":
            print("Exiting Kafka producer...")
            break

        if not topic:
            print("Topic name cannot be empty.")
            continue

        # Take message from user input
        message = input("Enter the message to send (or type 'exit' to quit): ")
        if message.lower() == "exit":
            print("Exiting Kafka producer...")
            break

        if not message:
            print("Message cannot be empty.")
            continue

        print(f"bootstrap.servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")

        # Ensure the topic exists or create it
        create_topic(topic)

        # Send the message to the specified topic
        producer.produce(topic, value=message.encode('utf-8'), callback=delivery_report)

        # Wait for all messages to be delivered
        producer.flush()
        print(f"Message '{message}' sent to topic '{topic}'")
    
    # Close the producer
    producer.close()

if __name__ == "__main__":
    main()
