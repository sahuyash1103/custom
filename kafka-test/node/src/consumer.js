const { Kafka, logLevel } = require("kafkajs");

// Kafka configuration
const kafkaConfig = {
  clientId: "logs-consumer-client",
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"],
  groupId: "logs-consumer", // Consumer group ID
};

console.log(`Kafka configuration: ${JSON.stringify(kafkaConfig)}`);

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
  logLevel: logLevel.INFO, // Set log level to INFO for standard logs
});

const consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

async function createConsumer() {
  try {
    await consumer.connect();
    console.log("Kafka consumer connected.");
    return consumer;
  } catch (error) {
    console.error(`Failed to connect consumer: ${error.message}`);
    process.exit(1); // Exit process on connection failure
  }
}

async function main() {
  // Get the topic to consume from user input
  const topic = (
    await prompt("Enter the Kafka topic name (or type 'exit' to quit): ")
  )
    .trim()
    .toLowerCase();

  if (topic === "exit") {
    console.log("Exiting Kafka consumer...");
    return;
  }

  if (!topic) {
    console.error("No topic entered. Exiting...");
    return;
  }

  // Create and connect the Kafka consumer
  await createConsumer();

  // Subscribe to the topic
  await consumer.subscribe({ topic: topic, fromBeginning: true });
  console.log(`Subscribed to topic: ${topic}`);

  try {
    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // Print the received message
        console.log(
          `Received message: ${message.value.toString()} from topic ${topic} [partition ${partition}]`
        );
      },
      eachBatch: async ({ batch }) => {
        // Log details about the batch being processed
        console.log(
          `Processing batch from topic ${batch.topic} with ${batch.messages.length} messages.`
        );
      },
    });
  } catch (error) {
    console.error(`Error occurred while consuming messages: ${error.message}`);
  } finally {
    // Close down the consumer gracefully
    // await consumer.disconnect();
    // console.log("Kafka consumer disconnected.");
  }
}

// Prompt wrapper for Node.js
async function prompt(question) {
  const readline = require("readline");
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer);
    });
  });
}

main().catch(console.error);
