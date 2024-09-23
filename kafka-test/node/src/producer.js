const { Kafka, logLevel } = require("kafkajs");

// Kafka configuration
const kafkaConfig = {
  brokers: ["localhost:9092"],
};

console.log(`Kafka configuration: ${JSON.stringify(kafkaConfig)}`);
console.log(
  `bootstrap.servers: ${
    process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"
  }`
);

const kafka = new Kafka({
  clientId: "kafka-producer-client",
  brokers: kafkaConfig.brokers,
  logLevel: logLevel.INFO,
});

const admin = kafka.admin();
const producer = kafka.producer();

async function createProducer() {
  await producer.connect();
  console.log("Kafka producer connected.");
  return producer;
}

async function deliveryReport(result) {
  // No direct callback like in confluent_kafka, but you can log success/failure here.
  if (result.errorCode) {
    console.error(`Message delivery failed: ${result.errorCode}`);
  } else {
    console.log(`Message delivered to ${result.topic} [${result.partition}]`);
  }
}

async function topicExists(topicName) {
  await admin.connect();
  const topics = await admin.listTopics();
  await admin.disconnect();
  return topics.includes(topicName);
}

async function createTopic(topicName) {
  const exists = await topicExists(topicName);
  if (!exists) {
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [
          {
            topic: topicName,
            numPartitions: 1,
            replicationFactor: 1,
          },
        ],
      });
      console.log(`Topic '${topicName}' created successfully.`);
    } catch (e) {
      console.error(`Failed to create topic '${topicName}': ${e.message}`);
    } finally {
      await admin.disconnect();
    }
  } else {
    console.log(`Topic '${topicName}' already exists.`);
  }
}

async function main() {
  await createProducer();

  try {
    while (true) {
      // Take topic name from user input
      const topic = (
        await prompt("Enter the Kafka topic name (or type 'exit' to quit): ")
      ).trim();
      if (topic.toLowerCase() === "exit") {
        console.log("Exiting Kafka producer...");
        break;
      }

      if (!topic) {
        console.log("Topic name cannot be empty.");
        continue;
      }

      // Take message from user input
      const message = (
        await prompt("Enter the message to send (or type 'exit' to quit): ")
      ).trim();
      if (message.toLowerCase() === "exit") {
        console.log("Exiting Kafka producer...");
        break;
      }

      if (!message) {
        console.log("Message cannot be empty.");
        continue;
      }

      // Ensure the topic exists or create it
      await createTopic(topic);

      // Send the message to the specified topic
      await producer.send({
        topic: topic,
        messages: [{ value: message }],
      });

      console.log(`Message '${message}' sent to topic '${topic}'`);
    }
  } catch (error) {
    console.error(`An error occurred: ${error.message}`);
  } finally {
    await producer.disconnect();
    console.log("Kafka producer disconnected.");
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
