const { Kafka } = require('kafkajs');

// Create and export a Kafka client instance
// Allow overriding via environment variables for easier local testing/deployment
const brokers = (process.env.KAFKA_BROKERS || '192.168.1.3:9092').split(',').map(b => b.trim()).filter(Boolean);
const clientId = process.env.KAFKA_CLIENT_ID || 'my-app';

const kafka = new Kafka({
  clientId,
  brokers,
});

module.exports = { kafka };
