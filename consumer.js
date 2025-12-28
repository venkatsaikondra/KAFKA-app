const { kafka } = require('./client');
const group = process.argv[2] || '';

async function runConsumer() {
  const consumer = kafka.consumer({ groupId: process.env.KAFKA_CONSUMER_GROUP || 'user-1' });
  const maxRetries = parseInt(process.env.CONSUMER_START_RETRIES || '10', 10);
  const baseDelayMs = parseInt(process.env.CONSUMER_RETRY_DELAY_MS || '1000', 10);

  // Graceful shutdown helper
  const shutdown = async () => {
    try {
      console.log('Disconnecting consumer...');
      await consumer.disconnect();
    } catch (err) {
      console.error('Error during consumer disconnect:', err);
    } finally {
      process.exit(0);
    }
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

  // Attempt to start the consumer with retries for transient coordinator errors
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Consumer start attempt ${attempt}/${maxRetries}...`);
      await consumer.connect();
      console.log('Consumer connected');

      await consumer.subscribe({ topic: 'rider-updates', fromBeginning: true });

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message && message.value ? message.value.toString() : null;
          console.log(`${group}[${topic}] partition ${partition}: ${value}`);
        },
      });

      // If consumer.run started successfully, exit the retry loop and keep running
      return;
    } catch (err) {
      const msg = (err && err.message) ? err.message : String(err);
      console.error(`Consumer start attempt ${attempt} failed:`, msg);

      // If we've exhausted attempts, rethrow to trigger shutdown
      if (attempt >= maxRetries) {
        console.error('Max consumer start retries reached. Exiting.');
        await shutdown();
        return;
      }

      // Disconnect cleanly before retrying
      try {
        await consumer.disconnect();
      } catch (dErr) {
        // ignore disconnect errors during retries
      }

      // Exponential backoff with jitter
      const backoff = Math.min(30000, baseDelayMs * Math.pow(2, attempt - 1));
      const jitter = Math.floor(Math.random() * 300);
      const waitMs = backoff + jitter;
      console.log(`Retrying in ${waitMs}ms...`);
      await new Promise((res) => setTimeout(res, waitMs));
    }
  }
}

runConsumer().catch((e) => {
  console.error('Fatal consumer error:', e);
  process.exit(1);
});
