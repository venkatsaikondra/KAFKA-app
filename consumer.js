const { kafka } = require('./client');

async function runConsumer() {
  const consumer = kafka.consumer({ groupId: 'user-1' });

  await consumer.connect();
  console.log('Consumer connected');

  await consumer.subscribe({
    topic: 'rider-updates',
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `[${topic}] partition ${partition}: ${message.value.toString()}`
      );
    },
  });
}

runConsumer().catch(console.error);
