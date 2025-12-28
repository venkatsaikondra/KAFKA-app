const { kafka } = require('./client');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function runProducer() {
  const producer = kafka.producer();

  // Helper to disconnect producer gracefully
  const cleanup = async () => {
    try {
      await producer.disconnect();
      console.log('Producer disconnected');
    } catch (err) {
      console.error('Error disconnecting producer:', err);
    }
  };

  process.once('SIGINT', () => {
    console.log('\nReceived SIGINT, shutting down...');
    rl.close();
  });

  rl.on('close', async () => {
    await cleanup();
    process.exit(0);
  });

  try {
    console.log('Producer connecting...');
    await producer.connect();
    console.log('Producer connected — enter messages in the format: name,location');

    rl.setPrompt('> ');
    rl.prompt();

    rl.on('line', async (line) => {
      const trimmed = (line || '').trim();
      if (!trimmed) {
        rl.prompt();
        return;
      }

      // Expect input like: "Tony Stark,Hyderabad"
      const parts = trimmed.split(',').map(p => p.trim());
      const riderName = parts[0] || 'unknown';
      const location = parts[1] || 'unknown';

      const payload = { name: riderName, location, timestamp: Date.now() };

      const partition = (location && location.toLowerCase().includes('north')) ? 0 : 1;

      try {
        const res = await producer.send({
          topic: 'rider-updates',
          messages: [
            { key: riderName, value: JSON.stringify(payload), partition },
          ],
        });
        console.log('Message produced:', res);
      } catch (sendErr) {
        console.error('Failed to send message:', sendErr);
      }

      rl.prompt();
    });

  } catch (err) {
    console.error('Producer initialization error:', err);
    await cleanup();
    process.exit(1);
  }
}

runProducer().catch((e) => {
  console.error('Fatal producer error:', e);
  process.exit(1);
});
