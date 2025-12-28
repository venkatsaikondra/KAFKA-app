const { kafka } = require('./client');

async function init() {
  const admin = kafka.admin();
  try {
    console.log('Admin connecting...');
    await admin.connect();
    console.log('Admin connection success....');

    console.log('creating topic []');
    const created = await admin.createTopics({
      topics: [
        {
          topic: 'rider-updates',
          numPartitions: 2,
        },
      ],
    });
    console.log('topics created success..', created);

  } catch (err) {
    console.error('Error in admin operations:', err);
  } finally {
    try {
      console.log('admin disconnection...');
      await admin.disconnect();
    } catch (dErr) {
      console.error('Error disconnecting admin:', dErr);
    }
  }
}

init().catch((e) => {
  console.error('Fatal error:', e);
  process.exit(1);
});
