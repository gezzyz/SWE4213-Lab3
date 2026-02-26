const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

let connection;
let channel;
const recordsLog = [];

const connectRabbitMQ = async () => {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();

    await channel.prefetch(1);

    await channel.assertExchange('appts', 'fanout', { durable: true });
    await channel.assertQueue('records', { durable: true });

    await channel.bindQueue('records', 'appts', '');

    console.log('Connected to RabbitMQ. Waiting for appointment records...');

    await startConsuming();
  } catch (err) {
    console.error('Error connecting to RabbitMQ:', err.message);
    console.log('Retrying in 5 seconds...');
    setTimeout(connectRabbitMQ, 5000);
  }
};

const printSummary = () => {
  console.log(`[Records] New appointment logged — total on record: ${recordsLog.length}`);
  recordsLog.forEach((record) => {
    console.log(`  ${record.patient_email} → ${record.doctor_name} (${record.reason}) at ${record.timestamp}`);
  });
};

const startConsuming = async () => {
  try {
    await channel.consume('records', async (msg) => {
      if (msg) {
        try {
          const appointment = JSON.parse(msg.content.toString());
          recordsLog.unshift(appointment);

          printSummary();

          channel.ack(msg);
        } catch (err) {
          console.error('Error processing message:', err);
          channel.nack(msg, false, true);
        }
      }
    }, { noAck: false });
  } catch (err) {
    console.error('Error starting consumer:', err);
    setTimeout(startConsuming, 5000);
  }
};

process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  try {
    if (channel) await channel.close();
    if (connection) await connection.close();
  } catch (err) {
    console.error('Error during shutdown:', err);
  }
  process.exit(0);
});

connectRabbitMQ();
