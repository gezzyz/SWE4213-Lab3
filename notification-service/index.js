const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

let connection;
let channel;

const connectRabbitMQ = async () => {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    
    await channel.prefetch(1);
    
    await channel.assertExchange('appts', 'fanout', { durable: true });
    const queue = await channel.assertQueue('notifications', { durable: true });
    
    await channel.bindQueue('notifications', 'appts', '');
    
    console.log('Connected to RabbitMQ. Waiting for appointment notifications...');
    
    await startConsuming();
  } catch (err) {
    console.error('Error connecting to RabbitMQ:', err.message);
    console.log('Retrying in 5 seconds...');
    setTimeout(connectRabbitMQ, 5000);
  }
};

const startConsuming = async () => {
  try {
    await channel.consume('notifications', async (msg) => {
      if (msg) {
        try {
          const appointment = JSON.parse(msg.content.toString());
          
          console.log(`[Notification] Sending confirmation to ${appointment.patient_email}`);
          console.log(`  Appointment ID : ${appointment.appointment_id}`);
          console.log(`  Doctor         : ${appointment.doctor_name}`);
          console.log(`  Reason         : ${appointment.reason}`);
          console.log(`  Status         : confirmed`);
          
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
