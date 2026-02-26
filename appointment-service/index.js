const express = require('express');
const axios = require('axios');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

let channel;

// Initialize RabbitMQ connection and declare exchange
const initRabbitMQ = async () => {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertExchange('appts', 'fanout', { durable: true });
    console.log('RabbitMQ connection established');
  } catch (err) {
    console.error('Error connecting to RabbitMQ:', err);
    setTimeout(initRabbitMQ, 5000); // Retry after 5 seconds
  }
};

app.post('/appointments', async (req, res) => {
    const{ patient_name, patient_email, doctor_id, reason } = req.body;
    if (!patient_name || !patient_email || !doctor_id || !reason) {
        return res.status(400).json({ error: 'Missing required fields' });
    }

    try {
      const response = await axios.post(
        `${process.env.DOCTOR_SERVICE_URL}/doctors/${doctor_id}/reserve`,
        { slots: 1 }
      );

      if (!response.data.success) {
        return res.status(409).json({
          success: false,
          reason: response.data.reason
        });
      }

      const appointment_id = uuidv4();
      const appointmentEvent = {
        appointment_id,
        patient_name,
        patient_email,
        doctor_id,
        doctor_name: response.data.doctor_name,
        reason,
        timestamp: new Date().toISOString()
      };

      if (channel) {
        channel.publish(
          'appts',
          '',
          Buffer.from(JSON.stringify(appointmentEvent))
        );
      }

      res.status(201).json({
        success: true,
        appointment_id,
        patient_name,
        patient_email,
        doctor_id,
        doctor_name: response.data.doctor_name,
        reason,
        message: 'Appointment created successfully'
      });
    } catch (err) {
      if (err.response && err.response.status === 409) {
        return res.status(409).json({
          success: false,
          reason: err.response.data.reason
        });
      }
      console.error('Error creating appointment:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
});

initRabbitMQ().then(() => {
  app.listen(PORT, () => {
    console.log(`Appointment service running on port ${PORT}`);
  });
}).catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});

