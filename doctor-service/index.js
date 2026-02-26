const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const PORT = 3000;

const pool = new Pool({
  host: process.env.DB_HOST || 'doctor-db',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'doctor_db',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
});

const initDB = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS doctors (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        specialty VARCHAR(255) NOT NULL,
        available_slots INTEGER NOT NULL
      );`);
    console.log('Database initialized');

    await pool.query(`
      INSERT INTO doctors (id, name, specialty, available_slots)
      VALUES
        ('D001', 'Dr. Alice Smith', 'Cardiology', 5),
        ('D002', 'Dr. Bob Johnson', 'Dermatology', 3),
        ('D003', 'Dr. Carol Williams', 'Neurology', 4)
      ON CONFLICT (id) DO NOTHING;`);
    console.log('Sample doctors inserted');
  } catch (err) {
    console.error('Error initializing database:', err.message);
    console.log('Retrying in 5 seconds...');
    setTimeout(initDB, 5000);
  }
};

app.get('/doctors', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM doctors');
    res.json(result.rows);
  } catch (err) { 
    console.error('Error fetching doctors:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/doctors/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query('SELECT * FROM doctors WHERE id = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Doctor not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Error fetching doctor:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});

app.post('/doctors/:id/reserve', async (req, res) => {
  const { id } = req.params;
  const { slots } = req.body;
  try {
    const doctorResult = await pool.query('SELECT * FROM doctors WHERE id = $1', [id]);
    if (doctorResult.rows.length === 0) {
      return res.status(404).json({ error: 'Doctor not found' });
    }

    const doctor = doctorResult.rows[0];

    if (doctor.available_slots < slots) {
      return res.status(409).json({
        success: false,
        reason: `${doctor.name} has no available slots.`
      });
    }

    const updatedResult = await pool.query(
      'UPDATE doctors SET available_slots = available_slots - $1 WHERE id = $2 RETURNING *',
      [slots, id]
    );

    const updatedDoctor = updatedResult.rows[0];

    return res.status(200).json({
      success: true,
      doctor_id: updatedDoctor.id,
      doctor_name: updatedDoctor.name,
      slots_remaining: updatedDoctor.available_slots
    });
  } catch (err) {
    console.error('Error reserving slots:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

initDB().then(() => {
  app.listen(PORT, () => {
    console.log(`Doctor service running on port ${PORT}`);
  });
});
