// api/db.js
import pg from 'pg';
//import dotenv from 'dotenv';
//dotenv.config();

const { Pool } = pg;

// Check for env variables
if(!process.env.DATABASE_URL) {
  console.error('DATABASE_URL env variable is missing!')
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Using SSL 
  ssl: {rejectUnauthorized: false},

  // Pooling size
  max: 10,

  // Timeouts
  connectionTimeoutMillis: 30000,
});

// Handle errors
pool.on('error', (err) => {
  console.error('Unexpected PostgreSQL pool error', err.message);
});

pool.on("remove", () => {
  console.log('Client removed from PostgreSQL pool.');
})

// Graceful shutdown
process.on('SIGTERM', async () =>{
  console.log('SIGTERM received, closing pool...');
  await pool.end();
  process.exit(0);
})

// Health checker
export const checkDBHealth = async () => {
  const client = await pool.connect();
  try {
    await client.query('SELECT 1');
    return true;
  } finally {
    client.release()
  }
}

// test connection 
export const testConnection = async () => {
  const client = await pool.connect();
  await client.query('SELECT 1');
  client.release();
  console.log('Database CONNECTED!')
};

export default pool;
