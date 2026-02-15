// test.js
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();
const { Client } = pg;


const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 30000,
});

async function test() {
  try {
    console.log('Connecting...');
    await client.connect();
    console.log('SUCCESS!');
    const res = await client.query('SELECT 1');
    console.log('Query:', res.rows);
    await client.end();
  } catch (err) {
    console.error('FAILED:', err.message);
    console.error('Code:', err.code);
    console.error('Full:', err);
  }
}

test();