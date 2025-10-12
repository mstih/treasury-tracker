// test-db.js
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();
const { Client } = pg;

(async () => {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  try {
    await client.connect();
    console.log('DB connected OK');
    const r = await client.query('SELECT now()');
    console.log('Server time:', r.rows[0]);
  } catch (e) {
    console.error('DB connect error:', e.message);
  } finally {
    try { await client.end(); } catch {}
  }
})();
