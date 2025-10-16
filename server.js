// api/server.js
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import pool from './db.js'
dotenv.config();

const app = express();

// Restrict CORS in production
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
app.use(cors({origin: allowedOrigin}));
app.use(express.json());

app.get('/healtz', async(req,res) => {
    try {
        await pool.query('SELECT 1');
        res.status(200).send('ok');
    } catch (error){
        console.error('Health check error: ', error);
        res.status(500).send('db error')
    }
});

app.get('/api/summary/today', async (req, res) => {
  try {
    const lastRowResult = await pool.query(
      'SELECT dts_date, tariff_millions, total_deposits_millions FROM tariff_daily ORDER BY dts_date DESC LIMIT 1'
    );
    const lastRow = lastRowResult.rows[0] ?? null;

    // Use explicit year; this respects server timezone - or override with ?year=YYYY
    const yearQuery = req.query.year ? Number(req.query.year) : new Date().getFullYear();

    const yearlyResult = await pool.query('SELECT * FROM tariff_yearly WHERE year=$1', [Number(yearQuery)]);
    const yearly = yearlyResult.rows ?? [];

    res.json({ lastRow, yearly });
  } catch (e) {
    console.error('/api/summary/today error', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/cumulative', async (req, res) => {
  const year = Number(req.query.year) || new Date().getFullYear();

  // validate year
  if (!Number.isInteger(year) || year < 1900 || year > 3000) {
    return res.status(400).json({ error: 'Invalid year' });
  }

  try {
    const q = `
      SELECT dts_date,
             tariff_millions,
             SUM(tariff_millions) OVER (ORDER BY dts_date) AS cumulative_tariff
      FROM tariff_daily
      WHERE EXTRACT(YEAR FROM dts_date) = $1
      ORDER BY dts_date;
    `;
    const rows = (await pool.query(q, [year])).rows;
    res.json(rows);
  } catch (error) {
    console.error('/api/cumulative error', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/monthly', async (req, res) => {
  const year = Number(req.query.year) || new Date().getFullYear();

  if (!Number.isInteger(year) || year < 1900 || year > 3000) {
    return res.status(400).json({ error: 'Invalid year' });
  }

  try {
    const q = `
      SELECT month,
             tariff_millions_sum,
             total_deposits_millions_sum,
             CASE WHEN total_deposits_millions_sum = 0 THEN 0
                  ELSE 100.0 * tariff_millions_sum / total_deposits_millions_sum END AS pct_od_total
      FROM tariff_monthly
      WHERE EXTRACT(YEAR FROM month) = $1
      ORDER BY month;
    `;
    const rows = (await pool.query(q, [year])).rows;
    res.json(rows);
  } catch (error) {
    console.error('/api/monthly error', error);
    res.status(500).json({ error: error.message });
  }
});

// graceful shutdown - close pool on SIGTERM/SIGINT
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => console.log(`API listening on port ${PORT}`));

async function shutdown(signal) {
  console.log(`Received ${signal}. Closing server and DB pool...`);
  server.close(async (err) => {
    if (err) {
      console.error('Error closing server', err);
      process.exit(1);
    }
    try {
      await pool.end();
      console.log('DB pool closed, exiting');
      process.exit(0);
    } catch (e) {
      console.error('Error closing DB pool', e);
      process.exit(1);
    }
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));