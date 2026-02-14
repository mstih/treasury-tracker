// api/server.js
// just some edit to enable actions on github
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import pool from './db.js'

// Load enviroment variables
dotenv.config();

const app = express();

// Security: Restrict CORS in production
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : ["*"];

app.use(cors({
  origin: (origin, callback) => {
    if(!origin || allowedOrigins.includes('*') || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Origin not allowed!'))
    }
  },
  credentials: true
}));

app.use(express.json());


// MIDDLEWARE: timeout 20s
app.use((req,res,next) => {
  req.setTimeout(20000, () => {
    res.status(408).json({error: 'Request timeout'});
  });
  next();
})

// =========================================
// HEALTH CHECK 
// =========================================
app.get('/healtz', async(req,res) => {
    try {
        await pool.query('SELECT 1');
        res.status(200).json({
          status: 'OK',
          timestamp: new Date().toISOString(),
          uptime: process.uptime()
        });
    } catch (error){
        console.error('Health check failed: ', error.message);
        res.status(503).json({
          status: 'ERROR',
          error: 'Database unavailable', 
          timestamp: new Date().toISOString()
        });
    } 
});

// ==========================================
// HELPER: Retry wrapper
// ==========================================
async function queryWithRetry(query, params, maxRetries=3) {
  let lastError;
  for(let i = 0; i < maxRetries; i++){
    try {
      return await pool.query(query, params);
    } catch (error) {
      lastError = error;
      // Only retry on connection error
      if(error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.code === '08006'){
        console.warn(`DB query attempt ${i+1} failed, retrying...`);
        await new Promise(r => setTimeout(r, 1000 * (i+1)));
      } else {
        throw error;
      }
    }
  }
  throw lastError;
}

// ===========================================
// VALIDATION HELPERS
// ===========================================
function validateYear(year) {
  const num = Number(year);
  if(!Number.isInteger(num) || num < 2000 || num > 2100){
    return {valid: false, error: 'Invalid year. Must be between 2000 and 2100'}
  }
  return {valid: true, value: num}
}

// ===========================================
// ROUTES
// ===========================================

// Gets the summary for the last date/specific date
app.get('/api/summary/today', async (req, res) => {
  try {
    // year
    const yearValidation = validateYear(req.query.year || new Date().getFullYear());
    if(!yearValidation.valid){
      return res.status(400).json({error: yearValidation.error})
    }
    const yearQuery = yearValidation.value;

    // Parallel queries
    const [lastRowResult, yearlyResult] = await Promise.all([
      pool.query(
        'SELECT dts_date, tariff_millions, total_deposits_millions FROM tariff_daily ORDER BY dts_date DESC LIMIT 1'
      ),
      pool.query('SELECT * FROM tariff_yearly WHERE year=$1', [yearQuery])
    ])
    
    const lastRow = lastRowResult.rows[0] ?? null;
    const yearly = yearlyResult.rows ?? [];

    res.json({ lastRow, yearly });
  } catch (error) {
    console.error('/api/summary/today error: ', error);
    res.status(500).json({
       error: 'Internal server error',
       message: process.env.NODE_ENV === 'development' ? error.message: undefined
      });
  }
});

// Get data for each specific day together with aggregate cumulatives of the same year
app.get('/api/cumulative', async (req, res) => {
  const yearValidation = validateYear(req.query.year || new Date().getFullYear());

  if(!yearValidation.valid){
    return res.status(400).json({error: yearValidation.error})
  }

  const year = yearValidation.value;

  try {
    const query = `
      SELECT dts_date,
             tariff_millions,
             SUM(tariff_millions) OVER (ORDER BY dts_date) AS cumulative_tariff
      FROM tariff_daily
      WHERE EXTRACT(YEAR FROM dts_date) = $1
      ORDER BY dts_date;
    `;

    const result = await queryWithRetry(query, [year]);
    res.json(result.rows)
  } catch (error) {
    console.error('/api/cumulative error: ', error);
    res.status(500).json({
      error: 'Failed to fetch cumulative data',
      message: process.env.NODE_ENV === 'development' ? error.message: undefined
    });
  }
});

// Get data for each month cumulative
app.get('/api/monthly', async (req, res) => {
  const yearValidation = validateYear(req.query.year || new Date().getFullYear());

  if(!yearValidation.valid){
    return res.status(400).json({error: yearValidation.error})
  }

  const year = yearValidation.value;

  try {
    const query = `
      SELECT month,
             tariff_millions_sum,
             total_deposits_millions_sum,
             CASE WHEN total_deposits_millions_sum = 0 THEN 0
                  ELSE 100.0 * tariff_millions_sum / total_deposits_millions_sum END AS pct_od_total
      FROM tariff_monthly
      WHERE EXTRACT(YEAR FROM month) = $1
      ORDER BY month;
    `;
    
    const result = await queryWithRetry(query, [year])
    res.json(result.rows);
  } catch (error) {
    console.error('/api/monthly error: ', error);
    res.status(500).json({
       error: 'Failed to fetch montlhy data',
       message: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// ===========================================
// ERROR HANDLING
// ===========================================
// Handle all other routes
app.use((req,res) => {
  res.status(404).json({error: 'Not Found'});
});

app.use((err,req,res,next) => {
  console.error('Unhandled error: ', err);
  res.status(500).json({
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// ===========================================
// SERVER STARTUP + others
// ===========================================
const PORT = process.env.PORT || 3000;

// Verify DB connection before starting server
async function startServer() {
  try {
    await pool.query('SELECT 1');
    console.log('Database connection verified');

    const server = app.listen(PORT, () => {
      console.log(`API listening on port ${PORT} in ${process.env.NODE_ENV || 'development'} mode`);
    });

    // Graceful shutdown
    async function shutdown(signal) {
      console.log(`\n${signal} received. Starting graceful shutdown...`);
      
      server.close((err) => {
        if (err) {
          console.error('Error closing server: ', err);
        }
      });

      try {
        await pool.end();
        console.log('Database pool closed.');
        process.exit(0);
      } catch (error) {
        console.error('Error closing database pool: ', error);
        process.exit(1);
      }
    }

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    // Handle other exceptions
    process.on('uncaughtException', (err) => {
      console.error('Uncaught Exception: ', err);
      shutdown('uncaughtException');
    });
    
  } catch (error) {
    console.error('Failed to connect to database: ', error);
    process.exit(1);
  }
}

// START SERVER
startServer();