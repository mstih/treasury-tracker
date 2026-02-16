import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config();

const app = express();
app.use(cors({origin: process.env.ALLOWED_ORIGIN || "*"}));
app.use(express.json())

// ================================
// Supabase JS client
// ================================
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
);

// ================================
// Validate helper
// ================================
function validateYear(year) {
  const num = Number(year);
  if (!Number.isInteger(num) || num < 2000 || num > 2100) {
    return { valid: false, error: 'Invalid year. Must be between 2000 and 2100' };
  }
  return { valid: true, value: num };
}

// ================================
// Routes
// ================================

// Health check
app.get('/healtz', (req,res) => {
    res.status(200).json({status: 'OK', timestamp: new Date().toISOString()});
});

// Today summary
app.get('/api/summary/today', async (req, res) => {
  try {
    const { data, error } = await supabase.rpc('get_summary_today');
    if (error) throw error;

    res.json({ lastRow: data[0] ?? null });
  } catch (error) {
    console.error('/api/summary/today error:', error.message);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Cumulative summary
app.get('/api/cumulative', async (req, res) => {
  try {
    const yearValidation = validateYear(req.query.year || new Date().getFullYear());
    if (!yearValidation.valid) return res.status(400).json({ error: yearValidation.error });
    const year = yearValidation.value;

    const { data, error } = await supabase.rpc('get_cumulative', { year });
    if (error) throw error;

    res.json(data);
  } catch (error) {
    console.error('/api/cumulative error:', error.message);
    res.status(500).json({ error: 'Failed to fetch cumulative data', message: error.message });
  }
});

// Monthly
app.get('/api/monthly', async (req, res) => {
  try {
    const yearValidation = validateYear(req.query.year || new Date().getFullYear());
    if (!yearValidation.valid) return res.status(400).json({ error: yearValidation.error });
    const year = yearValidation.value;

    const { data, error } = await supabase.rpc('get_monthly', { year });
    if (error) throw error;

    res.json(data);
  } catch (error) {
    console.error('/api/monthly error:', error.message);
    res.status(500).json({ error: 'Failed to fetch monthly data', message: error.message });
  }
});

// other routes 
app.use((req,res) => res.status(404).json({error: 'Not Found!'}));

// error handling
app.use((err,req,res,next) => {
    console.error('Unhandled error: ', err.message);
    res.status(500).json({error: 'Internal server error', message: err.message});
});

// ================================
// Start server
// ================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`API running on port ${PORT}...`)
});