// api/server.js
import express from 'express';
import cors from 'cors';
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();
const {Client} = pg;

const app = express();
app.use(cors());
app.use(express.json());

const getClient = () => new Client({connectionString: process.env.DATABASE_URL, ssl: {rejectUnauthorized: false}});


app.get('/api/summary/today', async (req, res) => {
    const client = getClient();
    try{
        await client.connect();
        const lastRow = (await client.query('SELECT dts_date, tariff_millions, total_deposits_millions FROM tariff_daily ORDER BY dts_date DESC LIMIT 1')).rows[0];
        const year = new Date().toLocaleString('en-US', {timezone: 'Europe/Ljubljana', year: 'numeric'});
        const yearly = (await client.query('SELECT * FROM tariff_yearly WHERE year=$1', [Number(year)]));
        res.json({lastRow, yearly});
    } catch (e) {
        res.status(500).json({error: e.message})
    } finally { try {await client.end()} catch {}}
});

app.get('/api/cumulative', async (req, res) => {
    const year = Number(req.query.year) || new Date().getFullYear();
    const client = getClient();
    try {
        await client.connect();
        // Cumulative function
        const q = `
        SELECT dts_date, tariff_millions, SUM(tariff_millions) OVER (ORDER BY dts_date) AS cumulative_tariff
        FROM tariff_daily
        WHERE EXTRACT(YEAR FROM dts_date) = $1
        ORDER BY dts_date;
        `;
        const rows = (await client.query(q, [year])).rows;
        res.json(rows);
    } catch (error) {
        res.status(500).json({error: error.message});
    } finally {try{await client.end();}catch{}}
});

app.get('/api/monthly', async (req,res) => {
    const year = Number(req.query.year) || new Date().getFullYear();
    const client = getClient();

    try {
        await client.connect();
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
        const rows = (await client.query(q, [year])).rows;
        res.json(rows);

    } catch (error) {
        res.status(500).json({error: error.message})
    } finally {try{await client.end();}catch{}}
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API listening on port ${PORT}`))