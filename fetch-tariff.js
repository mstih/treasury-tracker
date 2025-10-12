// debug-fetch-tariff.js - drop-in for debugging API schema issues
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import pg from 'pg';
import { DateTime } from 'luxon';
import dotenv from 'dotenv';
dotenv.config();

const { Client } = pg;
const DB = process.env.DATABASE_URL;
const FISCAL_BASE = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash';
const PAGE_SIZE = 500;

// Transform one day behind, if weekend then go back to last workday
function prevWorkingDay() {
  let day = DateTime.now().setZone('Europe/Ljubljana').minus({ days: 1 });
  while (day.weekday > 5) day = day.minus({ days: 1 });
  return day.toISODate();
}

async function fetchForDateRaw(date) {
  const res = await axios.get(FISCAL_BASE, {
    params: {
      'filter': `record_date:eq:${date}`,
      'page[size]': PAGE_SIZE
    },
    timeout: 20000
  });
  return res.data?.data || [];
}

// If there is no data, return null,
// Otherwise trim it, if still null return null
function safeStr(word) {
  if (word === null || word === undefined) return null;
  const final = String(word).trim();
  if (final.toLowerCase() === 'null' || final === '') return null;
  return final;
}

// Parse string to number value
function parseMillions(input) {
  if (input === null || input === undefined) return null;
  const num = Number(String(input).replace(/[^0-9.\-]/g, ''));
  return isNaN(num) ? null : num;
}

function isAggregateRow(row) {
  const cat = (safeStr(row.transaction_catg) || '').toLowerCase();
  const acct = (safeStr(row.account_type) || '').toLowerCase();
  return /public debt|public debt cash issues|table iiib|table iiia|table iii|treasury general account total|total deposits|total withdrawals|deposits total|public debt issues|total, deposits/i.test(cat)
      || /treasury general account total deposits|total deposits/i.test(acct);
}

// Check if there is a temp directory, if yes create file and save response to file
async function saveRawLocally(date, rows) {
  const dir = path.join(process.cwd(), 'raw-responses');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);
  const file = path.join(dir, `${date}.json`);
  fs.writeFileSync(file, JSON.stringify(rows, null, 2), 'utf8');
  console.log('Saved raw response to', file);
}

// Function that queries the database and stores the values and return values to calculate sums later
async function upsertDailyToDB(client, date, tariffMillions, totalMillions, rawRows) {
  const sql = `
    INSERT INTO tariff_daily (dts_date, tariff_millions, total_deposits_millions, raw)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (dts_date) DO UPDATE
      SET tariff_millions = EXCLUDED.tariff_millions,
          total_deposits_millions = EXCLUDED.total_deposits_millions,
          raw = EXCLUDED.raw,
          fetched_at = NOW()
    RETURNING tariff_millions, total_deposits_millions;
  `;
  const res = await client.query(sql, [date, tariffMillions, totalMillions, JSON.stringify(rawRows)]);
  return res.rows[0];
}

async function incMonthlyYearly(client, date, deltaTariff, deltaTotal){
  if (deltaTariff === 0 && deltaTotal === 0) return;

  const monthKey = date.slice(0,7) + '-01'; // convert to first day of each month for monthly sums
  const yearKey = Number(date.slice(0,4));

  // Insert or add deltas to monthly table
  await client.query(`
    INSERT INTO tariff_monthly (month, tariff_millions_sum, total_deposits_millions_sum)
    VALUES ($1, $2, $3)
    ON CONFLICT (month) DO UPDATE
      SET tariff_millions_sum = tariff_monthly.tariff_millions_sum + EXCLUDED.tariff_millions_sum,
          total_deposits_millions_sum = tariff_monthly.total_deposits_millions_sum + EXCLUDED.total_deposits_millions_sum,
          updated_at = NOW()
    `, [monthKey, deltaTariff, deltaTotal]);
  
  // Same for yearly
  await client.query(`
    INSERT INTO tariff_yearly (year, tariff_millions_sum, total_deposits_millions_sum)
    VALUES ($1, $2, $3)
    ON CONFLICT (year) DO UPDATE
      SET tariff_millions_sum = tariff_yearly.tariff_millions_sum + EXCLUDED.tariff_millions_sum,
          total_deposits_millions_sum = tariff_yearly.total_deposits_millions_sum + EXCLUDED.total_deposits_millions_sum,
          updated_at = NOW()
    `, [yearKey, deltaTariff, deltaTotal]);
}

async function main() {
  const date = prevWorkingDay();
  console.log('Target date:', date);

  let rows;
  try {
    rows = await fetchForDateRaw(date);
  } catch (e) {
    // Return error with API
    console.error('API fetch error:', e.response?.data ?? e.message);
    return process.exitCode = 1;
  }

  // No rows error
  if (!rows || rows.length === 0) {
    console.warn('No rows returned for date', date);
    return;
  }

  // Tell how many rows there is in response
  console.log('Rows returned:', rows.length);

  // EXTRACT tariff data from rows
  const tariffRow = rows.find(r => {
    const cat = safeStr(r.transaction_catg);
    return cat && cat.toLowerCase().includes('customs');
  });

  // Convert tariff value to number
  const tariffVal = tariffRow ? parseMillions(tariffRow.transaction_today_amt) : null;

  // Find all rows with deposits
  const totalRowExplicit = rows.find(r => {
    const cat = safeStr(r.transaction_catg) || '';
    return /(^|\s)total deposits(\s|$)/i.test(cat);
  }) || null;
  
  let totalVal = null;
  if (totalRowExplicit) {
    totalVal = parseMillions(totalRowExplicit.transaction_today_amt);
    console.log('Using explicit total deposits row:', safeStr(totalRowExplicit.transaction_catg), 'value=', totalVal, 'M');
  } else {
    // Fallback: sum deposit rows but exclude aggregate/summary lines
    const filtered = rows.filter(r => {
    const type = (safeStr(r.transaction_type) || '').toLowerCase();
    if (!type.includes('deposit')) return false;

    // exclude aggregate/summary lines
    const cat = (safeStr(r.transaction_catg) || '').toLowerCase();
    const acct = (safeStr(r.account_type) || '').toLowerCase();
    const isAggregate = /public debt|public debt cash issues|table iiib|table iiia|table iii|treasury general account total|total deposits|total withdrawals|deposits total|public debt issues|total, deposits|treasury general account total deposits/.test(cat) ||
                        /total deposits|treasury general account total deposits/.test(acct);
    if (isAggregate) return false;
    return true;
  });

    // Reduce function, sum up all deposits
    totalVal = filtered.reduce((s, r) => s + (parseMillions(r.transaction_today_amt) || 0), 0);
    console.log('Computed fallback total by summing filtered deposit rows. Count=', filtered.length, 'sum=', totalVal, 'M');
  }


  // Establish DB connection, if fails show error message
  let client;
  try {
    client = new Client({ connectionString: DB, ssl: { rejectUnauthorized: false }});
    await client.connect();
  } catch (e) {
    console.error('DB connect failed:', e.message);
    await saveRawLocally(date, rows);
    return process.exitCode = 1;
  }

  try{
    // If there are existing values
    const existingResponse = await client.query('SELECT tariff_millions, total_deposits_millions FROM tariff_daily WHERE dts_date=$1', [date])
    const existing = existingResponse.rows[0] || null;
    const oldTariffData = existing ? (existing.tariff_millions || 0 ): 0;
    const oldTotalData = existing ? (existing.total_deposits_millions || 0): 0;

    const tariffValRounded = tariffVal != null ? Math.round(tariffVal):0;
    const totalValRounded = totalVal != null ? Math.round(totalVal):0;

    // Update daily values and get current stored values
    await upsertDailyToDB(client, date, tariffValRounded, totalValRounded, rows);

    // Compute deltas = new - old (handle reruns or/and corrections)
    const deltaTariff = (tariffValRounded || 0) - Number(oldTariffData || 0)
    const deltaTotal = (totalValRounded || 0) - Number(oldTotalData || 0);

    // If ther is any difference then update
    if(deltaTariff !== 0 || deltaTotal !== 0){
      await incMonthlyYearly(client, date, deltaTariff, deltaTotal);
      console.log(`Aggregates updated: deltaTariff=${deltaTariff} deltaTotal=${deltaTotal}`)
    }else{
      console.log('No aggregates changes (delta = 0).')
    }
    console.log(`Upsert completed for date ${date}: tariff=${tariffValRounded}M ; total=${totalValRounded}M`)
  } catch (e){
    console.error('DB error during data update: ', e.message);
    await saveRawLocally(date, rows);
    process.exitCode = 1;
  } finally {
    try {await client.end();} catch {}
  }
}

main().catch(e => {
  console.error('Fatal error:', e.message || e);
  process.exit(1);
});