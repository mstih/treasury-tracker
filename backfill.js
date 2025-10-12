// backfill.js
// Usage:
//   node backfill.js 2025-01-01 2025-10-10
// If no args provided it defaults to 2025-01-01 -> yesterday (Europe/Ljubljana).
//
// Requirements: npm i axios pg luxon dotenv
// Ensure DATABASE_URL in .env points to your Postgres (Supabase) DB.
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { DateTime } from 'luxon';
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// Initialize client for database
const { Client } = pg;

const FISCAL_BASE = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash';
const PAGE_SIZE = 1000; // fetch many rows per page
const REQUEST_DELAY_MS = 500; // small delay between requests to be polite to API

// Convert all things that are not string into null value
function safeStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (s.toLowerCase() === 'null' || s === '') return null;
  return s;
}

// Transform strings into numbers or nothing
function toNum(v) {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}

// same aggregate detector as in fetch script
function isAggregateRow(r) {
  const cat = (safeStr(r.transaction_catg) || '').toLowerCase();
  const acct = (safeStr(r.account_type) || '').toLowerCase();
  return /public debt|public debt cash issues|table iiib|table iiia|table iii|treasury general account total|total deposits|total withdrawals|deposits total|public debt issues|total, deposits|treasury general account total deposits/i.test(cat)
      || /treasury general account total deposits|total deposits/i.test(acct);
}

// Sleeps for some timeout to wait 
async function sleep(ms){ return new Promise(resolve => setTimeout(resolve, ms)); }

// fetch one page for given filter and page[number]
async function fetchPage(params) {
  const res = await axios.get(FISCAL_BASE, { params, timeout: 30000 });
  return res.data;
}

// fetch all rows for date range using pagination
async function fetchRangeAll(startDate, endDate) {
  let page = 1;
  const all = [];
  while (true) {
    const params = {
      'filter': `record_date:gte:${startDate},record_date:lte:${endDate}`,
      'page[size]': PAGE_SIZE,
      'page[number]': page
    };
    const body = await fetchPage(params);
    const data = body?.data || [];
    all.push(...data);
    // check meta to see if there are more pages
    const meta = body?.meta;
    if (!meta) {
      // fallback: if fewer than page size, assume end
      if (data.length < PAGE_SIZE) break;
    } else {
      const totalPages = meta?.pagination?.total_pages || null;
      if (totalPages) {
        if (page >= totalPages) break;
      } else {
        // fallback
        if (data.length < PAGE_SIZE) break;
      }
    }
    page += 1;
    await sleep(REQUEST_DELAY_MS);
  }
  return all;
}

function computeTariffAndTotalForDate(rowsForDate) {
  // tariff: transaction_catg contains 'customs'
  const tariffRow = rowsForDate.find(r => {
    const cat = safeStr(r.transaction_catg) || '';
    return cat.toLowerCase().includes('customs');
  });
  const tariff = tariffRow ? toNum(tariffRow.transaction_today_amt) : null;

  // explicit total only if transaction_catg contains 'total deposits' (not account_type)
  const explicitTotalRow = rowsForDate.find(r => {
    const cat = safeStr(r.transaction_catg) || '';
    return /(^|\s)total deposits(\s|$)/i.test(cat);
  }) || null;

  let total = null;
  if (explicitTotalRow) {
    total = toNum(explicitTotalRow.transaction_today_amt);
  } else {
    // fallback: sum deposit rows excluding aggregate rows
    const filtered = rowsForDate.filter(r => {
      const t = (safeStr(r.transaction_type) || '').toLowerCase();
      if (!t.includes('deposit')) return false;
      if (isAggregateRow(r)) return false;
      return true;
    });
    total = filtered.reduce((s, r) => s + (toNum(r.transaction_today_amt) || 0), 0);
  }
  // Round to whole millions (integer)
  const tariffRounded = tariff != null ? Math.round(tariff) : null;
  const totalRounded = total != null ? Math.round(total) : null;
  return { tariff: tariffRounded, total: totalRounded };
}

async function upsertDaily(client, date, tariff, total, rawRows) {
  const sql = `
    INSERT INTO tariff_daily (dts_date, tariff_millions, total_deposits_millions, raw)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (dts_date) DO UPDATE
      SET tariff_millions = EXCLUDED.tariff_millions,
          total_deposits_millions = EXCLUDED.total_deposits_millions,
          raw = EXCLUDED.raw,
          fetched_at = NOW()
  `;
  await client.query(sql, [date, tariff, total, JSON.stringify(rawRows)]);
}

function groupByDate(rows) {
  const map = new Map();
  for (const r of rows) {
    const d = r.record_date;
    if (!d) continue;
    if (!map.has(d)) map.set(d, []);
    map.get(d).push(r);
  }
  return map;
}

async function main() {
  const argStart = process.argv[2];
  const argEnd = process.argv[3];

  // default period if not provided:
  const defaultStart = '2025-01-01';
  const ljYesterday = DateTime.now().setZone('Europe/Ljubljana').minus({ days: 1 }).toISODate();
  const defaultEnd = ljYesterday;

  const startDate = argStart || defaultStart;
  const endDate = argEnd || defaultEnd;

  console.log('Backfill range:', startDate, '->', endDate);

  // fetch all rows for range
  console.log('Fetching rows from FiscalData API (may take a while)...');
  let allRows;
  try {
    allRows = await fetchRangeAll(startDate, endDate);
  } catch (err) {
    console.error('Error fetching from API:', err.response?.data ?? err.message);
    process.exit(1);
  }
  console.log('Total rows fetched:', allRows.length);

  // group by date
  const grouped = groupByDate(allRows);
  const dates = Array.from(grouped.keys()).sort();

  // Connect to DB
  const client = new Client({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }});
  try {
    await client.connect();
  } catch (e) {
    console.error('DB connect failed:', e.message);
    process.exit(1);
  }

  // Upsert each date
  console.log('Upserting daily rows...');
  let processed = 0;
  for (const d of dates) {
    const rowsForDate = grouped.get(d) || [];
    const { tariff, total } = computeTariffAndTotalForDate(rowsForDate);
    try {
      await upsertDaily(client, d, tariff, total, rowsForDate);
      processed++;
      if (processed % 20 === 0) console.log('Upserted', processed, 'dates so far...');
    } catch (e) {
      console.error('Upsert failed for', d, e.message);
      // save raw locally for that date
      const dir = path.join(process.cwd(), 'raw-responses');
      if (!fs.existsSync(dir)) fs.mkdirSync(dir);
      fs.writeFileSync(path.join(dir, `failed-${d}.json`), JSON.stringify(rowsForDate, null, 2));
    }
  }

  console.log(`Done. Upserted ${processed} daily rows to tariff_daily.`);
  await client.end();

  console.log('Backfill complete. Now run the recompute SQL in Supabase to build monthly & yearly aggregates.');
}

main().catch(e => {
  console.error('Fatal error:', e.message || e);
  process.exit(1);
});
