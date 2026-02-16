// fetch-tariffs-final.js
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { DateTime } from 'luxon';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config();

// ==========================
// Supabase client
// ==========================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ==========================
// Constants
// ==========================
const FISCAL_BASE = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash';
const PAGE_SIZE = 500;

// ==========================
// Helpers
// ==========================
function setMinusDays(numDays) {
  let day = DateTime.now().setZone('Europe/Ljubljana').minus({ days: numDays });
  while (day.weekday > 5) day = day.minus({ days: 1 });
  return day.toISODate();
}

function safeStr(word) {
  if (word === null || word === undefined) return null;
  const final = String(word).trim();
  if (final.toLowerCase() === 'null' || final === '') return null;
  return final;
}

function parseMillions(input) {
  if (input === null || input === undefined) return null;
  const num = Number(String(input).replace(/[^0-9.\-]/g, ''));
  return isNaN(num) ? null : num;
}

function isAggregateRow(row) {
  const cat = (safeStr(row.transaction_catg) || '').toLowerCase();
  const acct = (safeStr(row.account_type) || '').toLowerCase();
  return /public debt|total deposits|treasury general account total/i.test(cat)
      || /total deposits|treasury general account total deposits/i.test(acct);
}

async function fetchForDateRaw(date) {
  const res = await axios.get(FISCAL_BASE, {
    params: {
      filter: `record_date:eq:${date}`,
      'page[size]': PAGE_SIZE
    },
    timeout: 20000
  });
  return res.data?.data || [];
}

async function saveRawLocally(date, rows) {
  const dir = path.join(process.cwd(), 'raw-responses');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);
  const file = path.join(dir, `${date}.json`);
  fs.writeFileSync(file, JSON.stringify(rows, null, 2), 'utf8');
  console.log('Saved raw response to', file);
}

// ==========================
// DB Upsert via Supabase RPC
// ==========================
async function upsertDailyToDB(date, tariffMillions, totalMillions, rawRows) {
  const { data, error } = await supabase.rpc('upsert_tariff_daily', {
    p_date: date,
    p_tariff: tariffMillions,
    p_total: totalMillions,
    p_raw: JSON.stringify(rawRows)
  });
  if (error) throw error;
  return data[0];
}

async function incMonthlyYearly(date, deltaTariff, deltaTotal) {
  if (deltaTariff === 0 && deltaTotal === 0) return;

  const { error } = await supabase.rpc('inc_monthly_yearly', {
    p_date: date,
    p_delta_tariff: deltaTariff,
    p_delta_total: deltaTotal
  });
  if (error) throw error;
}

// ==========================
// Main fetch logic
// ==========================
async function fetchTariffs(deltaDays) {
  const date = setMinusDays(deltaDays);
  console.log('Fetching target date:', date);

  let rows;
  try {
    rows = await fetchForDateRaw(date);
  } catch (e) {
    console.error('API fetch error:', e.response?.data ?? e.message);
    process.exitCode = 1;
    return;
  }

  if (!rows || rows.length === 0) {
    console.warn('No API rows returned for date', date);
    return;
  }

  console.log('Rows returned from API:', rows.length);

  // Extract tariff
  const tariffRow = rows.find(r => (safeStr(r.transaction_catg) || '').toLowerCase().includes('customs'));
  const tariffVal = tariffRow?.transaction_today_amt != null ? parseMillions(tariffRow.transaction_today_amt) : 0;

  // Extract total deposits
  const totalRowExplicit = rows.find(r => /total deposits/i.test(safeStr(r.transaction_catg) || ''));
  let totalVal = totalRowExplicit?.transaction_today_amt != null
      ? parseMillions(totalRowExplicit.transaction_today_amt)
      : 0;

  if (!totalVal) {
    // Fallback sum
    const filtered = rows.filter(r => (safeStr(r.transaction_type) || '').toLowerCase().includes('deposit') && !isAggregateRow(r));
    totalVal = filtered.reduce((s, r) => s + (parseMillions(r.transaction_today_amt) || 0), 0);
    console.log('Fallback total deposits sum:', totalVal, 'M');
  }

  try {
    // Get old values
    const existing = await supabase.rpc('get_existing_tariff_daily', { p_date: date });
    const oldTariffData = existing?.[0]?.tariff_millions || 0;
    const oldTotalData = existing?.[0]?.total_deposits_millions || 0;

    const tariffValRounded = Math.round(tariffVal);
    const totalValRounded = Math.round(totalVal);

    // Upsert new daily row
    await upsertDailyToDB(date, tariffValRounded, totalValRounded, rows);

    // Compute deltas
    const deltaTariff = tariffValRounded - oldTariffData;
    const deltaTotal = totalValRounded - oldTotalData;

    if (deltaTariff !== 0 || deltaTotal !== 0) {
      await incMonthlyYearly(date, deltaTariff, deltaTotal);
      console.log(`Aggregates updated: deltaTariff=${deltaTariff}, deltaTotal=${deltaTotal}`);
    } else {
      console.log('No aggregate changes (delta=0)');
    }

    console.log(`Upsert completed for ${date}: tariff=${tariffValRounded}M, total=${totalValRounded}M`);
  } catch (e) {
    console.error('DB error during update:', e.message);
    await saveRawLocally(date, rows);
    process.exitCode = 1;
  }
}

// ==========================
// Run main
// ==========================
async function main() {
  await fetchTariffs(7); // -2 working days
  await fetchTariffs(1); // -1 working day
  console.log('Done fetching tariffs');
}

main().catch(e => {
  console.error('Fatal error:', e.message || e);
  process.exit(1);
});
