import fs from 'node:fs/promises';
import path from 'node:path';

const DATA_PATH = path.resolve('public/data/political_data_map.json');
const MAX_AGE_HOURS = Number(process.env.DASHBOARD_DATA_MAX_AGE_HOURS || '36');

function isFiniteNumber(x) {
  return typeof x === 'number' && Number.isFinite(x);
}

function fail(msg) {
  console.error(`❌ ${msg}`);
  process.exit(1);
}

async function main() {
  let raw;
  try {
    raw = await fs.readFile(DATA_PATH, 'utf8');
  } catch {
    fail(`Missing dataset file at ${DATA_PATH}`);
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    fail('Dataset file is not valid JSON');
  }

  const { lastUpdated, political_data_map: map } = payload ?? {};

  if (!lastUpdated) fail('Missing top-level lastUpdated field');
  const updatedAtMs = Date.parse(lastUpdated);
  if (!Number.isFinite(updatedAtMs)) fail('lastUpdated is not a valid ISO date');

  const ageHours = (Date.now() - updatedAtMs) / 36e5;
  if (ageHours > MAX_AGE_HOURS) {
    fail(`Data is stale: ${ageHours.toFixed(1)}h old (max ${MAX_AGE_HOURS}h)`);
  }

  if (!map || typeof map !== 'object' || Array.isArray(map)) {
    fail('Missing/invalid political_data_map object');
  }

  const subreddits = Object.keys(map);
  if (subreddits.length === 0) fail('political_data_map has no subreddits');

  for (const subreddit of subreddits) {
    const entries = map[subreddit];
    if (!Array.isArray(entries) || entries.length === 0) {
      fail(`${subreddit}: no entries`);
    }

    const latest = entries[entries.length - 1] ?? {};
    if (typeof latest.date !== 'string' || !latest.date.includes('/')) {
      fail(`${subreddit}: latest entry missing MM/DD date`);
    }

    if (!isFiniteNumber(Number(latest.left)) || !isFiniteNumber(Number(latest.right))) {
      fail(`${subreddit}: latest entry missing numeric left/right`);
    }

    const total = Number(latest.left) + Number(latest.right);
    if (Math.abs(total - 1) > 0.06) {
      fail(`${subreddit}: left+right must be ~1.0 (got ${total.toFixed(3)})`);
    }

    if (!Number.isInteger(Number(latest.posts)) || Number(latest.posts) < 0) {
      fail(`${subreddit}: latest entry has invalid posts count`);
    }

    if (typeof latest.vibe !== 'string' || latest.vibe.trim() === '') {
      fail(`${subreddit}: latest entry missing vibe`);
    }

    if (!Array.isArray(latest.trending_politicians)) {
      fail(`${subreddit}: latest entry missing trending_politicians array`);
    }
  }

  console.log(`✅ Dataset looks healthy (${subreddits.length} subreddits, age ${ageHours.toFixed(1)}h)`);
}

main().catch((err) => fail(err.message || String(err)));
