import fs from 'node:fs/promises';
import path from 'node:path';
import vm from 'node:vm';
import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: path.resolve('.env') });

const { Pool } = pg;
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error('DATABASE_URL is not set in .env');
}

const DATA_YEAR = Number(process.env.DATA_YEAR || '2024');

function parsePoliticalDataMap(source) {
  const rewritten = source.replace(
    'export const political_data_map =',
    'globalThis.political_data_map ='
  );
  const context = { globalThis: {} };
  vm.createContext(context);
  vm.runInContext(rewritten, context);
  return context.globalThis.political_data_map;
}

function toIsoDate(mmdd, year) {
  const [mm, dd] = mmdd.split('/').map((x) => x.trim().padStart(2, '0'));
  return `${year}-${mm}-${dd}`;
}

async function main() {
  const dataJsPath = path.resolve('src/data.js');
  const dataJs = await fs.readFile(dataJsPath, 'utf8');
  const politicalDataMap = parsePoliticalDataMap(dataJs);

  const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  const client = await pool.connect();
  try {
    await client.query('begin');

    await client.query(`
      create table if not exists public.daily_subreddit_metrics (
        id bigserial primary key,
        snapshot_date date not null,
        subreddit text not null,
        left_score numeric(5,4) not null,
        right_score numeric(5,4) not null,
        posts_count integer not null,
        vibe text not null,
        trending_politicians jsonb not null default '[]'::jsonb,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        unique (snapshot_date, subreddit)
      );
    `);

    await client.query(`
      create index if not exists idx_daily_metrics_date
      on public.daily_subreddit_metrics(snapshot_date desc);
    `);

    await client.query(`
      create index if not exists idx_daily_metrics_subreddit
      on public.daily_subreddit_metrics(subreddit);
    `);

    let inserted = 0;

    for (const [subreddit, entries] of Object.entries(politicalDataMap)) {
      for (const entry of entries) {
        const snapshotDate = toIsoDate(entry.date, DATA_YEAR);
        const leftScore = Number(entry.left);
        const rightScore = Number(entry.right);
        const postsCount = Number(entry.posts ?? 0);
        const vibe = String(entry.vibe ?? 'Unknown');
        const trending = Array.isArray(entry.trending_politicians) ? entry.trending_politicians : [];

        await client.query(
          `
          insert into public.daily_subreddit_metrics
            (snapshot_date, subreddit, left_score, right_score, posts_count, vibe, trending_politicians)
          values ($1, $2, $3, $4, $5, $6, $7::jsonb)
          on conflict (snapshot_date, subreddit)
          do update set
            left_score = excluded.left_score,
            right_score = excluded.right_score,
            posts_count = excluded.posts_count,
            vibe = excluded.vibe,
            trending_politicians = excluded.trending_politicians,
            updated_at = now();
          `,
          [snapshotDate, subreddit, leftScore, rightScore, postsCount, vibe, JSON.stringify(trending)]
        );

        inserted += 1;
      }
    }

    const { rows } = await client.query(`
      select
        subreddit,
        to_char(snapshot_date, 'MM/DD') as date,
        left_score::float8 as left,
        right_score::float8 as right,
        posts_count as posts,
        vibe,
        trending_politicians
      from public.daily_subreddit_metrics
      order by subreddit asc, snapshot_date asc;
    `);

    const exportedMap = {};
    for (const row of rows) {
      if (!exportedMap[row.subreddit]) exportedMap[row.subreddit] = [];
      exportedMap[row.subreddit].push({
        date: row.date,
        left: row.left,
        right: row.right,
        posts: row.posts,
        vibe: row.vibe,
        trending_politicians: row.trending_politicians ?? [],
      });
    }

    const outPath = path.resolve('public/data/political_data_map.json');
    await fs.mkdir(path.dirname(outPath), { recursive: true });
    await fs.writeFile(
      outPath,
      JSON.stringify(
        {
          lastUpdated: new Date().toISOString(),
          political_data_map: exportedMap,
        },
        null,
        2
      )
    );

    await client.query('commit');
    console.log(`Synced ${inserted} records to Neon and exported ${rows.length} rows to public/data/political_data_map.json`);
  } catch (err) {
    await client.query('rollback');
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
