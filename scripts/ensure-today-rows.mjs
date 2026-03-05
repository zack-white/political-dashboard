import path from 'node:path';
import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: path.resolve('.env') });

const { Pool } = pg;
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error('DATABASE_URL is not set in .env');

const TZ = process.env.TZ || 'America/New_York';

async function main() {
  const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });
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

    const { rows: latestRows } = await client.query(`
      with ranked as (
        select *, row_number() over (partition by subreddit order by snapshot_date desc, updated_at desc) as rn
        from public.daily_subreddit_metrics
      )
      select subreddit, left_score, right_score, posts_count, vibe, trending_politicians
      from ranked
      where rn = 1;
    `);

    if (latestRows.length === 0) {
      console.log('No existing rows found; skipped ensure-today step.');
      await client.query('commit');
      return;
    }

    const { rows: todayRows } = await client.query(
      `select (now() at time zone $1)::date as today_date;`,
      [TZ]
    );
    const todayDate = todayRows[0].today_date;

    let inserted = 0;
    for (const row of latestRows) {
      await client.query(
        `
        insert into public.daily_subreddit_metrics
          (snapshot_date, subreddit, left_score, right_score, posts_count, vibe, trending_politicians)
        values ($1, $2, $3, $4, $5, $6, $7::jsonb)
        on conflict (snapshot_date, subreddit) do nothing;
        `,
        [
          todayDate,
          row.subreddit,
          Number(row.left_score),
          Number(row.right_score),
          Number(row.posts_count),
          String(row.vibe),
          JSON.stringify(row.trending_politicians ?? []),
        ]
      );
      inserted += 1;
    }

    await client.query('commit');
    console.log(`Ensured today's rows for ${latestRows.length} subreddits on ${todayDate}.`);
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
