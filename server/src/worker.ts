// server/src/worker.ts
import { Pool, PoolClient } from "@neondatabase/serverless";

/*───────────────────────────────────────────────────────────────────────────*
  Types
*───────────────────────────────────────────────────────────────────────────*/
type Store = { domain: string; token: string };

type Env = {
  DATABASE_URL: string;
  SHOPIFY_STORES?: string;      // JSON array: [{domain, token}, ...]
  PAGE_SIZE?: string;           // default "100" (max 250)
  MAX_PAGES_PER_RUN?: string;   // default "10"
  BACKFILL_TOKEN?: string;      // optional: require ?token=... for /api/admin/backfill
};

/*───────────────────────────────────────────────────────────────────────────*
  Helpers (time, http, store parsing)
*───────────────────────────────────────────────────────────────────────────*/
function log(...args: unknown[]) {
  const ts = new Date().toLocaleString("en-US", {
    timeZone: "America/Denver",
    hour12: false,
  });
  console.log(ts, ...args);
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    },
  });
}

function sanitizeDomain(d: string): string {
  if (!d) return "";
  d = d.trim().replace(/^https?:\/\//i, "");   // drop protocol
  d = d.replace(/\/.*/, "");                   // drop path/query
  d = d.replace(/^"+|"+$/g, "").replace(/^'+|'+$/g, ""); // strip quotes
  return d;
}

function isValidDomain(d: string): boolean {
  return /^[a-z0-9.-]+$/i.test(d) && d.includes(".");
}

function parseStores(raw?: string): Store[] {
  if (!raw) return [];
  try {
    const v = JSON.parse(raw);
    const arr = Array.isArray(v) ? v : (typeof v === "string" ? JSON.parse(v) : []);
    return arr.map((s: any) => ({
      domain: sanitizeDomain(String(s.domain ?? "")),
      token: String(s.token ?? ""),
    }));
  } catch {
    return [];
  }
}

/*───────────────────────────────────────────────────────────────────────────*
  Database helpers
*───────────────────────────────────────────────────────────────────────────*/
async function getClient(env: Env): Promise<PoolClient> {
  if (!env.DATABASE_URL?.startsWith("postgresql")) {
    throw new Error("Missing or invalid DATABASE_URL secret (wrangler secret put DATABASE_URL).");
  }
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  return await pool.connect();
}

/** Match your live schema: composite PK (channel_id,key), value TEXT NOT NULL */
async function ensureSyncState(client: PoolClient): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS sync_state (
      channel_id int NOT NULL,
      key        text NOT NULL,
      value      text NOT NULL,
      updated_at timestamptz DEFAULT now(),
      PRIMARY KEY (channel_id, key)
    )
  `);
}

async function getStagingColumns(client: PoolClient): Promise<Set<string>> {
  const r = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='staging_raw'
  `);
  return new Set(r.rows.map((x: any) => x.column_name));
}

async function getOrCreateShopifyChannelId(client: PoolClient): Promise<number> {
  await client.query(
    `INSERT INTO channels(name) VALUES ('Shopify') ON CONFLICT (name) DO NOTHING`
  );
  const r = await client.query(`SELECT id FROM channels WHERE name='Shopify' ORDER BY id LIMIT 1`);
  if (!r.rows?.[0]?.id) {
    const ins = await client.query(`INSERT INTO channels(name) VALUES ('Shopify') RETURNING id`);
    return Number(ins.rows[0].id);
  }
  return Number(r.rows[0].id);
}

async function queryRows<T = any>(client: PoolClient, sql: string, params: any[] = []): Promise<T[]> {
  const r = await client.query(sql, params);
  return r.rows as T[];
}

/** Ensure compatibility view for legacy KPI queries */
async function ensureCompatView(client: PoolClient): Promise<void> {
  await client.query(`
    CREATE OR REPLACE VIEW v_order_items_enriched AS
    SELECT
      id,
      order_id,
      sku,
      external_product_id,
      title,
      qty,
      unit_price,
      discount,
      tax,
      fees,
      landed_cost_alloc,
      external_item_id,
      (
        COALESCE(unit_price,0)
        - COALESCE(discount,0)
        + COALESCE(tax,0)
        + COALESCE(fees,0)
      )::numeric(18,2) AS line_total
    FROM order_items;
  `);
}

/*───────────────────────────────────────────────────────────────────────────*
  Cursor + schedule index (sync_state) — composite PK + TEXT value
*───────────────────────────────────────────────────────────────────────────*/
function cursorKey(domain: string) {
  return `shopify:cursor:${domain}`;
}
function enc(obj: any): string {
  return JSON.stringify(obj ?? {});
}
function dec(txt: any): any {
  try { return typeof txt === "string" ? JSON.parse(txt) : (txt ?? {}); } catch { return {}; }
}

async function getCursor(client: PoolClient, channelId: number, domain: string): Promise<string | null> {
  await ensureSyncState(client);
  const r = await client.query(
    `SELECT value FROM sync_state WHERE channel_id=$1 AND key=$2 LIMIT 1`,
    [channelId, cursorKey(domain)]
  );
  const obj = dec(r.rows?.[0]?.value ?? null);
  return typeof obj?.page_info === "string" ? obj.page_info : null;
}

async function setCursor(client: PoolClient, channelId: number, domain: string, pageInfo: string | null): Promise<void> {
  await ensureSyncState(client);
  const key = cursorKey(domain);
  if (!pageInfo) {
    await client.query(`DELETE FROM sync_state WHERE channel_id=$1 AND key=$2`, [channelId, key]);
    return;
  }
  await client.query(
    `INSERT INTO sync_state(channel_id, key, value, updated_at)
     VALUES ($1, $2, $3::text, now())
     ON CONFLICT (channel_id, key) DO UPDATE
       SET value = EXCLUDED.value, updated_at = now()`,
    [channelId, key, enc({ page_info: pageInfo })]
  );
}

const SCHEDULE_KEY = "shopify:schedule_idx";

async function getScheduleIndex(client: PoolClient, channelId: number): Promise<number> {
  await ensureSyncState(client);
  const r = await client.query(
    `SELECT value FROM sync_state WHERE channel_id=$1 AND key=$2 LIMIT 1`,
    [channelId, SCHEDULE_KEY]
  );
  const obj = dec(r.rows?.[0]?.value ?? null);
  return typeof obj?.idx !== "undefined" ? Number(obj.idx) || 0 : 0;
}

async function setScheduleIndex(client: PoolClient, channelId: number, idx: number): Promise<void> {
  await ensureSyncState(client);
  await client.query(
    `INSERT INTO sync_state(channel_id, key, value, updated_at)
     VALUES ($1, $2, $3::text, now())
     ON CONFLICT (channel_id, key) DO UPDATE
       SET value = EXCLUDED.value, updated_at = now()`,
    [channelId, SCHEDULE_KEY, enc({ idx })]
  );
}

/*───────────────────────────────────────────────────────────────────────────*
  HTTP Router (fetch) + Cron (scheduled)
*───────────────────────────────────────────────────────────────────────────*/
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "OPTIONS") return json({ ok: true });

    const url = new URL(req.url);

    // 1) Shops (from secret)
    if (url.pathname === "/api/shops" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      const shops = stores.map((s, i) => ({
        id: i + 1,
        handle: s.domain.split(".")[0],
        domain: s.domain,
      }));
      return shops.length ? json({ ok: true, shops }) : json({ ok: false, error: "No stores configured" });
    }

    // 2) Debug: store parsing preview
    if (url.pathname === "/api/debug/stores" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      const preview = stores.map((s) => {
        const d = sanitizeDomain(s.domain);
        return { raw: s.domain, sanitized: d, valid: isValidDomain(d) };
      });
      return json({ ok: true, preview });
    }

    // 3) Debug: DB ping
    if (url.pathname === "/api/debug/db" && req.method === "GET") {
      try {
        const client = await getClient(env);
        const r = await client.query("select now()");
        await client.release();
        return json({ ok: true, now: r?.rows?.[0]?.now ?? null });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    // 4) Debug: cursors dump
    if (url.pathname === "/api/debug/cursor" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      const client = await getClient(env);
      try {
        const channelId = await getOrCreateShopifyChannelId(client);
        const out: Record<string, string | null> = {};
        for (const s of stores) out[s.domain] = await getCursor(client, channelId, s.domain);
        return json({ ok: true, cursors: out });
      } finally {
        await client.release();
      }
    }

    // 5) Debug: reset a cursor (?store=domain)
    if (url.pathname === "/api/debug/reset" && req.method === "POST") {
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      if (!store) return json({ ok: false, error: "store param required" }, 400);
      const client = await getClient(env);
      try {
        const channelId = await getOrCreateShopifyChannelId(client);
        await setCursor(client, channelId, store, null);
        log("cursor:cleared", store);
        return json({ ok: true, cleared: store });
      } finally {
        await client.release();
      }
    }

    // 6) Debug: health snapshot (includes per-store totals)
    if (url.pathname === "/api/debug/health" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      try {
        const client = await getClient(env);
        try {
          const channelId = await getOrCreateShopifyChannelId(client);
          const mtNow = new Date().toLocaleString("en-US", { timeZone: "America/Denver", hour12: false });
          const scheduleIndex = await getScheduleIndex(client, channelId);

          // cursors
          const cursors: Record<string, string | null> = {};
          for (const s of stores) cursors[s.domain] = await getCursor(client, channelId, s.domain);

          // totals
          const orders = (await client.query(`select count(*)::int as n from orders`)).rows[0].n;
          const items  = (await client.query(`select count(*)::int as n from order_items`)).rows[0].n;
          const last   = (await client.query(`select max(placed_at) as t from orders`)).rows[0].t;

          // per-store orders
          const rsOrders = await client.query(`
            select coalesce(shop_domain,'(unknown)') as shop_domain,
                   count(*)::int as orders,
                   min(placed_at) as first_order_at,
                   max(placed_at) as last_order_at
            from orders
            group by 1
            order by 2 desc
          `);

          // per-store items
          const rsItems = await client.query(`
            select coalesce(o.shop_domain,'(unknown)') as shop_domain,
                   count(*)::int as items
            from order_items oi
            join orders o on o.id = oi.order_id
            group by 1
            order by 2 desc
          `);

          const itemsByStore: Record<string, number> = {};
          for (const row of rsItems.rows) itemsByStore[row.shop_domain] = row.items;

          const per_store = rsOrders.rows.map((r: any) => ({
            shop_domain: r.shop_domain,
            orders: r.orders,
            items: itemsByStore[r.shop_domain] ?? 0,
            first_order_at: r.first_order_at,
            last_order_at: r.last_order_at,
          }));

          return json({
            ok: true,
            now_mt: mtNow,
            schedule_index: scheduleIndex,
            stores: stores.map(s => s.domain),
            cursors,
            totals: { orders, items, last_order_at: last },
            per_store,
          });
        } finally {
          await client.release();
        }
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    // 7) Ingest trigger (supports ?store=, ?days=, ?reset=)
    if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
      try {
        const res = await runShopifyIngest(env, url.searchParams);
        return json({ ok: true, ...res });
      } catch (e: any) {
        log("ingest:error", e?.message ?? String(e));
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    // === KPIs: daily ===
    if (url.pathname === "/api/kpis/daily" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 14), 1), 365);
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      const client = await getClient(env);
      try {
        const sql =
          `SELECT * FROM v_daily_kpis_by_store
           WHERE day_mt >= (CURRENT_DATE - INTERVAL '${days} days')` +
          (store ? ` AND shop_domain = $1` : ``) +
          ` ORDER BY day_mt DESC, shop_domain`;
        const rows = await queryRows(client, sql, store ? [store] : []);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: sales by store daily ===
    if (url.pathname === "/api/kpis/sales" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 14), 1), 365);
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      const client = await getClient(env);
      try {
        const sql =
          `SELECT * FROM v_sales_by_store_daily
           WHERE day_mt >= (CURRENT_DATE - INTERVAL '${days} days')` +
          (store ? ` AND shop_domain = $1` : ``) +
          ` ORDER BY day_mt DESC, shop_domain`;
        const rows = await queryRows(client, sql, store ? [store] : []);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: rolling 7/30 as of today (MT) ===
    if (url.pathname === "/api/kpis/rolling" && req.method === "GET") {
      const client = await getClient(env);
      try {
        const rows = await queryRows(client, `SELECT * FROM v_kpis_rolling_7_30 ORDER BY shop_domain`);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: store summary (yesterday vs prior 7‑day avg) ===
    if (url.pathname === "/api/kpis/summary" && req.method === "GET") {
      const client = await getClient(env);
      try {
        const rows = await queryRows(client, `SELECT * FROM v_store_summary_yday_vs_prev7 ORDER BY shop_domain`);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: Top SKUs ===
    if (url.pathname === "/api/kpis/top-skus" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 30), 1), 365);
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      const limit = Math.min(Math.max(Number(url.searchParams.get("limit") || 50), 1), 500);
      const include365 = (url.searchParams.get("include365") || "").toLowerCase() === "true";

      const client = await getClient(env);
      try {
        await ensureCompatView(client);

        const params: any[] = [days];
        if (store) params.push(store);

        const sql = `
          WITH window_orders AS (
            SELECT
              oi.sku,
              COALESCE(oi.title,'')                     AS title,
              o.shop_domain,
              SUM(oi.qty)::int                          AS units_window,
              SUM(oi.line_total)::numeric(20,2)         AS revenue_window
            FROM v_order_items_enriched oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.placed_at >= (CURRENT_DATE - ($1::int || ' days')::interval)
              ${store ? `AND o.shop_domain = $2` : ``}
            GROUP BY 1,2,3
          )
          SELECT
            w.sku,
            w.title,
            w.shop_domain,
            w.units_window,
            w.revenue_window
            ${include365 ? `,
            x.units_365,
            x.revenue_365` : ``}
          FROM window_orders w
          ${include365 ? `
          LEFT JOIN LATERAL (
            SELECT
              SUM(oi2.qty)::int                  AS units_365,
              SUM(oi2.line_total)::numeric(20,2) AS revenue_365
            FROM v_order_items_enriched oi2
            JOIN orders o2 ON o2.id = oi2.order_id
            WHERE oi2.sku = w.sku
              AND o2.shop_domain = w.shop_domain
              AND o2.placed_at >= (CURRENT_DATE - INTERVAL '365 days')
          ) x ON TRUE
          ` : ``}
          ORDER BY w.revenue_window DESC NULLS LAST
          LIMIT ${limit};
        `;

        const rows = await queryRows(client, sql, params);
        return json({ ok: true, rows });
      } catch (e:any) {
        return json({ ok:false, error:e?.message || String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: Bottom SKUs ===
    if (url.pathname === "/api/kpis/bottom-skus" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 30), 1), 365);
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      const limit = Math.min(Math.max(Number(url.searchParams.get("limit") || 50), 1), 500);
      const include365 = (url.searchParams.get("include365") || "").toLowerCase() === "true";

      const client = await getClient(env);
      try {
        await ensureCompatView(client);

        const params: any[] = [days];
        if (store) params.push(store);

        const sql = `
          WITH window_orders AS (
            SELECT
              oi.sku,
              COALESCE(oi.title,'')                     AS title,
              o.shop_domain,
              SUM(oi.qty)::int                          AS units_window,
              SUM(oi.line_total)::numeric(20,2)         AS revenue_window
            FROM v_order_items_enriched oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.placed_at >= (CURRENT_DATE - ($1::int || ' days')::interval)
              ${store ? `AND o.shop_domain = $2` : ``}
            GROUP BY 1,2,3
          )
          SELECT
            w.sku,
            w.title,
            w.shop_domain,
            w.units_window,
            w.revenue_window
            ${include365 ? `,
            x.units_365,
            x.revenue_365` : ``}
          FROM window_orders w
          ${include365 ? `
          LEFT JOIN LATERAL (
            SELECT
              SUM(oi2.qty)::int                  AS units_365,
              SUM(oi2.line_total)::numeric(20,2) AS revenue_365
            FROM v_order_items_enriched oi2
            JOIN orders o2 ON o2.id = oi2.order_id
            WHERE oi2.sku = w.sku
              AND o2.shop_domain = w.shop_domain
              AND o2.placed_at >= (CURRENT_DATE - INTERVAL '365 days')
          ) x ON TRUE
          ` : ``}
          ORDER BY w.revenue_window ASC NULLS LAST, w.units_window ASC NULLS LAST
          LIMIT ${limit};
        `;

        const rows = await queryRows(client, sql, params);
        return json({ ok: true, rows });
      } catch (e:any) {
        return json({ ok:false, error:e?.message || String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === KPIs: Repeat Rates ===
    if (url.pathname === "/api/kpis/repeat-rates" && req.method === "GET") {
      const client = await getClient(env);
      try {
        const rows = await queryRows(client, `SELECT * FROM v_customer_repeat_rates ORDER BY shop_domain`);
        return json({ ok:true, rows });
      } catch (e:any) {
        return json({ ok:false, error:e?.message || String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === ADMIN: Backfill historical Shopify data ===
    // POST /api/admin/backfill?days=365&store=<optional>&hard_reset=true&token=XYZ
    if (url.pathname === "/api/admin/backfill" && req.method === "POST") {
      try {
        if (env.BACKFILL_TOKEN) {
          const token = url.searchParams.get("token") || "";
          if (token !== env.BACKFILL_TOKEN) {
            return json({ ok: false, error: "unauthorized" }, 401);
          }
        }

        const days = Math.min(Math.max(Number(url.searchParams.get("days") || 365), 1), 365);
        const store = sanitizeDomain(url.searchParams.get("store") || "");
        const hardReset = (url.searchParams.get("hard_reset") || "").toLowerCase() === "true";

        const res = await backfillShopify(env, { days, store, hardReset });
        return json({ ok: true, ...res });
      } catch (e:any) {
        log("backfill:error", e?.message || String(e));
        return json({ ok:false, error:e?.message || String(e) }, 500);
      }
    }

    // fallback
    return new Response("OK", { status: 200 });
  },

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    log("cron:start");
    ctx.waitUntil(
      runShopifyIngestRoundRobin(env)
        .then((res) => log("cron:done", res))
        .catch((e) => log("cron:error", e?.message || String(e)))
    );
  },
};

/*───────────────────────────────────────────────────────────────────────────*
  Round-robin cron + backfill + main ingest
*───────────────────────────────────────────────────────────────────────────*/
async function runShopifyIngestRoundRobin(env: Env): Promise<{ store: string; result: unknown }> {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);
  try {
    const channelId = await getOrCreateShopifyChannelId(client);

    const idx = await getScheduleIndex(client, channelId);
    const nextIdx = idx % stores.length;
    const store = stores[nextIdx];
    log("cron:store", store.domain, { nextIdx, totalStores: stores.length });

    const result = await runShopifyIngest(env, new URLSearchParams({ store: store.domain }));
    await setScheduleIndex(client, channelId, nextIdx + 1);

    return { store: store.domain, result };
  } finally {
    await client.release();
  }
}

/** Admin backfill: loop runShopifyIngest until cursor ends (per store or all). */
async function backfillShopify(env: Env, opts: { days: number; store?: string; hardReset?: boolean }) {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const target = opts.store ? sanitizeDomain(opts.store) : "";
  const list = target ? stores.filter(s => sanitizeDomain(s.domain) === target) : stores;

  const perStoreSummary: Record<string, { iterations: number; pages: number; orders: number }> = {};
  const maxIterations = 200; // safety cap

  for (const s of list) {
    const domain = sanitizeDomain(s.domain);
    let iterations = 0;
    let totalPages = 0;
    let totalOrders = 0;

    // optional hard reset
    if (opts.hardReset) {
      const c = await getClient(env);
      try {
        const channelId = await getOrCreateShopifyChannelId(c);
        await setCursor(c, channelId, domain, null);
        log("backfill:reset", { domain });
      } finally {
        await c.release();
      }
    }

    // loop until run returns fewer than max pages OR no orders
    while (iterations < maxIterations) {
      iterations++;
      const params = new URLSearchParams({
        store: domain,
        days: String(opts.days),
      });
      const res: any = await runShopifyIngest(env, params);
      const pagesThisRun = Number(res?.summary?.[domain]?.pages || 0);
      const ordersThisRun = Number(res?.summary?.[domain]?.ordersIngested || 0);
      totalPages += pagesThisRun;
      totalOrders += ordersThisRun;

      log("backfill:iteration", { domain, iterations, pagesThisRun, ordersThisRun });

      // heuristic: if we didn't hit your per-run page ceiling, or we got 0 orders, assume done
      const perRunCap = Math.min(Math.max(Number(env.MAX_PAGES_PER_RUN || 10), 1), 50);
      if (pagesThisRun < perRunCap || ordersThisRun === 0) break;
    }

    perStoreSummary[domain] = { iterations, pages: totalPages, orders: totalOrders };
  }

  return { days: opts.days, hardReset: !!opts.hardReset, stores: list.map(s => s.domain), perStoreSummary };
}

/* Core ingest: single pass (up to MAX_PAGES_PER_RUN) for one or all stores */
async function runShopifyIngest(env: Env, params: URLSearchParams): Promise<{
  summary: Record<string, { pages: number; ordersIngested: number }>;
}> {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);

  const limit    = Math.min(Math.max(Number(env.PAGE_SIZE || 100), 1), 250);
  const maxPages = Math.min(Math.max(Number(env.MAX_PAGES_PER_RUN || 10), 1), 50);
  const days     = Math.min(Math.max(Number(params.get("days") || 90), 1), 365);
  const reset    = (params.get("reset") || "").toLowerCase() === "true";

  await client.query(
    "CREATE EXTENSION IF NOT EXISTS pgcrypto; " +
    "CREATE TABLE IF NOT EXISTS staging_raw (" +
    " id UUID DEFAULT gen_random_uuid() PRIMARY KEY," +
    " payload JSONB NOT NULL," +
    " received_at TIMESTAMPTZ DEFAULT now()," +
    " domain text" +
    ");"
  );
  const stagingCols = await getStagingColumns(client);
  const channelId   = await getOrCreateShopifyChannelId(client);

  const targetDomainParam = params.get("store") || params.get("domain");
  const target = targetDomainParam ? sanitizeDomain(String(targetDomainParam)) : null;
  const list   = target ? stores.filter((s) => sanitizeDomain(s.domain) === target) : stores;

  const summary: Record<string, { pages: number; ordersIngested: number }> = {};

  try {
    for (const s of list) {
      const domain = sanitizeDomain(s.domain);
      if (!isValidDomain(domain)) throw new Error(`Invalid shop domain: "${s.domain}" -> "${domain}"`);

      const token = s.token;
      if (!/^shpat_/.test(token || "")) throw new Error(`Missing/invalid Admin API token for ${domain}`);

      if (reset) await setCursor(client, channelId, domain, null);

      let pages = 0;
      let total = 0;
      let nextPage: string | null = await getCursor(client, channelId, domain); // resume
      const since = new Date(Date.now() - days * 24 * 3600 * 1000).toISOString();

      log("ingest:start", { domain, days, resumeFromCursor: !!nextPage });

      while (pages < maxPages) {
        const qp = new URLSearchParams();
        qp.set("limit", String(limit));
        if (!nextPage) {
          qp.set("status", "any");
          qp.set("created_at_min", since);
        } else {
          qp.set("page_info", nextPage);
        }

        const urlStr = `https://${domain}/admin/api/2024-07/orders.json?${qp.toString()}`;
        const resp = await fetch(urlStr, {
          headers: {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
          },
        });

        if (!resp.ok) {
          const t = await resp.text();
          throw new Error(`Shopify ${domain} ${resp.status}: ${t}`);
        }

        const data: any = await resp.json();
        const orders = Array.isArray(data.orders) ? data.orders : [];

        if (orders.length) {
          // Build INSERT for existing staging_raw columns
          const cols = ["payload"];
          if (stagingCols.has("channel_id")) cols.push("channel_id");
          if (stagingCols.has("source"))     cols.push("source");
          if (stagingCols.has("kind"))       cols.push("kind");
          if (stagingCols.has("domain"))     cols.push("domain");

          const values: any[] = [];
          const placeholders: string[] = [];
          let paramIdx = 1;

          for (const o of orders) {
            const row: any[] = [JSON.stringify(o)];
            if (stagingCols.has("channel_id")) row.push(channelId);
            if (stagingCols.has("source"))     row.push("shopify");
            if (stagingCols.has("kind"))       row.push("order");
            if (stagingCols.has("domain"))     row.push(domain);

            placeholders.push("(" + row.map(() => `$${paramIdx++}`).join(",") + ")");
            values.push(...row);
          }

          const sql = `INSERT INTO staging_raw (${cols.join(",")}) VALUES ${placeholders.join(",")}`;
          await client.query(sql, values);
        }

        total += orders.length;
        pages++;

        // Parse Link header for next page_info
        const link = resp.headers.get("link") || resp.headers.get("Link");
        let newCursor: string | null = null;
        if (link) {
          const m = link.match(/<[^>]*[?&]page_info=([^&>]+)[^>]*>;\s*rel="next"/i);
          if (m) newCursor = m[1];
        }

        await setCursor(client, channelId, domain, newCursor);
        log("ingest:page", { domain, page: pages, orders: orders.length, hasNext: !!newCursor });

        if (!newCursor || orders.length === 0) break;
        nextPage = newCursor;
      }

      summary[domain] = { pages, ordersIngested: total };
      log("ingest:done", domain, { pages, total });
    }
  } finally {
    await client.release();
  }

  log("ingest:summary", {
    limit, maxPages, days,
    stores: list.map((s) => s.domain),
    summary,
  });

  return { summary };
}
