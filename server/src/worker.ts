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
  d = d.trim().replace(/^https?:\/\//i, "");
  d = d.replace(/\/.*/, "");
  d = d.replace(/^"+|"+$/g, "").replace(/^'+|'+$/g, "");
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
  await client.query(`INSERT INTO channels(name) VALUES ('Shopify') ON CONFLICT (name) DO NOTHING`);
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

          const cursors: Record<string, string | null> = {};
          for (const s of stores) cursors[s.domain] = await getCursor(client, channelId, s.domain);

          const orders = (await client.query(`select count(*)::int as n from orders`)).rows[0].n;
          const items  = (await client.query(`select count(*)::int as n from order_items`)).rows[0].n;
          const last   = (await client.query(`select max(placed_at) as t from orders`)).rows[0].t;

          const rsOrders = await client.query(`
            select coalesce(shop_domain,'(unknown)') as shop_domain,
                   count(*)::int as orders,
                   min(placed_at) as first_order_at,
                   max(placed_at) as last_order_at
            from orders
            group by 1
            order by 2 desc
          `);

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

    /*────────────────────────────  KPI APIs  ────────────────────────────*/

    // === Daily KPIs (orders, revenue, units, AOV, new/returning) ===
    // GET /api/kpis/daily?days=14&store=<domain?>
    if (url.pathname === "/api/kpis/daily" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 14), 1), 90);
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

    // === Sales by store per day (same base as v_sales_by_store_daily) ===
    // GET /api/kpis/sales?days=14&store=<domain?>
    if (url.pathname === "/api/kpis/sales" && req.method === "GET") {
      const days = Math.min(Math.max(Number(url.searchParams.get("days") || 14), 1), 90);
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

    // === Rolling KPIs (7/30-day) ===
    // GET /api/kpis/rolling
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

    // === Store summary (yesterday vs prior 7-day avg) ===
    // GET /api/kpis/summary
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

    /*────────────  Product Intelligence helpers (extra)  ────────────*/

    // === Top SKUs ===
    // GET /api/top_skus?days=30&limit=20&store=<domain?>
    // Uses live orders/order_items so you can set arbitrary days/limit.
    if (url.pathname === "/api/top_skus" && req.method === "GET") {
      const days  = Math.min(Math.max(Number(url.searchParams.get("days") || 30), 1), 365);
      const limit = Math.min(Math.max(Number(url.searchParams.get("limit") || 20), 1), 200);
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      const client = await getClient(env);
      try {
        const base = `
          SELECT
            COALESCE(o.shop_domain,'') AS shop_domain,
            oi.sku,
            COALESCE(oi.title, oi.sku, 'Unknown') AS title,
            SUM(oi.qty)::int AS units,
            SUM( (COALESCE(oi.unit_price,0)::numeric) * (COALESCE(oi.qty,0)) )::numeric(14,2) AS revenue
          FROM order_items oi
          JOIN orders o ON o.id = oi.order_id
          WHERE o.placed_at >= (now() - INTERVAL '${days} days')
          ${store ? `AND o.shop_domain = $1` : ``}
          GROUP BY 1,2,3
          ORDER BY revenue DESC NULLS LAST, units DESC
          LIMIT ${limit}
        `;
        const rows = await queryRows(client, base, store ? [store] : []);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // === Repeat rates by store (uses your view) ===
    // GET /api/repeat_rates
    if (url.pathname === "/api/repeat_rates" && req.method === "GET") {
      const client = await getClient(env);
      try {
        const rows = await queryRows(client, `
          SELECT shop_domain, customers_total, one_time_customers, repeat_customers, repeat_rate_pct, avg_ltv_repeat
          FROM v_customer_repeat_rates
          ORDER BY shop_domain
        `);
        return json({ ok: true, rows });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      } finally {
        await client.release();
      }
    }

    // Fallback root
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
  Round-robin cron + main ingest
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

async function runShopifyIngest(env: Env, params: URLSearchParams): Promise<{
  summary: Record<string, { pages: number; ordersIngested: number }>;
}> {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);

  // ingest caps
  const limit    = Math.min(Math.max(Number(env.PAGE_SIZE || 100), 1), 250);
  const maxPages = Math.min(Math.max(Number(env.MAX_PAGES_PER_RUN || 10), 1), 50);
  const days     = Math.min(Math.max(Number(params.get("days") || 90), 1), 365);
  const reset    = (params.get("reset") || "").toLowerCase() === "true";

  // ensure staging + schema awareness
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

  // target subset
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
      let nextPage: string | null = await getCursor(client, channelId, domain);
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
            Accept: "application/json",
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