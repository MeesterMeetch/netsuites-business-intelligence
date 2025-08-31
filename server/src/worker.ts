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

  // alerting
  SENDGRID_API_KEY?: string;    // optional: use SendGrid if present
  ALERT_EMAIL_FROM?: string;    // required for alerts (can be your personal email)
  ALERT_EMAIL_TO?: string;      // required for alerts (recipient)
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

/* CORS + JSON helpers (single source of truth) */
const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
};

function jsonResponse(body: any, status = 200, extra: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...CORS_HEADERS,
      ...extra,
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
  Email alerts (MailChannels by default, SendGrid if key present)
*───────────────────────────────────────────────────────────────────────────*/
async function sendAlertEmail(env: Env, subject: string, html: string) {
  const from = (env.ALERT_EMAIL_FROM || "").trim();
  const to   = (env.ALERT_EMAIL_TO   || "").trim();
  const sg   = (env.SENDGRID_API_KEY || "").trim();

  if (!from || !to) {
    // Silent no-op if not configured (avoid breaking the worker)
    log("alert:skipped (ALERT_EMAIL_FROM/TO not set)", { subject });
    return;
  }

  if (sg) {
    // SendGrid path
    const res = await fetch("https://api.sendgrid.com/v3/mail/send", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${sg}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: to }] }],
        from: { email: from, name: "Shopify Ingest" },
        subject,
        content: [{ type: "text/html", value: html }],
      }),
    });
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`SendGrid failed: ${res.status} ${t}`);
    }
    return;
  }

  // MailChannels (no account needed)
  const res = await fetch("https://api.mailchannels.net/tx/v1/send", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      personalizations: [{ to: [{ email: to }] }],
      from: { email: from, name: "Shopify Ingest" },
      subject,
      content: [{ type: "text/html", value: html }],
    }),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`MailChannels failed: ${res.status} ${t}`);
  }
}

function htmlEscape(s: string) {
  return s.replace(/[&<>"']/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[c] as string)
  );
}

async function alertOnError(env: Env, context: string, err: any) {
  const msg = (err?.message || String(err) || "").slice(0, 4000);
  const body =
    `<h3>${htmlEscape(context)} failed</h3>` +
    `<pre style="white-space:pre-wrap;font-family:ui-monospace,Menlo,monospace">${htmlEscape(msg)}</pre>` +
    `<p>Time (UTC): ${new Date().toISOString()}</p>`;
  try {
    await sendAlertEmail(env, `⚠️ ${context} error`, body);
  } catch (e: any) {
    log("alert:send_failed", e?.message || String(e));
  }
}

async function parseJsonSafe(resp: Response): Promise<any> {
  const ctype = resp.headers.get("content-type") || "";
  const txt = await resp.text(); // read once
  if (ctype.includes("application/json")) {
    try { return JSON.parse(txt); } catch (e) {
      throw new Error(`Invalid JSON from upstream: ${String(e)} :: ${txt.slice(0,400)}`);
    }
  }
  // not JSON; include a snippet so we can see what it was (HTML, text, etc.)
  throw new Error(`Non-JSON upstream response (${resp.status} ${resp.statusText}): ${txt.slice(0,400)}`);
}

/*───────────────────────────────────────────────────────────────────────────*
  DB robustness helpers (singleton pool + retries)
*───────────────────────────────────────────────────────────────────────────*/
let _pool: Pool | null = null;

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry<T>(fn: () => Promise<T>, tries = 5, baseMs = 400): Promise<T> {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (e: any) {
      lastErr = e;
      const msg = (e?.message || "").toLowerCase();
      const retryable =
        msg.includes("connection") ||
        msg.includes("terminating connection") ||
        msg.includes("client has encountered a connection error") ||
        msg.includes("server closed the connection") ||
        msg.includes("too many connections") ||
        msg.includes("timeout") ||
        msg.includes("not queryable");

      if (!retryable) break;
      const backoff = Math.min(baseMs * Math.pow(2, i), 3000);
      log("db:retry", { attempt: i + 1, backoff, error: e?.message });
      await sleep(backoff);
    }
  }
  throw lastErr;
}

/*───────────────────────────────────────────────────────────────────────────*
  Database helpers
*───────────────────────────────────────────────────────────────────────────*/
async function getClient(env: Env): Promise<PoolClient> {
  const cs = env.DATABASE_URL;
  if (!cs?.startsWith("postgresql")) {
    throw new Error("Missing or invalid DATABASE_URL secret (wrangler secret put DATABASE_URL).");
  }

  // Reuse a single Pool across requests (Workers isolates keep module scope warm)
  if (!_pool) {
    _pool = new Pool({ connectionString: cs });
  }

  // Acquire a client with retries
  const client = await withRetry(() => _pool!.connect(), 5, 400);

  // Quick ping with retries (helps when Neon just woke up or paused)
  try {
    await withRetry(() => client.query("select 1"), 3, 300);
    return client;
  } catch (e) {
    await client.release();
    throw e;
  }
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
  Shopify fetch with retry (you already had this—kept as-is)
*───────────────────────────────────────────────────────────────────────────*/
async function fetchShopifyWithRetry(url: string, headers: Record<string,string>, maxRetries = 4) {
  let attempt = 0;
  let lastErr: any = null;

  while (attempt <= maxRetries) {
    attempt++;
    const resp = await fetch(url, { headers });

    // success
    if (resp.ok) return resp;

    // retryable? (429 or 5xx)
    if (resp.status === 429 || (resp.status >= 500 && resp.status <= 599)) {
      const retryAfter = Number(resp.headers.get("retry-after") || "0");
      const backoff = retryAfter > 0
        ? Math.min(retryAfter * 1000, 10_000)
        : Math.min(500 * Math.pow(2, attempt - 1), 4000);
      const body = await resp.text().catch(() => "");
      log("shopify:retry", { attempt, status: resp.status, backoff, body: body?.slice(0, 200) });
      await sleep(backoff);
      continue;
    }

    // non‑retryable -> throw with body for diagnostics
    const text = await resp.text().catch(() => "");
    lastErr = new Error(`Shopify ${resp.status}: ${text}`);
    break;
  }

  if (lastErr) throw lastErr;
  throw new Error("Shopify fetch failed after retries");
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
// Schedule Helpers
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
    // CORS/preflight first
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: { ...CORS_HEADERS } });
    }

    try {
      const url = new URL(req.url);

      // Always-safe JSON ping
      if (url.pathname === "/api/debug/ping" && req.method === "GET") {
        return jsonResponse({ ok: true, now: new Date().toISOString() });
      }

      // DEBUG: manual alert test
      if (url.pathname === "/api/debug/alert-test" && req.method === "POST") {
        try {
          await sendAlertEmail(
            env,
            "Shopify Ingest Health ✔ (manual)",
            `<p>Test email from Worker.</p><p>UTC: ${new Date().toISOString()}</p>`
          );
          return jsonResponse({ ok: true, sent: true });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message || String(e) }, 500);
        }
      }

      // DEBUG: staging insert self-test
      if (url.pathname === "/api/debug/staging-insert-test" && req.method === "POST") {
        const client = await getClient(env);
        try {
          const stagingCols = await getStagingColumns(client);
          const wantCols = ["payload", "channel_id", "source", "kind", "domain"].filter(c => stagingCols.has(c));
          const channelId = await getOrCreateShopifyChannelId(client);
          const dummy = { hello: "world", ts: new Date().toISOString() };

          const cast = (c: string) =>
            c === "payload"    ? "::jsonb" :
            c === "channel_id" ? "::int"   :
            "::text";

          const vals = wantCols.map(c =>
            c === "payload" ? JSON.stringify(dummy) :
            c === "channel_id" ? channelId :
            c === "source" ? "debug" :
            c === "kind" ? "test" :
            c === "domain" ? "debug.test" : null
          );

          const placeholders = "(" + vals.map((_, i) => `$${i+1}${cast(wantCols[i])}`).join(",") + ")";
          const sql = `INSERT INTO staging_raw (${wantCols.join(",")}) VALUES ${placeholders}`;
          await client.query(sql, vals);

          return jsonResponse({ ok: true, wantCols });
        } catch (e:any) {
          return jsonResponse({ ok:false, error: e?.message || String(e) }, 500);
        } finally {
          await client.release();
        }
      }

      // 1) Shops (from secret)
      if (url.pathname === "/api/shops" && req.method === "GET") {
        const stores = parseStores(env.SHOPIFY_STORES);
        const shops = stores.map((s, i) => ({
          id: i + 1,
          handle: s.domain.split(".")[0],
          domain: s.domain,
        }));
        return shops.length
          ? jsonResponse({ ok: true, shops })
          : jsonResponse({ ok: false, error: "No stores configured" });
      }

      // 2) Debug: store parsing preview
      if (url.pathname === "/api/debug/stores" && req.method === "GET") {
        const stores = parseStores(env.SHOPIFY_STORES);
        const preview = stores.map((s) => {
          const d = sanitizeDomain(s.domain);
          return { raw: s.domain, sanitized: d, valid: isValidDomain(d) };
        });
        return jsonResponse({ ok: true, preview });
      }

      // 3) Debug: DB ping
      if (url.pathname === "/api/debug/db" && req.method === "GET") {
        try {
          const client = await getClient(env);
          const r = await client.query("select now()");
          await client.release();
          return jsonResponse({ ok: true, now: r?.rows?.[0]?.now ?? null });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
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
          return jsonResponse({ ok: true, cursors: out });
        } finally {
          await client.release();
        }
      }

      // 5) Debug: reset a cursor (?store=domain)
      if (url.pathname === "/api/debug/reset" && req.method === "POST") {
        const store = sanitizeDomain(url.searchParams.get("store") || "");
        if (!store) return jsonResponse({ ok: false, error: "store param required" }, 400);
        const client = await getClient(env);
        try {
          const channelId = await getOrCreateShopifyChannelId(client);
          await setCursor(client, channelId, store, null);
          log("cursor:cleared", store);
          return jsonResponse({ ok: true, cleared: store });
        } finally {
          await client.release();
        }
      }

// … keep everything above unchanged …

// 6) Debug: health snapshot (safe-full always JSON)
// GET /api/debug/health[?light=true]
if (url.pathname === "/api/debug/health" && req.method === "GET") {
  const stores = parseStores(env.SHOPIFY_STORES);
  const light = (url.searchParams.get("light") || "").toLowerCase() === "true";

  let client: PoolClient | null = null;
  try {
    client = await getClient(env);
    const channelId = await getOrCreateShopifyChannelId(client);
    const mtNow = new Date().toLocaleString("en-US", { timeZone: "America/Denver", hour12: false });
    const scheduleIndex = await getScheduleIndex(client, channelId);

    // cursors only (cheap)
    const cursors: Record<string, string | null> = {};
    for (const s of stores) cursors[s.domain] = await getCursor(client, channelId, s.domain);

    if (light) {
      return jsonResponse({
        ok: true,
        now_mt: mtNow,
        schedule_index: scheduleIndex,
        stores: stores.map(s => s.domain),
        cursors,
        light: true
      });
    }

    // full path: totals + per_store summaries (but always safe JSON)
    let orders = 0, items = 0, last: string | null = null, per_store: any[] = [];
    try {
      orders = (await client.query(`select count(*)::int as n from orders`)).rows[0].n;
      items  = (await client.query(`select count(*)::int as n from order_items`)).rows[0].n;
      last   = (await client.query(`select max(placed_at) as t from orders`)).rows[0].t;

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

      per_store = rsOrders.rows.map((r: any) => ({
        shop_domain: r.shop_domain,
        orders: r.orders,
        items: itemsByStore[r.shop_domain] ?? 0,
        first_order_at: r.first_order_at,
        last_order_at: r.last_order_at,
      }));
    } catch (innerErr: any) {
      // don’t throw → respond with partial info
      log("health:inner_error", innerErr?.message || String(innerErr));
    }

    return jsonResponse({
      ok: true,
      now_mt: mtNow,
      schedule_index: scheduleIndex,
      stores: stores.map(s => s.domain),
      cursors,
      totals: { orders, items, last_order_at: last },
      per_store,
      light: false
    });
  } catch (e: any) {
    return jsonResponse({
      ok: false,
      error: e?.message ?? String(e),
      now_mt: new Date().toISOString(),
      light,
      stores: stores.map(s => s.domain),
    }, 500);
  } finally {
    if (client) await client.release();
  }
}

      // 7) Ingest trigger
      if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
        try {
          const res = await runShopifyIngest(env, url.searchParams);
          return jsonResponse({ ok: true, ...res });
        } catch (e: any) {
          log("ingest:error", e?.message ?? String(e));
          await alertOnError(env, "Manual ingest", e);
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
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
          return jsonResponse({ ok: true, rows });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
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
          return jsonResponse({ ok: true, rows });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
        } finally {
          await client.release();
        }
      }

      // === KPIs: rolling 7/30 ===
      if (url.pathname === "/api/kpis/rolling" && req.method === "GET") {
        const client = await getClient(env);
        try {
          const rows = await queryRows(client, `SELECT * FROM v_kpis_rolling_7_30 ORDER BY shop_domain`);
          return jsonResponse({ ok: true, rows });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
        } finally {
          await client.release();
        }
      }

      // === KPIs: store summary ===
      if (url.pathname === "/api/kpis/summary" && req.method === "GET") {
        const client = await getClient(env);
        try {
          const rows = await queryRows(client, `SELECT * FROM v_store_summary_yday_vs_prev7 ORDER BY shop_domain`);
          return jsonResponse({ ok: true, rows });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message ?? String(e) }, 500);
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
          return jsonResponse({ ok: true, rows });
        } catch (e:any) {
          return jsonResponse({ ok:false, error:e?.message || String(e) }, 500);
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
          return jsonResponse({ ok: true, rows });
        } catch (e:any) {
          return jsonResponse({ ok:false, error:e?.message || String(e) }, 500);
        } finally {
          await client.release();
        }
      }

      // === KPIs: Repeat Rates ===
      if (url.pathname === "/api/kpis/repeat-rates" && req.method === "GET") {
        const client = await getClient(env);
        try {
          const rows = await queryRows(client, `SELECT * FROM v_customer_repeat_rates ORDER BY shop_domain`);
          return jsonResponse({ ok:true, rows });
        } catch (e:any) {
          return jsonResponse({ ok:false, error:e?.message || String(e) }, 500);
        } finally {
          await client.release();
        }
      }

      // === ADMIN: Backfill historical Shopify data ===
      if (url.pathname === "/api/admin/backfill" && req.method === "POST") {
        try {
          if (env.BACKFILL_TOKEN) {
            const token = url.searchParams.get("token") || "";
            if (token !== env.BACKFILL_TOKEN) {
              return jsonResponse({ ok: false, error: "unauthorized" }, 401);
            }
          }

          const days = Math.min(Math.max(Number(url.searchParams.get("days") || 365), 1), 365);
          const store = sanitizeDomain(url.searchParams.get("store") || "");
          const hardReset = (url.searchParams.get("hard_reset") || "").toLowerCase() === "true";

          const res = await backfillShopify(env, { days, store, hardReset });
          return jsonResponse({ ok: true, ...res });
        } catch (e:any) {
          log("backfill:error", e?.message || String(e));
          await alertOnError(env, "Admin backfill", e);
          return jsonResponse({ ok:false, error:e?.message || String(e) }, 500);
        }
      }

      // default fallback
      return jsonResponse({ ok: true, status: "OK" });
    } catch (e: any) {
      // Global guard: ALWAYS return JSON on unexpected errors
      const msg = e?.message || String(e);
      try { await alertOnError(env, "Unhandled fetch error", e); } catch {}
      return jsonResponse({ ok: false, error: msg }, 500);
    }
  },

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    log("cron:start");
    ctx.waitUntil(
      runShopifyIngestRoundRobin(env)
        .then((res) => log("cron:done", res))
        .catch(async (e) => {
          log("cron:error", e?.message || String(e));
          await alertOnError(env, "Cron ingest", e);
        })
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

  // Split multi-statement DDL into separate calls (avoids driver/proxy issues)
  try {
    await client.query("CREATE EXTENSION IF NOT EXISTS pgcrypto");
  } catch {
    // some managed Postgres restrict CREATE EXTENSION; ignore if it fails
  }

  await client.query(`
    CREATE TABLE IF NOT EXISTS staging_raw (
      id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
      payload JSONB NOT NULL,
      received_at TIMESTAMPTZ DEFAULT now(),
      domain text,
      channel_id int,
      source text,
      kind text
    )
  `);

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
        const resp = await fetchShopifyWithRetry(
          urlStr,
          {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
          },
          4 // retries
        );

        if (!resp.ok) {
          const t = await resp.text();
          throw new Error(`Shopify ${domain} ${resp.status}: ${t}`);
        }

        const data: any = await parseJsonSafe(resp);
        const orders = Array.isArray(data.orders) ? data.orders : [];

        if (orders.length) {
          // ►► SAFE, FIXED-ORDER, CASTED INSERT ◄◄
          const wantCols = ["payload", "channel_id", "source", "kind", "domain"].filter(c => stagingCols.has(c));

          let values: any[] = [];
          let placeholders: string[] = [];
          let paramIdx = 1;

          for (const o of orders) {
            const row: any[] = [];
            for (const c of wantCols) {
              if (c === "payload")        row.push(JSON.stringify(o));
              else if (c === "channel_id") row.push(channelId);
              else if (c === "source")     row.push("shopify");
              else if (c === "kind")       row.push("order");
              else if (c === "domain")     row.push(domain);
            }
            placeholders.push("(" + row.map(() => `$${paramIdx++}`).join(",") + ")");
            values.push(...row);
          }

          const cast = (c: string) =>
            c === "payload"    ? "::jsonb" :
            c === "channel_id" ? "::int"   :
            "::text";

          const castedGroups = placeholders.map(group => {
            const idxs = group.slice(1, -1).split(",");
            return "(" + idxs.map((idx, i) => `${idx}${cast(wantCols[i])}`).join(",") + ")";
          });

          if (values.length) {
            const sql = `INSERT INTO staging_raw (${wantCols.join(",")}) VALUES ${castedGroups.join(",")}`;
            await client.query(sql, values);
          }
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