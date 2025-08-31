// server/src/worker.ts
import { Pool, PoolClient } from "@neondatabase/serverless";

/*───────────────────────────────────────────────────────────────────────────*
  Types
*───────────────────────────────────────────────────────────────────────────*/
type Env = {
  DATABASE_URL: string;

  // alerting
  SENDGRID_API_KEY?: string;    // optional: use SendGrid if present
  ALERT_EMAIL_FROM?: string;    // required for alerts (can be your personal email)
  ALERT_EMAIL_TO?: string;      // required for alerts (recipient)
};

/*───────────────────────────────────────────────────────────────────────────*
  Helpers (time, http)
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

/*───────────────────────────────────────────────────────────────────────────*
  Email alerts (MailChannels by default, SendGrid if key present)
*───────────────────────────────────────────────────────────────────────────*/
async function sendAlertEmail(env: Env, subject: string, html: string) {
  const from = (env.ALERT_EMAIL_FROM || "").trim();
  const to   = (env.ALERT_EMAIL_TO   || "").trim();
  const sg   = (env.SENDGRID_API_KEY || "").trim();

  if (!from || !to) {
    log("alert:skipped (ALERT_EMAIL_FROM/TO not set)", { subject });
    return;
  }

  if (sg) {
    const res = await fetch("https://api.sendgrid.com/v3/mail/send", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${sg}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: to }] }],
        from: { email: from, name: "BI Worker" },
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
      from: { email: from, name: "BI Worker" },
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
  if (!cs || !(cs.startsWith("postgresql://") || cs.startsWith("postgres://"))) {
    throw new Error("Missing or invalid DATABASE_URL secret (wrangler secret put DATABASE_URL).");
  }

  if (!_pool) _pool = new Pool({ connectionString: cs });

  const client = await withRetry(() => _pool!.connect(), 5, 400);

  try {
    await withRetry(() => client.query("select 1"), 3, 300);
    return client;
  } catch (e) {
    await client.release();
    throw e;
  }
}

async function getStagingColumns(client: PoolClient): Promise<Set<string>> {
  const r = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='staging_raw'
  `);
  return new Set(r.rows.map((x: any) => x.column_name));
}

async function queryRows<T = any>(client: PoolClient, sql: string, params: any[] = []): Promise<T[]> {
  const r = await client.query(sql, params);
  return r.rows as T[];
}

/** Backwards-compat view for KPI SQL that expects line_total */
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
  HTTP Router (fetch) + Cron (scheduled)
*───────────────────────────────────────────────────────────────────────────*/
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // CORS/preflight
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: { ...CORS_HEADERS } });
    }

    try {
      const url = new URL(req.url);

      // 0) Ping
      if (url.pathname === "/api/debug/ping" && req.method === "GET") {
        return jsonResponse({ ok: true, now: new Date().toISOString() });
      }

      // 1) Manual alert test
      if (url.pathname === "/api/debug/alert-test" && req.method === "POST") {
        try {
          await sendAlertEmail(
            env,
            "BI Worker Health ✔ (manual)",
            `<p>Test email from Worker.</p><p>UTC: ${new Date().toISOString()}</p>`
          );
          return jsonResponse({ ok: true, sent: true });
        } catch (e: any) {
          return jsonResponse({ ok: false, error: e?.message || String(e) }, 500);
        }
      }

      // 2) Staging insert self-test (no Shopify; just writes a dummy row)
      if (url.pathname === "/api/debug/staging-insert-test" && req.method === "POST") {
        const client = await getClient(env);
        try {
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
          const wantCols = ["payload", "source", "kind", "domain"].filter(c => stagingCols.has(c));
          const dummy = { hello: "world", ts: new Date().toISOString() };

          const cast = (c: string) => (c === "payload" ? "::jsonb" : "::text");
          const vals = wantCols.map(c =>
            c === "payload" ? JSON.stringify(dummy) :
            c === "source"  ? "debug" :
            c === "kind"    ? "test" :
            c === "domain"  ? "debug.local" : null
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

      // 3) DB ping
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

      // 4) Health (safe-full JSON, no Shopify)
      //    GET /api/debug/health[?light=true]
      if (url.pathname === "/api/debug/health" && req.method === "GET") {
        const light = (url.searchParams.get("light") || "").toLowerCase() === "true";

        let client: PoolClient | null = null;
        try {
          const mtNow = new Date().toLocaleString("en-US", { timeZone: "America/Denver", hour12: false });

          if (light) {
            return jsonResponse({
              ok: true,
              now_mt: mtNow,
              light: true
            });
          }

          client = await getClient(env);

          // Try to compute totals & per-store if those tables exist; never throw.
          let orders = 0, items = 0, last: string | null = null, per_store: any[] = [];
          try {
            orders = (await client.query(`select count(*)::int as n from orders`)).rows[0].n;
            items  = (await client.query(`select count(*)::int as n from order_items`)).rows[0].n;
            last   = (await client.query(`select max(placed_at) as t from orders`)).rows[0].t ?? null;

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
            log("health:inner_error", innerErr?.message || String(innerErr));
          }

          return jsonResponse({
            ok: true,
            now_mt: mtNow,
            totals: { orders, items, last_order_at: last },
            per_store,
            light: false
          });
        } catch (e: any) {
          return jsonResponse({
            ok: false,
            error: e?.message ?? String(e),
            now_mt: new Date().toISOString(),
            light
          }, 500);
        } finally {
          if (client) await client.release();
        }
      }

      // === KPIs: daily ===
      if (url.pathname === "/api/kpis/daily" && req.method === "GET") {
        const days = Math.min(Math.max(Number(url.searchParams.get("days") || 14), 1), 365);
        const store = (url.searchParams.get("store") || "").trim();
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
        const store = (url.searchParams.get("store") || "").trim();
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
        const store = (url.searchParams.get("store") || "").trim();
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
        const store = (url.searchParams.get("store") || "").trim();
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

      // default fallback
      return jsonResponse({ ok: true, status: "OK" });
    } catch (e: any) {
      // Global guard: ALWAYS return JSON on unexpected errors
      const msg = e?.message || String(e);
      try { await alertOnError(env, "Unhandled fetch error", e); } catch {}
      return jsonResponse({ ok: false, error: msg }, 500);
    }
  },

  // No cron work anymore (Shopify removed) — safe no-op
  async scheduled(_event: ScheduledEvent, _env: Env, _ctx: ExecutionContext): Promise<void> {
    log("cron:noop");
  },
};