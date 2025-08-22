// server/src/worker.ts
import { Pool } from "@neondatabase/serverless";

/*───────────────────────────────────────────────────────────────────────────*
  Types
*───────────────────────────────────────────────────────────────────────────*/
type Env = {
  DATABASE_URL: string;
  SHOPIFY_STORES?: string;      // JSON array: [{domain, token}, ...]
  PAGE_SIZE?: string;           // default "100" (max 250)
  MAX_PAGES_PER_RUN?: string;   // default "10"
};

/*───────────────────────────────────────────────────────────────────────────*
  Helpers (time, http, store parsing)
*───────────────────────────────────────────────────────────────────────────*/
function log(...args: any[]) {
  // Mountain Time (Denver) for wrangler tail
  const ts = new Date().toLocaleString("en-US", {
    timeZone: "America/Denver",
    hour12: false,
  });
  console.log(ts, ...args);
}

function json(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    },
  });
}

function sanitizeDomain(d: string) {
  if (!d) return "";
  d = d.trim().replace(/^https?:\/\//i, "");
  d = d.replace(/\/.*/, ""); // strip path/query
  d = d.replace(/^\"+|\"+$/g, "").replace(/^'+|'+$/g, ""); // strip quotes
  return d;
}

function isValidDomain(d: string) {
  return /^[a-z0-9.-]+$/i.test(d) && d.includes(".");
}

function parseStores(raw?: string) {
  if (!raw) return [] as Array<{ domain: string; token: string }>;
  try {
    const v = JSON.parse(raw);
    const arr = Array.isArray(v) ? v : (typeof v === "string" ? JSON.parse(v) : []);
    return arr.map((s: any) => ({
      domain: sanitizeDomain(String(s.domain || "")),
      token: String(s.token || ""),
    }));
  } catch {
    return [];
  }
}

/*───────────────────────────────────────────────────────────────────────────*
  Database helpers
*───────────────────────────────────────────────────────────────────────────*/
async function getClient(env: Env) {
  if (!env.DATABASE_URL || !env.DATABASE_URL.startsWith("postgresql")) {
    throw new Error("Missing or invalid DATABASE_URL secret (wrangler secret put DATABASE_URL).");
  }
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  return await pool.connect();
}

async function ensureSyncState(client: any) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS sync_state (
      key text PRIMARY KEY,
      value jsonb,
      channel_id int,
      updated_at timestamptz DEFAULT now()
    )
  `);
}

async function getStagingColumns(client: any): Promise<Set<string>> {
  const r = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='staging_raw'
  `);
  return new Set(r.rows.map((x: any) => x.column_name));
}

async function getOrCreateShopifyChannelId(client: any): Promise<number> {
  try {
    await client.query(\`INSERT INTO channels(name) VALUES ('Shopify') ON CONFLICT (name) DO NOTHING\`);
  } catch {}
  const r = await client.query(\`SELECT id FROM channels WHERE name='Shopify' ORDER BY id LIMIT 1\`);
  if (!r.rows?.[0]?.id) {
    const ins = await client.query(\`INSERT INTO channels(name) VALUES ('Shopify') RETURNING id\`);
    return Number(ins.rows[0].id);
  }
  return Number(r.rows[0].id);
}

/*───────────────────────────────────────────────────────────────────────────*
  Cursor + schedule index (sync_state)
*───────────────────────────────────────────────────────────────────────────*/
function cursorKey(domain: string) {
  return \`shopify:cursor:\${domain}\`;
}
async function getCursor(client: any, domain: string): Promise<string | null> {
  await ensureSyncState(client);
  const r = await client.query(\`SELECT value FROM sync_state WHERE key=$1 LIMIT 1\`, [cursorKey(domain)]);
  return r.rows?.[0]?.value?.page_info ?? null;
}
async function setCursor(client: any, domain: string, pageInfo: string | null) {
  await ensureSyncState(client);
  if (!pageInfo) {
    await client.query(\`DELETE FROM sync_state WHERE key=$1\`, [cursorKey(domain)]);
    return;
  }
  await client.query(
    \`INSERT INTO sync_state(key, value, updated_at)
     VALUES ($1, jsonb_build_object('page_info', $2::text), now())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value, updated_at = now()\`,
    [cursorKey(domain), pageInfo]
  );
}

async function getScheduleIndex(client: any): Promise<number> {
  await ensureSyncState(client);
  const r = await client.query(\`SELECT value FROM sync_state WHERE key='shopify:schedule_idx' LIMIT 1\`);
  const v = r.rows?.[0]?.value;
  return v && typeof v.idx !== "undefined" ? Number(v.idx) || 0 : 0;
}
async function setScheduleIndex(client: any, idx: number) {
  await ensureSyncState(client);
  await client.query(
    \`INSERT INTO sync_state(key, value, updated_at)
     VALUES ('shopify:schedule_idx', jsonb_build_object('idx', $1::int), now())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value, updated_at = now()\`,
    [idx]
  );
}

/*───────────────────────────────────────────────────────────────────────────*
  Router: fetch + debug endpoints
*───────────────────────────────────────────────────────────────────────────*/
export default {
  async fetch(req: Request, env: Env) {
    if (req.method === "OPTIONS") return json({ ok: true });
    const url = new URL(req.url);

    // List configured shops (from secret)
    if (url.pathname === "/api/shops" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      const shops = stores.map((s, i) => ({
        id: i + 1,
        handle: s.domain.split(".")[0],
        domain: s.domain,
      }));
      return shops.length ? json({ ok: true, shops }) : json({ ok: false, error: "No stores configured" });
    }

    // Preview raw vs sanitized domains
    if (url.pathname === "/api/debug/stores" && req.method === "GET") {
      const stores = parseStores(env.SHOPIFY_STORES);
      const preview = stores.map((s) => ({
        raw: s.domain,
        sanitized: sanitizeDomain(s.domain),
        valid: isValidDomain(sanitizeDomain(s.domain)),
      }));
      return json({ ok: true, preview });
    }

    // DB ping
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

    // Inspect cursors
    if (url.pathname === "/api/debug/cursor" && req.method === "GET") {
      const client = await getClient(env);
      try {
        const out: Record<string, string | null> = {};
        for (const s of parseStores(env.SHOPIFY_STORES)) out[s.domain] = await getCursor(client, s.domain);
        return json({ ok: true, cursors: out });
      } finally {
        await client.release();
      }
    }
    // Clear a cursor
    if (url.pathname === "/api/debug/reset" && req.method === "POST") {
      const store = sanitizeDomain(url.searchParams.get("store") || "");
      if (!store) return json({ ok: false, error: "store param required" }, 400);
      const client = await getClient(env);
      try {
        await setCursor(client, store, null);
        log("cursor:cleared", store);
        return json({ ok: true, cleared: store });
      } finally {
        await client.release();
      }
    }

    // /api/debug/health — snapshot with per-store totals
    if (url.pathname === "/api/debug/health" && req.method === "GET") {
      try {
        const stores = parseStores(env.SHOPIFY_STORES);
        const client = await getClient(env);
        try {
          const mtNow = new Date().toLocaleString("en-US", { timeZone: "America/Denver", hour12: false });
          const idx = await getScheduleIndex(client);

          const cursors: Record<string, string | null> = {};
          for (const s of stores) cursors[s.domain] = await getCursor(client, s.domain);

          // global totals
          const r1 = await client.query(\`select count(*)::int as orders from orders\`);
          const r2 = await client.query(\`select count(*)::int as items from order_items\`);
          const r3 = await client.query(\`select max(placed_at) as last_order_at from orders\`);

          // per-store totals (orders)
          const rsOrders = await client.query(\`
            select coalesce(shop_domain,'(unknown)') as shop_domain,
                   count(*)::int as orders,
                   min(placed_at) as first,
                   max(placed_at) as last
            from orders
            group by 1
            order by 2 desc
          \`);

          // per-store totals (items)
          const rsItems = await client.query(\`
            select coalesce(o.shop_domain,'(unknown)') as shop_domain,
                   count(*)::int as items
            from order_items oi
            join orders o on o.id = oi.order_id
            group by 1
            order by 2 desc
          \`);

          const itemsByStore: Record<string, number> = {};
          for (const row of rsItems.rows) itemsByStore[row.shop_domain] = row.items;

          const per_store = rsOrders.rows.map((row: any) => ({
            shop_domain: row.shop_domain,
            orders: row.orders,
            items: itemsByStore[row.shop_domain] ?? 0,
            first_order_at: row.first,
            last_order_at: row.last
          }));

          return json({
            ok: true,
            now_mt: mtNow,
            schedule_index: idx,
            stores: stores.map(s => s.domain),
            cursors,
            totals: {
              orders: r1.rows?.[0]?.orders ?? 0,
              items:  r2.rows?.[0]?.items ?? 0,
              last_order_at: r3.rows?.[0]?.last_order_at ?? null
            },
            per_store
          });
        } finally {
          await client.release();
        }
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    // Ingest trigger
    if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
      try {
        const res = await runShopifyIngest(env, url.searchParams);
        return json({ ok: true, ...res });
      } catch (e: any) {
        log("ingest:error", e?.message ?? String(e));
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    return new Response("OK", { status: 200 });
  },

  // Cron: round‑robin one store/run (with logs)
  async scheduled(_controller: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    log("cron:start");
    ctx.waitUntil(
      runShopifyIngestRoundRobin(env)
        .then((res) => log("cron:done", res))
        .catch((e) => log("cron:error", e?.message || String(e)))
    );
  },
};

/*───────────────────────────────────────────────────────────────────────────*
  Round‑robin cron
*───────────────────────────────────────────────────────────────────────────*/
async function runShopifyIngestRoundRobin(env: Env) {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);
  try {
    const idx = await getScheduleIndex(client);
    const nextIdx = idx % stores.length;
    const store = stores[nextIdx];
    log("cron:store", store.domain, { nextIdx, totalStores: stores.length });

    const result = await runShopifyIngest(env, new URLSearchParams({ store: store.domain }));
    await setScheduleIndex(client, nextIdx + 1);
    return { store: store.domain, result };
  } finally {
    await client.release();
  }
}

/*───────────────────────────────────────────────────────────────────────────*
  Ingest: supports ?store=domain, ?days=N, ?reset=true
*───────────────────────────────────────────────────────────────────────────*/
async function runShopifyIngest(env: Env, params: URLSearchParams) {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);
  const limit = Math.min(Math.max(Number(env.PAGE_SIZE || 100), 1), 250);
  const maxPages = Math.min(Math.max(Number(env.MAX_PAGES_PER_RUN || 10), 1), 50);
  const days = Math.min(Math.max(Number(params.get("days") || 90), 1), 365);
  const reset = (params.get("reset") || "").toLowerCase() === "true";

  const stagingCols = await getStagingColumns(client);
  const channelId = await getOrCreateShopifyChannelId(client);

  const targetDomainParam = params.get("store") || params.get("domain");
  const target = targetDomainParam ? sanitizeDomain(String(targetDomainParam)) : null;
  const list = target ? stores.filter((s) => sanitizeDomain(s.domain) === target) : stores;

  const summary: Record<string, { pages: number; ordersIngested: number }> = {};

  try {
    // minimal staging (first run safety)
    await client.query(
      "CREATE EXTENSION IF NOT EXISTS pgcrypto; " +
        "CREATE TABLE IF NOT EXISTS staging_raw (" +
        " id UUID DEFAULT gen_random_uuid() PRIMARY KEY," +
        " payload JSONB NOT NULL," +
        " received_at TIMESTAMPTZ DEFAULT now()," +
        " domain text" +
        ");"
    );

    for (const s of list) {
      const domain = sanitizeDomain(s.domain);
      if (!isValidDomain(domain)) throw new Error(\`Invalid shop domain: "\${s.domain}" -> "\${domain}"\`);
      const token = s.token;
      if (!/^shpat_/.test(token || "")) throw new Error(\`Missing/invalid Admin API token for \${domain}\`);

      if (reset) await setCursor(client, domain, null);

      let pages = 0;
      let total = 0;
      let nextPage: string | null = await getCursor(client, domain); // resume if present
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

        const urlStr = \`https://\${domain}/admin/api/2024-07/orders.json?\${qp.toString()}\`;
        const resp = await fetch(urlStr, {
          headers: {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
            Accept: "application/json",
          },
        });
        if (!resp.ok) {
          const t = await resp.text();
          throw new Error(\`Shopify \${domain} \${resp.status}: \${t}\`);
        }

        const data: any = await resp.json();
        const orders = Array.isArray(data.orders) ? data.orders : [];

        if (orders.length) {
          // build insert dynamically if optional columns exist
          const cols = ["payload"];
          if (stagingCols.has("channel_id")) cols.push("channel_id");
          if (stagingCols.has("source")) cols.push("source");
          if (stagingCols.has("kind")) cols.push("kind");
          if (stagingCols.has("domain")) cols.push("domain");

          const perRowVals: any[][] = [];
          for (const o of orders) {
            const row: any[] = [JSON.stringify(o)];
            if (stagingCols.has("channel_id")) row.push(channelId);
            if (stagingCols.has("source")) row.push("shopify");
            if (stagingCols.has("kind")) row.push("order");
            if (stagingCols.has("domain")) row.push(domain);
            perRowVals.push(row);
          }

          const placeholders: string[] = [];
          const flat: any[] = [];
          let offset = 0;
          for (const r of perRowVals) {
            placeholders.push("(" + r.map((_, i) => "$" + (offset + i + 1)).join(",") + ")");
            flat.push(...r);
            offset += r.length;
          }

          const sql = \`INSERT INTO staging_raw (\${cols.join(",")}) VALUES \${placeholders.join(",")}\`;
          await client.query(sql, flat);
        }

        total += orders.length;
        pages++;

        // Link header -> page_info
        const link = resp.headers.get("link") || resp.headers.get("Link");
        let newCursor: string | null = null;
        if (link) {
          const m = link.match(/<[^>]*[?&]page_info=([^&>]+)[^>]*>;\s*rel="next"/i);
          if (m) newCursor = m[1];
        }

        await setCursor(client, domain, newCursor);
        log("ingest:page", { domain, page: pages, orders: orders.length, hasNext: !!newCursor });
        nextPage = newCursor;

        if (!nextPage || orders.length === 0) break;
      }

      summary[domain] = { pages, ordersIngested: total };
      log("ingest:done", domain, { pages, total });
    }
  } finally {
    await client.release();
  }

  log("ingest:summary", {
    limit,
    maxPages,
    days,
    stores: list.map((s) => s.domain),
    summary,
  });

  return { summary };
}
