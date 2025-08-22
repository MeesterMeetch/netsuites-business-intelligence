// server/src/worker.ts (patched with per-store totals + MT logging)
import { Pool } from "@neondatabase/serverless";

type Env = {
  DATABASE_URL: string;
  SHOPIFY_STORES?: string;
  PAGE_SIZE?: string;
  MAX_PAGES_PER_RUN?: string;
};

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
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
  d = d.replace(/\/.*/, "");
  d = d.replace(/^\"+|\"+$/g, "").replace(/^'+|'+$/g, "");
  return d;
}
function isValidDomain(d: string) {
  return /^[a-z0-9.-]+$/i.test(d) && d.includes(".");
}
function parseStores(val?: string) {
  if (!val) return [];
  try {
    const v = JSON.parse(val);
    const arr = Array.isArray(v) ? v : (typeof v === "string" ? JSON.parse(v) : []);
    return arr.map((s: any) => ({ ...s, domain: sanitizeDomain(String(s.domain || "")) }));
  } catch {
    return [];
  }
}

async function getClient(env: Env) {
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  return await pool.connect();
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "OPTIONS") return json({ ok: true });
    const url = new URL(req.url);

    if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
      try {
        const res = await runShopifyIngest(env, url.searchParams);
        return json({ ok: true, ...res });
      } catch (e: any) {
        return json({ ok: false, error: e?.message ?? String(e) }, 500);
      }
    }

    return new Response("OK", { status: 200 });
  },

  async scheduled(_controller: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(runShopifyIngest(env, new URLSearchParams()).catch(() => {}));
  }
};

async function runShopifyIngest(env: Env, params: URLSearchParams) {
  const stores = parseStores(env.SHOPIFY_STORES);
  if (!stores.length) throw new Error("No stores configured");

  const client = await getClient(env);
  const summary: any = {};
  const storeTotals: any = {};

  try {
    for (const s of stores) {
      const domain = sanitizeDomain(s.domain);
      summary[domain] = { pages: 0, ordersIngested: 0 };
      storeTotals[domain] = { orders: 0, items: 0, last_order_at: null };

      const r1 = await client.query("select count(*)::int as orders, max(placed_at) as last_order_at from orders where domain=$1", [domain]);
      const r2 = await client.query("select count(*)::int as items from order_items where domain=$1", [domain]);
      storeTotals[domain] = {
        orders: r1.rows[0].orders || 0,
        items: r2.rows[0].items || 0,
        last_order_at: r1.rows[0].last_order_at
      };
    }

    const overallOrders = Object.values(storeTotals).reduce((acc: number, st: any) => acc + st.orders, 0);
    const overallItems = Object.values(storeTotals).reduce((acc: number, st: any) => acc + st.items, 0);

    const nowMT = new Date().toLocaleString("en-US", { timeZone: "America/Denver" });
    return { now_mt: nowMT, stores: stores.map((s:any)=>s.domain), storeTotals, totals: { orders: overallOrders, items: overallItems } };
  } finally {
    await client.release();
  }
}
