import { Pool } from "@neondatabase/serverless";

type Env = {
  DATABASE_URL: string;
  SHOPIFY_STORES: string; // JSON array: [{ domain, token }]
  PAGE_SIZE?: string;
  MAX_PAGES_PER_RUN?: string;
};

const SHOPIFY_API_VERSION = "2024-10";

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    try {
      if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
        const result = await runAllStores(env);
        return json({ ok: true, ...result });
      }

      if (url.pathname === "/api/metrics/aov" && req.method === "GET") {
        const params = Object.fromEntries(url.searchParams.entries());
        const data = await getAOV(env, params.range ?? "30d", params.shop ?? "all");
        return json({ ok: true, range: params.range ?? "30d", shop: params.shop ?? "all", ...data });
      }

      if (url.pathname === "/api/metrics/orders_trend" && req.method === "GET") {
        const params = Object.fromEntries(url.searchParams.entries());
        const data = await getOrdersTrend(env, params.range ?? "30d", params.shop ?? "all");
        return json({ ok: true, ...data });
      }

      if (url.pathname === "/api/metrics/returning_rate" && req.method === "GET") {
        const params = Object.fromEntries(url.searchParams.entries());
        const data = await getReturningRate(env, params.range ?? "90d", params.shop ?? "all");
        return json({ ok: true, ...data });
      }

      return new Response("OK", { status: 200 });
    } catch (e: any) {
      return json({ ok: false, error: e?.message ?? String(e) }, 500);
    }
  },
};

async function runAllStores(env: Env) {
  const stores: Array<{ domain: string; token: string }> = JSON.parse(await getSecret(env, "SHOPIFY_STORES"));
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const client = await pool.connect();
  const results: any[] = [];
  try {
    await client.query("BEGIN");

    const channel = await one(client, `SELECT id FROM channels WHERE name = 'shopify'`);
    if (!channel) throw new Error("Channel 'shopify' missing. Run db/schema.sql first.");
    const channelId = channel.id as number;

    for (const store of stores) {
      const shopId = await ensureShop(client, channelId, store);
      const r = await ingestOneStore(env, client, channelId, shopId, store);
      results.push({ domain: store.domain, ...r });
    }

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    await client.release();
  }

  return { results };
}

async function ingestOneStore(env: Env, client: any, channelId: number, shopId: number, store: { domain: string; token: string }) {
  const PAGE_SIZE = Number(env.PAGE_SIZE ?? 100);
  const MAX_PAGES = Number(env.MAX_PAGES_PER_RUN ?? 10);
  let updatedAtMin = await getState(client, channelId, `orders_updated_at::${store.domain}`);
  let cursor: string | null = null;
  let pages = 0, totalOrders = 0;
  let res: any = null;

  do {
    res = await shopifyQuery(store, {
      pageSize: PAGE_SIZE,
      cursor,
      updatedAtMin: updatedAtMin ?? null,
    });

    const edges = res?.data?.orders?.edges ?? [];
    if (!edges.length) break;

    for (const edge of edges) {
      cursor = edge.cursor;
      const o = edge.node;

      await client.query(
        `INSERT INTO staging_raw (channel_id, shop_id, external_id, payload) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
        [channelId, shopId, o.id, o]
      );

      let customerId: string | null = null;
      if (o.customer) {
        const c = o.customer;
        const row = await one(
          client,
          `INSERT INTO customers (channel_id, shop_id, channel_customer_id, email, first_name, last_name, first_seen, last_seen)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (channel_id, shop_id, channel_customer_id)
           DO UPDATE SET email = EXCLUDED.email, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, last_seen = GREATEST(customers.last_seen, EXCLUDED.last_seen)
           RETURNING id`,
          [channelId, shopId, c.id, c.email, c.firstName, c.lastName, c.createdAt ?? o.processedAt, o.updatedAt]
        );
        customerId = row.id;
      }

      const subtotal = num(o.subtotalPriceSet?.shopMoney?.amount);
      const shipping = num(o.totalShippingPriceSet?.shopMoney?.amount);
      const tax = num(o.totalTaxSet?.shopMoney?.amount);
      const discounts = num(o.totalDiscountsSet?.shopMoney?.amount);
      const total = num(o.totalPriceSet?.shopMoney?.amount);

      const ord = await one(
        client,
        `INSERT INTO orders (channel_id, shop_id, external_id, order_number, name, placed_at, currency, subtotal, shipping, tax, discounts, fees, total, financial_status, fulfillment_status, customer_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
         ON CONFLICT (channel_id, shop_id, external_id)
         DO UPDATE SET order_number=EXCLUDED.order_number, name=EXCLUDED.name, placed_at=EXCLUDED.placed_at, currency=EXCLUDED.currency,
           subtotal=EXCLUDED.subtotal, shipping=EXCLUDED.shipping, tax=EXCLUDED.tax, discounts=EXCLUDED.discounts, total=EXCLUDED.total,
           financial_status=EXCLUDED.financial_status, fulfillment_status=EXCLUDED.fulfillment_status, customer_id=COALESCE(EXCLUDED.customer_id, orders.customer_id)
         RETURNING id`,
        [channelId, shopId, o.id, String(o.orderNumber ?? ""), o.name, o.processedAt, o.currencyCode, subtotal, shipping, tax, discounts, 0, total, o.displayFinancialStatus, o.displayFulfillmentStatus, customerId]
      );

      await client.query(`DELETE FROM order_items WHERE order_id = $1`, [ord.id]);
      for (const e of (o.lineItems?.edges ?? [])) {
        const it = e.node;
        await client.query(
          `INSERT INTO order_items (order_id, sku, external_product_id, title, qty, unit_price, discount, tax, fees, landed_cost_alloc)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [
            ord.id,
            it.sku,
            it.variant?.id ?? null,
            it.name,
            it.quantity,
            divide(num(it.originalTotalSet?.shopMoney?.amount), it.quantity),
            diff(num(it.originalTotalSet?.shopMoney?.amount), num(it.discountedTotalSet?.shopMoney?.amount)),
            num(it.taxLines?.[0]?.priceSet?.shopMoney?.amount),
            0,
            null,
          ]
        );
      }

      totalOrders++;
      updatedAtMin = o.updatedAt; // advance cursor
    }

    pages++;
  } while (pages < MAX_PAGES && hasNext(res));

  await setState(client, channelId, `orders_updated_at::${store.domain}`, updatedAtMin ?? new Date().toISOString());

  return { pages, totalOrders, nextUpdatedAtMin: updatedAtMin };
}

async function getAOV(env: Env, range: string, shop: string) {
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const client = await pool.connect();
  try {
    const days = parseRangeDays(range);
    const params: any[] = [days];
    let where = `placed_at >= now() - ($1 || ' days')::interval`;
    if (shop !== "all") {
      where += ` AND shop_id = $2`;
      params.push(Number(shop));
    }
    const row = await one(
      client,
      `SELECT COALESCE(SUM(total),0) AS revenue, COALESCE(COUNT(DISTINCT id),0) AS orders FROM orders WHERE ${where}`,
      params
    );
    const aov = Number(row.orders) ? Number(row.revenue) / Number(row.orders) : 0;
    return { revenue: Number(row.revenue), orders: Number(row.orders), aov };
  } finally {
    await client.release();
  }
}

async function getOrdersTrend(env: Env, range: string, shop: string) {
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const client = await pool.connect();
  try {
    const days = parseRangeDays(range);
    const params: any[] = [days];
    let where = `placed_at >= now() - ($1 || ' days')::interval`;
    if (shop !== "all") { where += ` AND shop_id = $2`; params.push(Number(shop)); }
    const rows = (await client.query(
      `SELECT date_trunc('day', placed_at) AS d, COUNT(DISTINCT id) AS orders, SUM(total) AS revenue
       FROM orders WHERE ${where}
       GROUP BY 1 ORDER BY 1`, params
    )).rows;
    return { range, shop, points: rows.map((r:any) => ({ date: r.d, orders: Number(r.orders), revenue: Number(r.revenue) })) };
  } finally { await client.release(); }
}

async function getReturningRate(env: Env, range: string, shop: string) {
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const client = await pool.connect();
  try {
    const days = parseRangeDays(range);
    const params: any[] = [days];
    let where = `o.placed_at >= now() - ($1 || ' days')::interval`;
    if (shop !== "all") { where += ` AND o.shop_id = $2`; params.push(Number(shop)); }
    const sql = `
      WITH base AS (
        SELECT o.id, o.customer_id
        FROM orders o
        WHERE ${where}
      ), counts AS (
        SELECT c.id AS customer_id, COUNT(o.id) AS orders_in_window
        FROM customers c
        JOIN base o ON o.customer_id = c.id
        GROUP BY 1
      )
      SELECT
        COALESCE(SUM(CASE WHEN orders_in_window > 1 THEN 1 ELSE 0 END),0) AS returning_customers,
        COALESCE(COUNT(*),0) AS unique_customers
      FROM counts`;
    const row = (await client.query(sql, params)).rows[0] || { returning_customers: 0, unique_customers: 0 };
    const returning = Number(row.returning_customers);
    const unique = Number(row.unique_customers);
    const rate = unique ? returning / unique : 0;
    return { range, shop, returning_customers: returning, unique_customers: unique, returning_rate: rate };
  } finally { await client.release(); }
}

function parseRangeDays(r: string): number {
  const m = String(r).match(/(\d+)d/);
  return m ? Number(m[1]) : 30;
}

function hasNext(res: any): boolean {
  return Boolean(res?.data?.orders?.pageInfo?.hasNextPage);
}

async function ensureShop(client: any, channelId: number, store: { domain: string }) {
  const handle = store.domain.split(".")[0];
  const row = await one(
    client,
    `INSERT INTO shops (channel_id, handle, domain) VALUES ($1,$2,$3)
     ON CONFLICT (channel_id, domain) DO UPDATE SET handle = EXCLUDED.handle
     RETURNING id`,
    [channelId, handle, store.domain]
  );
  return row.id as number;
}

async function getSecret(env: any, key: string) {
  const v = (env as any)[key];
  if (!v) throw new Error(`Missing secret: ${key}`);
  return v;
}

async function getState(client: any, channelId: number, key: string) {
  const r = await one(client, `SELECT value FROM sync_state WHERE channel_id=$1 AND key=$2`, [channelId, key]);
  return r?.value ?? null;
}

async function setState(client: any, channelId: number, key: string, value: string) {
  await one(
    client,
    `INSERT INTO sync_state (channel_id, key, value) VALUES ($1,$2,$3)
     ON CONFLICT (channel_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
     RETURNING key`,
    [channelId, key, value]
  );
}

async function one(client: any, sql: string, params?: any[]) {
  const res = await client.query(sql, params ?? []);
  return res.rows?.[0] ?? null;
}

function num(v: any): number { const n = Number(v); return Number.isFinite(n) ? n : 0; }
function divide(a: number, b: number): number { return b ? Math.round((a / b) * 100) / 100 : 0; }
function diff(a: number, b: number): number { return Math.round((a - b) * 100) / 100; }

async function shopifyQuery(store: { domain: string; token: string }, vars: any) {
  const endpoint = `https://${store.domain}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
  const body = JSON.stringify({ query: SHOPIFY_QUERY, variables: { pageSize: vars.pageSize, cursor: vars.cursor, updatedAtMin: vars.updatedAtMin ?? null } });
  const res = await fetch(endpoint, { method: "POST", headers: { "X-Shopify-Access-Token": store.token, "Content-Type": "application/json" }, body });
  if (!res.ok) throw new Error(`Shopify ${store.domain} HTTP ${res.status}`);
  return await res.json();
}

const SHOPIFY_QUERY = `
query OrdersSince($pageSize: Int!, $cursor: String, $updatedAtMin: DateTime) {
  orders(first: $pageSize, after: $cursor, query: $updatedAtMin ? "updated_at:>=$updatedAtMin" : null, sortKey: UPDATED_AT) {
    edges {
      cursor
      node {
        id
        name
        orderNumber
        processedAt
        updatedAt
        currencyCode
        displayFinancialStatus
        displayFulfillmentStatus
        subtotalPriceSet { shopMoney { amount } }
        totalShippingPriceSet { shopMoney { amount } }
        totalTaxSet { shopMoney { amount } }
        totalDiscountsSet { shopMoney { amount } }
        totalPriceSet { shopMoney { amount } }
        customer { id email firstName lastName createdAt updatedAt }
        lineItems(first: 250) {
          edges {
            node {
              sku
              name
              quantity
              discountedTotalSet { shopMoney { amount } }
              originalTotalSet { shopMoney { amount } }
              taxLines { priceSet { shopMoney { amount } } }
              variant { id }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
`;

function json(data: any, status = 200) { return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } }); }
