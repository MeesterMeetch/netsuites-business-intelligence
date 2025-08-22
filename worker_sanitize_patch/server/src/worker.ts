import { Client } from "@neondatabase/serverless";

export interface Env {
  DATABASE_URL: string;
  SHOPIFY_STORES: string;
  PAGE_SIZE: number;
  MAX_PAGES_PER_RUN: number;
}

function sanitizeDomain(raw: string): string {
  return raw
    .replace(/^https?:\/\//, "")
    .replace(/\/$/, "")
    .replace(/"/g, "")
    .trim();
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/api/shops") {
      try {
        const stores = JSON.parse(env.SHOPIFY_STORES || "[]");
        const shops = stores.map((s: any, i: number) => ({
          id: i + 1,
          handle: sanitizeDomain(s.domain.split(".myshopify.com")[0]),
          domain: sanitizeDomain(s.domain),
        }));
        return Response.json({ ok: true, shops });
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message });
      }
    }

    if (url.pathname === "/api/debug/stores") {
      try {
        const stores = JSON.parse(env.SHOPIFY_STORES || "[]");
        const shops = stores.map((s: any) => {
          const raw = s.domain;
          const sanitized = sanitizeDomain(raw);
          const valid = sanitized.endsWith("myshopify.com");
          return { raw, sanitized, valid, token: s.token?.slice(0, 6) + "..." };
        });
        return Response.json({ ok: true, shops });
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message });
      }
    }

    if (url.pathname === "/ingest/shopify/run" && req.method === "POST") {
      return runShopifyIngest(env, url.searchParams);
    }

    return new Response("Not found", { status: 404 });
  },
};

async function runShopifyIngest(env: Env, params: URLSearchParams): Promise<Response> {
  try {
    const stores = JSON.parse(env.SHOPIFY_STORES || "[]");
    if (!Array.isArray(stores) || stores.length === 0) {
      return Response.json({ ok: false, error: "No stores configured" });
    }

    const client = new Client(env.DATABASE_URL);
    await client.connect();

    for (const store of stores) {
      const domain = sanitizeDomain(store.domain);
      if (!domain.endsWith("myshopify.com")) {
        console.warn("Skipping invalid domain:", store.domain);
        continue;
      }
      const token = store.token;
      if (!token) continue;

      const url = `https://${domain}/admin/api/2024-07/orders.json?limit=5`;
      const res = await fetch(url, {
        headers: { "X-Shopify-Access-Token": token },
      });
      if (!res.ok) {
        console.error("Failed to fetch from", domain, res.status);
        continue;
      }
      const data = await res.json();
      if (Array.isArray(data.orders) && data.orders.length > 0) {
        for (const order of data.orders) {
          await client.query(
            `INSERT INTO staging_raw (payload) VALUES ($1)`,
            [order]
          );
        }
      }
    }

    await client.end();
    return Response.json({ ok: true });
  } catch (e: any) {
    return Response.json({ ok: false, error: e.message });
  }
}
