
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

function parseStores(val?: string) {
  if (!val) return [];
  try {
    const v = JSON.parse(val);
    if (Array.isArray(v)) return v;
    if (typeof v === "object" && v !== null && "domain" in v) return [v];
    if (typeof v === "string") {
      const v2 = JSON.parse(v);
      if (Array.isArray(v2)) return v2;
    }
  } catch {}
  return [];
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "OPTIONS") return json({ ok: true });
    const url = new URL(req.url);

    try {
      if (url.pathname === "/api/shops" && req.method === "GET") {
        // 1) DB first
        try {
          if (env.DATABASE_URL) {
            const pool = new Pool({ connectionString: env.DATABASE_URL });
            const client = await pool.connect();
            try {
              const rows = (await client.query(
                `SELECT id, handle, domain FROM shops WHERE COALESCE(is_active, TRUE) IS TRUE ORDER BY id`
              )).rows;
              if (rows?.length) {
                return json({ ok: true, shops: rows.map((r:any)=>({ id:Number(r.id), handle:r.handle, domain:r.domain })) });
              }
            } finally { /* @ts-ignore */ }
          }
        } catch {}

        // 2) Fallback to secret (tolerate double encoding)
        const parsed = parseStores(env.SHOPIFY_STORES);
        if (parsed.length) {
          const shops = parsed.map((s:any, i:number)=> ({
            id: i+1,
            handle: String(s.domain||"").split(".")[0],
            domain: s.domain
          }));
          return json({ ok: true, shops });
        }
        return json({ ok: false, error: "No stores configured" }, 200);
      }

      // Default
      return new Response("OK", { status: 200 });
    } catch (e:any) {
      return json({ ok: false, error: e?.message ?? String(e) }, 500);
    }
  }
};
