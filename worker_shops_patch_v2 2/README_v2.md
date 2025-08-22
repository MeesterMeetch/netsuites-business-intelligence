# Worker patch v2: tolerant SHOPIFY_STORES parsing

This version adds a double-encoding tolerant parser for the SHOPIFY_STORES secret.

Install:
1) Replace `server/src/worker.ts` with the one in this zip.
2) From `server/` run:
   npm install
   wrangler deploy

Test:
curl https://<your-worker>.workers.dev/api/shops
