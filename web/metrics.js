/**
 * metrics.js - KPIs + store selector + 14-day sales + Top SKUs + Repeat Rates
 *
 * Requires Worker endpoints you now have:
 *   GET /api/shops -> { ok:true, shops:[{id, handle, domain}] }
 *   GET /api/kpis/rolling -> { ok:true, rows:[...] }
 *   GET /api/kpis/daily?days=N&store=<domain?> -> { ok:true, rows:[...] }
 *   GET /api/top_skus?days=N&limit=M&store=<domain?>
 *   GET /api/repeat_rates
 */

const WORKER_BASE = "https://netsuite-bi-ingest.mitchbiworker.workers.dev";

// ---------- tiny utils ----------
async function fetchJSON(url) {
  const res = await fetch(url, { headers: { "Accept": "application/json" } });
  let data;
  try { data = await res.json(); } catch { throw new Error(`Bad JSON from ${url}`); }
  if (!res.ok || data?.ok === false) throw new Error(data?.error || res.statusText || `HTTP ${res.status}`);
  return data;
}
function num(x) {
  if (x == null) return 0;
  if (typeof x === "number") return x;
  const n = parseFloat(x);
  return Number.isFinite(n) ? n : 0;
}
function qs(path, params) {
  const url = new URL(path, WORKER_BASE);
  if (params) for (const [k,v] of Object.entries(params)) if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  return url.toString();
}
function $(id){ return document.getElementById(id); }

// ---------- shops ----------
async function loadShops() {
  const { shops=[] } = await fetchJSON(`${WORKER_BASE}/api/shops`);
  const sel = $("shop-selector");
  if (!sel) return;
  sel.innerHTML = "";

  const optAll = document.createElement("option");
  optAll.value = ""; // empty == all
  optAll.textContent = "All Stores";
  sel.appendChild(optAll);

  shops.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s.domain; // use domain in API
    opt.textContent = s.handle || s.domain;
    sel.appendChild(opt);
  });
}

// ---------- KPIs (tiles) ----------
async function loadKPIs(storeDomain = "") {
  const { rows: rollRows = [] } = await fetchJSON(`${WORKER_BASE}/api/kpis/rolling`);

  let r;
  if (storeDomain) {
    r = rollRows.find(row => (row.shop_domain || "") === storeDomain);
  } else {
    r = rollRows.reduce((acc, row) => {
      acc.orders_30d  += num(row.orders_30d);
      acc.revenue_30d += num(row.revenue_30d);
      acc.units_30d   += num(row.units_30d);
      return acc;
    }, { orders_30d: 0, revenue_30d: 0, units_30d: 0 });
    r.aov_30d = r.orders_30d > 0 ? r.revenue_30d / r.orders_30d : 0;
  }

  const orders30  = num(r?.orders_30d);
  const revenue30 = num(r?.revenue_30d);
  const aov30     = r?.aov_30d != null ? num(r.aov_30d) : (orders30 > 0 ? revenue30 / orders30 : 0);

  // returning rate from last 90 days
  const daily90 = await fetchJSON(qs("/api/kpis/daily", { days: 90, store: storeDomain || undefined }));
  const drows90 = Array.isArray(daily90?.rows) ? daily90.rows : [];
  let newOrders=0, returningOrders=0;
  for (const row of drows90) { newOrders += num(row.new_orders); returningOrders += num(row.returning_orders); }
  const totalNR = newOrders + returningOrders;
  const returningRate = totalNR > 0 ? (returningOrders / totalNR) : 0;

  // write tiles (if present)
  if ($("aov-value"))       $("aov-value").textContent       = `$${aov30.toFixed(2)}`;
  if ($("orders-value"))    $("orders-value").textContent    = orders30.toLocaleString();
  if ($("revenue-value"))   $("revenue-value").textContent   = `$${revenue30.toFixed(2)}`;
  if ($("returning-value")) $("returning-value").textContent = `${(returningRate * 100).toFixed(1)}%`;
}

// ---------- 14-day sales table ----------
async function loadSalesTable(storeDomain = "") {
  const elBody = $("sales-14d-tbody");
  if (!elBody) return;

  const { rows = [] } = await fetchJSON(qs("/api/kpis/daily", { days: 14, store: storeDomain || undefined }));
  const grouped = new Map(); // key: day -> { day, orders, revenue, units }
  for (const r of rows) {
    const key = r.day_mt || r.day || r.day_utc || r.day_local;
    if (!key) continue;
    const cur = grouped.get(key) || { day: key, orders: 0, revenue: 0, units: 0 };
    cur.orders += num(r.orders);
    cur.revenue += num(r.revenue_orders);
    cur.units += num(r.units);
    grouped.set(key, cur);
  }
  const data = Array.from(grouped.values()).sort((a,b) => (a.day < b.day ? 1 : -1));

  elBody.innerHTML = "";
  for (const r of data) {
    const tr = document.createElement("tr");
    const aov = r.orders > 0 ? (r.revenue / r.orders) : 0;

    tr.innerHTML = `
      <td>${String(r.day).slice(0,10)}</td>
      <td>${r.orders.toLocaleString()}</td>
      <td>$${r.revenue.toFixed(2)}</td>
      <td>${r.units.toLocaleString()}</td>
      <td>$${aov.toFixed(2)}</td>
    `;
    elBody.appendChild(tr);
  }
}

// ---------- Top SKUs + Repeat Rate (Product Intelligence panel) ----------
async function loadTopSkusAndRepeat(storeDomain = "") {
  const container = $("product-results-content");
  if (!container) return;

  const { rows: topRows = [] } = await fetchJSON(qs("/api/top_skus", {
    days: 30, limit: 20, store: storeDomain || undefined
  }));

  const rep = await fetchJSON(`/api/repeat_rates`.replace(/^\//, WORKER_BASE + "/"));
  const repRows = Array.isArray(rep?.rows) ? rep.rows : [];
  const repRow = storeDomain
    ? repRows.find(r => (r.shop_domain || "") === storeDomain)
    : repRows.reduce((acc, r) => {
        acc.customers_total    += num(r.customers_total);
        acc.one_time_customers += num(r.one_time_customers);
        acc.repeat_customers   += num(r.repeat_customers);
        return acc;
      }, { customers_total:0, one_time_customers:0, repeat_customers:0 });

  const repeatRatePct = repRow && repRow.customers_total > 0
    ? ( (repRow.repeat_customers / repRow.customers_total) * 100 )
    : (repRow?.repeat_rate_pct ?? 0);

  // render
  let html = `
    <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:10px;">
      <div style="font-weight:600;">Repeat Rate (last horizon):</div>
      <div id="repeat-rate-value" style="font-weight:700;">${(Number(repeatRatePct)||0).toFixed(2)}%</div>
    </div>
    <div style="overflow:auto; max-height:420px; border:1px solid #e5e7eb; border-radius:10px;">
      <table class="nice-table" style="width:100%; border-collapse:collapse;">
        <thead>
          <tr>
            <th style="text-align:left;padding:.5rem;">SKU</th>
            <th style="text-align:left;padding:.5rem;">Title</th>
            <th style="text-align:right;padding:.5rem;">Units</th>
            <th style="text-align:right;padding:.5rem;">Revenue</th>
            <th style="text-align:left;padding:.5rem;">Store</th>
          </tr>
        </thead>
        <tbody>
          ${topRows.map(r => `
            <tr>
              <td style="padding:.5rem;">${r.sku || ""}</td>
              <td style="padding:.5rem;">${r.title || ""}</td>
              <td style="padding:.5rem; text-align:right;">${num(r.units).toLocaleString()}</td>
              <td style="padding:.5rem; text-align:right;">$${num(r.revenue).toFixed(2)}</td>
              <td style="padding:.5rem;">${r.shop_domain || ""}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
  container.innerHTML = html;
}

// ---------- boot ----------
document.addEventListener("DOMContentLoaded", async () => {
  try {
    await loadShops();
    await loadKPIs();           // all stores
    await loadSalesTable();     // all stores
    await loadTopSkusAndRepeat(); // all stores (if panel exists)

    const sel = $("shop-selector");
    if (sel) {
      sel.addEventListener("change", async (e) => {
        const domain = e.target.value; // "" for all
        await loadKPIs(domain);
        await loadSalesTable(domain);
        await loadTopSkusAndRepeat(domain);
      });
    }
  } catch (err) {
    console.error(err);
    const el = $("error-banner");
    if (el) el.textContent = String(err.message || err);
    alert(`Error: ${err.message || err}`);
  }
});