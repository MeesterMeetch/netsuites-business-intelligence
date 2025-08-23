/**
 * metrics.js — live KPIs, sparkline, sales table (export with window),
 * Top/Bottom SKUs, Repeat Rates, and KPI loading shimmer.
 */

const WORKER_BASE = "https://netsuite-bi-ingest.mitchbiworker.workers.dev";

/* ---------- utils ---------- */ 
async function fetchJSON(url) {
  const res = await fetch(url, { headers: { "Accept": "application/json" } });
  let data;
  try { data = await res.json(); } catch { throw new Error(`Bad JSON from ${url}`); }
  if (!res.ok || data?.ok === false) throw new Error(data?.error || res.statusText || `HTTP ${res.status}`);
  return data;
}
function num(x) { if (x == null) return 0; const n = typeof x === "number" ? x : parseFloat(x); return Number.isFinite(n) ? n : 0; }
function qs(path, params) {
  const url = new URL(path, WORKER_BASE);
  if (params) for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }
  return url.toString();
}
function $(id){ return document.getElementById(id); }
function fmtMoney(n){ return `$${num(n).toFixed(2)}`; }
function downloadCSV(filename, rows) {
  if (!rows?.length) return;
  const header = Object.keys(rows[0]);
  const csv = [header.join(","), ...rows.map(r => header.map(h => {
    const v = r[h] ?? "";
    if (typeof v === "string" && /[",\n]/.test(v)) return `"${v.replace(/"/g,'""')}"`;
    return v;
  }).join(","))].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
}

/* ---------- global state ---------- */
const Last = {
  topSkus: [],
  bottomSkus: [],
  repeatRates: [],
  newRetDaily: [],
  salesTable: [],     // current window rows for export
  salesWindowDays: 14 // current selected window for sales table
};

/* ---------- shops ---------- */
async function loadShops() {
  const { shops = [] } = await fetchJSON(`${WORKER_BASE}/api/shops`);
  const ids = ["shop-selector","top-skus-store","bottom-skus-store","repeat-store"];
  ids.forEach(id => {
    const sel = $(id);
    if (!sel) return;
    sel.innerHTML = "";
    const optAll = document.createElement("option");
    optAll.value = ""; optAll.textContent = "All Stores";
    sel.appendChild(optAll);
    shops.forEach(s => {
      const opt = document.createElement("option");
      opt.value = s.domain;
      opt.textContent = s.handle || s.domain;
      sel.appendChild(opt);
    });
  });
}

/* ---------- KPI shimmer ---------- */
function setKpiLoading(isLoading) {
  const ids = ["orders-value","revenue-value","aov-value","returning-value"];
  ids.forEach(id => {
    const el = $(id);
    if (!el) return;
    if (isLoading) { el.classList.add("shimmer"); el.textContent = " "; }
    else { el.classList.remove("shimmer"); }
  });
}

/* ---------- KPI tiles + sparkline ---------- */
async function loadKPIs(storeDomain = "") {
  setKpiLoading(true);
  try {
    // rolling 7/30
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
      }, { orders_30d:0, revenue_30d:0, units_30d:0 });
      r.aov_30d = r.orders_30d > 0 ? r.revenue_30d / r.orders_30d : 0;
    }
    const orders30  = num(r?.orders_30d);
    const revenue30 = num(r?.revenue_30d);
    const aov30     = r?.aov_30d != null ? num(r.aov_30d) : (orders30>0 ? revenue30/orders30 : 0);

    $("orders-value").textContent  = orders30.toLocaleString();
    $("revenue-value").textContent = fmtMoney(revenue30);
    $("aov-value").textContent     = fmtMoney(aov30);

    // returning rate (90d)
    const { rows: rows90 = [] } = await fetchJSON(qs("/api/kpis/daily", { days: 90, store: storeDomain || undefined }));
    let newOrders=0, returningOrders=0;
    for (const row of rows90) { newOrders += num(row.new_orders); returningOrders += num(row.returning_orders); }
    const totalNR = newOrders + returningOrders;
    const returningRate = totalNR > 0 ? (returningOrders / totalNR) : 0;
    $("returning-value").textContent = `${(returningRate * 100).toFixed(1)}%`;

    // sparkline (30d)
    const { rows: rows30 = [] } = await fetchJSON(qs("/api/kpis/daily", { days: 30, store: storeDomain || undefined }));
    Last.newRetDaily = rows30
      .map(r => ({ day: (r.day_mt || "").slice(0,10), new_orders: num(r.new_orders), returning_orders: num(r.returning_orders) }))
      .sort((a,b)=> a.day < b.day ? -1 : 1);
    drawNewReturningSparkline(Last.newRetDaily);
  } finally {
    setKpiLoading(false);
  }
}

let sparkChart;
function drawNewReturningSparkline(data) {
  const ctx = $("newret-sparkline");
  if (!ctx) return;
  const labels = data.map(d => d.day);
  const newData = data.map(d => d.new_orders);
  const retData = data.map(d => d.returning_orders);

  if (sparkChart) sparkChart.destroy();
  sparkChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        { label: "New", data: newData, borderWidth: 2, tension: .3, fill: false },
        { label: "Returning", data: retData, borderWidth: 2, tension: .3, fill: false },
      ]
    },
    options: {
      responsive: true,
      plugins: { legend: { display: true, position: "bottom" } },
      interaction: { intersect: false, mode: "index" },
      elements: { point: { radius: 0 } },
      scales: { x: { display: false }, y: { display: false } }
    }
  });

  const totalNew = newData.reduce((a,b)=>a+b,0);
  const totalRet = retData.reduce((a,b)=>a+b,0);
  const cap = $("sparkline-caption");
  if (cap) cap.textContent = `New ${totalNew.toLocaleString()} vs Returning ${totalRet.toLocaleString()}`;
}

/* ---------- sales table (windowed) + CSV ---------- */
async function loadSalesTable(storeDomain = "", days = Last.salesWindowDays) {
  Last.salesWindowDays = days;
  const { rows = [] } = await fetchJSON(qs("/api/kpis/daily", { days, store: storeDomain || undefined }));

  // aggregate by day across stores when "All Stores"
  const grouped = new Map();
  for (const r of rows) {
    const key = (r.day_mt || "").slice(0,10);
    if (!key) continue;
    const cur = grouped.get(key) || { day: key, orders: 0, revenue: 0, units: 0 };
    cur.orders += num(r.orders);
    cur.revenue += num(r.revenue_orders);
    cur.units += num(r.units);
    grouped.set(key, cur);
  }
  const data = Array.from(grouped.values()).sort((a,b)=> a.day < b.day ? 1 : -1);
  Last.salesTable = data;

  // draw table
  const tbody = $("sales-rows") || $("sales-14d-tbody"); // support both ids
  if (tbody) {
    tbody.innerHTML = "";
    for (const r of data) {
      const aov = r.orders > 0 ? (r.revenue / r.orders) : 0;
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.day}</td>
        <td style="text-align:right">${r.orders.toLocaleString()}</td>
        <td style="text-align:right">${fmtMoney(r.revenue)}</td>
        <td style="text-align:right">${r.units.toLocaleString()}</td>
        <td style="text-align:right">${fmtMoney(aov)}</td>
      `;
      tbody.appendChild(tr);
    }
  }

  ensureSalesControls(); // make sure the selector + export button exist
}

function ensureSalesControls() {
  // Find a good container to park controls near the table.
  // If you have a specific wrapper, give it id="sales-controls".
  let host = document.getElementById("sales-controls");
  if (!host) {
    // try to mount near the global store selector
    const sel = document.getElementById("shop-selector");
    if (sel && sel.parentElement) host = sel.parentElement;
  }
  if (!host) host = document.body;

  // Create window <select>
  if (!document.getElementById("sales-window-select")) {
    const label = document.createElement("label");
    label.style.display = "inline-flex";
    label.style.alignItems = "center";
    label.style.gap = ".5rem";
    label.style.marginLeft = "auto";
    label.style.marginRight = ".5rem";
    label.innerHTML = `Window: `;
    const select = document.createElement("select");
    select.id = "sales-window-select";
    [1,7,14,30,90,365].forEach(d => {
      const opt = document.createElement("option");
      opt.value = String(d);
      opt.textContent = `${d}d`;
      if (d === Last.salesWindowDays) opt.selected = true;
      select.appendChild(opt);
    });
    select.addEventListener("change", async () => {
      const store = $("shop-selector")?.value || "";
      await loadSalesTable(store, Number(select.value));
    });
    label.appendChild(select);
    host.appendChild(label);
  }

  // Create single Export button
  if (!document.getElementById("sales-export-csv")) {
    const btn = document.createElement("button");
    btn.id = "sales-export-csv";
    btn.className = "btn btn-secondary";
    btn.textContent = "Download CSV";
    btn.style.marginLeft = ".5rem";
    btn.addEventListener("click", () => {
      // Convert Last.salesTable (day, orders, revenue, units, aov)
      const rows = Last.salesTable.map(r => {
        const aov = r.orders > 0 ? (r.revenue / r.orders) : 0;
        return { day: r.day, orders: r.orders, revenue: r.revenue.toFixed(2), units: r.units, aov: aov.toFixed(2) };
      });
      downloadCSV(`sales_${Last.salesWindowDays}d.csv`, rows);
    });
    host.appendChild(btn);
  }
}

/* ---------- Top / Bottom SKUs ---------- */
function skuUnitsWnd(r){ return num(r.units_wnd ?? r.units_window); }
function skuRevenueWnd(r){ return num(r.revenue_wnd ?? r.revenue_window); }

async function loadTopSkus() {
  const store = $("top-skus-store")?.value || "";
  const days = Number($("top-skus-days")?.value || 30);
  const limit = Number($("top-skus-limit")?.value || 50);
  const include365 = !!$("top-skus-365")?.checked;

  const { rows = [] } = await fetchJSON(qs("/api/kpis/top-skus", {
    days, store: store || undefined, limit, include365: include365 ? "true" : "false"
  }));

  Last.topSkus = rows;
  const show365 = include365;
  document.querySelectorAll(".col-365").forEach(th => th.style.display = show365 ? "" : "none");

  const tbody = $("top-skus-tbody");
  if (tbody) {
    tbody.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.sku || ""}</td>
        <td>${r.title || ""}</td>
        <td style="text-align:right">${skuUnitsWnd(r).toLocaleString()}</td>
        <td style="text-align:right">${fmtMoney(skuRevenueWnd(r))}</td>
        <td class="col-365" style="text-align:right; ${show365 ? "" : "display:none"}">${r.units_365 != null ? num(r.units_365).toLocaleString() : ""}</td>
        <td class="col-365" style="text-align:right; ${show365 ? "" : "display:none"}">${r.revenue_365 != null ? fmtMoney(r.revenue_365) : ""}</td>
      `;
      tbody.appendChild(tr);
    }
  }
}

async function loadBottomSkus() {
  const store = $("bottom-skus-store")?.value || "";
  const days = Number($("bottom-skus-days")?.value || 30);
  const limit = Number($("bottom-skus-limit")?.value || 50);
  const include365 = !!$("bottom-skus-365")?.checked;

  const { rows = [] } = await fetchJSON(qs("/api/kpis/bottom-skus", {
    days, store: store || undefined, limit, include365: include365 ? "true" : "false"
  }));

  Last.bottomSkus = rows;
  const show365 = include365;
  const wrap = $("bottom-skus-tbody")?.closest(".table-wrap");
  if (wrap) wrap.querySelectorAll(".col-365").forEach(th => th.style.display = show365 ? "" : "none");

  const tbody = $("bottom-skus-tbody");
  if (tbody) {
    tbody.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.sku || ""}</td>
        <td>${r.title || ""}</td>
        <td style="text-align:right">${skuUnitsWnd(r).toLocaleString()}</td>
        <td style="text-align:right">${fmtMoney(skuRevenueWnd(r))}</td>
        <td class="col-365" style="text-align:right; ${show365 ? "" : "display:none"}">${r.units_365 != null ? num(r.units_365).toLocaleString() : ""}</td>
        <td class="col-365" style="text-align:right; ${show365 ? "" : "display:none"}">${r.revenue_365 != null ? fmtMoney(r.revenue_365) : ""}</td>
      `;
      tbody.appendChild(tr);
    }
  }
}

/* ---------- Repeat Rates ---------- */
async function loadRepeatRates() {
  const store = $("repeat-store")?.value || "";
  const showLtv = !!$("repeat-show-ltv")?.checked;

  const { rows = [] } = await fetchJSON(`${WORKER_BASE}/api/kpis/repeat-rates`);
  const filtered = store ? rows.filter(r => (r.shop_domain || "") === store) : rows;
  Last.repeatRates = filtered;

  const tbody = $("repeat-tbody");
  if (tbody) {
    tbody.innerHTML = "";
    for (const r of filtered) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.shop_domain || "(unknown)"}</td>
        <td style="text-align:right">${num(r.customers_total).toLocaleString()}</td>
        <td style="text-align:right">${num(r.one_time_customers).toLocaleString()}</td>
        <td style="text-align:right">${num(r.repeat_customers).toLocaleString()}</td>
        <td style="text-align:right">${num(r.repeat_rate_pct).toFixed(2)}%</td>
        <td class="col-ltv" style="text-align:right">${r.avg_ltv_repeat != null ? fmtMoney(r.avg_ltv_repeat) : ""}</td>
      `;
      tbody.appendChild(tr);
    }
    document.querySelectorAll(".col-ltv").forEach(el => el.style.display = showLtv ? "" : "none");
  }
}

/* ---------- ensure panel export buttons ---------- */
function ensureButtons() {
  // Top SKUs
  const topControls = document.querySelector("#top-skus-store")?.closest(".controls");
  if (topControls && !topControls.querySelector("#top-skus-export")) {
    const btn = document.createElement("button");
    btn.id = "top-skus-export";
    btn.className = "btn";
    btn.textContent = "Export CSV";
    btn.addEventListener("click", () => downloadCSV("top_skus.csv", Last.topSkus));
    topControls.appendChild(btn);
  }
  // Bottom SKUs
  const botControls = document.querySelector("#bottom-skus-store")?.closest(".controls");
  if (botControls && !botControls.querySelector("#bottom-skus-export")) {
    const btn = document.createElement("button");
    btn.id = "bottom-skus-export";
    btn.className = "btn";
    btn.textContent = "Export CSV";
    btn.addEventListener("click", () => downloadCSV("bottom_skus.csv", Last.bottomSkus));
    botControls.appendChild(btn);
  }
  // Repeat Rates
  const repControls = document.querySelector("#repeat-store")?.closest(".controls");
  if (repControls && !repControls.querySelector("#repeat-export")) {
    const btn = document.createElement("button");
    btn.id = "repeat-export";
    btn.className = "btn";
    btn.textContent = "Export CSV";
    btn.addEventListener("click", () => downloadCSV("repeat_rates.csv", Last.repeatRates));
    repControls.appendChild(btn);
  }
}

/* ---------- boot ---------- */
document.addEventListener("DOMContentLoaded", async () => {
  try {
    await loadShops();

    // initial loads
    await loadKPIs("");             // all stores
    await loadSalesTable("", 14);   // all stores, 14d default
    await loadTopSkus();
    await loadBottomSkus();
    await loadRepeatRates();

    ensureButtons();
    ensureSalesControls();

    // global store selector -> refresh everything
    $("shop-selector")?.addEventListener("change", async (e) => {
      const domain = e.target.value || "";
      await loadKPIs(domain);
      await loadSalesTable(domain, Last.salesWindowDays);
      ["top-skus-store","bottom-skus-store","repeat-store"].forEach(id => { if ($(id)) $(id).value = domain; });
      await loadTopSkus();
      await loadBottomSkus();
      await loadRepeatRates();
    });

    // Top
    $("top-skus-refresh")?.addEventListener("click", loadTopSkus);
    ["top-skus-store","top-skus-days","top-skus-365"].forEach(id => $(id)?.addEventListener("change", loadTopSkus));
    $("top-skus-limit")?.addEventListener("input", () => {
      const el = $("top-skus-limit"); el.value = String(Math.max(1, Math.min(500, Number(el.value || 50))));
    });
    $("top-skus-limit")?.addEventListener("change", loadTopSkus);

    // Bottom
    $("bottom-skus-refresh")?.addEventListener("click", loadBottomSkus);
    ["bottom-skus-store","bottom-skus-days","bottom-skus-365"].forEach(id => $(id)?.addEventListener("change", loadBottomSkus));
    $("bottom-skus-limit")?.addEventListener("input", () => {
      const el = $("bottom-skus-limit"); el.value = String(Math.max(1, Math.min(500, Number(el.value || 50))));
    });
    $("bottom-skus-limit")?.addEventListener("change", loadBottomSkus);

    // Repeat
    $("repeat-refresh")?.addEventListener("click", loadRepeatRates);
    $("repeat-store")?.addEventListener("change", loadRepeatRates);
    $("repeat-show-ltv")?.addEventListener("change", loadRepeatRates);
  } catch (err) {
    console.error(err);
    const el = $("error-banner");
    if (el) el.textContent = String(err.message || err);
    alert(`Error: ${err.message || err}`);
  }
});