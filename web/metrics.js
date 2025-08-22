/**
 * metrics.js - basic KPIs + store selector
 */

const WORKER_BASE = "https://<your-worker-subdomain>.workers.dev";

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

async function loadShops() {
  const shops = await fetchJSON(`${WORKER_BASE}/api/shops`);
  const sel = document.getElementById("shop-selector");
  sel.innerHTML = "";
  const optAll = document.createElement("option");
  optAll.value = "all"; optAll.textContent = "All Stores";
  sel.appendChild(optAll);
  shops.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.id;
    opt.textContent = s.handle || s.domain;
    sel.appendChild(opt);
  });
}

async function loadKPIs(shop="all") {
  const aov = await fetchJSON(`${WORKER_BASE}/api/metrics/aov?range=30d&shop=${shop}`);
  const orders = await fetchJSON(`${WORKER_BASE}/api/metrics/orders?range=30d&shop=${shop}`);
  const returning = await fetchJSON(`${WORKER_BASE}/api/metrics/returning_rate?range=90d&shop=${shop}`);

  document.getElementById("aov-value").textContent = `$${aov.aov.toFixed(2)}`;
  document.getElementById("orders-value").textContent = orders.orders;
  document.getElementById("revenue-value").textContent = `$${orders.revenue.toFixed(2)}`;
  document.getElementById("returning-value").textContent = `${(returning.returning_rate*100).toFixed(1)}%`;
}

document.addEventListener("DOMContentLoaded", async () => {
  await loadShops();
  await loadKPIs();
  document.getElementById("shop-selector").addEventListener("change", (e) => {
    loadKPIs(e.target.value);
  });
});
