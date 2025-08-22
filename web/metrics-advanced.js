(async function(){
  const WORKER_BASE = "https://<your-worker-subdomain>.workers.dev"; // set me

  // Elements (create your own containers or use existing)
  const els = {
    revenueCanvas: document.getElementById('revenueTrend'),
    topProductsBody: document.getElementById('topProductsBody'),
    marginBody: document.getElementById('channelMarginBody'),
    lowStockBody: document.getElementById('lowStockBody'),
    cohortsBody: document.getElementById('cohortsBody'),
    shopSelect: document.getElementById('shopSelect')
  };

  let revChart;

  async function load(shop){
    // Revenue trend
    const rev = await (await fetch(`${WORKER_BASE}/api/metrics/revenue_trend?range=30d&shop=${shop}`)).json();
    if(rev?.ok){
      const labels = rev.points.map(p=> new Date(p.date).toLocaleDateString());
      const revenue = rev.points.map(p=> p.revenue);
      if (revChart) revChart.destroy();
      const ctx = els.revenueCanvas.getContext('2d');
      revChart = new Chart(ctx, { type: 'line', data: { labels, datasets: [{ label: 'Revenue', data: revenue, tension: .3, fill: false }] }, options: { plugins:{legend:{display:false}}, scales:{x:{display:false}, y:{display:false}}, elements:{point:{radius:0}} } });
    }

    // Top products
    const tops = await (await fetch(`${WORKER_BASE}/api/metrics/top_products?range=30d&shop=${shop}&limit=10`)).json();
    if(tops?.ok){
      els.topProductsBody.innerHTML = (tops.items||[]).map(it=> `<tr><td>${it.sku||''}</td><td>${(it.title||'').replace(/</g,'&lt;')}</td><td>${it.units.toLocaleString()}</td><td>$${it.revenue.toFixed(2)}</td></tr>`).join('');
    }

    // Channel margin
    const cm = await (await fetch(`${WORKER_BASE}/api/metrics/channel_margin?range=30d&shop=${shop}`)).json();
    if(cm?.ok){
      els.marginBody.innerHTML = (cm.channels||[]).map(c=> `<tr><td>${c.channel}</td><td>$${c.revenue.toFixed(2)}</td><td>$${c.cost.toFixed(2)}</td><td>$${c.margin.toFixed(2)}</td></tr>`).join('');
    }

    // Low stock
    const ls = await (await fetch(`${WORKER_BASE}/api/metrics/low_stock?threshold=10&shop=${shop}`)).json();
    if(ls?.ok){
      els.lowStockBody.innerHTML = (ls.items||[]).map(s=> `<tr><td>${s.sku}</td><td>${s.on_hand ?? 0}</td><td>${s.committed ?? 0}</td><td>${s.backordered ?? 0}</td><td>${new Date(s.updated_at).toLocaleString()}</td></tr>`).join('');
    }

    // Cohorts (simple table render)
    const ch = await (await fetch(`${WORKER_BASE}/api/metrics/cohorts?months=6&shop=${shop}`)).json();
    if(ch?.ok){
      // group by cohort
      const groups = {};
      for (const p of ch.points||[]) {
        groups[p.cohort] = groups[p.cohort] || {};
        groups[p.cohort][p.offset_month] = p.active;
      }
      const rows = Object.keys(groups).sort().map(cohort => {
        const cells = [];
        for (let m=0;m<=6;m++) cells.push(`<td>${groups[cohort][m]||0}</td>`);
        return `<tr><td>${cohort}</td>${cells.join('')}</tr>`;
      }).join('');
      els.cohortsBody.innerHTML = rows;
    }
  }

  // Initialize shop selector (assumes existing select like previous bundle)
  if (els.shopSelect) {
    const shops = await (await fetch(`${WORKER_BASE}/api/shops`)).json();
    if (shops?.ok) {
      for (const s of shops.shops) {
        const opt = document.createElement('option');
        opt.value = String(s.id);
        opt.textContent = `${s.handle} (${s.domain.split('.')[0]})`;
        els.shopSelect.appendChild(opt);
      }
    }
    els.shopSelect.addEventListener('change', e=> load(e.target.value));
  }

  await load('all');
})();