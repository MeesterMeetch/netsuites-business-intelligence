// src/components/TopBottomSkus.tsx
import React, { useEffect, useMemo, useState } from "react";

const WORKER_BASE = "https://netsuite-bi-ingest.mitchbiworker.workers.dev";

type Row = {
  sku: string;
  title: string;
  shop_domain: string;
  units_window: number;
  revenue_window: string | number;
  // optionally present if include365=true
  units_365?: number;
  revenue_365?: string | number;
};

function toCSV(rows: Row[]) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = (v: any) =>
    String(v ?? "")
      .replace(/"/g, '""')
      .replace(/\n/g, " ");
  const lines = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => `"${esc((r as any)[h])}"`).join(",")),
  ];
  return lines.join("\n");
}

function downloadCSV(filename: string, csv: string) {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export default function TopBottomSkus() {
  const [days, setDays] = useState(30);
  const [limit, setLimit] = useState(50);
  const [store, setStore] = useState<string>(""); // optional domain filter
  const [include365, setInclude365] = useState(false);

  const [topRows, setTopRows] = useState<Row[]>([]);
  const [bottomRows, setBottomRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const qs = useMemo(() => {
    const p = new URLSearchParams();
    p.set("days", String(days));
    p.set("limit", String(limit));
    if (store) p.set("store", store);
    if (include365) p.set("include365", "true");
    return p.toString();
  }, [days, limit, store, include365]);

  useEffect(() => {
    let abort = false;
    setLoading(true);
    setErr(null);

    const fetchBoth = async () => {
      try {
        const [topRes, bottomRes] = await Promise.all([
          fetch(`${WORKER_BASE}/api/kpis/top-skus?${qs}`),
          fetch(`${WORKER_BASE}/api/kpis/bottom-skus?${qs}`),
        ]);

        const topJson = await topRes.json();
        const bottomJson = await bottomRes.json();

        if (!abort) {
          if (!topJson.ok) throw new Error(topJson.error || "Top SKUs error");
          if (!bottomJson.ok) throw new Error(bottomJson.error || "Bottom SKUs error");
          setTopRows(topJson.rows || []);
          setBottomRows(bottomJson.rows || []);
        }
      } catch (e: any) {
        if (!abort) setErr(e?.message || String(e));
      } finally {
        if (!abort) setLoading(false);
      }
    };

    fetchBoth();
    return () => { abort = true; };
  }, [qs]);

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Top / Bottom SKUs</h2>

      <div className="flex gap-3 items-end">
        <label className="flex flex-col">
          <span>Days</span>
          <input type="number" min={1} max={365} value={days}
                 onChange={e => setDays(Number(e.target.value))}
                 className="border rounded p-2 w-24"/>
        </label>
        <label className="flex flex-col">
          <span>Limit</span>
          <input type="number" min={1} max={500} value={limit}
                 onChange={e => setLimit(Number(e.target.value))}
                 className="border rounded p-2 w-24"/>
        </label>
        <label className="flex flex-col">
          <span>Store (optional)</span>
          <input type="text" placeholder="essential-electric-supply.myshopify.com"
                 value={store} onChange={e => setStore(e.target.value)}
                 className="border rounded p-2 w-[520px]"/>
        </label>
        <label className="flex items-center gap-2">
          <input type="checkbox" checked={include365}
                 onChange={e => setInclude365(e.target.checked)} />
          Include 365d columns
        </label>
      </div>

      {loading && <div>Loading…</div>}
      {err && <div className="text-red-600">Error: {err}</div>}

      <div className="flex gap-6">
        <section className="flex-1">
          <div className="flex justify-between items-center mb-2">
            <h3 className="font-medium">Top SKUs</h3>
            <button
              className="border rounded px-3 py-1"
              onClick={() => downloadCSV(`top-skus-${days}d.csv`, toCSV(topRows))}
            >
              Export CSV
            </button>
          </div>
          <table className="w-full border text-sm">
            <thead>
              <tr className="bg-gray-50">
                <th className="border p-2 text-left">SKU</th>
                <th className="border p-2 text-left">Title</th>
                <th className="border p-2 text-left">Store</th>
                <th className="border p-2 text-right">Units</th>
                <th className="border p-2 text-right">Revenue</th>
                {include365 && <>
                  <th className="border p-2 text-right">Units 365</th>
                  <th className="border p-2 text-right">Revenue 365</th>
                </>}
              </tr>
            </thead>
            <tbody>
              {topRows.map((r, i) => (
                <tr key={r.sku + i}>
                  <td className="border p-2">{r.sku}</td>
                  <td className="border p-2">{r.title}</td>
                  <td className="border p-2">{r.shop_domain}</td>
                  <td className="border p-2 text-right">{r.units_window}</td>
                  <td className="border p-2 text-right">{r.revenue_window}</td>
                  {include365 && <>
                    <td className="border p-2 text-right">{r.units_365 ?? ""}</td>
                    <td className="border p-2 text-right">{r.revenue_365 ?? ""}</td>
                  </>}
                </tr>
              ))}
              {!topRows.length && !loading && <tr><td className="p-3" colSpan={7}>No data</td></tr>}
            </tbody>
          </table>
        </section>

        <section className="flex-1">
          <div className="flex justify-between items-center mb-2">
            <h3 className="font-medium">Bottom SKUs</h3>
            <button
              className="border rounded px-3 py-1"
              onClick={() => downloadCSV(`bottom-skus-${days}d.csv`, toCSV(bottomRows))}
            >
              Export CSV
            </button>
          </div>
          <table className="w-full border text-sm">
            <thead>
              <tr className="bg-gray-50">
                <th className="border p-2 text-left">SKU</th>
                <th className="border p-2 text-left">Title</th>
                <th className="border p-2 text-left">Store</th>
                <th className="border p-2 text-right">Units</th>
                <th className="border p-2 text-right">Revenue</th>
                {include365 && <>
                  <th className="border p-2 text-right">Units 365</th>
                  <th className="border p-2 text-right">Revenue 365</th>
                </>}
              </tr>
            </thead>
            <tbody>
              {bottomRows.map((r, i) => (
                <tr key={r.sku + i}>
                  <td className="border p-2">{r.sku}</td>
                  <td className="border p-2">{r.title}</td>
                  <td className="border p-2">{r.shop_domain}</td>
                  <td className="border p-2 text-right">{r.units_window}</td>
                  <td className="border p-2 text-right">{r.revenue_window}</td>
                  {include365 && <>
                    <td className="border p-2 text-right">{r.units_365 ?? ""}</td>
                    <td className="border p-2 text-right">{r.revenue_365 ?? ""}</td>
                  </>}
                </tr>
              ))}
              {!bottomRows.length && !loading && <tr><td className="p-3" colSpan={7}>No data</td></tr>}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}