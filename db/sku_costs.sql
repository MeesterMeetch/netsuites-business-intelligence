-- Table for NetSuite COGS per SKU with effective dating
CREATE TABLE IF NOT EXISTS sku_costs (
  sku TEXT NOT NULL,
  cost NUMERIC(18,4) NOT NULL,           -- per-unit landed cost
  effective_from DATE NOT NULL,          -- start date (inclusive)
  effective_to   DATE,                   -- end date (inclusive), NULL = open-ended
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (sku, effective_from)
);
CREATE INDEX IF NOT EXISTS idx_sku_costs_sku ON sku_costs (sku);
CREATE INDEX IF NOT EXISTS idx_sku_costs_range ON sku_costs (sku, effective_from, effective_to);
