-- Unified schema with multi-store support (safe to run on empty DB)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS channels (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO channels (name) VALUES ('shopify')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS shops (
  id SERIAL PRIMARY KEY,
  channel_id INT NOT NULL REFERENCES channels(id),
  handle TEXT NOT NULL,
  domain TEXT NOT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(channel_id, domain)
);

CREATE TABLE IF NOT EXISTS staging_raw (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  channel_id INT NOT NULL REFERENCES channels(id),
  shop_id INT REFERENCES shops(id),
  external_id TEXT,
  payload JSONB NOT NULL,
  received_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_state (
  channel_id INT NOT NULL REFERENCES channels(id),
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (channel_id, key)
);

CREATE TABLE IF NOT EXISTS customers (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  channel_id INT NOT NULL REFERENCES channels(id),
  shop_id INT REFERENCES shops(id),
  channel_customer_id TEXT,
  email TEXT,
  first_name TEXT,
  last_name TEXT,
  first_seen TIMESTAMPTZ,
  last_seen TIMESTAMPTZ,
  lifetime_value NUMERIC(18,2) DEFAULT 0,
  order_count INT DEFAULT 0,
  UNIQUE(channel_id, shop_id, channel_customer_id)
);

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  channel_id INT NOT NULL REFERENCES channels(id),
  shop_id INT REFERENCES shops(id),
  external_id TEXT NOT NULL,
  order_number TEXT,
  name TEXT,
  placed_at TIMESTAMPTZ NOT NULL,
  currency TEXT,
  subtotal NUMERIC(18,2),
  shipping NUMERIC(18,2),
  tax NUMERIC(18,2),
  discounts NUMERIC(18,2),
  fees NUMERIC(18,2),
  total NUMERIC(18,2),
  financial_status TEXT,
  fulfillment_status TEXT,
  customer_id UUID REFERENCES customers(id),
  UNIQUE(channel_id, shop_id, external_id)
);

CREATE TABLE IF NOT EXISTS order_items (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  sku TEXT,
  external_product_id TEXT,
  title TEXT,
  qty INT NOT NULL,
  unit_price NUMERIC(18,2),
  discount NUMERIC(18,2),
  tax NUMERIC(18,2),
  fees NUMERIC(18,2),
  landed_cost_alloc NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS inventory_snapshots (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  channel_id INT NOT NULL REFERENCES channels(id),
  shop_id INT REFERENCES shops(id),
  sku TEXT NOT NULL,
  on_hand INT,
  committed INT,
  backordered INT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_placed_at ON orders (placed_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_channel_shop ON orders (channel_id, shop_id, placed_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_order_id ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_customers_shop_email ON customers (shop_id, email);
