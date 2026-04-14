create table if not exists products (
  id uuid primary key default gen_random_uuid(),
  brand text,
  name text not null,
  sku text unique not null,
  barcode text,
  asin text,
  rrp_ex_vat_gbp numeric(10,2),
  rrp_ex_vat_usd numeric(10,2),
  buy_cost_exc_vat_gbp numeric(10,2),
  trade_price_ex_vat_gbp numeric(10,2),
  trade_price_ex_vat_usd numeric(10,2),
  quoted_usd_per_gbp numeric(12,6),
  fx_quote_date date,
  markup numeric(10,4),
  discount_vs_rrp numeric(10,4),
  case_pack integer,
  uk_flag text,
  us_flag text,
  category text,
  cost_price_gbp numeric(10,2) not null,
  sell_price_gbp numeric(10,2) not null,
  weight_kg numeric(6,3),
  created_at timestamptz default now()
);

create table if not exists warehouse_stock (
  id uuid primary key default gen_random_uuid(),
  product_id uuid references products(id) on delete cascade,
  warehouse text check (warehouse in ('uk','us')) not null,
  qty_on_hand integer not null default 0,
  qty_reserved integer not null default 0,
  reorder_point integer not null default 10,
  unique(product_id, warehouse)
);
