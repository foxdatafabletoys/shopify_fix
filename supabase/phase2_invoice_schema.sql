-- Phase 2: Invoice persistence for Stretch dashboard
-- These tables are NOT touched by the reseed script (which only truncates products + warehouse_stock).
-- Uses the existing customers table (adds region column if missing).

alter table customers add column if not exists region text;

create table if not exists invoices (
  id uuid primary key default gen_random_uuid(),
  customer_id uuid references customers(id) on delete set null,
  order_number text,
  order_date date,
  invoice_date date not null,
  total_units integer not null default 0,
  total_value_gbp numeric(12,2) not null default 0,
  source_filename text,
  created_at timestamptz default now(),
  unique(order_number, customer_id)
);

create table if not exists invoice_lines (
  id uuid primary key default gen_random_uuid(),
  invoice_id uuid references invoices(id) on delete cascade not null,
  product_id uuid references products(id) on delete set null,
  sku text not null,
  description text,
  barcode text,
  trade_price_gbp numeric(10,2) not null,
  quantity integer not null,
  subtotal_gbp numeric(12,2) not null,
  created_at timestamptz default now()
);

create index if not exists idx_invoices_invoice_date on invoices(invoice_date);
create index if not exists idx_invoices_customer_id on invoices(customer_id);
create index if not exists idx_invoice_lines_invoice_id on invoice_lines(invoice_id);
create index if not exists idx_invoice_lines_sku on invoice_lines(sku);
create index if not exists idx_invoice_lines_product_id on invoice_lines(product_id);
