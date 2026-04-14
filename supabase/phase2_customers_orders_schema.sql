create extension if not exists pgcrypto;

create table if not exists customers (
  id uuid primary key default gen_random_uuid(),
  company_name text not null,
  contact_name text,
  email text,
  billing_address text,
  currency_preference text check (currency_preference in ('GBP','USD','EUR')) default 'GBP',
  vat_number text,
  notes text,
  created_at timestamptz default now()
);

create table if not exists orders (
  id uuid primary key default gen_random_uuid(),
  order_ref text unique not null,
  customer_id uuid references customers(id),
  customer_name text not null,
  status text check (status in (
    'draft','confirmed','partially_fulfilled','fulfilled','backordered'
  )) default 'draft',
  currency text check (currency in ('GBP','USD','EUR')) default 'GBP',
  fx_rate numeric(8,4) default 1.0,
  total_gbp numeric(12,2),
  notes text,
  created_at timestamptz default now()
);

create table if not exists order_lines (
  id uuid primary key default gen_random_uuid(),
  order_id uuid references orders(id) on delete cascade,
  product_id uuid references products(id),
  product_name text not null,
  sku text,
  ean text,
  qty_ordered integer not null,
  qty_fulfilled integer default 0,
  qty_backordered integer default 0,
  unit_price_gbp numeric(10,2) not null,
  warehouse_allocated text check (
    warehouse_allocated in ('london','charleston','hanover')
  ),
  fulfilment_status text check (fulfilment_status in (
    'fulfilled','partial','backordered','unmatched'
  )),
  created_at timestamptz default now()
);

create index if not exists idx_customers_company_name on customers (company_name);
create index if not exists idx_orders_customer_id on orders (customer_id);
create index if not exists idx_order_lines_order_id on order_lines (order_id);

insert into customers (company_name, currency_preference, notes)
select 'Fox & Fable', 'GBP', 'Seeded from order form data.'
where not exists (
  select 1
  from customers
  where lower(company_name) = lower('Fox & Fable')
);
