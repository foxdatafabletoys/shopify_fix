alter table products add column if not exists rrp_ex_vat_usd numeric(10,2);
alter table products add column if not exists trade_price_ex_vat_usd numeric(10,2);
alter table products add column if not exists quoted_usd_per_gbp numeric(12,6);
alter table products add column if not exists fx_quote_date date;

update products
set quoted_usd_per_gbp = round((trade_price_ex_vat_usd / nullif(trade_price_ex_vat_gbp, 0))::numeric, 6)
where quoted_usd_per_gbp is null
  and trade_price_ex_vat_usd is not null
  and trade_price_ex_vat_gbp is not null
  and trade_price_ex_vat_gbp > 0;
