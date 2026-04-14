-- Generated from:
-- 1) Toys Stock List (1) (2).xlsx [Internal sheet]
-- 2) FoxFableOrderForm-17326.xlsx - Sheet1 (1) (2).xlsx [weight + trade qty enrichment by barcode]
begin;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Root', 'LED01000', '602573655900', 'Leder Games', 22.40, 24.64, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 5), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Halo: Flashpoint - Spartan Edition', 'MGHA102', '5060924983921', 'Mantic Games', 41.67, 46.67, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Halo: Flashpoint -  ODST Feet First Into Hell', 'MGHAU102', '5060924985994', 'Mantic Games', 54.17, 60.67, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Halo: Flashpoint -  Rise of the Banished', 'MGHAB106', '5060924985475', 'Mantic Games', 41.67, 46.67, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Halo: Flashpoint -  The Master Chief, Humanity''s Greatest Weapon', 'MGHA117', '5060924984065', 'Mantic Games', 5.21, 5.84, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Halo: Flashpoint -  Fireteam Cerberus', 'MGHAU301', '5060924985192', 'Mantic Games', 10.42, 11.67, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ+ Gears', 'SG 307', '5414301525691', 'Smartgames', 5.95, 6.55, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 8), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Reverso', 'SG 403', '5414301525677', 'Smartgames', 2.30, 2.53, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 24), ('charleston', 24), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ+ Noodles', 'SG 309', '5414301526520', 'Smartgames', 5.95, 6.55, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 8), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('My First Farm Animals', 'SMX 221', '5414301249863', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Gooal!', 'SGT 320', '5414301524465', 'Smartgames', 4.60, 5.06, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 8), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Three Little Piggies', 'SG 023', '5414301518754', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Horse Academy', 'SG 097', '5414301524434', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('My First Safari Animals', 'SMX 220', '5414301249856', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ Circle', 'SG 311', '5414301526773', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 4), ('charleston', 4), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Genius Square', 'SGHP 001', '5414301525370', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 6), ('charleston', 6), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('My First Dinosaurs', 'SMX 223', '5414301250418', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Smart Farmer', 'SG 091', '5414301522034', 'Smartgames', 8.25, 9.08, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Little Red Riding Hood', 'SG 021', '5414301518389', 'Smartgames', 9.15, 10.07, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Cats & Boxes', 'SG 450', '5414301524953', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 6), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Jump In''', 'SG 421', '5414301519898', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 6), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ Square', 'SG 312', '5414301526483', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 4), ('charleston', 4), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Asteroid Escape', 'SG 426', '5414301521167', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 6), ('charleston', 6), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('Smart Dog', 'SG 451', '5414301525660', 'Smartgames', 6.85, 7.54, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 6), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ Six Pro', 'SG 479', '5414301524540', 'Smartgames', 5.50, 6.05, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 12), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IQ+ Love', 'SG 302', '5414301524397', 'Smartgames', 5.95, 6.55, null)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 8), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('MAELSTROM BATTALION: LEAGUES OF VOTANN', '99120118027', '5011921251407', 'Games Workshop', 59.85, 69.43, 0.472)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 10), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('NECROMUNDA: CORE GANG TACTICS CARDS ENG', '60050599022', '5011921213658', 'Games Workshop', 7.70, 8.93, 0.084)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('RAVEN GUARD KAYVAAN SHRIKE', '99120101292', '5011921140831', 'Games Workshop', 16.53, 19.17, 0.052)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ORKS: BATTLEWAGON', '99120103084', '5011921156887', 'Games Workshop', 45.60, 52.90, 0.611)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('GENESTEALER CULTS: NEOPHYTE HYBRIDS', '99120117020', '5011921171941', 'Games Workshop', 20.24, 23.48, 0.145)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('GRAND CATHAY:DEFENDERS O/T GREAT BASTION', '99122720009', '5011921253289', 'Games Workshop', 73.53, 85.29, 0.878)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('AELDARI: WRAITHLORD', '99120104085', '5011921172849', 'Games Workshop', 22.80, 26.45, 0.133)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ORKS: BOYZ (COMBAT PATROL)', '99120103106', '5011921163441', 'Games Workshop', 20.24, 23.48, 0.164)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('COMBAT PATROL: ORKS', '99120103115', '5011921204021', 'Games Workshop', 59.85, 69.43, 0.052)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('WH40K: BOARDING ACTIONS TERRAIN SET', '99120199105', '5011921182565', 'Games Workshop', 77.81, 90.26, 4.427)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('MIDDLE-EARTH SBG: TREEBEARD MIGHTY ENT', '99121499046', '5011921138852', 'Games Workshop', 31.07, 36.04, 0.135)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('T''AU EMPIRE: GHOSTKEEL BATTLESUIT', '99120113078', '5011921169993', 'Games Workshop', 33.06, 38.35, 0.213)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ASTRA MILITARUM: BANEBLADE', '99120105107', '5011921184088', 'Games Workshop', 68.40, 79.34, 1.406)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('A/MILITARUM: ROGAL DORN BATTLE TANK', '99120105098', '5011921181520', 'Games Workshop', 36.05, 41.82, 0.534)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IMPERIAL KNIGHTS: KNIGHT ARMIGERS', '99120108080', '5011921173990', 'Games Workshop', 34.20, 39.67, 0.310)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('TYRANIDS: VENOMTHROPES', '99120106057', '5011921173709', 'Games Workshop', 28.22, 32.74, 0.189)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ADEPTUS MECHANICUS: SKITARII', '99120116033', '5011921155934', 'Games Workshop', 20.24, 23.48, 0.190)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('BLACK TEMPLARS: HIGH MARSHAL HELBRECHT', '99120101363', '5011921155668', 'Games Workshop', 20.24, 23.48, 0.081)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ADEPTA SORORITAS JUNITH ERUITA', '99120108055', '5011921156740', 'Games Workshop', 20.24, 23.48, 0.114)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ADEPTA SORORITAS: PARAGON WARSUIT', '99120108046', '5011921139255', 'Games Workshop', 28.22, 32.74, 0.204)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('DEATHWATCH UPGRADES', '99070109007', '5011921149001', 'Games Workshop', 6.70, 7.77, 0.022)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('COMBAT PATROL: AELDARI', '99120104097', '5011921221219', 'Games Workshop', 59.85, 69.43, 0.525)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: REPULSOR', '99120101311', '5011921142408', 'Games Workshop', 33.06, 38.35, 0.523)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: BALLISTUS DREADNOUGHT', '99120101393', '5011921200504', 'Games Workshop', 25.37, 29.43, 0.154)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('IMPERIAL KNIGHTS: KNIGHT DOMINUS', '99120108081', '5011921174003', 'Games Workshop', 67.26, 78.02, 0.690)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('T''AU EMPIRE: CRISIS BATTLESUITS', '99120113072', '5011921169931', 'Games Workshop', 31.07, 36.04, 0.231)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: BRUTALIS DREADNOUGHT', '99120101371', '5011921171569', 'Games Workshop', 29.64, 34.38, 0.237)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ORKS: GHAZGHKULL THRAKA', '99120103079', '5011921135165', 'Games Workshop', 28.22, 32.74, 0.167)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES GLADIATOR', '99120101282', '5011921138579', 'Games Workshop', 33.06, 38.35, 0.476)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ULTRAMARINES ROBOUTE GUILLIMAN', '99120101327', '5011921142569', 'Games Workshop', 25.37, 29.43, 0.111)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ZONE MORTALIS: PLATFORMS & STAIRS', '99120599015', '5011921131419', 'Games Workshop', 28.22, 32.74, 0.675)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('WARHAMMER 40000: STARTER SET (ENG)', '60010199058', '5011921199211', 'Games Workshop', 39.62, 45.96, 1.270)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('TYRANIDS: GENESTEALERS', '99120106068', '5011921200337', 'Games Workshop', 20.24, 23.48, 0.196)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('CHAOS SPACE MARINES: DARK COMMUNE', '99120102146', '5011921165414', 'Games Workshop', 20.24, 23.48, 0.090)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('NECRONS: CTAN SHARD OF THE NIGHTBRINGER', '99120110088', '5011921249510', 'Games Workshop', 47.03, 54.55, 0.221)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('AELDARI: SKYWEAVERS', '99120111005', '5011921172894', 'Games Workshop', 20.24, 23.48, 0.133)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('DARK ANGELS RAVENWING COMMAND SQUAD', '99120101361', '5011921153015', 'Games Workshop', 22.80, 26.45, 0.198)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('DARK ANGELS LION EL''JOHNSON', '99120101378', '5011921181377', 'Games Workshop', 25.37, 29.43, 0.114)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('CHAOS S/M: VASHTORR THE ARKIFANE', '99120102180', '5011921200351', 'Games Workshop', 36.77, 42.65, 0.134)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('T''AU EMPIRE: COMMANDER FARSIGHT', '99120113086', '5011921199549', 'Games Workshop', 24.23, 28.11, 0.122)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('KILL TEAM: STARTER SET (ENGLISH)', '60010199071', '5011921231188', 'Games Workshop', 39.62, 45.96, 2.230)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('DEATH GUARD: BLIGHTLORD TERMINATORS', '99120102124', '5011921153534', 'Games Workshop', 22.80, 26.45, 0.161)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('NECRONS: SZAREKH THE SILENT KING', '99120110047', '5011921135189', 'Games Workshop', 60.14, 69.76, 0.460)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('NECROMUNDA:ZONE MORTALIS:GANG STRONGHOLD', '99120599030', '5011921141616', 'Games Workshop', 36.77, 42.65, 1.800)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('TYRANIDS: DEATHLEAPER', '99120106067', '5011921200320', 'Games Workshop', 22.80, 26.45, 0.112)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ADEPTUS MECHANICUS: SERBERYS RAIDERS', '99120116041', '5011921156016', 'Games Workshop', 22.80, 26.45, 0.159)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('AELDARI: SHINING SPEARS', '99120104071', '5011921162765', 'Games Workshop', 24.23, 28.11, 0.168)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('WORLD EATERS: LORD INVOCATUS', '99120102155', '5011921173273', 'Games Workshop', 24.23, 28.11, 0.160)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('BLACK TEMPLARS: GRIMALDUS & RETINUE', '99120101364', '5011921155675', 'Games Workshop', 20.24, 23.48, 0.095)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: TERMINATOR SQUAD', '99120101398', '5011921201303', 'Games Workshop', 24.23, 28.11, 0.207)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('EMPEROR''S CHILDREN: NOISE MARINES', '99120102204', '5011921225910', 'Games Workshop', 24.23, 28.11, 0.208)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: COMPANY HEROES', '99120101389', '5011921200467', 'Games Workshop', 24.23, 28.11, 0.159)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('TYRANIDS: NORN EMISSARY', '99120106064', '5011921200221', 'Games Workshop', 42.18, 48.93, 0.331)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('WORLD EATERS: JAKHALS', '99120102156', '5011921173280', 'Games Workshop', 20.24, 23.48, 0.158)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('CHAOS SPACE MARINES: FORGEFIEND', '99120102165', '5011921178186', 'Games Workshop', 31.07, 36.04, 0.393)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('AELDARI: AVATAR OF KHAINE', '99120104072', '5011921162772', 'Games Workshop', 39.62, 45.96, 0.236)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('COMBAT PATROL: BLOOD ANGELS', '99120101412', '5011921227228', 'Games Workshop', 59.85, 69.43, 0.620)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('CHAOS SPACE MARINES: CHOSEN', '99120102141', '5011921163236', 'Games Workshop', 22.80, 26.45, 0.154)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ADEPTA SORORITAS: ENGINES OF REDEMPTION', '99120108059', '5011921156788', 'Games Workshop', 22.80, 26.45, 0.203)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('S/MARINES: STERNGUARD VETERAN SQUAD', '99120101390', '5011921200474', 'Games Workshop', 22.80, 26.45, 0.201)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES PRIMARIS INFILTRATORS', '99120101325', '5011921142545', 'Games Workshop', 22.80, 26.45, 0.255)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES VINDICATOR', '99120101341', '5011921146024', 'Games Workshop', 27.08, 31.41, 0.463)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('COMBAT PATROL: DARK ANGELS', '99120101406', '5011921203789', 'Games Workshop', 59.85, 69.43, 0.619)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('THOUSAND SONS EXALTED SORCERERS', '99120102134', '5011921153725', 'Games Workshop', 22.80, 26.45, 0.164)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('SPACE MARINES: REDEMPTOR DREADNOUGHT', '99120101310', '5011921142378', 'Games Workshop', 28.22, 32.74, 0.226)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('CHAOS SPACE MARINES: POSSESSED', '99120102140', '5011921163229', 'Games Workshop', 22.80, 26.45, 0.141)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('KILLZONE: VOLKUS', '99120199129', '5011921229444', 'Games Workshop', 48.45, 56.20, 1.940)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('BLOOD BOWL: THIRD SEASON EDITION (ENG)', '60010999014', '5011921249053', 'Games Workshop', 50.16, 58.19, 3.127)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 0), ('charleston', 1), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('ORKS: RUNTHERD AND GRETCHIN', '99120103092', '5011921156986', 'Games Workshop', 9.12, 10.58, 0.073)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('BLACK TEMPLARS CASTELLAN', '99120101367', '5011921162871', 'Games Workshop', 14.25, 16.53, 0.053)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('LORD OF THE RINGS:FELLOWSHIP OF THE RING', '99121499033', '5011921109227', 'Games Workshop', 21.66, 25.13, 0.077)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
with upsert_product as (
  insert into products (name, sku, barcode, category, cost_price_gbp, sell_price_gbp, weight_kg) values ('TYRANIDS: NEUROLICTOR', '99120106072', '5011921200382', 'Games Workshop', 15.39, 17.85, 0.049)
  on conflict (sku) do update set
    name = excluded.name,
    barcode = excluded.barcode,
    category = excluded.category,
    cost_price_gbp = excluded.cost_price_gbp,
    sell_price_gbp = excluded.sell_price_gbp,
    weight_kg = coalesce(excluded.weight_kg, products.weight_kg)
  returning id
)
insert into warehouse_stock (product_id, warehouse, qty_on_hand, qty_reserved, reorder_point)
select upsert_product.id, warehouse_rows.warehouse, warehouse_rows.qty_on_hand, 0, 10 from upsert_product cross join (values ('london', 1), ('charleston', 0), ('hanover', 0)) as warehouse_rows(warehouse, qty_on_hand) on conflict (product_id, warehouse) do update set qty_on_hand = excluded.qty_on_hand, qty_reserved = excluded.qty_reserved, reorder_point = excluded.reorder_point;
commit;
