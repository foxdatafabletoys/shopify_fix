# Shopify bulk sync — setup & run guide

This script wipes products with no SKU from your Shopify store and rebuilds the catalog from your two spreadsheets. Read this whole page before running anything; the delete phase is permanent.

## What gets imported

| Source | Rows | Used for |
|---|---|---|
| Games Workshop Store List.xlsx | 1,974 | All Games Workshop products. Master catalog. |
| everything else.xlsx | 504 → 150 used | Non-GW items only (Funko, Pokemon, books, etc.). GW rows in this sheet are skipped to avoid duplicates with the master catalog. |

## Pricing rules baked in

- **Games Workshop**: `price = UKR × 0.79`, `compare_at = UKR` (21% off RRP, shown as a discount).
- **Funko**: `price = SRP × 0.89`, `compare_at = SRP` (11% off).
- **Everything else**: `price = SRP`, no compare-at price.
- Cost-per-item set from `GBD` (GW) or `Unit Cost` (others).
- Inventory tracking is on; quantities come from `Store Quantity` (GW) or `Available` (others). Zero-stock items are imported and marked out of stock.

## Delete behaviour

The delete phase paginates every product currently on your Shopify store and deletes only the ones whose variants all have **no SKU**. Anything with a SKU/Product Code is kept untouched. You confirmed this in chat.

---

## Step 1 — Get your Shopify Admin API token

You're creating a "custom app" inside your own store. This takes about 3 minutes.

1. Open your Shopify admin: `https://<your-store>.myshopify.com/admin`
2. Click **Settings** (bottom left).
3. Click **Apps and sales channels** → **Develop apps**.
   - If you see a button "Allow custom app development", click it and confirm.
4. Click **Create an app**. Name it something like `Foxfable Bulk Sync`. Pick yourself as developer.
5. Open the new app → **Configuration** tab → **Admin API integration → Configure**.
6. Grant these scopes (search the list and tick each one):
   - `read_products`, `write_products`
   - `read_inventory`, `write_inventory`
   - `read_locations`
7. Click **Save** at the top.
8. Go to the **API credentials** tab → click **Install app** → confirm.
9. Under **Admin API access token** click **Reveal token once**. It looks like:
   ```
   shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   ```
   Copy it now — Shopify only shows it once. If you lose it you can rotate and get a new one from the same screen.

## Step 2 — Note your store handle

Your store handle is the bit before `.myshopify.com`. If your admin URL is
`https://telemachus-foxfable.myshopify.com/admin`, the handle is `telemachus-foxfable`.

## Step 3 — Create the .env file

In the `shopify_sync` folder, copy `.env.example` to `.env` and fill it in:

```
SHOPIFY_STORE=telemachus-foxfable
SHOPIFY_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SHOPIFY_LOCATION=
```

Leave `SHOPIFY_LOCATION` blank — the script will auto-detect your store's primary location. Only fill it in if you want to push stock to a non-primary location.

## Step 4 — Install Python dependencies

In a Terminal:

```bash
cd "/Users/alessaweiler/Documents/telemachus_foxfable/cleaning up inventory/shopify_sync"
python3 -m pip install -r requirements.txt
```

If `python3` isn't installed, install it from python.org first (or `brew install python`).

## Step 5 — Dry run (no API calls)

Always do this first. It reads the spreadsheets, builds the product list, and writes `preview.csv` so you can sanity-check the data:

```bash
python3 shopify_sync.py --dry-run
```

Open `preview.csv` in Numbers/Excel. Spot-check 10–20 rows: prices, SKUs, vendors, quantities. If anything looks wrong, tell me and I'll fix the script before you run it live.

## Step 6 — Live delete

When the preview looks right:

```bash
python3 shopify_sync.py --delete
```

This pages through every existing Shopify product and deletes any whose variants all have empty SKUs. Anything with a Product Code on Shopify is kept. Watch the log; it prints each title before deleting.

## Step 7 — Live import

```bash
python3 shopify_sync.py --import
```

Expect this to take roughly 10–20 minutes for ~2,100 products. Shopify rate-limits at ~50 cost points/second per store, so the script throttles itself automatically.

If the run dies partway through, look at `sync.log` and `failures.tsv`. Resume from where it stopped:

```bash
python3 shopify_sync.py --import --start-at 850
```

## Step 8 — Spot-check on Shopify

After it finishes, open `https://<your-store>.myshopify.com/admin/products` and:

- Confirm the total product count matches `preview.csv` row count (minus any rows in `failures.tsv`).
- Pick 3 random products and confirm price, compare-at, SKU, barcode, vendor, weight, inventory.

---

## Files in this folder

| File | What it is |
|---|---|
| `shopify_sync.py` | The script. |
| `SETUP.md` | This guide. |
| `.env.example` | Template for credentials — copy to `.env` and fill in. |
| `.env` | Your secrets. **Never share or commit.** |
| `requirements.txt` | Python deps. |
| `preview.csv` | Dry-run output: what will be sent to Shopify. |
| `sync.log` | Append-only run log. Safe to delete. |
| `failures.tsv` | Any failed creates with their error. Useful for retry. |

## Common gotchas

- **"Could not authenticate"** → token is wrong or app isn't installed. Re-do step 1.8.
- **"403 / scope missing"** → you forgot a scope in step 1.6. Edit the app, add it, click "Update" then re-install.
- **"Throttled"** in the log → fine, the script waits and retries. Just let it run.
- **Some products fail with "Title has already been taken"** → Shopify enforces unique titles per product. Look at `failures.tsv`; usually a duplicate row in the sheet.
- **Inventory shows 0 even though sheet has stock** → the script auto-picks the primary location. If your inventory lives at a different location, set `SHOPIFY_LOCATION` in `.env`. Find IDs at Settings → Locations.

## Want this scheduled?

Right now this is one-shot: delete + reload. If you later want it to keep Shopify in sync with edits to the spreadsheets nightly (or whenever they change), tell me and I'll switch the create logic to upsert-by-SKU and wire it to a scheduled task.
