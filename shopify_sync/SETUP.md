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
   - `read_publications`, `write_publications` if you want script-created collections published to the storefront
   - `write_files` for the GW photo-sync lane. `--gw-refresh-cache` only refreshes the local GW cache; live `--photo-sync` is the step that writes Shopify media.
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

Leave `SHOPIFY_LOCATION` blank unless you need to override the location. Blank is the supported default and the script auto-detects your store's primary location. If you do set it, the canonical example is a Shopify GID like `gid://shopify/Location/12345678901`; numeric IDs also work and are normalized internally.

## GW auto-download + photo sync

The GW image lane uses a repo-local cache at `shopify_sync/gw_photo_cache/current/`. The operator workflow is:

1. Refresh the cache from the GW Product Images area:
   ```bash
   python3 shopify_sync.py --gw-refresh-cache
   ```
2. Run a dry run first:
   ```bash
   python3 shopify_sync.py --photo-sync --dry-run
   ```
3. Review `photo_sync_preview.csv`, plus any appended `photo_sync_missing.tsv`, `photo_sync_ambiguous.tsv`, and `photo_sync_failures.tsv`.
4. Apply for real:
   ```bash
   python3 shopify_sync.py --photo-sync
   ```

Hard prerequisites:

- `write_files` must be granted on the custom app before live `--photo-sync`.
- Photo sync stays a separate GW-only lane; it does not extend `--update`.
- First pass supports direct `.jpg`, `.jpeg`, and `.png` files plus `.zip` packs that contain those image files. Other archive types still remain out of scope.
- Authenticated GW download is follow-up scope.

Notes:

- Repo-local cache path: `shopify_sync/gw_photo_cache/current/`.
- Dry run is read-only on the Shopify side: it inspects Shopify state and the local GW cache but performs no Shopify write mutations.
- Live photo sync only detaches old product-media associations after replacement files are attached and reordered.
- The GraphQL sequence used by `--photo-sync` remains:
  - `stagedUploadsCreate`
  - direct `PUT` upload of each local image to the returned staged target
  - `fileCreate`
  - poll `fileStatus` until `READY`
  - `fileUpdate.referencesToAdd`
  - `productReorderMedia`
  - `fileUpdate.referencesToRemove`
- The API proof artifact for this lane is `.omx/plans/gw-shopify-photo-sync-api-proof.md`.
- Keep the app on Shopify Admin API `2025-01` unless the proof artifact records a deliberate version bump.

## Step 4 — Install Python dependencies

In a Terminal:

```bash
cd "/Users/alessaweiler/Documents/telemachus/telemachus/shopify_sync"
python3 -m pip install -r requirements.txt
```

If `python3` isn't installed, install it from python.org first (or `brew install python`).

## Step 5 — Dry run (no API calls)

Always do this first. It reads the spreadsheets, builds the product list, and writes `preview.csv` so you can sanity-check the data:

```bash
python3 shopify_sync.py --dry-run
```

Open `preview.csv` in Numbers/Excel. Spot-check 10–20 rows: prices, SKUs, vendors, quantities. If anything looks wrong, tell me and I'll fix the script before you run it live.

## Step 6 — Preflight

Run this before any live delete/import pass:

```bash
python3 shopify_sync.py --preflight
```

This validates Shopify auth and resolves the location without deleting or creating anything. If it fails, fix the token, app scopes, or `SHOPIFY_LOCATION`, then rerun it.

## Delete all current collections

If you need to wipe the existing Shopify collections before rebuilding your storefront structure, use the dedicated collection-delete lane. This deletes collections only; it does not delete products.

Dry run first:

```bash
python3 shopify_sync.py --delete-collections --dry-run
```

Apply for real:

```bash
python3 shopify_sync.py --delete-collections
```

Notes:

- This removes both manual and automated collections returned by Shopify.
- Products remain in the store; they are just no longer grouped into those collections.
- This lane must run on its own and cannot be combined with import/update/photo-sync flags.

## Generate storefront collections and assign existing products

This lane creates a Wayland-style smart collection set, stamps deterministic auto-collection tags onto existing Shopify products, and lets Shopify place those products into smart collections from tag rules.

Dry run first:

```bash
python3 shopify_sync.py --generate-collections --dry-run
```

Apply for real:

```bash
python3 shopify_sync.py --generate-collections
```

Recommended sequence for a clean rebuild:

```bash
python3 shopify_sync.py --delete-collections
python3 shopify_sync.py --generate-collections
```

Notes:

- The dry run writes `collection_generation_preview.csv` and `collection_generation_unmatched.csv`.
- The live run creates any missing smart collections in the configured Wayland-style set, retags existing products, and updates matching existing smart collections to the expected tag rule.
- If a matching collection handle already exists as a manual collection, the script stops and tells you to delete or rename it first.
- New collections are unpublished by default in Shopify, so the script attempts to publish them to the current channel. If the app is missing publication scopes, collection creation still succeeds but the collections may stay hidden on the storefront until you add `read_publications` and `write_publications`.
- Some categories such as `Latest Releases`, `Pre-Orders`, and several board/card subtypes are best-effort heuristics based on current Shopify metadata. The lane corrects those tags when you rerun it.

## Step 7 — Live delete

When the preview and preflight both look right:

```bash
python3 shopify_sync.py --delete
```

This is still available as a standalone delete-only run. It pages through every existing Shopify product and deletes any whose variants all have empty SKUs. Anything with a Product Code on Shopify is kept.

## Step 8 — Live import

```bash
python3 shopify_sync.py --import
```

Expect this to take roughly 10–20 minutes for ~2,100 products. Shopify rate-limits at ~50 cost points/second per store, so the script throttles itself automatically.

If the run dies partway through, look at `sync.log` and `failures.tsv`. Resume from where it stopped:

```bash
python3 shopify_sync.py --import --start-at 850
```

## Step 9 — Guarded combined run

If you are intentionally doing both live phases in one pass:

```bash
python3 shopify_sync.py --all
```

This runs the live delete phase and then the live import phase. Use it only after the dry run and preflight checks.

## Step 10 — Refreshing prices and inventory from a new sheet (`--update`)

When you receive an updated `Games Workshop Store List.xlsx` (or edit `everything else.xlsx`) and want Shopify to match without recreating products, use `--update`. It matches existing products by SKU and pushes only the fields that changed: `price`, `compare_at_price`, `cost`, and on-hand quantity.

Workflow:

1. Drop the new spreadsheet in this folder, replacing the old `Games Workshop Store List.xlsx`. Optionally rename the old one to `Games Workshop Store List.backup-YYYY-MM-DD.xlsx` so you have a rollback.
2. Dry run — reads Shopify (no writes) and writes a diff to `update_preview.csv`:
   ```bash
   python3 shopify_sync.py --update --dry-run
   ```
   Each row in `update_preview.csv` shows `shopify_*` (current) vs `sheet_*` (new) for the SKUs that would change. Open it, sanity-check.
3. Apply for real:
   ```bash
   python3 shopify_sync.py --update
   ```

Notes:

- SKUs in the sheet that don't yet exist on Shopify are **skipped**, not created. If you also need to create new products, run `--import` first (or in a follow-up pass).
- SKUs on Shopify that aren't in the sheet are left alone.
- If you want to include the same on-hand quantity for items even when nothing changed, the current logic deliberately doesn't — it only writes when the value differs, to keep the run fast and the inventory adjustment log clean.

## Step 11 — Spot-check on Shopify

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
| `preview.csv` | Dry-run output for `--import`: what will be sent to Shopify. |
| `update_preview.csv` | Diff produced by `--update` (real or dry-run): per-SKU before/after for changed rows. |
| `gw_photo_cache/current/` | Repo-local GW image cache refreshed by `--gw-refresh-cache`. Keep untracked. |
| `photo_sync_preview.csv` | Dry-run output for `--photo-sync`: per-SKU match status and source paths. |
| `photo_sync_manifest.json` | Resume manifest for live `--photo-sync`. Do not commit. |
| `photo_sync_missing.tsv` | SKUs with no matching cached image set. Appended across runs. |
| `photo_sync_ambiguous.tsv` | SKUs with ambiguous cached image sets or Shopify SKU matches. Appended across runs. |
| `photo_sync_failures.tsv` | Live photo-sync failures. Appended across runs. |
| `sync.log` | Append-only run log. Safe to delete. |
| `failures.tsv` | Any failed creates with their error. Useful for retry. |

## Common gotchas

- **"Could not authenticate"** → token is wrong or app isn't installed. Re-do step 1.8, then rerun `--preflight`.
- **`read_locations` is missing or preflight says there are no locations** → add `read_locations` in step 1.6, save, reinstall the app, then rerun `--preflight`.
- **`Configured SHOPIFY_LOCATION not found or inaccessible`** or **`SHOPIFY_LOCATION must be either...`** → the override is wrong. Delete `SHOPIFY_LOCATION` to use the auto-detected primary location, or replace it with a valid Shopify location GID / numeric ID.
- **"Throttled"** in the log → fine, the script waits and retries. Just let it run.
- **Some products fail with "Title has already been taken"** → Shopify enforces unique titles per product. Look at `failures.tsv`; usually a duplicate row in the sheet.
- **Inventory shows 0 even though sheet has stock** → the script is probably using the wrong location. Leave `SHOPIFY_LOCATION` blank for the primary location, or set a valid override if your stock lives elsewhere.
- **`--gw-refresh-cache` only supports direct image files from the GW Product Images area** → first pass is limited to `.jpg`, `.jpeg`, and `.png`. Ignore ZIPs, PDFs, WEBP, GIF, TIFF, and HTML pages.
- **`photo_sync_missing.tsv`, `photo_sync_ambiguous.tsv`, and `photo_sync_failures.tsv` keep appending** → clear or archive them before a fresh review pass if you do not want stale rows mixed in.
- **`--photo-sync` is SKU-sensitive** → exact product-code/SKU matches are preferred; fallback title-slug matching can become ambiguous.
- **`--photo-sync` runs separately** → do not combine it with `--delete`, `--import`, `--update`, or `--all`.

## Want this scheduled?

Now that `--update` exists, scheduling is straightforward: a daily/weekly cron or scheduled task that runs `python3 shopify_sync.py --update` will keep Shopify's prices and inventory aligned with the sheets. Adding new products still goes through `--import`. Tell me if you want me to wire it up.
