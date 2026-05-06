# Setup

## Requirements

The project is a local Python CLI with a small dependency set:

- Python 3.10+ is the practical baseline.
- `pandas`
- `openpyxl`
- `requests`

Install from [requirements.txt](../requirements.txt).

## Create a Local Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The repo does not need a build step, packaging step, or service startup step.

## Configure Shopify Access

Copy the example env file:

```bash
cp .env.example .env
```

Fill in:

- `SHOPIFY_STORE`
  The store subdomain only, for example `telemachus-foxfable`.
- `SHOPIFY_TOKEN`
  A Shopify Admin API access token, typically starting with `shpat_`.
- `SHOPIFY_LOCATION`
  Optional. Leave blank to auto-detect the primary location. If set, numeric IDs and Shopify GIDs are both accepted and normalized.
- `PHOTO_SOURCE_SUPPLIER_ROOTS`
  Optional. Comma-separated local directories checked before web search for staged supplier photos.
  The current matcher accepts SKU-prefixed supplier folders, including split-code SKUs with spaces or hyphens. Verified examples include:
  - `985-47709-Pop-Vinyl-Batman-1989-Joker-w-Hat-w-Chase`
  - `985 47709-pop-vinyl-batman-1989-joker-w-hat-w-chase`
  - `120000594-bookshop-window-puzzle`
  - `900 JKM91-jurassic-world-hammond-collection-ornitholestes`
  - `100-10111-pokemon-cards-sv-prismatic-evolutions`
  Slug-only fallback folders also work when the folder name matches the product title slug, for example `Bookshop-Window-Puzzle`.

OS environment variables override `.env` values if both are present.

## Source Data Expected By The Repo

The code expects these spreadsheet files in the repo root:

- `Games Workshop Store List.xlsx`
- `everything else.xlsx`

The import and update flows read directly from those files. If they are missing, strict modes fail fast.

## First-Run Validation

Run:

```bash
python shopify_sync.py --preflight
```

This checks:

- Shopify authentication
- the shop name
- the inventory location that later update/import phases will use

If you only plan to use photo workflows, the script runs a lighter photo preflight automatically for those commands.

## Safe First Commands

### Preview the parsed catalog

```bash
python shopify_sync.py --dry-run
```

This writes `preview.csv` and does not call Shopify.

### Preview SKU updates

```bash
python shopify_sync.py --update --dry-run
```

This reads Shopify, compares by SKU, and writes `update_preview.csv`.

### Preview media recovery

```bash
python shopify_sync.py --photo-source-web-all --dry-run
python shopify_sync.py --recover-zero-media-images --dry-run
```

These create reviewable media artifacts without applying product changes.

Current recovery notes:

- On the current stale zero-media snapshot, a local simulation showed the full non-GW non-book tail (`80` rows) can stage as supplier-local winners when the supplier folders are present and named in the accepted forms above.
- The book-provider lane is vendor-gated. The current allowlist covers the book-heavy vendors seen in the stale queue, including `VIZ Media LLC`, `Poisoned Pen Press`, `Scholastic Inc.`, `Simon & Schuster`, `Orion Books`, `Piatkus`, `Macmillan`, `pan macmillan`, `bluebird`, and `FSC`.

## Common Operational Commands

### Catalog commands

```bash
python shopify_sync.py --import
python shopify_sync.py --update
python shopify_sync.py --all
```

Use `--start-at <n>` with `--import` or `--all` to resume a partial import.

### Collection commands

```bash
python shopify_sync.py --delete-collections
python shopify_sync.py --generate-collections
python shopify_sync.py --update-collection-images --dry-run
python shopify_sync.py --update-collection-images
```

Note that `--generate-collections` is live-only and refuses `--dry-run`.

### Store visibility commands

```bash
python shopify_sync.py --publish-online-store-backfill --dry-run
python shopify_sync.py --publish-online-store-backfill
python shopify_sync.py --reconcile-online-store-image-visibility --dry-run
python shopify_sync.py --reconcile-online-store-image-visibility
```

### GW cache commands

```bash
python shopify_sync.py --gw-refresh-cache --dry-run
python shopify_sync.py --gw-refresh-cache
python shopify_sync.py --gw-build-archive-index --dry-run
python shopify_sync.py --gw-build-archive-index
```

These maintain local discovery data used by the media workflows.

### Photo commands

```bash
python shopify_sync.py --photo-sync --dry-run
python shopify_sync.py --photo-sync
python shopify_sync.py --photo-sync-existing-files-all --dry-run
python shopify_sync.py --photo-sync-existing-files-all
python shopify_sync.py --photo-sync-staged-local-all --photo-root ./fallback_photos --dry-run
python shopify_sync.py --photo-sync-staged-local-all --photo-root ./fallback_photos
```

If `--photo-sync` runs without `--photo-root`, it expects a populated default cache in `gw_photo_cache/current`.

## Files and Directories Created During Use

Expect the repo to become a working directory with persistent state:

- `preview.csv`
- `update_preview.csv`
- `collection_generation_preview.csv`
- `collection_image_preview.csv`
- `online_store_backfill_preview.csv`
- `online_store_image_visibility_preview.csv`
- `photo_sync_preview.csv`
- `photo_source_preview.csv`
- `photo_source_review.csv`
- `photo_sync_manifest.json`
- `photo_source_manifest.json`
- `gw_photo_cache/`
- `photo_source_cache/`
- `sync.log`
- `failures.tsv`

These are normal outputs, not temporary noise.

## Running Tests

The repo uses `unittest`:

```bash
python tests/test_shopify_sync.py
```

If you are working inside a virtual environment, use that interpreter.

## Troubleshooting

### Auth failures

- Confirm `SHOPIFY_STORE` is just the subdomain, not a full URL.
- Confirm `SHOPIFY_TOKEN` is an Admin API token with the scopes needed by products, inventory, files, publications, and collections.
- Run `python shopify_sync.py --preflight`.

### Location failures

- Leave `SHOPIFY_LOCATION` blank to use auto-detection.
- If you set it manually, use either a numeric location ID or a full Shopify GID.

### Missing photo cache

If `--photo-sync` fails because the default GW cache is empty:

```bash
python shopify_sync.py --gw-refresh-cache
```

Or provide an explicit `--photo-root`.

### Noisy repo state

This codebase intentionally stores operational artifacts in the repo root. That is expected, but it means you should check `git status` carefully before committing unrelated changes.

### Destructive delete behavior

Treat `--delete` as a live destructive operation. The current implementation does not provide a true dry-run path for that phase.
