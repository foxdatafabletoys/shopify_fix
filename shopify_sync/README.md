# Shopify Sync

`shopify_sync` is a repository-local Python operations tool for maintaining a Shopify catalog from spreadsheets and repairing storefront media. The codebase is centered on one main CLI script, [shopify_sync.py](./shopify_sync.py), plus one helper module, [gw_cache_refresh.py](./gw_cache_refresh.py), that builds a local Games Workshop image cache.

The project is built for an operator workflow rather than as a reusable package. It reads source spreadsheets, normalizes them into an internal `Product` model, talks to Shopify through the Admin GraphQL API, writes preview/audit artifacts into the repo, and applies one maintenance phase at a time.

## What This Codebase Does

The current implementation supports these feature areas:

- Spreadsheet-driven catalog import from `Games Workshop Store List.xlsx` and `everything else.xlsx`.
- Catalog update by SKU for price, compare-at price, cost, and on-hand inventory.
- Shopify product deletion for rebuild workflows.
- Managed smart collection generation, retagging, and collection image maintenance.
- Online Store publication backfill.
- Store visibility reconciliation for `Online Store` and `Google & YouTube`, based on whether an active product has Shopify media.
- Games Workshop image-cache discovery and refresh from official GW resource sources and a trade-feed endpoint.
- Product image attachment from staged local files.
- Product image attachment from existing Shopify Files.
- Zero-media product image discovery from supplier roots, the GW cache, official GW resource packs, Open Library, Google Books, Amazon detail pages, and public search results.
- Conservative recovery of zero-media products by staging local winners and then applying them back to Shopify.
- Repo-local manifests, preview CSVs, and TSV audit logs for dry runs and review.

## Key Safety Model

This script is intentionally phase-oriented. Most job flags must be run on their own, and `main()` rejects unsafe combinations.

Important current behaviors:

- `--preflight` is the safest first live check. It validates auth and resolves the Shopify location used for inventory updates.
- Plain `--dry-run` builds `preview.csv` without contacting Shopify.
- `--update`, photo-sync commands, publication reconciliation, and other selective phases support real dry-run previews with Shopify reads.
- `--generate-collections` cannot run with `--dry-run`; it is implemented as a live-only rebuild.
- `--delete` currently runs a full delete pass over all products returned by Shopify and is invoked live even if `--dry-run` is also present. Treat it as destructive.

## Main Commands

The main CLI surface is:

```bash
python shopify_sync.py --preflight
python shopify_sync.py --dry-run
python shopify_sync.py --import
python shopify_sync.py --update --dry-run
python shopify_sync.py --generate-collections
python shopify_sync.py --photo-source-web-all --dry-run
python shopify_sync.py --recover-zero-media-images
```

Operational groups:

- Catalog import and cleanup:
  `--delete`, `--import`, `--update`, `--all`, `--start-at`
- Collection management:
  `--delete-collections`, `--generate-collections`, `--update-collection-images`
- Store visibility:
  `--publish-online-store-backfill`, `--reconcile-online-store-image-visibility`
- Photo and media workflows:
  `--gw-refresh-cache`, `--gw-build-archive-index`, `--photo-sync`, `--photo-sync-existing-files`, `--photo-sync-existing-files-all`, `--photo-source-web-all`, `--recover-zero-media-images`, `--photo-sync-staged-local-all`, `--photo-root`

## Typical Workflows

### 1. Validate setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python shopify_sync.py --preflight
```

### 2. Preview an import

```bash
python shopify_sync.py --dry-run
```

This reads the spreadsheets, deduplicates products by SKU, and writes `preview.csv`.

### 3. Update live catalog values

```bash
python shopify_sync.py --update --dry-run
python shopify_sync.py --update
```

This compares spreadsheet values against Shopify by SKU and writes `update_preview.csv` before you apply.

### 4. Repair missing images conservatively

```bash
python shopify_sync.py --photo-source-web-all --dry-run
python shopify_sync.py --photo-source-web-all
python shopify_sync.py --photo-sync-staged-local-all --photo-root ./photo_source_cache/current --dry-run
python shopify_sync.py --photo-sync-staged-local-all --photo-root ./photo_source_cache/current
```

Or use the single recovery lane:

```bash
python shopify_sync.py --recover-zero-media-images --dry-run
python shopify_sync.py --recover-zero-media-images
```

Supplier-root notes:

- If `PHOTO_SOURCE_SUPPLIER_ROOTS` is set in `.env`, the recovery lane tries those local supplier folders before web search.
- Exact-style supplier folders can be SKU-prefixed, including split-code SKUs with spaces or hyphens. Examples the current matcher accepts:
  - `985-47709-Pop-Vinyl-Batman-1989-Joker-w-Hat-w-Chase`
  - `985 47709-pop-vinyl-batman-1989-joker-w-hat-w-chase`
  - `120000594-bookshop-window-puzzle`
  - `900 JKM91-jurassic-world-hammond-collection-ornitholestes`
  - `100-10111-pokemon-cards-sv-prismatic-evolutions`
- Slug-only fallback folders also work when the folder name matches the product title slug, for example `Bookshop-Window-Puzzle`.
- On the current stale zero-media snapshot, a local simulation showed the entire non-GW non-book tail (`80` rows) can stage as supplier-local winners when those folders are present and named in the accepted forms above.
- The book-provider lane is vendor-gated. The current allowlist covers the book-heavy vendors seen in the stale queue, including `VIZ Media LLC`, `Poisoned Pen Press`, `Scholastic Inc.`, `Simon & Schuster`, `Orion Books`, `Piatkus`, `Macmillan`, `pan macmillan`, `bluebird`, and `FSC`.

### 5. Reconcile channel visibility after image cleanup

```bash
python shopify_sync.py --reconcile-online-store-image-visibility --dry-run
python shopify_sync.py --reconcile-online-store-image-visibility
```

This workflow reconciles `Online Store` and `Google & YouTube` together for active products only. Draft and archived products are skipped. Products with any Shopify media are published to both channels, products with no Shopify media are unpublished from both channels, and zero inventory does not block either action.

## Generated Artifacts

The repo intentionally keeps its operational outputs next to the code. Common artifacts include:

- `preview.csv`: spreadsheet import preview.
- `update_preview.csv`: SKU update diff preview.
- `collection_generation_preview.csv`: proposed managed collection memberships.
- `collection_generation_unmatched.csv`: products that did not match the managed taxonomy.
- `collection_image_preview.csv`: planned or applied collection image updates.
- `online_store_backfill_preview.csv`: publication backfill preview.
- `online_store_image_visibility_preview.csv`: visibility reconciliation preview for `Online Store` and `Google & YouTube`.
- `photo_sync_preview.csv`: photo sync decisions and outcomes.
- `photo_sync_manifest.json`: per-SKU photo-sync state tracking.
- `photo_source_preview.csv`: zero-media sourcing decisions.
- `photo_source_review.csv`: cases needing manual review.
- `photo_source_manifest.json`: source-discovery state tracking.
- `photo_source_missing.tsv`, `photo_source_ambiguous.tsv`, `photo_source_failures.tsv`: audit outputs for non-winners.
- `gw_photo_cache/`: local GW cache.
- `photo_source_cache/`: staged image winners and recovery runs.
- `sync.log`: timestamped operational log.
- `failures.tsv`: persistent failure log for import/update/media issues.

## Repository Layout

- [shopify_sync.py](./shopify_sync.py): main CLI, spreadsheet parsing, Shopify API wrapper, collection logic, publication logic, and media workflows.
- [gw_cache_refresh.py](./gw_cache_refresh.py): GW resource discovery, archive extraction, trade-feed discovery, and cache publication.
- [tests/test_shopify_sync.py](./tests/test_shopify_sync.py): large regression suite covering CLI behavior, Shopify GraphQL handling, collection rules, publication flows, media sync, and source discovery.
- [requirements.txt](./requirements.txt): minimal runtime dependencies.
- [docs/architecture.md](./docs/architecture.md): internal structure and data flow.
- [docs/setup.md](./docs/setup.md): environment and operator setup.
- [docs/how-it-works.md](./docs/how-it-works.md): end-to-end workflow explanation.

## Documentation Map

- Architecture: [docs/architecture.md](./docs/architecture.md)
- Setup: [docs/setup.md](./docs/setup.md)
- How it works: [docs/how-it-works.md](./docs/how-it-works.md)

## Testing

The repo uses the standard library `unittest` suite in `tests/test_shopify_sync.py`.

```bash
python tests/test_shopify_sync.py
```
