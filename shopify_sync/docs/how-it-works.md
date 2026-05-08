# How It Works

## End-to-End Model

This codebase works like an operator-controlled pipeline:

1. read source spreadsheets
2. normalize rows into internal product records
3. optionally query Shopify for current state
4. build a preview or decision report
5. apply one operational phase
6. write logs, manifests, and review artifacts back into the repo

It is not a daemon and it does not maintain its own external database. Every run starts from local files plus live Shopify reads.

## Product Import Flow

The import path starts with `prepare_products_for_import()`, which:

- parses the Games Workshop spreadsheet
- parses the general inventory spreadsheet
- skips GW rows from the general sheet to avoid double counting
- deduplicates the merged catalog by SKU
- writes `preview.csv`

Each row is normalized into a `Product` dataclass with:

- title
- SKU
- barcode
- vendor
- product type
- tags
- HTML description
- price and compare-at price
- cost
- weight
- quantity
- source marker

After preflight, `phase_import()` creates Shopify products and then attempts to publish them to the Online Store.

## Update Flow

The update lane uses the same parsed `Product` list, but instead of creating records, it queries existing Shopify variants by SKU and compares:

- price
- compare-at price
- cost
- on-hand inventory

In dry-run mode it writes `update_preview.csv`. In live mode it applies only the changed values through GraphQL mutations.

The design assumption is that SKU is the stable join key between spreadsheets and Shopify.

## Collection Flow

Managed collections are generated from product classification rules rather than curated by hand inside Shopify.

The flow is:

1. fetch current Shopify products with enough metadata to classify them
2. match each product against a fixed collection-spec list
3. write a collection membership preview and unmatched-product report
4. remove existing collections
5. update product tags so only the managed taxonomy is regenerated while unrelated tags remain
6. create smart collections for non-empty groups
7. publish those collections to sales channels when possible
8. set collection images from the first alphabetical product that already has an image

This makes collections generated state. If someone hand-edits the managed taxonomy in Shopify, the next generation pass can overwrite that intent.

## Publication Flow

There are two publication-related maintenance jobs.

`--publish-online-store-backfill`:

- finds products that exist but are not published to the current Online Store publication
- writes a preview in dry-run mode
- publishes them in live mode

`--reconcile-online-store-image-visibility`:

- checks active products only and skips draft or archived products
- treats zero inventory as still eligible for publish or unpublish decisions
- checks whether products have attached Shopify media
- publishes products with media to `Online Store` and `Google & YouTube`
- unpublishes products without media from `Online Store` and `Google & YouTube`

This lane is meant to keep channel visibility aligned with product completeness, using the same media-driven rule in both directions.

## GW Cache Flow

The Games Workshop image cache is a local intermediate asset store. `gw_cache_refresh.py` builds it by:

- parsing anchors from the official GW resources page
- recognizing direct image links, HTML landing pages, and supported archives
- downloading assets with retries
- extracting ZIP archives when needed
- flattening and normalizing filenames
- discovering additional packs from a trade-feed API
- staging everything into a temporary cache root
- publishing the staged result into `gw_photo_cache/current`

The photo-sync lane uses that published cache as a stable local source of truth.

## Photo Sync Flow

`phase_photo_sync()` is the application layer for product media.

It supports two main source modes:

- staged local files
- existing Shopify Files

And two product scopes:

- GW-only
- full catalog

The matching flow is:

1. identify existing Shopify products and their SKUs
2. build image indexes by product code and title slug
3. for each in-scope product, try to find a single confident image set
4. mark missing or ambiguous cases in preview and TSV outputs
5. if live, upload or attach files
6. wait for file readiness
7. attach files to products and reorder media
8. if fallback-audit mode is enabled, update a metafield definition and write per-product fallback state

The all-catalog staged-local mode is what turns already-reviewed local images into applied product media.

## Photo Source Discovery Flow

The photo-source lane exists for products that have no Shopify media yet.

The process is deliberately conservative:

1. load the current photo-source manifest
2. identify in-catalog products with zero Shopify media
3. build a product-specific search query
4. check local supplier roots first
5. check GW cache and official GW sources for relevant products
6. check book-oriented sources for books and similar products
7. if needed, query public search providers
8. fetch a limited number of result pages
9. score candidate images using product-code, title, and page-detail signals
10. classify the outcome as `winner`, `review`, `missing`, or `failed`
11. write CSV/TSV outputs for operator review
12. if live, download only winners into a local pack directory and write `_source.json`

The main safety mechanism is that close calls become review rows instead of automatic product changes.

## Recovery Flow

`--recover-zero-media-images` is a composed workflow:

1. create a timestamped recovery run directory under `photo_source_cache/recovery_runs`
2. run the photo-source discovery lane into that isolated winner root
3. if winner packs were created, immediately run photo sync against just those staged winners

This is useful because it keeps each recovery attempt self-contained and reproducible.

## Logging, Manifests, and Review Artifacts

The code writes status continuously to:

- `sync.log`
- preview CSV files
- TSV issue logs
- JSON manifests

This means a run leaves behind enough evidence to answer:

- what was attempted
- what matched
- what was skipped
- what failed
- what still needs review

That artifact-first design is one of the core operating principles of the repo.

## Why The Code Looks This Way

This repository optimizes for a human operator running controlled maintenance jobs against a real Shopify store. That leads to a few consistent design choices:

- one large script instead of many deployable services
- explicit one-phase commands instead of automatic chaining
- dry-run previews before most writes
- local caches instead of repeated remote fetches
- conservative image automation with review queues
- persistent repo-local artifacts for traceability

The result is operationally practical, even though it is less modular than a typical library or web app.
