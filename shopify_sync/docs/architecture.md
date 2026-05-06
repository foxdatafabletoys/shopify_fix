# Architecture

## Overview

This repository is an operations script with a thin helper module, not a layered web application. Most of the system lives in `shopify_sync.py`, which combines:

- configuration loading
- spreadsheet parsing and normalization
- product and media matching logic
- Shopify Admin GraphQL calls
- per-phase orchestration
- local preview and manifest writing

`gw_cache_refresh.py` is the main supporting boundary. It is responsible for discovering Games Workshop image packs from official resource sources, normalizing them into a cache structure, and publishing that cache into a repo-local current directory that the photo workflows can consume.

## Core Components

### 1. Configuration and environment

`load_env()` reads a local `.env` file and then lets OS environment variables override it. The key runtime settings are:

- `SHOPIFY_STORE`
- `SHOPIFY_TOKEN`
- `SHOPIFY_LOCATION`
- `PHOTO_SOURCE_SUPPLIER_ROOTS`

This keeps the script deploy-free: the repo itself is the runtime environment.

### 2. Data model

The main internal records are dataclasses:

- `Product`: normalized catalog record derived from spreadsheets.
- `PhotoAssetSet`: a local folder or grouped set of images that can be matched back to products.
- `GWOfficialResourcePackRef`: reference to an official GW resource pack plus optional archive-member metadata.
- `ShopifyImageFile`: normalized Shopify File/media record used by existing-file linking.
- `PhotoSourceSearchResult`, `PhotoSourceCandidate`, `PhotoSourceDecision`: the search/scoring model for missing-image recovery.

The important architectural point is that nearly every workflow normalizes its inputs into one of these structures before it decides whether to write preview files or call Shopify.

### 3. Spreadsheet ingestion

The source catalog comes from two spreadsheets:

- `Games Workshop Store List.xlsx`
- `everything else.xlsx`

The code parses each into `Product` objects, applies pricing rules, extracts metadata such as barcode/vendor/tags, and then merges and deduplicates products by SKU.

The pricing logic is domain-specific:

- Games Workshop products are discounted from UKR.
- Funko products are discounted from SRP.
- Other inventory uses SRP directly.

Inventory quantity and cost are also normalized here, which means downstream Shopify phases operate on one canonical model instead of raw sheet rows.

### 4. Shopify API layer

The `Shopify` class is the system’s network boundary. It owns:

- the GraphQL endpoint and auth headers
- retry handling for `429`, `5xx`, and GraphQL throttle errors
- pagination across products, collections, files, and jobs
- mutations for product creation, updates, deletion, collection management, publication, media/file attachment, and metafield definition setup

Architecturally, the rest of the script does not build ad hoc HTTP requests. It routes Shopify operations through this wrapper, which centralizes:

- auth failure reporting
- throttling behavior
- GraphQL error formatting
- polling for async file/job readiness

### 5. Phase orchestration

The main program is phase-driven. `build_parser()` defines mutually exclusive or operationally isolated job flags. `main()` then:

1. validates flag combinations
2. performs special-case dry-run handling
3. loads environment and authenticates
4. resolves either import preflight or photo-only preflight
5. dispatches to exactly one safe operational lane, or a small fixed sequence in the case of `--all`

This is the main architectural control surface. The repo is intentionally not event-driven or service-based; it is a command-phase dispatcher.

## Major Execution Lanes

### Catalog lane

This lane uses spreadsheet parsing and SKU matching to manage the core catalog:

- `phase_delete()`
- `phase_import()`
- `phase_update()`

The import and update phases operate on normalized `Product` objects. Import creates products and publishes them to the Online Store. Update reads existing variants by SKU and pushes price/cost/inventory changes.

### Collection lane

Collection management is built around a managed taxonomy:

- `build_collection_matches()`
- `phase_generate_collections()`
- `phase_update_collection_images()`

The generation flow classifies products into a fixed spec set, removes only legacy managed tags, preserves unrelated tags, recreates smart collections, publishes them, and sets collection images from the first alphabetical product image.

This lane is intentionally opinionated: it treats collections as generated state, not hand-managed content.

### Publication lane

Storefront visibility is handled separately from catalog import:

- `phase_publish_online_store_backfill()`
- `phase_reconcile_online_store_image_visibility()`

The first ensures products are published to the Online Store publication. The second makes visibility follow media presence so zero-image products can be unpublished automatically.

### Photo cache lane

`gw_cache_refresh.py` supports the photo system by building a local GW cache from:

- the official GW resources page
- downloadable archives
- the GW trade-feed API

It stages discoveries into a temporary location and then publishes into `gw_photo_cache/current`. That publish step is important because it gives the rest of the code a stable read target.

### Photo sync lane

`phase_photo_sync()` is the main media application engine. It can source images from:

- staged local image folders
- existing Shopify Files

It discovers existing Shopify products by SKU, matches image candidates by product code or normalized title slug, writes preview rows, and then either uploads/attaches files or records missing/ambiguous/failure outcomes.

For the all-catalog staged-local mode, it can also ensure a fallback-image metafield definition and write audit state for products that used fallback media.

### Photo source lane

`phase_photo_source_web_all()` is the most complex decision engine in the codebase. It is responsible for finding likely images for products with zero Shopify media.

Its rough architecture is:

1. identify candidate products with no media
2. build a product-specific query
3. consult local supplier roots
4. consult the GW cache and official GW pack indexes where relevant
5. consult book-oriented sources such as Open Library, Google Books, and Amazon detail pages
6. fall back to public search providers and fetch a bounded number of candidate pages
7. score candidate images
8. choose `winner`, `review`, `missing`, or `failed`
9. write preview/review/audit outputs
10. if live, download only winning images into the local cache plus `_source.json` metadata

This lane is intentionally conservative. It uses thresholds, ambiguity checks, and margin checks so weak candidates get routed to review instead of being auto-applied.

### Recovery lane

`recover_zero_media_images()` composes two existing lanes:

1. run the photo source workflow into an isolated `photo_source_cache/recovery_runs/<timestamp>/winners` directory
2. if winners exist, run photo sync against that directory

That separation is a useful architectural pattern in this repo: discovery and application are kept distinct even when there is a convenience command to chain them.

## Persistent State and Artifacts

The script relies heavily on repo-local artifacts instead of a database:

- CSV previews for human review
- TSV failure and ambiguity logs
- JSON manifests for per-SKU workflow state
- local image cache directories
- `sync.log` for timestamped execution history

This makes the codebase operationally transparent. The tradeoff is that the repo directory is stateful and accumulates large working artifacts over time.

## Testing Strategy

The test suite in `tests/test_shopify_sync.py` is broad and behavior-focused. It covers:

- GraphQL error handling and retry behavior
- CLI argument handling
- location and preflight logic
- product creation and update flows
- collection generation and image updates
- publication workflows
- photo sync and fallback media application
- zero-media discovery and recovery
- GW cache refresh and trade-feed parsing

Architecturally, that means the repo’s safety net is concentrated in one large regression file rather than distributed across small unit-test modules.

## Important Design Tradeoffs

- Single-script design: easy to operate locally, harder to modularize.
- Repo-local state: transparent and auditable, but noisy and heavy.
- Phase isolation: safer for store operations, slower for bulk operator workflows.
- Conservative media automation: fewer bad images applied automatically, more review outputs to manage.
- GraphQL wrapper centralization: simpler operational semantics, but the `Shopify` class is large and multi-responsibility.

## Risks and Sharp Edges

- Top-level prose comments and the actual implementation are not always perfectly aligned; operator docs should follow live code behavior.
- `--delete` is destructive and currently not guarded by a true dry-run path.
- The repo mixes source code with generated operational outputs, so it is easy for the worktree to become noisy.
- The code assumes spreadsheet shape and vendor-specific heuristics that are tightly coupled to this business workflow.
