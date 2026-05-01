# GW Trade-Feed Image Fallback

**Date:** 2026-05-01
**Mode:** plan / direct (interview confirmed scope)
**Repo:** shopify_fix
**Branch (start):** main
**Plan owner:** data@foxandfabletoysellers.com

## Required input before implementation

- **`GW_TRADE_FEED_URL`** — the exact URL on `trade.games-workshop.com` of the dated "TR-…-Download jpg" listing the user described. Implementation cannot start without this constant. The plan calls it out as a single-line code change; everything else is wired around it.

## Requirements summary

Extend the existing Games Workshop image workflow (`shopify_sync/gw_cache_refresh.py` + `phase_photo_sync` in `shopify_sync/shopify_sync.py`) so that, in addition to the existing `https://trade.games-workshop.com/resources/` resource-packs index, it also pulls images from a second `trade.games-workshop.com` listing whose entries follow the format:

```
<DD/MM/YYYY HH:MM (AM|PM)>  TR-<cat>-<seq>-<SKU>-<slug>  Download jpg
```

The new feed is the **fallback source** for the **5,402 GW SKUs in `photo_sync_missing.tsv`** (paint pots, dice, magazines, starter sets, etc. that are not present in `/resources/` packs) and **must disambiguate the 18 SKUs in `photo_sync_ambiguous.tsv`** (multiple matches in `/resources/` for the same SKU). The SKU is literally embedded in the filename slug, so matching is exact-by-code, not fuzzy.

**Integration shape (decided in interview):** *Extend `gw_cache_refresh.py`* — same cache directory, same status JSON, same downstream `--photo-sync`, single `--gw-refresh-cache` CLI flag fetches both sources.

**Default scope (decided in interview):** *Fill misses + overwrite ambiguous*. SKUs already at `state=media_applied` from `/resources/` are left untouched. SKUs flagged ambiguous are re-resolved with the trade feed taking priority because its SKU-in-filename match is higher-confidence than fuzzy title slugs.

**Feed shape (decided in interview):** Unknown / verify at runtime. Implementation includes an adaptive probe that handles three possibilities: full archive, rolling recent-additions window, or paginated archive.

## Acceptance criteria

All criteria are testable (unit tests, dry-run output, or numeric counts in status JSON).

1. **New constant exists**: `shopify_sync.py` defines `GW_TRADE_FEED_URL = "<user-provided>"` next to `GW_RESOURCES_URL` (line 141).
2. **New discovery function**: `gw_cache_refresh.py` exposes `discover_trade_feed_packs(url: str, session: requests.Session) -> tuple[list[ResourcePack], str]` with the same return shape as `discover_resource_packs` (line 315) so it can be merged with no other changes.
3. **Filename slug parser**: a unit test `test_parse_trade_feed_entry` proves the parser extracts `(date_str, sku, slug, image_url)` from a fixture HTML containing at least these three real shapes:
   - `TR-06-214-99122720012-Warhammer-The-Old-World-Grand-Cathay-Peasant-Levy`
   - `TR-WD05-60-60249999666-White-Dwarf-524-ENG`
   - `TR-BL3315-60040181450-Black-Library-Hive-Slipcase-Version-Part-1-and-2-ENG`
   And rejects malformed lines without raising.
4. **Pagination probe**: `discover_trade_feed_packs` follows `rel="next"` / `?page=N` / "Load more" if present, capped at `MAX_TRADE_FEED_PAGES = 200`. Stops early when a page yields 0 new image URLs (deduped against `seen_urls`). Probe outcome is logged to status JSON as `trade_feed.pagination = "single|next-link|page-param|exhausted"`.
5. **Cache merge**: `refresh_gw_cache` (line 508) calls `discover_resource_packs` then `discover_trade_feed_packs`, concatenates the two pack lists, and continues the existing publish flow with no changes to `publish_staging_cache` or filesystem layout. A unit test asserts that when both discoverers return packs, the resulting cache contains both sets of files.
6. **Filename preservation**: trade-feed images written to `gw_photo_cache/current/<pack-dir>/` keep the SKU-bearing slug as the basename so `_extract_asset_match_code(pack.label)` (called by `build_photo_source_gw_cache_indexes`, `shopify_sync.py:2068`) extracts the correct SKU. A unit test asserts `_extract_asset_match_code("TR-06-214-99122720012-…")` returns `"99122720012"`.
7. **Source-priority tiebreaker**: when the same SKU has both a `/resources/` asset and a trade-feed asset, `resolve_photo_asset` (`shopify_sync.py:2272`) prefers the trade-feed asset. Implementation: tag each `PhotoAssetSet` with a `source_priority: int` (trade-feed=10, resources=5) at index time and update `_choose_best_photo_asset_set` to sort by priority desc before existing title-similarity ranking. A unit test asserts that with one ambiguous SKU and a trade-feed candidate, `match_type == "exact_best"` and the chosen asset's source is `trade-feed`.
8. **Ambiguous count → 0**: after a real `--gw-refresh-cache` followed by `--photo-sync --dry-run`, `photo_sync_ambiguous.tsv` has 0 lines (down from 18). Verified by running the command and checking `wc -l`.
9. **Missing count drops by ≥ 50%**: after the same run, `photo_sync_missing.tsv` line count is at most 2,701 (50% of 5,402). Threshold conservative because some missing SKUs may not exist on the trade feed at all (e.g., discontinued items). Final number recorded in `gw_photo_cache_status.json` under `trade_feed.matched_sku_count`.
10. **Status JSON extended**: `gw_photo_cache_status.json` gains a `trade_feed` sub-object: `{ url, started_at, finished_at, last_success_at, image_count, page_count, pagination_strategy, matched_sku_count, failure_reason }`. A unit test asserts the schema exists after a dry run.
11. **No regression on existing flow**: all 13 existing tests in `tests/test_shopify_sync.py` that touch `gw_*` or `photo_sync_*` still pass. Specifically `test_gw_refresh_cache_runs_without_shopify_credentials` (line 1054), `test_photo_sync_dry_run_writes_preview_and_makes_no_writes` (line 2122), `test_photo_sync_live_run_uses_file_first_sequence` (line 2162).
12. **Docs**: `shopify_sync/SETUP.md` "GW auto-download + photo sync" section (line 67–107) gains a paragraph documenting the trade-feed source, the `GW_TRADE_FEED_URL` constant, and the priority-over-resources behavior for ambiguous SKUs.

## Implementation steps

### Step 1 — Add the constant and CLI passthrough
- File: `shopify_sync/shopify_sync.py` near line 141 (alongside `GW_RESOURCES_URL`).
- Change: add `GW_TRADE_FEED_URL = "<user-provided>"`.
- File: `shopify_sync/shopify_sync.py` argparse block (line 5976+).
- Change: pass `trade_feed_url=GW_TRADE_FEED_URL` into `gw_cache_refresh.refresh_gw_cache(...)` at the call site of `--gw-refresh-cache`. No new CLI flag.

### Step 2 — Implement `discover_trade_feed_packs`
- File: `shopify_sync/gw_cache_refresh.py` immediately after `discover_resource_packs` (line 315–407).
- Reuse: `AnchorParser` (line 59), `parse_anchors` (line 154), `fetch_text` (line 182), `is_supported_image_url` (line 132), `IMAGE_SUFFIXES` (line 20).
- Logic:
  1. `fetch_text(session, url)` → HTML.
  2. `parse_anchors(html)` → list of `AnchorRecord`. For each anchor whose `href` matches `IMAGE_SUFFIXES`, derive the slug from the link text or filename (the `TR-…` portion before `.jpg`).
  3. Build one `ResourcePack` per image: `ResourcePack(label=<TR-slug>, images=[ImageTarget(url=<img_url>, filename=<TR-slug>.jpg)], archives=[])`. Each pack is one image so the existing publish loop (line 568–597) drops files into per-pack subdirs and `_extract_asset_match_code` runs on the slug.
  4. Pagination: detect `<a rel="next">` or `?page=N` in the same HTML; if found, recurse on the next page. Cap at `MAX_TRADE_FEED_PAGES = 200`. Maintain `seen_urls: set[str]` across pages; stop early when a page produces zero new URLs.
  5. Return `(packs, source_marker="GW Trade Feed")`.

### Step 3 — Wire into `refresh_gw_cache`
- File: `shopify_sync/gw_cache_refresh.py` line 508.
- Change: add parameter `trade_feed_url: str | None = None`. After `packs, source_marker = discover_resource_packs(resources_url, session)` (line 521), if `trade_feed_url`, call `feed_packs, feed_marker = discover_trade_feed_packs(trade_feed_url, session)` and `packs.extend(feed_packs)`. Track per-source counts in the `status` dict under a new `trade_feed` sub-object. The dry-run branch logs both counts separately.

### Step 4 — Source-priority on the index
- File: `shopify_sync/shopify_sync.py` near `PhotoAssetSet` definition (search for `class PhotoAssetSet` / `@dataclass class PhotoAssetSet`). Add `source_priority: int = 0` (defaulted so existing constructors still work).
- File: `shopify_sync/shopify_sync.py` `discover_photo_asset_sets` (referenced from line 2072). When a discovered asset set sits inside a directory whose name starts with `TR-` (the trade-feed pack-dir prefix), set `source_priority = 10`. Otherwise leave at default `5` for `/resources/` packs.
- File: `shopify_sync/shopify_sync.py` `_choose_best_photo_asset_set` (referenced from line 2281). Sort candidates by `(-source_priority, -similarity_score)` instead of similarity alone. This is the only place the new field is read.

### Step 5 — Status JSON extension
- File: `shopify_sync/gw_cache_refresh.py` `refresh_gw_cache` (line 545–558 status dict, line 601–610 success update).
- Change: add `trade_feed: {url, image_count, page_count, pagination_strategy, matched_sku_count: None}`. `matched_sku_count` is filled in by a separate post-publish step that diffs the cache index against `photo_sync_manifest.json`. Implement that as a new helper `count_trade_feed_matches(cache_root, manifest_path) -> int`.

### Step 6 — Tests
- File: `shopify_sync/tests/test_shopify_sync.py`.
- Add fixtures and tests:
  - `test_parse_trade_feed_entry_shapes` — covers acceptance criterion 3.
  - `test_discover_trade_feed_packs_paginates_until_empty` — mocks `fetch_text` to return three pages with `?page=2`/`?page=3`, asserts cap-aware termination.
  - `test_refresh_gw_cache_merges_resources_and_trade_feed` — mocks both discoverers, asserts merged `packs` and that `gw_photo_cache_status.json` has both sub-objects after publish.
  - `test_resolve_photo_asset_prefers_trade_feed_for_ambiguous` — covers acceptance criterion 7.
  - `test_status_json_has_trade_feed_subobject` — covers criterion 10.
- Update existing assertion in `test_gw_refresh_cache_runs_without_shopify_credentials` (line 1054) so it tolerates the new status sub-object (no breaking change).

### Step 7 — Docs
- File: `shopify_sync/SETUP.md`.
- Insert a "Trade feed (fallback for misses)" subsection under "GW auto-download + photo sync" (line 67–107). Document the `GW_TRADE_FEED_URL` constant, the source-priority rule, and the expected drop in `photo_sync_missing.tsv` / `photo_sync_ambiguous.tsv`.

## Risks and mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|-----------|
| R1 | The trade-feed page requires session cookies/JS that the existing `requests`-only fetcher cannot render. | Medium | Blocks the workflow entirely. | Step 2 starts with a manual `curl -sL <url>` probe. If JS is required, fall back to a `playwright`-headless render only for that one URL. Plan adds a feature flag `GW_TRADE_FEED_RENDER = "static" \| "headless"`. |
| R2 | The "Download jpg" action is not a direct `<a href="…jpg">` but a click handler that POSTs / redirects. | Medium | Image URL parsing fails. | Probe step inspects the actual anchor: if href is `javascript:`, capture the `data-*` attribute that holds the asset id and construct `https://trade.games-workshop.com/asset/<id>.jpg` (validated against one real example before writing the parser). |
| R3 | Pagination uses infinite scroll (XHR) instead of links. | Low–Medium | Only top-of-feed entries scraped. | Probe in Step 2 inspects network behavior. If XHR, replace the link-following loop with a `?page=N` or `?cursor=…` loop driven by the JSON endpoint discovered. Acceptance criterion 4 already permits this branch. |
| R4 | The trade feed contains entries whose slug parses to a SKU we already have, but the **image is wrong** (e.g., generic placeholder or stock photo from a different region). | Medium | Wrong images uploaded for some SKUs. | The source-priority tiebreaker is per-SKU, not per-image. Add a one-time sanity dry-run: for each ambiguous-overwrite candidate, log a side-by-side preview row in `photo_sync_preview.csv` (existing CSV) with `source=trade-feed` so the user can spot-check before running live. |
| R5 | Filename collisions when `/resources/` and trade-feed both publish a file named `TR-…-99122720012.jpg`. | Low | One file overwrites the other in `gw_photo_cache/current/`. | Existing `unique_filename` helper (`gw_cache_refresh.py:225`) already disambiguates with `(N)` suffixes. No new code, but add an explicit unit test for the collision case. |
| R6 | The trade feed page is huge (10k+ entries) and a full crawl exhausts disk or takes hours. | Low | Slow first-time refresh. | `MAX_TRADE_FEED_PAGES=200` cap. Status JSON records page count; if cap is hit, log a clear warning. Cache writes are streamed (existing behavior in `fetch_binary`). Disk: 5,402 SKUs × ~300KB JPG ≈ 1.6GB worst case — acceptable. |
| R7 | Re-running `--gw-refresh-cache` after the trade feed grows wastes bandwidth re-downloading every image. | Medium | Slow weekly refresh. | The publish flow already wipes `_staging` and rebuilds from scratch (line 561). For now, accept the trade-off — adding incremental refresh is out of scope for this plan. Note as a follow-up. |
| R8 | The 18 ambiguous SKUs include legitimate cases where multiple trade-feed entries also share a SKU (e.g., a paint and a paint-set both branded with the same code). | Low | Source-priority ties don't disambiguate. | When two trade-feed candidates remain after priority sort, fall through to existing title-similarity tiebreaker. If still tied, mark as ambiguous (not silently pick one). Existing `resolve_photo_asset` already handles this. |
| R9 | URL is wrong / requires region selection (e.g., `/uk/` vs `/us/`). | Medium | 0 results returned. | Step 2 probe verifies non-empty page parse before proceeding. If empty, log clearly and exit non-zero so the user notices. |

## Verification steps

In order:

1. `python -m pytest shopify_sync/tests/test_shopify_sync.py -k "gw or photo_sync or trade_feed" -v` — all green, including 5 new tests from Step 6.
2. `python shopify_sync/shopify_sync.py --gw-refresh-cache --dry-run` — output reports both `discover_resource_packs` count AND `discover_trade_feed_packs` count, no network writes.
3. `python shopify_sync/shopify_sync.py --gw-refresh-cache` — full refresh, exit 0, `gw_photo_cache_status.json.trade_feed.image_count > 0`, `pagination_strategy` set to one of `single|next-link|page-param|exhausted`.
4. `python shopify_sync/shopify_sync.py --photo-sync --dry-run` — `wc -l photo_sync_ambiguous.tsv` == 0; `wc -l photo_sync_missing.tsv` ≤ 2701; `photo_sync_preview.csv` shows new rows with `source=trade-feed` for previously-missing SKUs.
5. Manual spot-check: open 10 randomly-sampled rows of `photo_sync_preview.csv` where `source=trade-feed` and confirm the image URL renders the correct product in a browser.
6. `python shopify_sync/shopify_sync.py --photo-sync` (live) — runs to completion against Shopify; resulting `photo_sync_manifest.json` shows the previously-missing SKUs at `state=media_applied` with `source=trade-feed` in the `reasons` list.

## Out of scope

- Incremental / delta refresh of the trade feed (always full refresh for now; risk R7).
- A separate CLI flag for "trade-feed only" refresh (would re-introduce code paths that the chosen integration shape avoids).
- Replacing matches that already came from `/resources/` and are currently `state=media_applied` (only ambiguous and missing are touched, per scope decision).
- Generalizing the trade-feed scraper to non-GW vendors. The new code lives inside `gw_cache_refresh.py` and is GW-specific.

## Open questions

- **GW_TRADE_FEED_URL** — provide before Step 1.
- Sample HTML — if you can paste the HTML of a single entry (anchor + surrounding tags), the parser can be written and tested without a probe round-trip.

## Plan file location

`.omc/plans/gw-trade-feed-image-fallback.md`
