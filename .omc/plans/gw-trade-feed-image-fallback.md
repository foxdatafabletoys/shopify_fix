# GW Trade-Feed Image Fallback (revised after probe)

**Date:** 2026-05-01
**Mode:** plan / direct
**Repo:** shopify_fix
**Branch (start):** main
**Status:** all open questions resolved by live probing — ready to implement

## Resolved unknowns (all probed against the live site, 2026-05-01)

| Unknown | Resolution |
|---|---|
| Where do the missing-SKU images come from? | The `/resources/` page is a **WordPress media library** that pulls assets via a REST endpoint — `GET https://trade.games-workshop.com/wp-json/gw/v2/media`. The static HTML has no anchors, which is why the existing `discover_resource_packs` scraper finds zero entries here. |
| Required parameters | `fe=1&group=<term-id>&per_page=24&page=<N>&lang=en&country=220` (220 is the UK country term-id; comes from the `gw_country_id` cookie on first page load and falls back to query param). |
| Auth | None. Public endpoint. **But** Cloudflare blocks plain `python-urllib` user agents — must send a normal browser `User-Agent` and a `Referer: https://trade.games-workshop.com/resources/` header. Nonce in `gwAssetData.nonce` is **not validated** for unauthenticated GETs. |
| JSON schema | `{ page, page_count, total_items, assets: [{id, title, file_name, file_url, file_small_url, file_large_url, filesize, mime_type, file_caption, file_alt_text, cover_image_*, downloadable, countries[]}], nonce }` |
| Image-bearing groups | `46` ("Images & Logos" — 9,723 image/jpeg assets, 406 pages of 24) and `47` (4,811 image/jpeg assets, 201 pages of 24). Other groups serve PDFs / spreadsheets / archives — skip. |
| SKU is in the asset shape | Yes — `file_name` is `<SKU>_<slug>.jpg` (e.g., `99122720012_PeasantLevyBOX.jpg`). The `title` field also has a `TR-<cat>-<seq>-<SKU>-<slug>` form. Existing `_extract_asset_match_code` (`shopify_sync.py:927`) already handles this filename shape (it picks the longest 8-14 digit run). |
| Coverage | 6-page sample of g46+g47 contained 67 unique SKUs; **59 of 67 (88%) appear in `photo_sync_missing.tsv`**. Project full crawl covers a large fraction of the 2,082 unique missing SKUs (line file has duplicates; unique count is 2,082, not 5,402). |
| Pagination | Server returns `page_count` in every response — deterministic, no probe needed. Walk `page=1..page_count`. |

## Why the existing cache doesn't already have these

`gw_cache_refresh.discover_resource_packs` reads the static HTML at `https://trade.games-workshop.com/resources/`, parses `<a>` tags, and follows `.jpg`/`.zip` hrefs. The `media-library__results` container in the static HTML is **empty**; tiles are rendered client-side from the JSON endpoint. The existing scraper therefore only sees the 2 hard-coded ZIP packs (e.g., `B200a 11_24.zip`) that happen to be in the HTML for cookie-set users. It has never seen any of the 14k+ JPGs.

## Requirements summary

Add a new discovery path inside `shopify_sync/gw_cache_refresh.py` that calls the `gw/v2/media` REST endpoint, walks both image-bearing groups (46, 47), and lands JPGs into the existing `gw_photo_cache/current/` tree. The downstream `--photo-sync` flow needs no behavior change — it already reads the cache and matches on filename SKU via `_extract_asset_match_code`. A small `source_priority` tweak makes trade-feed assets win over the legacy `/resources/` packs for the 18 ambiguous SKUs.

**Integration shape:** extend `gw_cache_refresh.py`. Same cache root, same status JSON, no new CLI flag — `--gw-refresh-cache` runs both discovery paths.

**Scope:** fill the 2,082 unique missing SKUs and overwrite the 18 ambiguous matches.

## Acceptance criteria

All criteria are testable.

1. **New constants** in `shopify_sync/gw_cache_refresh.py`:
   ```python
   GW_TRADE_FEED_BASE = "https://trade.games-workshop.com/wp-json/gw/v2/media"
   GW_TRADE_FEED_IMAGE_GROUPS: tuple[int, ...] = (46, 47)
   GW_TRADE_FEED_COUNTRY = 220   # UK; matches gw_country_id cookie
   GW_TRADE_FEED_LANG = "en"
   GW_TRADE_FEED_PAGE_SIZE = 24
   GW_TRADE_FEED_USER_AGENT = "Mozilla/5.0 (compatible; FoxAndFableShopifySync/1.0)"
   GW_TRADE_FEED_REFERER = "https://trade.games-workshop.com/resources/"
   GW_TRADE_FEED_REQUEST_DELAY_SECONDS = 0.25
   ```
2. **New discovery function** `discover_trade_feed_packs(session: requests.Session, *, groups=GW_TRADE_FEED_IMAGE_GROUPS, country=GW_TRADE_FEED_COUNTRY, lang=GW_TRADE_FEED_LANG, page_size=GW_TRADE_FEED_PAGE_SIZE, max_pages: int | None = None) -> tuple[list[ResourcePack], str]` returns one `ResourcePack` per asset (one image per pack, archives empty), with `pack.label = asset.file_name` so the existing publish flow + SKU extractor work unchanged.
3. **Cloudflare-friendly request layer**: `discover_trade_feed_packs` configures the session with the browser UA + Referer header before making any call. Existing `_get_with_retries` (line 160) is reused for retry/backoff.
4. **Group-by-group pagination**: for each group, fetch `page=1` first to read `page_count`, then walk `page=2..page_count` (or `min(page_count, max_pages)`). Sleep `GW_TRADE_FEED_REQUEST_DELAY_SECONDS` between requests. Per-group dedupe by `asset.id` (not `file_url` — the URL contains spaces in some entries, e.g. `B200a 11_24.zip`).
5. **Mime filter**: only `mime_type` starting with `image/` is yielded. PDFs/zips returned by groups outside (46, 47) are not requested at all.
6. **Filename preservation**: each `ImageTarget` uses `asset.file_url` and `asset.file_name` verbatim — the SKU stays in the basename so `_extract_asset_match_code(file_name) == "<SKU>"`. Verified by adding a unit test against the three real fixture rows captured during the probe.
7. **`refresh_gw_cache` integration**: `gw_cache_refresh.refresh_gw_cache(...)` (line 508) keeps its existing `discover_resource_packs(...)` call, then unconditionally calls `discover_trade_feed_packs(session)` and concatenates the pack lists. The existing publish loop (line 568) writes both sources into `gw_photo_cache/current/` with no other change. Status JSON gains a `trade_feed: { page_count_by_group, image_count, error_count, started_at, finished_at, last_success_at, request_count }` sub-object.
8. **Source-priority tiebreaker**: `PhotoAssetSet` gains `source_priority: int = 0`. `discover_photo_asset_sets` (`shopify_sync.py:1229`) inspects each asset's parent directory name; if the directory was created from a trade-feed pack (label is a bare `*.jpg` filename without spaces, no `Product Images` ancestry), `source_priority = 10`, else `5`. `_choose_best_photo_asset_set` (referenced from line 2281) sorts candidates by `(-source_priority, -title_similarity)` instead of similarity alone. **Acceptance:** with one ambiguous SKU and a trade-feed candidate, `match_type == "exact_best"` and the chosen path lives under a trade-feed pack dir.
9. **Ambiguous count → 0**: after `--gw-refresh-cache` and `--photo-sync --dry-run`, `wc -l photo_sync_ambiguous.tsv` returns `0` (down from 18).
10. **Missing count drops by ≥ 70%**: after the same run, the unique-SKU count in `photo_sync_missing.tsv` is at most 625 (≤ 30% of the 2,082 unique missing SKUs). Threshold derived from the 88% sample hit rate, with a buffer for SKUs that genuinely don't have a trade-feed image.
11. **Cache size sanity**: `gw_photo_cache/current/` gains roughly **14,500–14,600 new files** (groups 46+47 totals minus overlap with existing packs). Disk: ≈ 4–5 GB at ~300 KB average. Recorded in status JSON `trade_feed.image_count`.
12. **No regression**: existing tests in `tests/test_shopify_sync.py` that touch `gw_*` or `photo_sync_*` pass — `test_gw_refresh_cache_runs_without_shopify_credentials` (line 1054), `test_gw_refresh_cache_rejects_invalid_combinations` (line 1064), `test_photo_sync_dry_run_writes_preview_and_makes_no_writes` (line 2122), `test_photo_sync_live_run_uses_file_first_sequence` (line 2162).
13. **Docs**: `shopify_sync/SETUP.md` "GW auto-download + photo sync" section (line 67–107) gains a paragraph documenting the two discovery sources and the trade-feed REST endpoint.

## Implementation steps

### Step 1 — Constants and fixtures
- File: `shopify_sync/gw_cache_refresh.py`. Add the constants above near `GW_RESOURCES_URL` references. Tests reuse them.
- File: `shopify_sync/tests/fixtures/gw_trade_feed_page_1.json` (new). Capture exactly one real page-1 response from group 46 (24 assets, ~12 KB) so unit tests run offline.

### Step 2 — `discover_trade_feed_packs`
- File: `shopify_sync/gw_cache_refresh.py`. Add immediately after `discover_resource_packs` (line 407).
- Signature and behavior described in acceptance criterion 2 / 3 / 4 / 5 / 6.
- Reuse: `_get_with_retries` (line 160) for retry/backoff; `ResourcePack` / `ImageTarget` dataclasses (lines 47–56).
- Returns `(packs, source_marker="GW Trade Feed")` so the merged-pack list keeps a clear lineage in the status JSON.

### Step 3 — Wire into `refresh_gw_cache`
- File: `shopify_sync/gw_cache_refresh.py`, line 508.
- Change: after `packs, source_marker = discover_resource_packs(...)` (line 521), call `feed_packs, _ = discover_trade_feed_packs(session)` and `packs.extend(feed_packs)`. Build the `trade_feed` sub-object on the status dict (line 545–558) and update success/failure paths (line 601–610, 620–628). The `dry_run` branch logs both counts.
- The session must have the browser UA + Referer set before the trade-feed call. Easiest: have `discover_trade_feed_packs` set them on the passed-in session before its first request (idempotent).

### Step 4 — `PhotoAssetSet.source_priority`
- File: `shopify_sync/shopify_sync.py`. Search for `class PhotoAssetSet` near line 1230. Add `source_priority: int = 0` (defaulted so existing constructors still work).
- File: `shopify_sync/shopify_sync.py`, `discover_photo_asset_sets` (line 1229–1263). After computing `name_seed`, decide `source_priority` from path heuristics: a parent directory whose own name matches `^\d{8,14}_` (the trade-feed convention) → `10`. Anything else → default `0`.
- File: `shopify_sync/shopify_sync.py`, `_choose_best_photo_asset_set` (referenced from line 2281). Replace the existing similarity-only sort with a tuple key `(-source_priority, -similarity_score)`.
- Note: this is the **only** behavioral change in `shopify_sync.py`. All other callers of `PhotoAssetSet` remain identical.

### Step 5 — Status JSON extension
- Already covered in Step 3. Helper `count_trade_feed_matches(cache_root: Path, manifest_path: Path) -> int` walks `cache_root` once after publish, extracts SKU from each filename via `_extract_asset_match_code`, and intersects with current `photo_sync_manifest.json`. Result stored under `trade_feed.matched_sku_count`.

### Step 6 — Tests
- File: `shopify_sync/tests/test_shopify_sync.py` (or a sibling `test_gw_cache_refresh.py` if length warrants).
- New tests:
  - `test_discover_trade_feed_packs_parses_real_fixture` — feeds the saved JSON fixture through a `responses.activate`-style mock and asserts SKU extraction against `99122720012`, `99122720011`, `60043005001` (the three captured during the probe).
  - `test_discover_trade_feed_packs_walks_all_pages` — mocks two pages with `page_count=2`, asserts all assets are returned and pagination stops correctly.
  - `test_discover_trade_feed_packs_filters_non_image_mime_types` — mock returns a mix of `image/jpeg` and `application/pdf`, asserts only the JPEGs become packs.
  - `test_discover_trade_feed_packs_dedupes_by_id` — same asset id across two pages → one pack.
  - `test_refresh_gw_cache_merges_resources_and_trade_feed` — both discoverers mocked, asserts merged status JSON has `trade_feed` sub-object and `pack_count == len(resources_packs) + len(feed_packs)`.
  - `test_resolve_photo_asset_prefers_trade_feed_for_ambiguous` — covers acceptance criterion 8.
- Update `test_gw_refresh_cache_runs_without_shopify_credentials` (line 1054) only if the new status sub-object breaks a strict-equality assertion. Prefer additive assertions.

### Step 7 — Docs
- File: `shopify_sync/SETUP.md`. Insert a "Trade-feed (REST source for missing/ambiguous SKUs)" subsection under the existing "GW auto-download + photo sync" section.
- Mention: endpoint URL, image-bearing groups, country term-id, expected ~14k extra files, and the priority-over-resources rule for ambiguous SKUs.

## Risks and mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Cloudflare ramps up bot protection and starts blocking even browser-UA traffic. | Low–Medium | Refresh fails. | Use `_get_with_retries` (existing 3-tier exponential backoff). On 403, log a clear "Cloudflare blocked" error and fail loudly. Don't silently fall through to the legacy scraper. |
| R2 | The endpoint's country/lang/group params change schema. | Low | Empty results. | Acceptance criterion 9 is a hard check (ambiguous → 0); acceptance criterion 10 is a hard check (missing drops 70%). A monitoring run after each refresh validates both. |
| R3 | An asset's `file_name` doesn't begin with the SKU (edge cases like `Logo.jpg`). | Low | Some packs land in cache without a matchable code. | `_extract_asset_match_code` already returns `""` when no 8-14 digit run is found. Such packs become unmatched, which is acceptable — they sit in the cache as inert files until the next refresh. |
| R4 | Two trade-feed entries share the same SKU (e.g., box-front + back). | Medium | `resolve_photo_asset` returns "ambiguous" with no winner. | Existing `_choose_best_photo_asset_set` then falls through to title similarity. Trade-feed asset titles are descriptive enough (`-PeasantLevy-BOX` vs `-PeasantLevy-PROMO`) that the similarity check will pick one cleanly in practice. If still tied, mark as ambiguous (preserves correctness; user fixes manually). |
| R5 | Trade-feed image is wrong for the SKU (e.g., GW occasionally uses a generic placeholder). | Low–Medium | Wrong image uploaded. | Add a one-time sanity dry-run: every newly-applied trade-feed match is logged in `photo_sync_preview.csv` with `source=trade-feed`. User spot-checks before live `--photo-sync`. |
| R6 | Cache balloons to >5 GB and pushes git or laptop disk over a limit. | Low | Disk pressure. | `gw_photo_cache/` is already in `.gitignore` (verify). Disk: ~4–5 GB is acceptable on the user's dev box. `_staging` is wiped on every run. |
| R7 | The full crawl (607 pages × 0.25 s) takes ~2.5 minutes plus 14k binary downloads at ~300 KB each ≈ ~4 GB → ~30 minutes on a typical home connection. | High (it will be slow) | Long first refresh. | Acceptable; runs once or weekly. Log progress every 50 pages. Make it incremental in a follow-up by storing `id` set and skipping known assets. |
| R8 | The country term-id 220 is wrong for non-UK shops. | Low | Empty results when the user's account/region differs. | Cookie probe at script start: GET `/resources/` with cookies enabled, parse `Set-Cookie` for `gw_country_id`, prefer that value over the constant. Falls back to `220` if cookie absent. |
| R9 | Filename collision between `/resources/` packs and trade-feed assets. | Low | One overwrites the other. | Existing `unique_filename` (line 225) disambiguates with `(N)` suffixes. New unit test for the collision case. |

## Verification steps

1. `python -m pytest shopify_sync/tests/test_shopify_sync.py -k "gw or photo_sync or trade_feed" -v` — all green, including the 6 new tests from Step 6.
2. `python shopify_sync/shopify_sync.py --gw-refresh-cache --dry-run` — output reports both `discover_resource_packs` and `discover_trade_feed_packs` counts; `discover_trade_feed_packs` count > 14,000; no network writes to disk.
3. `python shopify_sync/shopify_sync.py --gw-refresh-cache` — full refresh, exit 0, `gw_photo_cache_status.json.trade_feed.image_count > 14000`, all groups walked.
4. `python shopify_sync/shopify_sync.py --photo-sync --dry-run` — `wc -l photo_sync_ambiguous.tsv == 0`; unique SKUs in `photo_sync_missing.tsv` ≤ 625; new rows in `photo_sync_preview.csv` show `source=trade-feed`.
5. Manual spot-check: 10 random preview rows where `source=trade-feed`. Open the trade-feed image URL in a browser and confirm it matches the product title.
6. `python shopify_sync/shopify_sync.py --photo-sync` (live) — runs to completion against Shopify; previously-missing SKUs land at `state=media_applied` in `photo_sync_manifest.json`.

## Out of scope

- Incremental crawl (skip already-cached `id`s). Keep simple full-refresh now; mark as a follow-up.
- A separate CLI flag for trade-feed-only refresh.
- Replacing successful `/resources/` matches that are already at `state=media_applied`.
- Generalizing the REST scraper to non-GW vendors.
- Region-aware crawl (multi-country fan-out). Single country (220 / UK) is enough; `file_url` returns the same image regardless of country term-id in the requests we tested.

## Open questions

None. All previously-unknown items have been resolved by live probing.

## Plan file location

`.omc/plans/gw-trade-feed-image-fallback.md`
