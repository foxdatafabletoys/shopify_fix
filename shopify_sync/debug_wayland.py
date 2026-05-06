"""Diagnostic: trace resolve_wayland_source for specific products.

Prints, for each product:
  - search URL
  - number of candidate links extracted
  - first 5 candidate (url, link_title) pairs
  - per-candidate: page_title, title_score, sku_present, description_text length

Run with the same venv:
  ../../.venv/bin/python debug_wayland.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import description_backfill as db


# (label, F&F title, F&F SKU)  -- a mix: items expected to exist on Wayland and items that may not.
PROBES: list[tuple[str, str, str]] = [
    # Mainstream GW miniatures — should definitely be on Wayland
    ("dracothian_guard", "STORMCAST ETERNALS: DRACOTHIAN GUARD", "99120218074"),
    ("aos_skaventide", "AGE OF SIGMAR: SKAVENTIDE (PAPERBACK)", ""),  # blank SKU on purpose? actually fill below
    # Actual failing items from the dry-run
    ("orks_ghazghkull_dice", "ORKS: GHAZGHKULL DICE", "99220103008"),
    ("yarrick_omnibus", "YARRICK: THE OMNIBUS (PB)", "60100181550"),
    ("armageddon_deathwatch", "ARMAGEDDON BATTALION: DEATHWATCH", "99120109017"),
    ("ossiarch_null_myriad", "OSSIARCH BONEREAPERS:NULL MYRIAD PHALANX", "99120207261"),
]


def trace(session: requests.Session, label: str, title: str, sku: str) -> None:
    print(f"\n=== {label} ===")
    print(f"  F&F title: {title}")
    print(f"  F&F SKU:   {sku}")

    search_url = db.build_search_url(title, sku)
    print(f"  search:    {search_url}")

    scraper = db._get_wayland_scraper(session)

    t0 = time.time()
    try:
        search_html = scraper.fetch_html(search_url)
    except Exception as exc:
        print(f"  fetch FAILED: {exc!r}")
        return
    print(f"  fetched search ({len(search_html):,} bytes, {time.time()-t0:.1f}s)")

    raw_anchors = db._parse_search_candidates(search_html, search_url)
    print(f"  raw anchors with text: {len(raw_anchors)}")
    # Filter out obvious nav/global links (the first column is full URL)
    product_links = [
        (url, txt) for url, txt in raw_anchors
        if "/search" not in url and "/account" not in url
        and "favicon" not in url and "_next" not in url
        and url.count("/") <= 4 and len(txt) > 3
    ]
    print(f"  candidate product links (heuristic filter): {len(product_links)}")
    for i, (url, txt) in enumerate(product_links[:8]):
        print(f"    [{i}] {url[:100]}  | title='{txt[:60]}'")

    # Also try the helper's own _search_wayland_candidates result (includes the guess URL)
    candidate_links, _ = db._search_wayland_candidates(session, title=title, sku=sku)
    print(f"  _search_wayland_candidates returned: {len(candidate_links)}")
    for i, (url, txt) in enumerate(candidate_links[:5]):
        print(f"    [{i}] {url[:100]}  | link_title='{txt[:60]}'")

    print(f"  scoring top 5 candidates:")
    for i, (page_url, link_title) in enumerate(candidate_links[:5]):
        try:
            page_html = scraper.fetch_html(page_url)
        except Exception as exc:
            print(f"    [{i}] FETCH FAILED: {exc!r}")
            continue
        page_title = db._extract_page_title(page_html) or link_title
        description_text = db._extract_description_text(page_html)
        sku_present = db._sku_present_in_text(page_html, sku)
        title_score = db._title_similarity(title, page_title)
        verdict = []
        if not description_text:
            verdict.append("NO_DESC")
        if title_score < 0.6:
            verdict.append("LOW_TITLE")
        if not sku_present and title_score < 0.85:
            verdict.append("NO_SKU_AND_TITLE<0.85")
        if not verdict:
            verdict.append("ACCEPT")
        print(
            f"    [{i}] page_title='{page_title[:50]}' title_score={title_score:.2f} "
            f"sku_present={sku_present} desc_len={len(description_text)} -> {','.join(verdict)}"
        )


def main() -> None:
    session = requests.Session()
    try:
        for label, title, sku in PROBES:
            trace(session, label, title, sku)
    finally:
        db.close_wayland_scraper(session)


if __name__ == "__main__":
    main()
