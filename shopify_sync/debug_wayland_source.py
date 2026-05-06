"""Diagnostic: show exactly what _extract_description_text pulls from a real
Wayland product page through the same Playwright path the backfill uses.

For each probe URL, prints:
  - HTTP status + page byte count (via Playwright .content())
  - Whether the response looks like a Cloudflare challenge page
  - The raw <meta name="description"> value (this is what _extract_description_text
    returns first if present)
  - Any og:description and twitter:description tags
  - Whether the page has application/ld+json blobs (used by the helpers module's
    richer extractor; description_backfill.py does NOT use these today)
  - Length of the longest non-meta paragraph (>= 40 chars) on the page
  - The actual string that _extract_description_text() returns -> this is what
    the backfill script passes to OpenRouter as `source_text`.

Run with the same venv:
  cd /Users/toys-data/Documents/GitHub/shopify_fix/shopify_sync
  source .venv/bin/activate
  python debug_wayland_source.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import description_backfill as db


# Real product URLs that resolve_wayland_source returned in the canary run.
PROBES: list[tuple[str, str]] = [
    (
        "armageddon_deathwatch",
        "https://www.waylandgames.co.uk/armageddon-battalion-deathwatch-99120109017",
    ),
    (
        "orks_wazdakka",
        "https://www.waylandgames.co.uk/orks-wazdakka-gutsmek-99120103128",
    ),
]


def trace(session: requests.Session, label: str, url: str) -> None:
    print(f"\n=== {label} ===")
    print(f"  url: {url}")
    scraper = db._get_wayland_scraper(session)

    try:
        html = scraper.fetch_html(url)
    except Exception as exc:
        print(f"  fetch FAILED: {exc!r}")
        return

    print(f"  page bytes: {len(html):,}")

    if db.WaylandPlaywrightScraper._looks_like_challenge_page(html):
        print("  CLOUDFLARE CHALLENGE PAGE — Playwright did not get past it")
        return

    meta_desc = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html, flags=re.IGNORECASE,
    )
    og_desc = re.search(
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        html, flags=re.IGNORECASE,
    )
    tw_desc = re.search(
        r'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\']([^"\']+)["\']',
        html, flags=re.IGNORECASE,
    )
    print(f"  <meta name=description>:    {meta_desc.group(1)[:200] if meta_desc else 'NONE'}")
    print(f"  <meta og:description>:      {og_desc.group(1)[:200] if og_desc else 'NONE'}")
    print(f"  <meta twitter:description>: {tw_desc.group(1)[:200] if tw_desc else 'NONE'}")

    ld_blobs = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, flags=re.DOTALL | re.IGNORECASE,
    )
    print(f"  application/ld+json blobs: {len(ld_blobs)}")
    for i, blob in enumerate(ld_blobs[:2]):
        snippet = re.sub(r"\s+", " ", blob).strip()[:300]
        print(f"    [{i}] {snippet}")

    parser = db._TextCollector()
    parser.feed(html)
    text = parser.text()
    paras = [line for line in text.splitlines() if len(line) >= 40]
    print(f"  body paragraphs >=40 chars: {len(paras)}")
    if paras:
        longest = max(paras, key=len)
        print(f"  longest body paragraph ({len(longest)} chars):")
        print(f"    {longest[:400]}{'...' if len(longest) > 400 else ''}")

    extracted = db._extract_description_text(html)
    print(f"\n  >>> _extract_description_text returns ({len(extracted)} chars):")
    print(f"      {extracted[:600]}{'...' if len(extracted) > 600 else ''}")
    print(f"  >>> THIS is what gets passed to OpenRouter as `source_text`.")


def main() -> None:
    session = requests.Session()
    try:
        for label, url in PROBES:
            trace(session, label, url)
    finally:
        db.close_wayland_scraper(session)


if __name__ == "__main__":
    main()
