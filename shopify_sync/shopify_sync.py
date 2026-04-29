#!/usr/bin/env python3
"""
Shopify bulk delete + import script for Telemachus Foxfable.

Reads two spreadsheets:
  - "Games Workshop Store List.xlsx"  (master GW catalog, 1,974 items)
  - "everything else.xlsx"             (general inventory; only non-GW rows used)

What it does:
  --delete     Deletes existing Shopify products that have NO SKU on any variant.
               (Products that already have a Product Code/SKU are kept.)
  --delete-collections
               Deletes existing Shopify collections.
  --generate-collections
               Creates a Wayland-style smart collection set and auto-tags existing
               Shopify products so they fall into those smart collections.
  --import     Creates new Shopify products from the spreadsheets.
  --update     Matches existing Shopify products by SKU and updates price,
               compare_at_price, cost, and on-hand quantity from the sheets.
               Skips products that don't yet exist in Shopify.
  --all        Runs --delete then --import.
  --preflight  Validates Shopify auth and location readiness with no write side effects.
  --dry-run    Reads sheets, builds the product list, writes preview.csv,
               and makes ZERO API calls. Always run this first.
               When combined with --update, queries Shopify (read-only) and
               writes update_preview.csv showing the diff that would be applied.

Pricing logic:
  - Games Workshop:    price = UKR * 0.79   compare_at = UKR    (21% off)
  - Funko (FUNKO mfg): price = SRP * 0.89   compare_at = SRP    (11% off)
  - Everything else:   price = SRP                              (no discount)

Cost-per-item is set to GBD (GW) or Unit Cost (others) when present.
Inventory tracking is enabled; quantities come from Store Quantity (GW) or Available (others).

Required env vars (or .env file in same folder):
  SHOPIFY_STORE      e.g. "telemachus-foxfable"  (the part before .myshopify.com)
  SHOPIFY_TOKEN      Admin API access token starting with "shpat_..."
  SHOPIFY_LOCATION   (optional) numeric location ID or gid://shopify/Location/...
                     blank is the primary supported path and auto-detects the location

Usage:
  python shopify_sync.py --dry-run
  python shopify_sync.py --preflight
  python shopify_sync.py --delete
  python shopify_sync.py --delete-collections --dry-run
  python shopify_sync.py --delete-collections
  python shopify_sync.py --generate-collections --dry-run
  python shopify_sync.py --generate-collections
  python shopify_sync.py --import
  python shopify_sync.py --update --dry-run
  python shopify_sync.py --update
  python shopify_sync.py --all
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import mimetypes
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

from gw_cache_refresh import refresh_gw_cache

API_VERSION = "2025-01"
HERE = Path(__file__).resolve().parent
SHEET_DIR = HERE
GW_FILE = SHEET_DIR / "Games Workshop Store List.xlsx"
INV_FILE = SHEET_DIR / "everything else.xlsx"
PREVIEW_CSV = HERE / "preview.csv"
UPDATE_PREVIEW_CSV = HERE / "update_preview.csv"
PHOTO_SYNC_PREVIEW_CSV = HERE / "photo_sync_preview.csv"
PHOTO_SYNC_MANIFEST_JSON = HERE / "photo_sync_manifest.json"
PHOTO_SYNC_MISSING_TSV = HERE / "photo_sync_missing.tsv"
PHOTO_SYNC_AMBIGUOUS_TSV = HERE / "photo_sync_ambiguous.tsv"
PHOTO_SYNC_FAILURES_TSV = HERE / "photo_sync_failures.tsv"
COLLECTION_GENERATION_PREVIEW_CSV = HERE / "collection_generation_preview.csv"
COLLECTION_GENERATION_UNMATCHED_CSV = HERE / "collection_generation_unmatched.csv"
GW_RESOURCES_URL = "https://trade.games-workshop.com/resources/"
GW_PHOTO_CACHE_ROOT = HERE / "gw_photo_cache"
GW_PHOTO_CACHE_CURRENT = GW_PHOTO_CACHE_ROOT / "current"
GW_PHOTO_CACHE_STAGING = GW_PHOTO_CACHE_ROOT / "_staging"
GW_PHOTO_CACHE_STATUS_JSON = HERE / "gw_photo_cache_status.json"
LOG_FILE = HERE / "sync.log"
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
LATEST_GW_RELEASE_LIMIT = 60
AUTO_COLLECTION_TAG_PREFIX = "AUTO_COLLECTION::"

# Tolerance for treating two money values as equal (Shopify rounds to 2 dp).
MONEY_EPSILON = 0.005
NON_GW_MINIATURE_VENDORS = {
    "archon studio",
    "mantic games",
    "warlord games",
}
CARD_PRODUCT_MARKERS = (
    "card game",
    "tcg",
    "starter deck",
    "starter set",
    "booster",
    "booster box",
    "blister",
    "gift collection",
    "elite trainer box",
    "etb",
    "theme deck",
    "structure deck",
    "duel deck",
    "deck",
)
RPG_PRODUCT_MARKERS = (
    "roleplaying",
    "role playing",
    "starter set",
    "starter box",
    "beginner box",
    "essentials kit",
    "core rulebook",
    "rulebook",
    "player's handbook",
    "players handbook",
    "gm screen",
    "game master's screen",
    "adventure",
    "campaign",
)
WAYLAND_COLLECTION_SPECS: list[tuple[str, str]] = [
    ("Games Workshop", "games-workshop"),
    ("Adeptus Titanicus", "adeptus-titanicus"),
    ("Age of Sigmar", "age-of-sigmar"),
    ("Black Library", "black-library"),
    ("Blood Bowl", "blood-bowl"),
    ("Citadel Colour", "citadel-colour"),
    ("Kill Team", "kill-team"),
    ("Legions Imperialis", "legions-imperialis"),
    ("Middle-earth Strategy Battle Game", "middle-earth-strategy-battle-game"),
    ("Necromunda", "necromunda"),
    ("The Horus Heresy", "the-horus-heresy"),
    ("The Old World", "the-old-world"),
    ("Warcry", "warcry"),
    ("Warhammer 40k", "warhammer-40k"),
    ("Warhammer Underworlds", "warhammer-underworlds"),
    ("Warhammer Quest", "warhammer-quest"),
    ("White Dwarf", "white-dwarf"),
    ("Pre-Orders", "pre-orders"),
    ("Latest Releases", "latest-releases"),
    ("Board & Card Games", "board-card-games"),
    ("Board Game Pre-Orders", "board-game-pre-orders"),
    ("Card Game Pre-Orders", "card-game-pre-orders"),
    ("Two-Player Games", "two-player-games"),
    ("Award Winning Games", "award-winning-games"),
    ("Co-operative Games", "co-operative-games"),
    ("Dice Games", "dice-games"),
    ("Essential Games", "essential-games"),
    ("Family Games", "family-games"),
    ("Gateway Games", "gateway-games"),
    ("Legacy Games", "legacy-games"),
    ("Living Card Games", "living-card-games"),
    ("Miniatures Games", "miniatures-games"),
    ("Party Games", "party-games"),
    ("Roll & Write", "roll-write"),
    ("Strategy Games", "strategy-games"),
    ("Thematic Games", "thematic-games"),
    ("Wargames", "wargames"),
    ("Getting Started", "getting-started"),
    ("Get Started Board Gaming", "get-started-board-gaming"),
    ("Get Started Card Gaming", "get-started-card-gaming"),
    ("Get Started Role-Playing", "get-started-role-playing"),
    ("Collectable Card Games", "collectable-card-games"),
    ("Digimon Card Game", "digimon-card-game"),
    ("Disney Lorcana", "disney-lorcana"),
    ("Gundam Card Game", "gundam-card-game"),
    ("Magic: The Gathering", "magic-the-gathering"),
    ("One Piece Card Game", "one-piece-card-game"),
    ("Oshi Push TCG", "oshi-push-tcg"),
    ("Pokemon", "pokemon"),
    ("Riftbound TCG", "riftbound-tcg"),
    ("Star Wars: Unlimited", "star-wars-unlimited"),
    ("Yu-Gi-Oh!", "yu-gi-oh"),
    ("Role-Playing Games", "role-playing-games"),
    ("Cosmere", "cosmere"),
    ("Call of Cthulhu", "call-of-cthulhu"),
    ("Dune: Adventures in the Imperium", "dune-adventures-in-the-imperium"),
    ("Dungeons & Dragons", "dungeons-dragons"),
    ("Fallout: The Roleplaying Game", "fallout-the-roleplaying-game"),
    ("Pathfinder", "pathfinder"),
    ("Starfinder", "starfinder"),
    ("The One Ring", "the-one-ring"),
    ("Vampire: The Masquerade", "vampire-the-masquerade"),
    ("Jigsaws", "jigsaws"),
]
BOARD_CARD_CHILDREN = {
    "Board Game Pre-Orders",
    "Card Game Pre-Orders",
    "Two-Player Games",
    "Award Winning Games",
    "Co-operative Games",
    "Dice Games",
    "Essential Games",
    "Family Games",
    "Gateway Games",
    "Legacy Games",
    "Living Card Games",
    "Miniatures Games",
    "Party Games",
    "Roll & Write",
    "Strategy Games",
    "Thematic Games",
    "Wargames",
}
GETTING_STARTED_CHILDREN = {
    "Get Started Board Gaming",
    "Get Started Card Gaming",
    "Get Started Role-Playing",
}
CCG_CHILDREN = {
    "Digimon Card Game",
    "Disney Lorcana",
    "Gundam Card Game",
    "Magic: The Gathering",
    "One Piece Card Game",
    "Oshi Push TCG",
    "Pokemon",
    "Riftbound TCG",
    "Star Wars: Unlimited",
    "Yu-Gi-Oh!",
}
RPG_CHILDREN = {
    "Cosmere",
    "Call of Cthulhu",
    "Dune: Adventures in the Imperium",
    "Dungeons & Dragons",
    "Fallout: The Roleplaying Game",
    "Pathfinder",
    "Starfinder",
    "The One Ring",
    "Vampire: The Masquerade",
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# Env / .env loading
# ---------------------------------------------------------------------------

def load_env() -> dict[str, str]:
    """Load env vars from a .env file in HERE if present, then OS env wins."""
    env: dict[str, str] = {}
    dotenv = HERE / ".env"
    if dotenv.exists():
        for raw in dotenv.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("SHOPIFY_STORE", "SHOPIFY_TOKEN", "SHOPIFY_LOCATION"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Product:
    title: str
    sku: str
    barcode: str = ""
    vendor: str = ""
    product_type: str = ""
    tags: list[str] = field(default_factory=list)
    description_html: str = ""
    price: float = 0.0
    compare_at_price: float | None = None
    cost: float | None = None
    weight_grams: float | None = None
    quantity: int = 0
    source: str = ""  # "GW" or "INV"

    def to_preview_row(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "title": self.title,
            "sku": self.sku,
            "barcode": self.barcode,
            "vendor": self.vendor,
            "product_type": self.product_type,
            "tags": ", ".join(self.tags),
            "price": f"{self.price:.2f}" if self.price else "",
            "compare_at_price": f"{self.compare_at_price:.2f}" if self.compare_at_price else "",
            "cost": f"{self.cost:.2f}" if self.cost is not None else "",
            "weight_grams": f"{self.weight_grams:.0f}" if self.weight_grams else "",
            "quantity": self.quantity,
        }


@dataclass
class PhotoAssetSet:
    key: str
    label: str
    product_code: str = ""
    title_slug: str = ""
    image_paths: list[Path] = field(default_factory=list)

    def fingerprint(self) -> str:
        digest = hashlib.sha1()
        for path in self.image_paths:
            stat = path.stat()
            digest.update(str(path).encode("utf-8"))
            digest.update(str(stat.st_size).encode("utf-8"))
            digest.update(str(int(stat.st_mtime)).encode("utf-8"))
        return digest.hexdigest()


# ---------------------------------------------------------------------------
# Spreadsheet parsing
# ---------------------------------------------------------------------------

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v).strip()


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_int(v: Any) -> int:
    f = _safe_float(v)
    if f is None:
        return 0
    return int(round(f))


def _clean_barcode(v: Any) -> str:
    """Barcodes often arrive as floats in scientific notation; convert to digits."""
    s = _safe_str(v)
    if not s:
        return ""
    # If it parses as a float like 5.011921e+12, drop the decimal exponent
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return s
    except ValueError:
        return s


def _round_money(x: float) -> float:
    return round(x + 1e-9, 2)


def _normalize_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower())
    return slug.strip("-")


def _normalize_search_text(value: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()
    return f" {compact} " if compact else " "


def _extract_product_code(value: str) -> str:
    matches = re.findall(r"(?<!\d)(\d{8,14})(?!\d)", value or "")
    if not matches:
        return ""
    return max(matches, key=len)


def _extract_title_slug(value: str) -> str:
    base = Path(value or "").stem
    code = _extract_product_code(base)
    if code:
        _, _, tail = base.partition(code)
        base = tail.lstrip(" -_")
    return _normalize_slug(base)


def parse_gw(path: Path) -> list[Product]:
    df = pd.read_excel(path, sheet_name=0)
    products: list[Product] = []
    for _, row in df.iterrows():
        title = _safe_str(row.get("Description"))
        sku = _safe_str(row.get("Product Code"))
        if not title or not sku:
            continue
        ukr = _safe_float(row.get("UKR"))
        if not ukr or ukr <= 0:
            continue  # cannot sell without an RRP
        gbd = _safe_float(row.get("GBD"))
        weight_kg = _safe_float(row.get("Weight (kg)"))

        system = _safe_str(row.get("System"))
        race = _safe_str(row.get("Race"))
        ss_code = _safe_str(row.get("SS Code"))
        barcode = _clean_barcode(row.get("Barcode"))
        country = _safe_str(row.get("Country of Origin"))
        commodity = _safe_str(row.get("Commodity Code"))

        tags = ["Games Workshop"]
        if system and system.lower() != "nan":
            tags.append(system)
        if race and race.lower() not in ("", "nan"):
            tags.append(race)

        meta_lines = []
        if ss_code:
            meta_lines.append(f"SS Code: {ss_code}")
        if system:
            meta_lines.append(f"System: {system}")
        if race:
            meta_lines.append(f"Race: {race}")
        if country:
            meta_lines.append(f"Country of Origin: {country}")
        if commodity:
            meta_lines.append(f"Commodity Code: {commodity}")
        description_html = "<br>".join(meta_lines)

        products.append(Product(
            title=title,
            sku=sku,
            barcode=barcode,
            vendor="Games Workshop",
            product_type=system or "Games Workshop",
            tags=tags,
            description_html=description_html,
            price=_round_money(ukr * 0.79),
            compare_at_price=_round_money(ukr),
            cost=_round_money(gbd) if gbd else None,
            weight_grams=(weight_kg * 1000.0) if weight_kg else None,
            quantity=_safe_int(row.get("Store Quantity")),
            source="GW",
        ))
    return products


def parse_inventory(path: Path, skip_gw: bool = True) -> list[Product]:
    df = pd.read_excel(path, sheet_name=0)
    products: list[Product] = []
    seen_skus: set[str] = set()
    for _, row in df.iterrows():
        title = _safe_str(row.get("Product Name"))
        sku = _safe_str(row.get("SKU"))
        if not title or not sku:
            continue
        manufacturer = _safe_str(row.get("Manufacturer"))
        if skip_gw and manufacturer.lower() == "games workshop":
            continue
        if sku in seen_skus:
            continue  # dedupe duplicate rows in the inventory export
        seen_skus.add(sku)

        srp = _safe_float(row.get("SRP"))
        wholesale = _safe_float(row.get("Wholesale Price"))
        unit_cost = _safe_float(row.get("Unit Cost"))
        if not srp or srp <= 0:
            srp = wholesale  # fall back to wholesale if SRP missing
        if not srp or srp <= 0:
            continue  # cannot sell without a price

        is_funko = manufacturer.upper() == "FUNKO"
        if is_funko:
            price = _round_money(srp * 0.89)
            compare_at = _round_money(srp)
        else:
            price = _round_money(srp)
            compare_at = None

        barcode = _clean_barcode(row.get("UPC/EAN"))
        asin = _safe_str(row.get("ASIN"))
        distributor = _safe_str(row.get("Distributor"))

        tags = []
        if manufacturer:
            tags.append(manufacturer)
        if distributor:
            tags.append(f"Distributor: {distributor}")
        if asin:
            tags.append(f"ASIN: {asin}")

        meta_lines = []
        if manufacturer:
            meta_lines.append(f"Manufacturer: {manufacturer}")
        if asin:
            meta_lines.append(f"ASIN: {asin}")
        if distributor:
            meta_lines.append(f"Distributor: {distributor}")
        description_html = "<br>".join(meta_lines)

        products.append(Product(
            title=title,
            sku=sku,
            barcode=barcode,
            vendor=manufacturer or "Foxfable",
            product_type=manufacturer or "General",
            tags=tags,
            description_html=description_html,
            price=price,
            compare_at_price=compare_at,
            cost=_round_money(unit_cost) if unit_cost is not None else None,
            weight_grams=None,
            quantity=_safe_int(row.get("Available")),
            source="INV",
        ))
    return products


def build_product_list(strict: bool = False) -> list[Product]:
    if not GW_FILE.exists():
        msg = f"GW file not found: {GW_FILE}"
        if strict:
            raise RuntimeError(msg)
        log(f"WARNING: {msg}")
        gw: list[Product] = []
    else:
        try:
            gw = parse_gw(GW_FILE)
        except Exception as e:
            if strict:
                raise RuntimeError(f"Failed to parse {GW_FILE.name}: {e}") from e
            log(f"WARNING: failed to parse {GW_FILE.name}: {e}")
            gw = []
        log(f"Parsed {len(gw)} products from {GW_FILE.name}")
    if not INV_FILE.exists():
        msg = f"Inventory file not found: {INV_FILE}"
        if strict:
            raise RuntimeError(msg)
        log(f"WARNING: {msg}")
        inv: list[Product] = []
    else:
        try:
            inv = parse_inventory(INV_FILE, skip_gw=True)
        except Exception as e:
            if strict:
                raise RuntimeError(f"Failed to parse {INV_FILE.name}: {e}") from e
            log(f"WARNING: failed to parse {INV_FILE.name}: {e}")
            inv = []
        log(f"Parsed {len(inv)} non-GW products from {INV_FILE.name}")

    # final dedupe across both sheets by SKU
    seen: set[str] = set()
    merged: list[Product] = []
    for p in gw + inv:
        if p.sku in seen:
            continue
        seen.add(p.sku)
        merged.append(p)
    if strict and not merged:
        raise RuntimeError("No products found after parsing and dedupe; aborting before live sync.")
    log(f"Total unique products to import: {len(merged)}")
    return merged


def build_gw_product_list(strict: bool = False) -> list[Product]:
    if not GW_FILE.exists():
        msg = f"GW file not found: {GW_FILE}"
        if strict:
            raise RuntimeError(msg)
        log(f"WARNING: {msg}")
        return []
    try:
        products = parse_gw(GW_FILE)
    except Exception as e:
        if strict:
            raise RuntimeError(f"Failed to parse {GW_FILE.name}: {e}") from e
        log(f"WARNING: failed to parse {GW_FILE.name}: {e}")
        return []
    if strict and not products:
        raise RuntimeError("No GW products found after parsing; aborting before photo sync.")
    log(f"Parsed {len(products)} GW products for photo sync")
    return products


def write_preview(products: list[Product]) -> None:
    cols = [
        "source", "title", "sku", "barcode", "vendor", "product_type",
        "tags", "price", "compare_at_price", "cost", "weight_grams", "quantity",
    ]
    with PREVIEW_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in products:
            w.writerow(p.to_preview_row())
    log(f"Wrote preview: {PREVIEW_CSV}")


def default_photo_root() -> Path:
    return GW_PHOTO_CACHE_CURRENT


def photo_root_has_images(root: Path) -> bool:
    if not root.exists() or not root.is_dir():
        return False
    return any(path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES for path in root.rglob("*"))


def resolve_photo_sync_root(photo_root: Path | None) -> Path:
    if photo_root is not None:
        log(f"Photo sync using explicit --photo-root override: {photo_root}")
        return photo_root
    root = default_photo_root()
    if not photo_root_has_images(root):
        raise RuntimeError(
            f"Default GW photo cache is missing or empty: {root}. "
            "Run --gw-refresh-cache first or provide --photo-root."
        )
    return root


def discover_photo_asset_sets(root: Path) -> list[PhotoAssetSet]:
    if not root.exists():
        raise RuntimeError(f"Photo root not found: {root}")
    if not root.is_dir():
        raise RuntimeError(f"Photo root must be a directory: {root}")

    grouped: dict[tuple[str, str], list[Path]] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        rel_parent = path.parent.relative_to(root)
        if rel_parent == Path("."):
            group_key = f"file:{path.stem}"
            label = path.stem
        else:
            group_key = f"dir:{rel_parent.as_posix()}"
            label = rel_parent.as_posix()
        grouped.setdefault((group_key, label), []).append(path)

    asset_sets: list[PhotoAssetSet] = []
    for (group_key, label), paths in sorted(grouped.items(), key=lambda item: item[0][1]):
        sorted_paths = sorted(paths)
        name_seed = label if group_key.startswith("dir:") else sorted_paths[0].stem
        asset_sets.append(PhotoAssetSet(
            key=group_key,
            label=label,
            product_code=_extract_product_code(name_seed),
            title_slug=_extract_title_slug(name_seed),
            image_paths=sorted_paths,
        ))
    if not asset_sets:
        raise RuntimeError(f"No image files found under photo root: {root}")
    return asset_sets


def build_photo_indexes(
    asset_sets: list[PhotoAssetSet],
) -> tuple[dict[str, list[PhotoAssetSet]], dict[str, list[PhotoAssetSet]]]:
    by_code: dict[str, list[PhotoAssetSet]] = {}
    by_slug: dict[str, list[PhotoAssetSet]] = {}
    for asset_set in asset_sets:
        if asset_set.product_code:
            by_code.setdefault(asset_set.product_code, []).append(asset_set)
        if asset_set.title_slug:
            by_slug.setdefault(asset_set.title_slug, []).append(asset_set)
    return by_code, by_slug


def resolve_photo_asset(
    product: Product,
    by_code: dict[str, list[PhotoAssetSet]],
    by_slug: dict[str, list[PhotoAssetSet]],
) -> tuple[str, str, PhotoAssetSet | None, str]:
    exact_matches = by_code.get(product.sku, [])
    if len(exact_matches) == 1:
        return "replace", "exact", exact_matches[0], ""
    if len(exact_matches) > 1:
        return "skip", "ambiguous", None, "multiple exact code matches"

    slug = _normalize_slug(product.title)
    slug_matches = by_slug.get(slug, [])
    if len(slug_matches) == 1:
        return "replace", "fallback", slug_matches[0], ""
    if len(slug_matches) > 1:
        return "skip", "ambiguous", None, "multiple title-slug matches"
    return "skip", "missing", None, "no matching photo asset set"


def load_photo_manifest(path: Path = PHOTO_SYNC_MANIFEST_JSON) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse photo sync manifest {path}: {e}") from e
    if not isinstance(data, dict):
        raise RuntimeError(f"Photo sync manifest must be a JSON object: {path}")
    return data


def save_photo_manifest(manifest: dict[str, Any], path: Path = PHOTO_SYNC_MANIFEST_JSON) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def update_photo_manifest_entry(
    manifest: dict[str, Any],
    sku: str,
    **fields: Any,
) -> dict[str, Any]:
    entry = manifest.setdefault(sku, {})
    entry.update(fields)
    entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return entry


def append_photo_log(path: Path, rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as fh:
        for sku, title, detail in rows:
            fh.write(f"{sku}\t{title}\t{detail}\n")


def build_photo_sync_preview_row(
    product: Product,
    status: str,
    match_type: str,
    photo_root: Path,
    asset_set: PhotoAssetSet | None = None,
    reason: str = "",
) -> dict[str, Any]:
    image_paths = asset_set.image_paths if asset_set else []
    return {
        "sku": product.sku,
        "title": product.title,
        "status": status,
        "match_type": match_type,
        "asset_label": asset_set.label if asset_set else "",
        "image_count": len(image_paths),
        "reason": reason,
        "source_mode": "staged-local-files",
        "source_paths": "|".join(str(path.relative_to(photo_root)) for path in image_paths),
    }


def update_and_save_photo_manifest_entry(
    manifest: dict[str, Any],
    manifest_path: Path,
    sku: str,
    **fields: Any,
) -> dict[str, Any]:
    entry = update_photo_manifest_entry(manifest, sku, **fields)
    save_photo_manifest(manifest, manifest_path)
    return entry


def _is_games_workshop_record(record: dict[str, Any]) -> bool:
    vendor = (record.get("vendor") or "").strip().lower()
    raw_tags = record.get("tags") or []
    tags = raw_tags if isinstance(raw_tags, list) else [raw_tags]
    normalized_tags = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    return vendor == "games workshop" or "games workshop" in normalized_tags


def _iter_chunks(items: list[str], size: int = 250) -> Iterable[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _record_tags(record: dict[str, Any]) -> list[str]:
    raw_tags = record.get("tags") or []
    if isinstance(raw_tags, list):
        return [str(tag).strip() for tag in raw_tags if str(tag).strip()]
    return [str(raw_tags).strip()] if str(raw_tags).strip() else []


def _record_tag_prefix(record: dict[str, Any], prefixes: Iterable[str]) -> bool:
    tags = [tag.lower() for tag in _record_tags(record)]
    return any(tag.startswith(prefix.lower()) for tag in tags for prefix in prefixes)


def _record_has_phrase(record: dict[str, Any], phrase: str) -> bool:
    return _normalize_search_text(phrase).strip() in record.get("search_text", " ")


def _record_has_any_phrase(record: dict[str, Any], phrases: Iterable[str]) -> bool:
    return any(_record_has_phrase(record, phrase) for phrase in phrases)


def _record_has_all_phrases(record: dict[str, Any], phrases: Iterable[str]) -> bool:
    return all(_record_has_phrase(record, phrase) for phrase in phrases)


def _is_games_workshop_product(record: dict[str, Any]) -> bool:
    return (record.get("vendor") or "").strip().lower() == "games workshop"


def _is_non_gw_miniatures_product(record: dict[str, Any]) -> bool:
    vendor = (record.get("vendor") or "").strip().lower()
    return (
        vendor in NON_GW_MINIATURE_VENDORS
        or _record_has_any_phrase(record, ("miniature", "miniatures"))
    )


def _is_card_game_product(record: dict[str, Any]) -> bool:
    return _record_has_any_phrase(record, CARD_PRODUCT_MARKERS)


def _is_rpg_product(record: dict[str, Any]) -> bool:
    return _record_has_any_phrase(record, RPG_PRODUCT_MARKERS)


def _assign_collection(match_map: dict[str, set[str]], record: dict[str, Any], collection_title: str) -> None:
    match_map.setdefault(record["id"], set()).add(collection_title)


def classify_wayland_collection_titles(record: dict[str, Any]) -> set[str]:
    matches: set[str] = set()
    vendor = (record.get("vendor") or "").strip().lower()
    is_gw = vendor == "games workshop"
    is_card = _is_card_game_product(record)
    is_rpg = _is_rpg_product(record)
    is_non_gw_minis = _is_non_gw_miniatures_product(record)
    is_preorder = _record_has_any_phrase(record, ("pre order", "preorder", "pre-orders"))

    if is_gw:
        matches.add("Games Workshop")
        if _record_has_any_phrase(record, ("adeptus titanicus",)):
            matches.add("Adeptus Titanicus")
        if _record_tag_prefix(record, ("aos -",)) or _record_has_any_phrase(record, ("age of sigmar",)):
            matches.add("Age of Sigmar")
        if _record_has_any_phrase(record, ("black library",)):
            matches.add("Black Library")
        if _record_has_any_phrase(record, ("blood bowl",)):
            matches.add("Blood Bowl")
        if _record_tag_prefix(record, ("paint -", "spray -", "hobby -")) or _record_has_any_phrase(record, ("citadel",)):
            matches.add("Citadel Colour")
        if _record_has_any_phrase(record, ("kill team",)):
            matches.add("Kill Team")
        if _record_tag_prefix(record, ("l/imperialis ", "legions imperialis -")) or _record_has_any_phrase(record, ("legions imperialis",)):
            matches.add("Legions Imperialis")
        if _record_tag_prefix(record, ("middle-earth -", "middle earth -")) or _record_has_any_phrase(record, ("middle earth", "middle-earth")):
            matches.add("Middle-earth Strategy Battle Game")
        if _record_has_any_phrase(record, ("necromunda",)):
            matches.add("Necromunda")
        if _record_tag_prefix(record, ("hh ",)) or _record_has_any_phrase(record, ("horus heresy",)):
            matches.add("The Horus Heresy")
        if _record_tag_prefix(record, ("old world -",)) or _record_has_any_phrase(record, ("the old world", "old world")):
            matches.add("The Old World")
        if _record_has_any_phrase(record, ("warcry",)):
            matches.add("Warcry")
        if (
            _record_tag_prefix(record, ("40k -",))
            or _record_has_any_phrase(record, ("warhammer 40 000", "warhammer 40000", "warhammer 40k", "wh40k", "40k"))
        ):
            matches.add("Warhammer 40k")
        if _record_has_any_phrase(record, ("underworlds",)):
            matches.add("Warhammer Underworlds")
        if _record_has_any_phrase(record, ("warhammer quest",)):
            matches.add("Warhammer Quest")
        if _record_has_any_phrase(record, ("white dwarf",)):
            matches.add("White Dwarf")
        if is_preorder:
            matches.add("Pre-Orders")
        return matches

    if _record_has_phrase(record, "articulate"):
        matches.update({"Party Games", "Family Games"})
    if _record_has_any_phrase(record, ("two player", "two-player", "2 player", "duel")):
        matches.add("Two-Player Games")
    if _record_has_any_phrase(record, ("co operative", "co-operative", "cooperative", "co-op")):
        matches.add("Co-operative Games")
    if _record_has_any_phrase(record, ("dice game", "dice games", "dice throne")):
        matches.add("Dice Games")
    if _record_has_any_phrase(record, ("award winning",)):
        matches.add("Award Winning Games")
    if _record_has_any_phrase(record, ("legacy",)):
        matches.add("Legacy Games")
    if _record_has_any_phrase(record, ("living card game", "lcg")):
        matches.add("Living Card Games")
    if _record_has_any_phrase(record, ("roll write", "roll & write")):
        matches.add("Roll & Write")
    if _record_has_any_phrase(record, ("strategy",)):
        matches.add("Strategy Games")
    if _record_has_any_phrase(record, ("theme", "thematic")):
        matches.add("Thematic Games")
    if _record_has_any_phrase(record, ("puzzle", "jigsaw")):
        matches.add("Jigsaws")
    if is_non_gw_minis:
        matches.update({"Miniatures Games", "Wargames", "Strategy Games", "Thematic Games"})
    if _record_has_phrase(record, "starter set") and (is_non_gw_minis or _record_has_phrase(record, "board game")):
        matches.update({"Gateway Games", "Get Started Board Gaming"})
    if is_preorder and not is_card and not is_rpg:
        matches.add("Board Game Pre-Orders")
    if is_card and _record_has_phrase(record, "digimon"):
        matches.add("Digimon Card Game")
    if _record_has_any_phrase(record, ("lorcana",)):
        matches.add("Disney Lorcana")
    if is_card and _record_has_phrase(record, "gundam"):
        matches.add("Gundam Card Game")
    if _record_has_any_phrase(record, ("magic the gathering", "magic: the gathering", "mtg")) and is_card:
        matches.add("Magic: The Gathering")
    if _record_has_phrase(record, "one piece") and is_card:
        matches.add("One Piece Card Game")
    if _record_has_any_phrase(record, ("oshi push",)) and is_card:
        matches.add("Oshi Push TCG")
    if _record_has_phrase(record, "pokemon") and is_card:
        matches.add("Pokemon")
    if _record_has_any_phrase(record, ("riftbound",)) and is_card:
        matches.add("Riftbound TCG")
    if _record_has_any_phrase(record, ("star wars unlimited", "star wars: unlimited")):
        matches.add("Star Wars: Unlimited")
    if _record_has_any_phrase(record, ("yu gi oh", "yu-gi-oh", "yugioh")) and is_card:
        matches.add("Yu-Gi-Oh!")
    if is_preorder and is_card:
        matches.add("Card Game Pre-Orders")
    if is_card and _record_has_any_phrase(record, ("starter deck", "starter set", "beginner", "battle academy")):
        matches.add("Get Started Card Gaming")
    if _record_has_phrase(record, "cosmere") and is_rpg:
        matches.add("Cosmere")
    if _record_has_phrase(record, "call of cthulhu") and is_rpg:
        matches.add("Call of Cthulhu")
    if _record_has_any_phrase(record, ("dune adventures in the imperium", "dune: adventures in the imperium")):
        matches.add("Dune: Adventures in the Imperium")
    if _record_has_any_phrase(record, ("dungeons dragons", "dungeons & dragons", "d d")) and is_rpg:
        matches.add("Dungeons & Dragons")
    if _record_has_any_phrase(record, ("fallout roleplaying", "fallout: the roleplaying game")):
        matches.add("Fallout: The Roleplaying Game")
    if _record_has_all_phrases(record, ("pathfinder", "rulebook")) or _record_has_all_phrases(record, ("pathfinder", "beginner")):
        matches.add("Pathfinder")
    if _record_has_phrase(record, "starfinder") and is_rpg:
        matches.add("Starfinder")
    if _record_has_any_phrase(record, ("the one ring",)) and is_rpg:
        matches.add("The One Ring")
    if _record_has_any_phrase(record, ("vampire the masquerade", "vampire: the masquerade")):
        matches.add("Vampire: The Masquerade")
    if is_rpg and _record_has_any_phrase(record, ("starter set", "starter box", "beginner box", "essentials kit")):
        matches.add("Get Started Role-Playing")
    if matches & BOARD_CARD_CHILDREN:
        matches.add("Board & Card Games")
    if matches & GETTING_STARTED_CHILDREN:
        matches.add("Getting Started")
    if matches & CCG_CHILDREN:
        matches.add("Collectable Card Games")
    if matches & RPG_CHILDREN:
        matches.add("Role-Playing Games")
    return matches


def collection_marker_tag_from_handle(handle: str) -> str:
    return f"{AUTO_COLLECTION_TAG_PREFIX}{handle}"


def collection_marker_tag(collection_title: str) -> str:
    for title, handle in WAYLAND_COLLECTION_SPECS:
        if title == collection_title:
            return collection_marker_tag_from_handle(handle)
    raise KeyError(f"Unknown collection title: {collection_title}")


def wayland_collection_marker_tags(collection_titles: Iterable[str]) -> list[str]:
    tags = [collection_marker_tag(title) for title in collection_titles]
    return sorted(set(tags))


def wayland_auto_collection_tag_set() -> set[str]:
    return {collection_marker_tag_from_handle(handle) for _, handle in WAYLAND_COLLECTION_SPECS}


WAYLAND_COLLECTION_HANDLES = {handle for _, handle in WAYLAND_COLLECTION_SPECS}


def managed_wayland_collection_tag(collection: dict[str, Any]) -> str:
    if collection.get("collection_type") != "smart":
        return ""
    tag_rules = [
        rule.get("condition") or ""
        for rule in collection.get("rules") or []
        if (rule.get("column") or "").upper() == "TAG"
        and (rule.get("relation") or "").upper() == "EQUALS"
    ]
    if len(tag_rules) != 1:
        return ""
    tag = tag_rules[0].strip()
    return tag if tag.startswith(AUTO_COLLECTION_TAG_PREFIX) else ""


def is_managed_wayland_collection(collection: dict[str, Any], expected_tag: str = "") -> bool:
    if (collection.get("handle") or "") not in WAYLAND_COLLECTION_HANDLES:
        return False
    tag = managed_wayland_collection_tag(collection)
    if not tag:
        return False
    return not expected_tag or tag == expected_tag


def smart_collection_tags_for_product(product: Product) -> list[str]:
    record = {
        "id": product.sku or product.title,
        "title": product.title,
        "vendor": product.vendor,
        "product_type": product.product_type,
        "tags": product.tags,
        "created_at": "",
        "skus": [product.sku] if product.sku else [],
        "search_text": _normalize_search_text(" ".join([
            product.title,
            product.vendor,
            product.product_type,
            *product.tags,
            product.sku,
        ])),
    }
    return wayland_collection_marker_tags(classify_wayland_collection_titles(record))


def build_wayland_collection_matches(
    products: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, set[str]]]:
    by_collection: dict[str, list[dict[str, Any]]] = {title: [] for title, _ in WAYLAND_COLLECTION_SPECS}
    matches_by_product: dict[str, set[str]] = {
        record["id"]: classify_wayland_collection_titles(record)
        for record in products
    }

    gw_products = sorted(
        (record for record in products if _is_games_workshop_product(record)),
        key=lambda item: item.get("created_at") or "",
        reverse=True,
    )
    for record in gw_products[:LATEST_GW_RELEASE_LIMIT]:
        matches_by_product[record["id"]].add("Latest Releases")

    unmatched: list[dict[str, Any]] = []
    for record in products:
        product_matches = matches_by_product[record["id"]]
        if not product_matches:
            unmatched.append(record)
            continue
        for title, _ in WAYLAND_COLLECTION_SPECS:
            if title in product_matches:
                by_collection[title].append(record)

    return by_collection, unmatched, matches_by_product


# ---------------------------------------------------------------------------
# Shopify GraphQL client
# ---------------------------------------------------------------------------

class Shopify:
    def __init__(self, store: str, token: str):
        self.store = store
        self.token = token
        self.endpoint = f"https://{store}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def gql(self, query: str, variables: dict[str, Any] | None = None,
            max_retries: int = 6) -> dict[str, Any]:
        body = {"query": query, "variables": variables or {}}
        for attempt in range(max_retries):
            r = self.session.post(self.endpoint, json=body, timeout=60)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", 2))
                log(f"  rate-limited (429), sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt
                log(f"  server error {r.status_code}, retry in {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                try:
                    err_data = r.json()
                    detail = json.dumps(err_data)[:1000]
                except Exception:
                    detail = r.text[:1000]
                raise RuntimeError(f"Shopify HTTP {r.status_code}: {detail}")
            try:
                data = r.json()
            except Exception as e:
                raise RuntimeError(f"Bad JSON from Shopify: {e}\n{r.text[:500]}")
            if "errors" in data:
                errors = data.get("errors", [])
                throttled = any(_graphql_error_code(err) == "THROTTLED" for err in errors)
                if throttled:
                    time.sleep(1.5)
                    continue
                raise RuntimeError(f"GraphQL errors: {_format_graphql_errors(errors)}")
            # Respect cost throttle
            cost = data.get("extensions", {}).get("cost", {})
            ts = cost.get("throttleStatus", {})
            avail = ts.get("currentlyAvailable", 1000)
            req = ts.get("restoreRate", 50)
            if avail < 200:
                time.sleep(max(0.5, (250 - avail) / max(req, 1)))
            return data["data"]
        raise RuntimeError("Exceeded max retries on GraphQL request")

    def get_shop_name(self) -> str:
        data = self.gql("""
            query {
              shop { name }
            }
        """)
        return data["shop"]["name"]

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------
    def get_primary_location_id(self) -> str:
        data = self.gql("""
            query {
              locations(first: 25) {
                edges { node { id name isPrimary fulfillsOnlineOrders } }
              }
            }
        """)
        edges = data["locations"]["edges"]
        if not edges:
            raise RuntimeError("No locations found on this Shopify store")
        for e in edges:
            if e["node"].get("isPrimary"):
                return e["node"]["id"]
        return edges[0]["node"]["id"]

    def validate_location_id(self, location_id: str) -> str:
        normalized = normalize_location_id(location_id)
        data = self.gql("""
            query($id: ID!) {
              location(id: $id) {
                id
                name
              }
            }
        """, {"id": normalized})
        location = data.get("location")
        if not location:
            raise RuntimeError(f"Configured SHOPIFY_LOCATION not found or inaccessible: {normalized}")
        return location["id"]

    # ------------------------------------------------------------------
    # Products: list + delete
    # ------------------------------------------------------------------
    def iter_all_products(self) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    variants(first: 10) {
                      edges { node { id sku } }
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor})
            edges = data["products"]["edges"]
            for e in edges:
                yield e["node"]
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def delete_product(self, product_id: str) -> None:
        q = """
            mutation($input: ProductDeleteInput!) {
              productDelete(input: $input) {
                deletedProductId
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"input": {"id": product_id}})
        errs = data["productDelete"]["userErrors"]
        if errs:
            raise RuntimeError(f"productDelete errors: {errs}")

    # ------------------------------------------------------------------
    # Collections: list + delete
    # ------------------------------------------------------------------
    def iter_all_collections(self) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String) {
              collections(first: 100, after: $cursor) {
                edges {
                  node {
                    id
                    title
                    handle
                    productsCount {
                      count
                    }
                    ruleSet {
                      appliedDisjunctively
                      rules {
                        column
                        relation
                        condition
                      }
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor})
            edges = data["collections"]["edges"]
            for edge in edges:
                node = edge["node"]
                yield {
                    "id": node["id"],
                    "title": node.get("title") or "",
                    "handle": node.get("handle") or "",
                    "products_count": int((node.get("productsCount") or {}).get("count") or 0),
                    "collection_type": "smart" if node.get("ruleSet") else "custom",
                    "rules": (node.get("ruleSet") or {}).get("rules") or [],
                }
            if not data["collections"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["collections"]["pageInfo"]["endCursor"]

    def delete_collection(self, collection_id: str) -> None:
        q = """
            mutation($input: CollectionDeleteInput!) {
              collectionDelete(input: $input) {
                deletedCollectionId
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"input": {"id": collection_id}})
        errs = data["collectionDelete"]["userErrors"]
        if errs:
            raise RuntimeError(f"collectionDelete errors: {errs}")

    def iter_existing_for_collection_generation(self) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    vendor
                    productType
                    tags
                    createdAt
                    variants(first: 10) {
                      edges {
                        node {
                          sku
                        }
                      }
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor})
            for edge in data["products"]["edges"]:
                node = edge["node"]
                skus = [
                    (variant_edge["node"].get("sku") or "").strip()
                    for variant_edge in node["variants"]["edges"]
                    if (variant_edge["node"].get("sku") or "").strip()
                ]
                tags = [str(tag).strip() for tag in node.get("tags") or [] if str(tag).strip()]
                search_parts = [
                    node.get("title") or "",
                    node.get("vendor") or "",
                    node.get("productType") or "",
                    *tags,
                    *skus,
                ]
                yield {
                    "id": node["id"],
                    "title": node.get("title") or "",
                    "vendor": node.get("vendor") or "",
                    "product_type": node.get("productType") or "",
                    "tags": tags,
                    "created_at": node.get("createdAt") or "",
                    "skus": skus,
                    "search_text": _normalize_search_text(" ".join(search_parts)),
                }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def create_smart_collection(self, title: str, handle: str, tag: str, description_html: str = "") -> dict[str, Any]:
        q = """
            mutation($input: CollectionInput!) {
              collectionCreate(input: $input) {
                collection {
                  id
                  title
                  handle
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {
            "input": {
                "title": title,
                "handle": handle,
                "descriptionHtml": description_html,
                "ruleSet": {
                    "appliedDisjunctively": False,
                    "rules": [
                        {
                            "column": "TAG",
                            "relation": "EQUALS",
                            "condition": tag,
                        }
                    ],
                },
            }
        })
        errs = data["collectionCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"collectionCreate errors for {title!r}: {errs}")
        collection = data["collectionCreate"].get("collection")
        if not collection:
            raise RuntimeError(f"collectionCreate did not return a collection for {title!r}")
        return collection

    def update_smart_collection(self, collection_id: str, title: str, handle: str, tag: str) -> None:
        q = """
            mutation($input: CollectionInput!) {
              collectionUpdate(input: $input) {
                collection {
                  id
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {
            "input": {
                "id": collection_id,
                "title": title,
                "handle": handle,
                "ruleSet": {
                    "appliedDisjunctively": False,
                    "rules": [
                        {
                            "column": "TAG",
                            "relation": "EQUALS",
                            "condition": tag,
                        }
                    ],
                },
            }
        })
        errs = data["collectionUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"collectionUpdate errors for {title!r}: {errs}")

    def publish_to_current_channel(self, resource_id: str) -> None:
        q = """
            mutation($id: ID!) {
              publishablePublishToCurrentChannel(id: $id) {
                publishable {
                  ... on Collection {
                    id
                  }
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"id": resource_id})
        errs = data["publishablePublishToCurrentChannel"]["userErrors"]
        if errs:
            raise RuntimeError(f"publishablePublishToCurrentChannel errors: {errs}")

    def iter_publications(self) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String) {
              publications(first: 100, after: $cursor) {
                edges {
                  node {
                    id
                    name
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor})
            for edge in data["publications"]["edges"]:
                node = edge["node"]
                yield {
                    "id": node["id"],
                    "name": node.get("name") or "",
                }
            if not data["publications"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["publications"]["pageInfo"]["endCursor"]

    def publish_to_all_channels(self, resource_id: str) -> int:
        publications = list(self.iter_publications())
        if not publications:
            self.publish_to_current_channel(resource_id)
            return 1
        q = """
            mutation($id: ID!, $input: [PublicationInput!]!) {
              publishablePublish(id: $id, input: $input) {
                publishable {
                  ... on Collection {
                    id
                  }
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {
            "id": resource_id,
            "input": [{"publicationId": publication["id"]} for publication in publications],
        })
        errs = data["publishablePublish"]["userErrors"]
        if errs:
            raise RuntimeError(f"publishablePublish errors: {errs}")
        return len(publications)

    def update_product_tags(self, product_id: str, tags: list[str]) -> None:
        q = """
            mutation($product: ProductUpdateInput!) {
              productUpdate(product: $product) {
                product { id }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"product": {"id": product_id, "tags": tags}})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productUpdate errors for {product_id}: {errs}")

    # ------------------------------------------------------------------
    # Product create (with variant + inventory)
    # ------------------------------------------------------------------
    def create_product(self, p: Product, location_id: str) -> str:
        # Step 1: create the product shell and use Shopify's standalone variant.
        create_q = """
            mutation($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
              productCreate(product: $product, media: $media) {
                product {
                  id
                  variants(first: 1) {
                    edges { node { id inventoryItem { id } } }
                  }
                }
                userErrors { field message }
              }
            }
        """
        product_tags = sorted(set(p.tags) | set(smart_collection_tags_for_product(p)))
        product_input: dict[str, Any] = {
            "title": p.title,
            "vendor": p.vendor or "Foxfable",
            "productType": p.product_type or "",
            "tags": product_tags,
            "descriptionHtml": p.description_html,
            "status": "ACTIVE",
        }

        data = self.gql(create_q, {"product": product_input, "media": []})
        errs = data["productCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productCreate errors for {p.sku}: {errs}")
        product = data["productCreate"]["product"]
        product_id = product["id"]
        v_edges = product["variants"]["edges"]
        if not v_edges:
            return product_id
        variant_id = v_edges[0]["node"]["id"]
        inventory_item_id = v_edges[0]["node"]["inventoryItem"]["id"]

        # Step 2: update the automatically created standalone variant.
        variant_input: dict[str, Any] = {
            "id": variant_id,
            "price": f"{p.price:.2f}",
            "inventoryPolicy": "DENY",
            "taxable": True,
        }
        if p.compare_at_price is not None:
            variant_input["compareAtPrice"] = f"{p.compare_at_price:.2f}"
        if p.barcode:
            variant_input["barcode"] = p.barcode

        inventory_item_input: dict[str, Any] = {
            "sku": p.sku,
            "tracked": True,
            "requiresShipping": True,
        }
        if p.cost is not None and p.cost > 0:
            inventory_item_input["cost"] = f"{p.cost:.2f}"
        if p.weight_grams is not None and p.weight_grams > 0:
            inventory_item_input["measurement"] = {
                "weight": {
                    "value": round(p.weight_grams, 2),
                    "unit": "GRAMS",
                }
            }
        variant_input["inventoryItem"] = inventory_item_input

        update_q = """
            mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants {
                  id
                  inventoryItem { id }
                }
                userErrors { field message }
              }
            }
        """
        update_data = self.gql(update_q, {
            "productId": product_id,
            "variants": [variant_input],
        })
        update_errs = update_data["productVariantsBulkUpdate"]["userErrors"]
        if update_errs:
            raise RuntimeError(f"productVariantsBulkUpdate errors for {p.sku}: {update_errs}")

        # Step 3: set inventory level at the location
        inv_q = """
            mutation($input: InventorySetOnHandQuantitiesInput!) {
              inventorySetOnHandQuantities(input: $input) {
                inventoryAdjustmentGroup { id }
                userErrors { field message }
              }
            }
        """
        inv_data = self.gql(inv_q, {
            "input": {
                "reason": "correction",
                "referenceDocumentUri": "logistics://foxfable/initial-load",
                "setQuantities": [{
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,
                    "quantity": int(p.quantity or 0),
                }],
            }
        })
        inv_errs = inv_data["inventorySetOnHandQuantities"]["userErrors"]
        if inv_errs:
            log(f"  warn: failed to set inventory on {p.sku}: {inv_errs}")

        return product_id

    # ------------------------------------------------------------------
    # Update existing products by SKU
    # ------------------------------------------------------------------
    def iter_existing_for_update(self, location_id: str) -> Iterable[dict[str, Any]]:
        """Yield one record per variant that has a SKU, with current price/cost/qty.

        Each record has keys: product_id, title, variant_id, sku, price,
        compare_at_price, cost, inventory_item_id, on_hand.
        """
        cursor = None
        page_q = """
            query($cursor: String, $locationId: ID!) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    variants(first: 25) {
                      edges {
                        node {
                          id
                          sku
                          price
                          compareAtPrice
                          inventoryItem {
                            id
                            unitCost { amount }
                            inventoryLevel(locationId: $locationId) {
                              quantities(names: ["on_hand"]) { name quantity }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor, "locationId": location_id})
            for edge in data["products"]["edges"]:
                node = edge["node"]
                for v_edge in node["variants"]["edges"]:
                    v = v_edge["node"]
                    sku = (v.get("sku") or "").strip()
                    if not sku:
                        continue
                    inv_item = v.get("inventoryItem") or {}
                    unit_cost = (inv_item.get("unitCost") or {}).get("amount")
                    inv_level = inv_item.get("inventoryLevel") or {}
                    on_hand = 0
                    for q in inv_level.get("quantities") or []:
                        if q.get("name") == "on_hand":
                            on_hand = int(q.get("quantity") or 0)
                            break
                    yield {
                        "product_id": node["id"],
                        "title": node.get("title") or "",
                        "variant_id": v["id"],
                        "sku": sku,
                        "price": _safe_float(v.get("price")),
                        "compare_at_price": _safe_float(v.get("compareAtPrice")),
                        "cost": _safe_float(unit_cost),
                        "inventory_item_id": inv_item.get("id"),
                        "on_hand": on_hand,
                    }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def update_variant_fields(self, product_id: str, variant_input: dict[str, Any]) -> None:
        q = """
            mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants { id }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"productId": product_id, "variants": [variant_input]})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productVariantsBulkUpdate errors: {errs}")

    def set_on_hand(self, inventory_item_id: str, location_id: str, quantity: int) -> None:
        q = """
            mutation($input: InventorySetOnHandQuantitiesInput!) {
              inventorySetOnHandQuantities(input: $input) {
                inventoryAdjustmentGroup { id }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {
            "input": {
                "reason": "correction",
                "referenceDocumentUri": "logistics://foxfable/sheet-update",
                "setQuantities": [{
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,
                    "quantity": int(quantity),
                }],
            }
        })
        errs = data["inventorySetOnHandQuantities"]["userErrors"]
        if errs:
            raise RuntimeError(f"inventorySetOnHandQuantities errors: {errs}")

    # ------------------------------------------------------------------
    # Photo sync for existing GW products
    # ------------------------------------------------------------------
    def iter_existing_for_photo_sync(self) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    vendor
                    tags
                    variants(first: 25) {
                      edges {
                        node {
                          sku
                        }
                      }
                    }
                    media(first: 50) {
                      edges {
                        node {
                          id
                        }
                      }
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"cursor": cursor})
            for edge in data["products"]["edges"]:
                node = edge["node"]
                media_ids = [m["node"]["id"] for m in node["media"]["edges"]]
                for v_edge in node["variants"]["edges"]:
                    sku = (v_edge["node"].get("sku") or "").strip()
                    if not sku:
                        continue
                    yield {
                        "product_id": node["id"],
                        "title": node.get("title") or "",
                        "vendor": node.get("vendor") or "",
                        "tags": node.get("tags") or [],
                        "sku": sku,
                        "media_ids": media_ids,
                    }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def staged_uploads_create(self, image_paths: list[Path]) -> list[dict[str, Any]]:
        inputs = []
        for path in image_paths:
            inputs.append({
                "filename": path.name,
                "mimeType": mimetypes.guess_type(path.name)[0] or "image/jpeg",
                "resource": "IMAGE",
                "httpMethod": "PUT",
                "fileSize": str(path.stat().st_size),
            })
        q = """
            mutation($input: [StagedUploadInput!]!) {
              stagedUploadsCreate(input: $input) {
                stagedTargets {
                  url
                  resourceUrl
                  parameters { name value }
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"input": inputs})
        errs = data["stagedUploadsCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"stagedUploadsCreate errors: {errs}")
        return data["stagedUploadsCreate"]["stagedTargets"]

    def upload_file_to_staged_target(self, image_path: Path, target: dict[str, Any]) -> str:
        headers = {item["name"]: item["value"] for item in target["parameters"]}
        headers.setdefault("Content-Type", mimetypes.guess_type(image_path.name)[0] or "image/jpeg")
        with image_path.open("rb") as fh:
            response = requests.put(
                target["url"],
                data=fh.read(),
                headers=headers,
                timeout=120,
            )
        if response.status_code >= 400:
            raise RuntimeError(f"Staged upload failed for {image_path.name}: HTTP {response.status_code}")
        return target["resourceUrl"]

    def file_create(self, source_urls: list[str], alt_text: str) -> list[dict[str, Any]]:
        files = [{
            "originalSource": source_url,
            "contentType": "IMAGE",
            "alt": alt_text,
        } for source_url in source_urls]
        q = """
            mutation($files: [FileCreateInput!]!) {
              fileCreate(files: $files) {
                files {
                  id
                  fileStatus
                  alt
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"files": files})
        errs = data["fileCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"fileCreate errors: {errs}")
        return data["fileCreate"]["files"]

    def wait_for_files_ready(self, file_ids: list[str], timeout_seconds: int = 240) -> list[str]:
        if not file_ids:
            return []
        q = """
            query($id: ID!) {
              node(id: $id) {
                ... on File {
                  id
                  fileStatus
                }
              }
            }
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            statuses: dict[str, str] = {}
            for file_id in file_ids:
                data = self.gql(q, {"id": file_id})
                node = data.get("node")
                if not node:
                    continue
                statuses[node["id"]] = node.get("fileStatus") or ""
            if statuses and all(status == "READY" for status in statuses.values()):
                return file_ids
            if any(status == "FAILED" for status in statuses.values()):
                raise RuntimeError(f"File processing failed: {statuses}")
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for files to become READY: {file_ids}")

    def file_update(self, files: list[dict[str, Any]]) -> None:
        q = """
            mutation($files: [FileUpdateInput!]!) {
              fileUpdate(files: $files) {
                files { id }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"files": files})
        errs = data["fileUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"fileUpdate errors: {errs}")

    def attach_files_to_product(self, file_ids: list[str], product_id: str) -> None:
        self.file_update([
            {"id": file_id, "referencesToAdd": [product_id]}
            for file_id in file_ids
        ])

    def detach_files_from_product(self, file_ids: list[str], product_id: str) -> None:
        self.file_update([
            {"id": file_id, "referencesToRemove": [product_id]}
            for file_id in file_ids
        ])

    def wait_for_job(self, job_id: str, timeout_seconds: int = 240) -> None:
        q = """
            query($id: ID!) {
              node(id: $id) {
                ... on Job {
                  id
                  done
                }
              }
            }
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            data = self.gql(q, {"id": job_id})
            job = data.get("node")
            if job and job.get("done"):
                return
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for job completion: {job_id}")

    def reorder_product_media(self, product_id: str, media_ids: list[str]) -> None:
        if not media_ids:
            return
        moves = [{"id": media_id, "newPosition": index} for index, media_id in enumerate(media_ids)]
        q = """
            mutation($id: ID!, $moves: [MoveInput!]!) {
              productReorderMedia(id: $id, moves: $moves) {
                job { id done }
                mediaUserErrors { field message }
              }
            }
        """
        data = self.gql(q, {"id": product_id, "moves": moves})
        errs = data["productReorderMedia"]["mediaUserErrors"]
        if errs:
            raise RuntimeError(f"productReorderMedia errors: {errs}")
        job = data["productReorderMedia"].get("job")
        if job and job.get("id") and not job.get("done"):
            self.wait_for_job(job["id"])


# ---------------------------------------------------------------------------
# Phases: delete / import
# ---------------------------------------------------------------------------

def phase_delete(client: Shopify, dry: bool) -> None:
    log("=== DELETE phase: removing ALL products in the shop ===")
    deleted = 0
    for prod in client.iter_all_products():
        log(f"  delete: {prod['title']!r}  ({prod['id']})")
        if not dry:
            try:
                client.delete_product(prod["id"])
                deleted += 1
            except Exception as e:
                log(f"  ERROR deleting {prod['id']}: {e}")
    log(f"DELETE summary: deleted={deleted}, dry_run={dry}")


def phase_delete_collections(client: Shopify, dry: bool) -> None:
    log("=== DELETE COLLECTIONS phase: removing managed Wayland collections ===")
    deleted = 0
    for collection in client.iter_all_collections():
        if not is_managed_wayland_collection(collection):
            log(
                "  skip unmanaged collection: "
                f"{collection['title']!r} [{collection['collection_type']}] "
                f"handle={collection['handle']!r} ({collection['id']})"
            )
            continue
        log(
            "  delete collection: "
            f"{collection['title']!r} [{collection['collection_type']}] "
            f"handle={collection['handle']!r} ({collection['id']})"
        )
        if not dry:
            try:
                client.delete_collection(collection["id"])
                deleted += 1
            except Exception as e:
                log(f"  ERROR deleting collection {collection['id']}: {e}")
    log(f"DELETE COLLECTIONS summary: deleted={deleted}, dry_run={dry}")


def write_collection_generation_preview(
    by_collection: dict[str, list[dict[str, Any]]],
    unmatched: list[dict[str, Any]],
) -> None:
    preview_cols = [
        "collection_title",
        "collection_handle",
        "product_id",
        "title",
        "vendor",
        "product_type",
        "skus",
        "tags",
    ]
    with COLLECTION_GENERATION_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=preview_cols)
        writer.writeheader()
        for title, handle in WAYLAND_COLLECTION_SPECS:
            for record in by_collection[title]:
                writer.writerow({
                    "collection_title": title,
                    "collection_handle": handle,
                    "product_id": record["id"],
                    "title": record["title"],
                    "vendor": record["vendor"],
                    "product_type": record["product_type"],
                    "skus": "|".join(record["skus"]),
                    "tags": "|".join(record["tags"]),
                })

    unmatched_cols = ["product_id", "title", "vendor", "product_type", "skus", "tags"]
    with COLLECTION_GENERATION_UNMATCHED_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=unmatched_cols)
        writer.writeheader()
        for record in unmatched:
            writer.writerow({
                "product_id": record["id"],
                "title": record["title"],
                "vendor": record["vendor"],
                "product_type": record["product_type"],
                "skus": "|".join(record["skus"]),
                "tags": "|".join(record["tags"]),
            })


def phase_generate_collections(client: Shopify, dry: bool) -> None:
    log("=== GENERATE COLLECTIONS phase: building Wayland-style smart collections ===")
    products = list(client.iter_existing_for_collection_generation())
    existing_collections = {item["handle"]: item for item in client.iter_all_collections()}
    by_collection, unmatched, matches_by_product = build_wayland_collection_matches(products)
    write_collection_generation_preview(by_collection, unmatched)
    log(
        f"  wrote collection preview: {COLLECTION_GENERATION_PREVIEW_CSV} "
        f"and unmatched report: {COLLECTION_GENERATION_UNMATCHED_CSV}"
    )
    matched_count = sum(len(items) for items in by_collection.values())
    non_empty = sum(1 for items in by_collection.values() if items)
    log(
        f"  planned memberships={matched_count} across {non_empty}/{len(WAYLAND_COLLECTION_SPECS)} "
        f"collections; unmatched_products={len(unmatched)}"
    )
    for title, _ in WAYLAND_COLLECTION_SPECS:
        if by_collection[title]:
            log(f"  match count: {title} -> {len(by_collection[title])}")

    if dry:
        return

    auto_tags = wayland_auto_collection_tag_set()
    updated_products = 0
    created = 0
    refreshed = 0
    published = 0
    for record in products:
        desired_auto_tags = set(wayland_collection_marker_tags(matches_by_product[record["id"]]))
        current_tags = set(record["tags"])
        next_tags = sorted((current_tags - auto_tags) | desired_auto_tags)
        if next_tags != sorted(current_tags):
            client.update_product_tags(record["id"], next_tags)
            updated_products += 1

    for title, handle in WAYLAND_COLLECTION_SPECS:
        collection = existing_collections.get(handle)
        marker_tag = collection_marker_tag_from_handle(handle)
        if collection and collection["collection_type"] != "smart":
            raise RuntimeError(
                f"Existing collection handle {handle!r} is a manual collection. "
                "Delete it first with --delete-collections or rename it before regenerating."
            )
        if collection and not is_managed_wayland_collection(collection, expected_tag=marker_tag):
            raise RuntimeError(
                f"Existing smart collection handle {handle!r} is not managed by this script. "
                "Refusing to overwrite a non-marker rule set."
            )

        if collection:
            refreshed += 1
            client.update_smart_collection(collection["id"], title, handle, marker_tag)
        else:
            collection = client.create_smart_collection(title, handle, marker_tag)
            created += 1

        try:
            published += client.publish_to_all_channels(collection["id"])
        except Exception as e:
            log(
                f"  warn: could not publish collection {title!r} to all sales channels: {e}. "
                "The collection may remain hidden until the app has publication scopes."
            )

    log(
        f"GENERATE COLLECTIONS summary: tagged_products={updated_products} created={created} refreshed={refreshed} "
        f"publication_links={published} unmatched_products={len(unmatched)}"
    )


def phase_import(client: Shopify, products: list[Product], location_id: str, dry: bool,
                 start_at: int = 0) -> None:
    log(f"=== IMPORT phase: creating {len(products)} products (starting at index {start_at}) ===")
    if dry:
        log("(dry-run: not contacting Shopify)")
        return
    created = 0
    failed = 0
    for i, p in enumerate(products):
        if i < start_at:
            continue
        try:
            pid = client.create_product(p, location_id)
            created += 1
            if (created % 25) == 0:
                log(f"  progress: created {created}/{len(products) - start_at}  (last: {p.sku})")
        except Exception as e:
            failed += 1
            log(f"  FAILED {p.sku} ({p.title!r}): {e}")
            with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                fh.write(f"{i}\t{p.sku}\t{p.title}\t{e}\n")
    log(f"IMPORT summary: created={created}, failed={failed}")


def _money_eq(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) < MONEY_EPSILON


def _fmt_money(v: float | None) -> str:
    return "" if v is None else f"{float(v):.2f}"


def phase_update(client: Shopify, products: list[Product], location_id: str, dry: bool) -> None:
    """Update existing Shopify products in place from the parsed sheet rows.

    Matches by SKU. For each match, pushes price, compare_at_price, cost, and
    on-hand quantity if they differ. Skips SKUs that don't yet exist in Shopify.
    """
    log("=== UPDATE phase: matching by SKU and pushing field changes ===")
    log(f"  fetching existing Shopify products at location {location_id} ...")
    by_sku: dict[str, dict[str, Any]] = {}
    for rec in client.iter_existing_for_update(location_id):
        # If a SKU appears on multiple variants, last one wins; flag it.
        if rec["sku"] in by_sku:
            log(f"  warn: duplicate SKU on Shopify side: {rec['sku']}")
        by_sku[rec["sku"]] = rec
    log(f"  found {len(by_sku)} variants in Shopify with a SKU")

    diff_rows: list[dict[str, Any]] = []
    missing: list[str] = []
    unchanged = 0
    price_updates = qty_updates = cost_updates = 0
    failed = 0

    for p in products:
        existing = by_sku.get(p.sku)
        if not existing:
            missing.append(p.sku)
            continue

        variant_input: dict[str, Any] = {"id": existing["variant_id"]}
        inventory_item_input: dict[str, Any] = {}
        change_summary: list[str] = []

        if not _money_eq(existing["price"], p.price):
            variant_input["price"] = f"{p.price:.2f}"
            change_summary.append(
                f"price {_fmt_money(existing['price'])}->{p.price:.2f}"
            )
            price_updates += 1
        if not _money_eq(existing["compare_at_price"], p.compare_at_price):
            variant_input["compareAtPrice"] = (
                f"{p.compare_at_price:.2f}" if p.compare_at_price is not None else None
            )
            change_summary.append(
                f"compare_at {_fmt_money(existing['compare_at_price'])}->"
                f"{_fmt_money(p.compare_at_price)}"
            )
            price_updates += 1
        if p.cost is not None and not _money_eq(existing["cost"], p.cost):
            inventory_item_input["cost"] = f"{p.cost:.2f}"
            change_summary.append(
                f"cost {_fmt_money(existing['cost'])}->{p.cost:.2f}"
            )
            cost_updates += 1

        qty_changed = int(existing["on_hand"]) != int(p.quantity or 0)
        if qty_changed:
            change_summary.append(
                f"on_hand {existing['on_hand']}->{int(p.quantity or 0)}"
            )
            qty_updates += 1

        has_variant_change = len(variant_input) > 1 or bool(inventory_item_input)
        if not has_variant_change and not qty_changed:
            unchanged += 1
            continue

        diff_rows.append({
            "sku": p.sku,
            "title": p.title,
            "changes": "; ".join(change_summary),
            "shopify_price": _fmt_money(existing["price"]),
            "sheet_price": f"{p.price:.2f}",
            "shopify_compare_at": _fmt_money(existing["compare_at_price"]),
            "sheet_compare_at": _fmt_money(p.compare_at_price),
            "shopify_cost": _fmt_money(existing["cost"]),
            "sheet_cost": _fmt_money(p.cost),
            "shopify_on_hand": existing["on_hand"],
            "sheet_on_hand": int(p.quantity or 0),
        })

        if dry:
            continue

        try:
            if has_variant_change:
                if inventory_item_input:
                    variant_input["inventoryItem"] = inventory_item_input
                client.update_variant_fields(existing["product_id"], variant_input)
            if qty_changed:
                client.set_on_hand(
                    existing["inventory_item_id"],
                    location_id,
                    int(p.quantity or 0),
                )
        except Exception as e:
            failed += 1
            log(f"  FAILED update {p.sku} ({p.title!r}): {e}")
            with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                fh.write(f"update\t{p.sku}\t{p.title}\t{e}\n")

    # Always write the diff CSV so it's available after a real run too.
    cols = [
        "sku", "title", "changes",
        "shopify_price", "sheet_price",
        "shopify_compare_at", "sheet_compare_at",
        "shopify_cost", "sheet_cost",
        "shopify_on_hand", "sheet_on_hand",
    ]
    with UPDATE_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in diff_rows:
            w.writerow(row)
    log(f"  wrote diff: {UPDATE_PREVIEW_CSV} ({len(diff_rows)} rows)")

    if missing:
        log(f"  {len(missing)} sheet SKUs not found in Shopify (skipped). "
            f"First few: {missing[:5]}")

    log(
        f"UPDATE summary: changes={len(diff_rows)} unchanged={unchanged} "
        f"missing_in_shopify={len(missing)} price_field_updates={price_updates} "
        f"cost_updates={cost_updates} qty_updates={qty_updates} failed={failed} "
        f"dry_run={dry}"
    )


def _graphql_error_code(err: Any) -> str | None:
    if isinstance(err, dict):
        return err.get("extensions", {}).get("code")
    return None


def _format_graphql_errors(errors: Any) -> str:
    if not isinstance(errors, list):
        return json.dumps(errors)[:1000]
    formatted: list[str] = []
    for err in errors:
        if isinstance(err, dict):
            formatted.append(json.dumps(err, sort_keys=True))
        else:
            formatted.append(str(err))
    return " | ".join(formatted)[:1000]


def normalize_location_id(location_id: str) -> str:
    raw = (location_id or "").strip()
    if not raw:
        raise RuntimeError("SHOPIFY_LOCATION was provided but is blank.")
    if raw.startswith("gid://shopify/Location/"):
        return raw
    if raw.isdigit():
        return f"gid://shopify/Location/{raw}"
    raise RuntimeError(
        "SHOPIFY_LOCATION must be either a numeric location ID or a gid://shopify/Location/... value."
    )


def prepare_products_for_import() -> list[Product]:
    products = build_product_list(strict=True)
    try:
        write_preview(products)
    except Exception as e:
        raise RuntimeError(f"Failed to write preview CSV: {e}") from e
    return products


def resolve_location_for_import(client: Shopify, env: dict[str, str]) -> str:
    explicit = (env.get("SHOPIFY_LOCATION") or "").strip()
    if explicit:
        return client.validate_location_id(explicit)
    return client.get_primary_location_id()


def run_preflight(client: Shopify, env: dict[str, str]) -> str:
    shop_name = client.get_shop_name()
    location_id = resolve_location_for_import(client, env)
    log(f"Preflight OK: authenticated shop {shop_name!r}")
    log(f"Preflight OK: location {location_id}")
    return location_id


def run_photo_sync_preflight(client: Shopify) -> None:
    shop_name = client.get_shop_name()
    log(f"Photo sync preflight OK: authenticated shop {shop_name!r}")


def phase_photo_sync(
    client: Shopify,
    products: list[Product],
    photo_root: Path,
    dry: bool,
    manifest_path: Path = PHOTO_SYNC_MANIFEST_JSON,
) -> None:
    log("=== PHOTO SYNC phase: matching GW products to staged local image folders ===")
    asset_sets = discover_photo_asset_sets(photo_root)
    by_code, by_slug = build_photo_indexes(asset_sets)
    manifest = load_photo_manifest(manifest_path)

    by_sku: dict[str, dict[str, Any]] = {}
    duplicate_shopify_skus: set[str] = set()
    for rec in client.iter_existing_for_photo_sync():
        if rec["sku"] in by_sku:
            log(f"  warn: duplicate SKU on Shopify side for photo sync: {rec['sku']}")
            duplicate_shopify_skus.add(rec["sku"])
            continue
        by_sku[rec["sku"]] = rec
    log(f"  found {len(by_sku)} existing Shopify variants with SKU anchors")

    preview_rows: list[dict[str, Any]] = []
    missing_rows: list[tuple[str, str, str]] = []
    ambiguous_rows: list[tuple[str, str, str]] = []
    failure_rows: list[tuple[str, str, str]] = []

    gw_products = [p for p in products if p.source == "GW"]
    for product in gw_products:
        existing = by_sku.get(product.sku)
        if not existing:
            reason = "SKU not found in Shopify"
            preview_rows.append(build_photo_sync_preview_row(
                product,
                status="skip_missing_shopify",
                match_type="",
                photo_root=photo_root,
                reason=reason,
            ))
            missing_rows.append((product.sku, product.title, reason))
            continue
        if product.sku in duplicate_shopify_skus:
            reason = "multiple Shopify products share this SKU"
            preview_rows.append(build_photo_sync_preview_row(
                product,
                status="skip_ambiguous_shopify",
                match_type="ambiguous",
                photo_root=photo_root,
                reason=reason,
            ))
            ambiguous_rows.append((product.sku, product.title, reason))
            continue
        if not _is_games_workshop_record(existing):
            reason = "Shopify product failed Games Workshop identity gate"
            preview_rows.append(build_photo_sync_preview_row(
                product,
                status="skip_non_gw",
                match_type="",
                photo_root=photo_root,
                reason=reason,
            ))
            continue

        status, match_type, asset_set, reason = resolve_photo_asset(product, by_code, by_slug)
        preview_rows.append(build_photo_sync_preview_row(
            product,
            status=status,
            match_type=match_type,
            photo_root=photo_root,
            asset_set=asset_set,
            reason=reason,
        ))
        if not asset_set:
            if match_type == "ambiguous":
                ambiguous_rows.append((product.sku, product.title, reason))
            else:
                missing_rows.append((product.sku, product.title, reason))
            continue
        if dry:
            continue

        fingerprint = asset_set.fingerprint()
        prior_entry = dict(manifest.get(product.sku, {}))
        prior_state = prior_entry.get("state")
        reset_resume_state = (
            prior_entry.get("asset_fingerprint") != fingerprint
            or prior_entry.get("product_id") != existing["product_id"]
        )
        if (
            prior_state == "completed"
            and prior_entry.get("asset_fingerprint") == fingerprint
            and prior_entry.get("product_id") == existing["product_id"]
        ):
            log(f"  resume: skipping already-synced photo set for {product.sku}")
            continue

        try:
            if reset_resume_state:
                old_media_ids = existing["media_ids"]
                file_ids: list[str] = []
                effective_prior_state = None
            else:
                old_media_ids = prior_entry.get("old_media_ids", existing["media_ids"])
                file_ids = prior_entry.get("new_file_ids") or []
                effective_prior_state = prior_state
            entry = update_and_save_photo_manifest_entry(
                manifest,
                manifest_path,
                sku=product.sku,
                state="preparing",
                product_id=existing["product_id"],
                source_mode="staged-local-files",
                asset_label=asset_set.label,
                asset_fingerprint=fingerprint,
                old_media_ids=old_media_ids,
                detached_old_media=False,
                new_file_ids=file_ids,
                error="",
                source_paths=[str(path) for path in asset_set.image_paths],
            )

            if (
                not file_ids
                or effective_prior_state == "failed"
            ):
                staged_targets = client.staged_uploads_create(asset_set.image_paths)
                if len(staged_targets) != len(asset_set.image_paths):
                    raise RuntimeError(
                        "stagedUploadsCreate returned an unexpected number of targets: "
                        f"expected {len(asset_set.image_paths)}, got {len(staged_targets)}"
                    )
                resource_urls = [
                    client.upload_file_to_staged_target(path, target)
                    for path, target in zip(asset_set.image_paths, staged_targets)
                ]
                created_files = client.file_create(resource_urls, product.title)
                file_ids = [item["id"] for item in created_files]
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="files_created",
                    new_file_ids=file_ids,
                )

            if effective_prior_state not in {"files_ready", "associated", "reordered", "completed"}:
                client.wait_for_files_ready(file_ids)
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="files_ready",
                )
                effective_prior_state = "files_ready"

            if effective_prior_state not in {"associated", "reordered", "completed"}:
                client.attach_files_to_product(file_ids, existing["product_id"])
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="associated",
                )
                effective_prior_state = "associated"

            if effective_prior_state not in {"reordered", "completed"}:
                client.reorder_product_media(existing["product_id"], file_ids)
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="reordered",
                )
                effective_prior_state = "reordered"

            old_media_ids = entry.get("old_media_ids") or []
            if old_media_ids and not entry.get("detached_old_media"):
                client.detach_files_from_product(old_media_ids, existing["product_id"])
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    detached_old_media=True,
                )

            update_and_save_photo_manifest_entry(
                manifest,
                manifest_path,
                sku=product.sku,
                state="completed",
            )
        except Exception as e:
            update_and_save_photo_manifest_entry(
                manifest,
                manifest_path,
                sku=product.sku,
                state="failed",
                error=str(e),
            )
            detail = str(e)
            log(f"  FAILED photo sync {product.sku} ({product.title!r}): {detail}")
            failure_rows.append((product.sku, product.title, detail))

    cols = [
        "sku", "title", "status", "match_type", "asset_label",
        "image_count", "reason", "source_mode", "source_paths",
    ]
    with PHOTO_SYNC_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in preview_rows:
            writer.writerow(row)
    log(f"  wrote photo sync preview: {PHOTO_SYNC_PREVIEW_CSV} ({len(preview_rows)} rows)")

    append_photo_log(PHOTO_SYNC_MISSING_TSV, missing_rows)
    append_photo_log(PHOTO_SYNC_AMBIGUOUS_TSV, ambiguous_rows)
    append_photo_log(PHOTO_SYNC_FAILURES_TSV, failure_rows)

    log(
        f"PHOTO SYNC summary: candidates={len(gw_products)} "
        f"missing={len(missing_rows)} ambiguous={len(ambiguous_rows)} "
        f"failed={len(failure_rows)} dry_run={dry}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Shopify bulk delete + import")
    parser.add_argument("--dry-run", action="store_true", help="Don't call Shopify; just preview.")
    parser.add_argument("--gw-refresh-cache", dest="do_gw_refresh_cache", action="store_true",
                        help="Refresh the repo-local Games Workshop photo cache.")
    parser.add_argument("--preflight", action="store_true",
                        help="Validate auth and location readiness without delete/import side effects.")
    parser.add_argument("--delete", action="store_true", help="Run delete phase.")
    parser.add_argument("--delete-collections", dest="do_delete_collections", action="store_true",
                        help="Delete only managed Wayland Shopify collections.")
    parser.add_argument("--generate-collections", dest="do_generate_collections", action="store_true",
                        help="Create Wayland-style smart collections and auto-tag existing Shopify products.")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Run import phase.")
    parser.add_argument("--update", dest="do_update", action="store_true",
                        help="Update existing products in place (matched by SKU).")
    parser.add_argument("--photo-sync", dest="do_photo_sync", action="store_true",
                        help="Replace GW product media from the repo-local cache or an explicit local photo root.")
    parser.add_argument("--photo-root", type=Path,
                        help="Root folder containing staged GW image folders/files for --photo-sync.")
    parser.add_argument("--all", action="store_true", help="Run delete then import.")
    parser.add_argument("--start-at", type=int, default=0,
                        help="Resume import from this product index (after a partial run).")
    args = parser.parse_args()

    if not (args.dry_run or args.preflight or args.delete or args.do_delete_collections
            or args.do_generate_collections
            or args.do_gw_refresh_cache or args.do_import or args.do_update
            or args.do_photo_sync or args.all):
        parser.print_help()
        return 1

    if args.do_gw_refresh_cache:
        invalid_refresh_combo = (
            args.do_photo_sync or args.do_update or args.do_import or args.delete
            or args.do_delete_collections or args.do_generate_collections
            or args.all or args.preflight
            or args.start_at != 0 or args.photo_root is not None
        )
        if invalid_refresh_combo:
            raise RuntimeError(
                "--gw-refresh-cache must run separately and cannot be combined with "
                "--photo-sync, --update, --import, --delete, --delete-collections, "
                "--generate-collections, --all, --preflight, --start-at, or --photo-root."
            )
        refresh_gw_cache(
            resources_url=GW_RESOURCES_URL,
            cache_root=GW_PHOTO_CACHE_ROOT,
            status_path=GW_PHOTO_CACHE_STATUS_JSON,
            dry=args.dry_run,
            logger=log,
        )
        if args.dry_run:
            log("GW cache refresh dry-run complete. Review the discovery log, then re-run with --gw-refresh-cache to publish.")
        return 0

    if args.do_photo_sync and (args.delete or args.do_import or args.do_update or args.all):
        raise RuntimeError("--photo-sync must run separately from delete/import/update/all phases.")
    if args.do_photo_sync and args.preflight:
        raise RuntimeError("--photo-sync cannot be combined with --preflight.")
    if args.do_photo_sync and args.start_at != 0:
        raise RuntimeError("--start-at is only valid with --import/--all, not --photo-sync.")
    if args.do_delete_collections and (
        args.preflight or args.delete or args.do_import or args.do_update
        or args.do_photo_sync or args.do_generate_collections
        or args.all or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--delete-collections must run separately from preflight/delete/import/update/"
            "photo-sync/all and cannot be combined with --start-at or --photo-root."
        )
    if args.do_generate_collections and (
        args.preflight or args.delete or args.do_delete_collections or args.do_import
        or args.do_update or args.do_photo_sync or args.all
        or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--generate-collections must run separately from preflight/delete/delete-collections/"
            "import/update/photo-sync/all and cannot be combined with --start-at or --photo-root."
        )

    # Plain --dry-run (without --update / --photo-sync) means: read sheets,
    # write preview.csv, don't contact Shopify. --update/--photo-sync dry-runs
    # are handled below since they need Shopify reads.
    if (
        args.dry_run
        and not args.do_update
        and not args.do_photo_sync
        and not args.do_generate_collections
        and not args.do_delete_collections
    ):
        prepare_products_for_import()
        log("Dry run complete. Review preview.csv, then re-run with --delete, --import, or --update.")
        return 0

    env = load_env()
    store = env.get("SHOPIFY_STORE")
    token = env.get("SHOPIFY_TOKEN")
    if not store or not token:
        log("ERROR: SHOPIFY_STORE and SHOPIFY_TOKEN must be set in env or .env file.")
        log("See SETUP.md for instructions.")
        return 2

    client = Shopify(store, token)
    if args.preflight:
        run_preflight(client, env)
        return 0

    if args.do_delete_collections:
        phase_delete_collections(client, dry=args.dry_run)
        return 0
    if args.do_generate_collections:
        phase_generate_collections(client, dry=args.dry_run)
        if args.dry_run:
            log(
                "Collection generation dry-run complete. Review "
                "collection_generation_preview.csv and collection_generation_unmatched.csv, "
                "then re-run with --generate-collections (no --dry-run) to apply."
            )
        return 0

    if args.delete and not args.all and not args.do_import:
        phase_delete(client, dry=False)
        return 0

    products = build_gw_product_list(strict=True) if args.do_photo_sync else prepare_products_for_import()
    if args.do_photo_sync:
        run_photo_sync_preflight(client)
        location_id = ""
    else:
        location_id = run_preflight(client, env)
        log(f"Using location: {location_id}")

    if args.delete or args.all:
        phase_delete(client, dry=False)
    if args.do_import or args.all:
        phase_import(client, products, location_id, dry=False, start_at=args.start_at)
    if args.do_update:
        phase_update(client, products, location_id, dry=args.dry_run)
        if args.dry_run:
            log(
                "Update dry-run complete. Review update_preview.csv, then re-run "
                "with --update (no --dry-run) to apply."
            )
    if args.do_photo_sync:
        phase_photo_sync(client, products, resolve_photo_sync_root(args.photo_root), dry=args.dry_run)
        if args.dry_run:
            log(
                "Photo-sync dry-run complete. Review photo_sync_preview.csv, then re-run "
                "with --photo-sync (no --dry-run) to apply."
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
