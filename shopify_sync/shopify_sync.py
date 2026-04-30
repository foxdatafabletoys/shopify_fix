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
               Deletes all existing Shopify collections, removes only the old
               managed taxonomy tags from products, applies the managed
               taxonomy conservatively, recreates populated smart collections,
               and sets each collection image from the first product image.
  --update-collection-images
               For each managed smart collection, picks the first product
               (alphabetically by title) that has an image and copies that
               image onto the collection. Writes collection_image_preview.csv.
  --import     Creates new Shopify products from the spreadsheets.
  --update     Matches existing Shopify products by SKU and updates price,
               compare_at_price, cost, and on-hand quantity from the sheets.
               Skips products that don't yet exist in Shopify.
  --publish-online-store-backfill
               Finds existing Shopify products that are not published to the
               current publication and publishes them to `Online Store`.
  --reconcile-online-store-image-visibility
               Reconciles `Online Store` visibility against product media:
               publishes products with any attached Shopify media and unpublishes products with none.
  --photo-sync-staged-local-all
               Applies curated staged local fallback images across the full
               catalog and writes the fallback-image audit metafield after
               successful media apply.
  --photo-source-web-all
               Discovers zero-media catalog products, searches public web
               sources for likely product-specific images, and stages only
               high-confidence winners into the local source cache.
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
  python shopify_sync.py --delete-collections
  python shopify_sync.py --generate-collections
  python shopify_sync.py --update-collection-images --dry-run
  python shopify_sync.py --update-collection-images
  python shopify_sync.py --import
  python shopify_sync.py --update --dry-run
  python shopify_sync.py --update
  python shopify_sync.py --publish-online-store-backfill --dry-run
  python shopify_sync.py --publish-online-store-backfill
  python shopify_sync.py --reconcile-online-store-image-visibility --dry-run
  python shopify_sync.py --reconcile-online-store-image-visibility
  python shopify_sync.py --photo-source-web-all --dry-run
  python shopify_sync.py --photo-source-web-all
  python shopify_sync.py --photo-sync-staged-local-all --photo-root ./fallback_photos --dry-run
  python shopify_sync.py --photo-sync-staged-local-all --photo-root ./fallback_photos
  python shopify_sync.py --all
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import math
import mimetypes
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

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
PHOTO_SOURCE_PREVIEW_CSV = HERE / "photo_source_preview.csv"
PHOTO_SOURCE_MANIFEST_JSON = HERE / "photo_source_manifest.json"
PHOTO_SOURCE_MISSING_TSV = HERE / "photo_source_missing.tsv"
PHOTO_SOURCE_AMBIGUOUS_TSV = HERE / "photo_source_ambiguous.tsv"
PHOTO_SOURCE_FAILURES_TSV = HERE / "photo_source_failures.tsv"
PHOTO_SOURCE_UNMAPPED_SHOPIFY_TSV = HERE / "photo_source_unmapped_shopify.tsv"
COLLECTION_GENERATION_PREVIEW_CSV = HERE / "collection_generation_preview.csv"
COLLECTION_GENERATION_UNMATCHED_CSV = HERE / "collection_generation_unmatched.csv"
COLLECTION_IMAGE_PREVIEW_CSV = HERE / "collection_image_preview.csv"
ONLINE_STORE_BACKFILL_PREVIEW_CSV = HERE / "online_store_backfill_preview.csv"
ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV = HERE / "online_store_image_visibility_preview.csv"
GW_RESOURCES_URL = "https://trade.games-workshop.com/resources/"
GW_PHOTO_CACHE_ROOT = HERE / "gw_photo_cache"
GW_PHOTO_CACHE_CURRENT = GW_PHOTO_CACHE_ROOT / "current"
GW_PHOTO_CACHE_STAGING = GW_PHOTO_CACHE_ROOT / "_staging"
GW_PHOTO_CACHE_STATUS_JSON = HERE / "gw_photo_cache_status.json"
PHOTO_SOURCE_CACHE_ROOT = HERE / "photo_source_cache"
PHOTO_SOURCE_CACHE_CURRENT = PHOTO_SOURCE_CACHE_ROOT / "current"
PHOTO_SOURCE_CACHE_STAGING = PHOTO_SOURCE_CACHE_ROOT / "_staging"
LOG_FILE = HERE / "sync.log"
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
PHOTO_SYNC_SOURCE_STAGED_LOCAL = "staged-local-files"
PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING = "shopify-existing-files"
PHOTO_SYNC_SCOPE_GW = "gw"
PHOTO_SYNC_SCOPE_ALL = "all"
PHOTO_SOURCE_MANIFEST_VERSION = 1
PHOTO_SYNC_AUDIT_VERSION = 1
PHOTO_SYNC_STATE_MEDIA_APPLIED = "media_applied"
PHOTO_SYNC_STATE_AUDIT_PENDING = "audit_pending"
LATEST_GW_RELEASE_LIMIT = 60
AUTO_COLLECTION_TAG_PREFIX = "AUTO_COLLECTION::"
ONLINE_STORE_PUBLICATION_NAME = "Online Store"
PHOTO_SOURCE_SEARCH_URL = "https://html.duckduckgo.com/html/"
PHOTO_SOURCE_MAX_RESULT_URLS = 8
PHOTO_SOURCE_MAX_CANDIDATE_PAGES = 5
PHOTO_SOURCE_MAX_IMAGE_DOWNLOADS = 3
PHOTO_SOURCE_HTML_TIMEOUT_SECONDS = 15
PHOTO_SOURCE_IMAGE_TIMEOUT_SECONDS = 30
PHOTO_SOURCE_FETCH_RETRY_DELAYS_SECONDS = (1.0, 2.0)
PHOTO_SOURCE_WINNER_THRESHOLD = 85
PHOTO_SOURCE_AMBIGUOUS_THRESHOLD = 70
PHOTO_SOURCE_MARGIN_THRESHOLD = 15
PHOTO_SOURCE_USER_AGENT = "Mozilla/5.0 (compatible; FoxfablePhotoSource/1.0; +https://foxfable.co.uk)"
PHOTO_SOURCE_DUPLICATE_SKU_REASON = "multiple Shopify products share this SKU"
PHOTO_SOURCE_BANNED_DOMAIN_FRAGMENTS = (
    "facebook.com",
    "instagram.com",
    "pinterest.",
    "reddit.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
)
PHOTO_SOURCE_DISALLOWED_URL_MARKERS = (
    "/forum",
    "/forums",
    "/search",
    "/topic",
    "/threads",
    "/board",
)
PHOTO_SOURCE_PRODUCT_PAGE_MARKERS = (
    "add to cart",
    "product details",
    "specification",
    "specifications",
    "manufacturer",
    "barcode",
    "ean",
    "upc",
    "sku",
)
PHOTO_SOURCE_GENERIC_PAGE_MARKERS = (
    "accessories",
    "category",
    "collection",
    "forum",
    "guide",
    "news",
    "search",
)
FALLBACK_IMAGE_METAFIELD_NAMESPACE = "$app"
FALLBACK_IMAGE_METAFIELD_KEY = "fallback_image_used"
FALLBACK_IMAGE_METAFIELD_NAME = "Fallback image used"
FALLBACK_IMAGE_METAFIELD_TYPE = "boolean"
FALLBACK_IMAGE_METAFIELD_ADMIN_ACCESS = "MERCHANT_READ"

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


@dataclass(frozen=True)
class CollectionRuleSpec:
    column: str
    relation: str
    condition: str


@dataclass(frozen=True)
class ManagedCollectionSpec:
    title: str
    handle: str
    kind: str
    rules: tuple[CollectionRuleSpec, ...]
    applied_disjunctively: bool = False


def _tag_collection_spec(title: str, handle: str, tag: str) -> ManagedCollectionSpec:
    return ManagedCollectionSpec(
        title=title,
        handle=handle,
        kind="tag",
        rules=(CollectionRuleSpec("TAG", "EQUALS", tag),),
    )


def _vendor_collection_spec(title: str, handle: str, vendor: str, extra_tag: str = "") -> ManagedCollectionSpec:
    rules = [CollectionRuleSpec("VENDOR", "EQUALS", vendor)]
    applied_disjunctively = False
    if extra_tag:
        rules.append(CollectionRuleSpec("TAG", "EQUALS", extra_tag))
        applied_disjunctively = True
    return ManagedCollectionSpec(
        title=title,
        handle=handle,
        kind="vendor",
        rules=tuple(rules),
        applied_disjunctively=applied_disjunctively,
    )


def _smart_collection_spec(
    title: str,
    handle: str,
    rules: list[tuple[str, str, str]],
    *,
    applied_disjunctively: bool = False,
) -> ManagedCollectionSpec:
    return ManagedCollectionSpec(
        title=title,
        handle=handle,
        kind="smart",
        rules=tuple(CollectionRuleSpec(column, relation, condition) for column, relation, condition in rules),
        applied_disjunctively=applied_disjunctively,
    )


MANUAL_COLLECTION_HANDLES = {"bestsellers"}
INTERNAL_NEW_ARRIVAL_TAG = "new-arrival"

PRIMARY_GAME_SYSTEM_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Warhammer 40,000", "warhammer-40k", "warhammer-40k"),
    _tag_collection_spec("Warhammer: Age of Sigmar", "age-of-sigmar", "age-of-sigmar"),
    _tag_collection_spec("Warhammer: The Old World", "warhammer-old-world", "old-world"),
    _tag_collection_spec("The Horus Heresy", "horus-heresy", "horus-heresy"),
    _tag_collection_spec("Kill Team", "kill-team", "kill-team"),
    _tag_collection_spec("Necromunda", "necromunda", "necromunda"),
    _tag_collection_spec("Warcry", "warcry", "warcry"),
    _tag_collection_spec("Blood Bowl", "blood-bowl", "blood-bowl"),
    _tag_collection_spec("Middle-earth SBG", "middle-earth-sbg", "middle-earth"),
    _tag_collection_spec("Disney Lorcana", "lorcana", "lorcana"),
    _tag_collection_spec("Pokémon TCG", "pokemon-tcg", "pokemon"),
    _tag_collection_spec("Magic: The Gathering", "magic-the-gathering", "magic"),
    _tag_collection_spec("Yu-Gi-Oh!", "yu-gi-oh", "yugioh"),
    _tag_collection_spec("Flesh and Blood", "flesh-and-blood", "flesh-and-blood"),
    _tag_collection_spec("One Piece TCG", "one-piece-tcg", "one-piece"),
    _tag_collection_spec("Dungeons & Dragons", "dungeons-and-dragons", "d-and-d"),
    _tag_collection_spec("Pathfinder", "pathfinder", "pathfinder"),
    _tag_collection_spec("Board Games", "board-games", "board-game"),
]

FACTION_40K_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Space Marines", "space-marines", "space-marines"),
    _tag_collection_spec("Dark Angels", "dark-angels", "dark-angels"),
    _tag_collection_spec("Blood Angels", "blood-angels", "blood-angels"),
    _tag_collection_spec("Space Wolves", "space-wolves", "space-wolves"),
    _tag_collection_spec("Black Templars", "black-templars", "black-templars"),
    _tag_collection_spec("Deathwatch", "deathwatch", "deathwatch"),
    _tag_collection_spec("Grey Knights", "grey-knights", "grey-knights"),
    _tag_collection_spec("Adepta Sororitas", "adepta-sororitas", "sisters-of-battle"),
    _tag_collection_spec("Adeptus Custodes", "adeptus-custodes", "custodes"),
    _tag_collection_spec("Astra Militarum", "astra-militarum", "astra-militarum"),
    _tag_collection_spec("Adeptus Mechanicus", "adeptus-mechanicus", "ad-mech"),
    _tag_collection_spec("Imperial Knights", "imperial-knights", "imperial-knights"),
    _tag_collection_spec("Agents of the Imperium", "agents-of-the-imperium", "agents"),
    _tag_collection_spec("Chaos Space Marines", "chaos-space-marines", "chaos-space-marines"),
    _tag_collection_spec("World Eaters", "world-eaters", "world-eaters"),
    _tag_collection_spec("Death Guard", "death-guard", "death-guard"),
    _tag_collection_spec("Thousand Sons", "thousand-sons", "thousand-sons"),
    _tag_collection_spec("Chaos Daemons", "chaos-daemons", "chaos-daemons"),
    _tag_collection_spec("Chaos Knights", "chaos-knights", "chaos-knights"),
    _tag_collection_spec("Tyranids", "tyranids", "tyranids"),
    _tag_collection_spec("Genestealer Cults", "genestealer-cults", "genestealer-cults"),
    _tag_collection_spec("Aeldari", "aeldari", "aeldari"),
    _tag_collection_spec("Drukhari", "drukhari", "drukhari"),
    _tag_collection_spec("Harlequins", "harlequins", "harlequins"),
    _tag_collection_spec("Ynnari", "ynnari", "ynnari"),
    _tag_collection_spec("T'au Empire", "t-au-empire", "tau"),
    _tag_collection_spec("Necrons", "necrons", "necrons"),
    _tag_collection_spec("Orks", "orks", "orks"),
    _tag_collection_spec("Leagues of Votann", "leagues-of-votann", "votann"),
]

FACTION_AOS_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Stormcast Eternals", "stormcast-eternals", "stormcast-eternals"),
    _tag_collection_spec("Cities of Sigmar", "cities-of-sigmar", "cities-of-sigmar"),
    _tag_collection_spec("Daughters of Khaine", "daughters-of-khaine", "daughters-of-khaine"),
    _tag_collection_spec("Fyreslayers", "fyreslayers", "fyreslayers"),
    _tag_collection_spec("Idoneth Deepkin", "idoneth-deepkin", "idoneth-deepkin"),
    _tag_collection_spec("Kharadron Overlords", "kharadron-overlords", "kharadron-overlords"),
    _tag_collection_spec("Lumineth Realm-Lords", "lumineth-realm-lords", "lumineth"),
    _tag_collection_spec("Seraphon", "seraphon", "seraphon"),
    _tag_collection_spec("Sylvaneth", "sylvaneth", "sylvaneth"),
    _tag_collection_spec("Slaves to Darkness", "slaves-to-darkness", "slaves-to-darkness"),
    _tag_collection_spec("Blades of Khorne", "blades-of-khorne", "blades-of-khorne"),
    _tag_collection_spec("Disciples of Tzeentch", "disciples-of-tzeentch", "disciples-of-tzeentch"),
    _tag_collection_spec("Hedonites of Slaanesh", "hedonites-of-slaanesh", "hedonites-of-slaanesh"),
    _tag_collection_spec("Maggotkin of Nurgle", "maggotkin-of-nurgle", "maggotkin-of-nurgle"),
    _tag_collection_spec("Skaven", "skaven", "skaven"),
    _tag_collection_spec("Beasts of Chaos", "beasts-of-chaos", "beasts-of-chaos"),
    _tag_collection_spec("Flesh-eater Courts", "flesh-eater-courts", "flesh-eater-courts"),
    _tag_collection_spec("Nighthaunt", "nighthaunt", "nighthaunt"),
    _tag_collection_spec("Ossiarch Bonereapers", "ossiarch-bonereapers", "ossiarch-bonereapers"),
    _tag_collection_spec("Soulblight Gravelords", "soulblight-gravelords", "soulblight-gravelords"),
    _tag_collection_spec("Gloomspite Gitz", "gloomspite-gitz", "gloomspite-gitz"),
    _tag_collection_spec("Kruleboyz", "kruleboyz", "kruleboyz"),
    _tag_collection_spec("Ironjawz", "ironjawz", "ironjawz"),
    _tag_collection_spec("Bonesplitterz", "bonesplitterz", "bonesplitterz"),
    _tag_collection_spec("Ogor Mawtribes", "ogor-mawtribes", "ogor-mawtribes"),
    _tag_collection_spec("Sons of Behemat", "sons-of-behemat", "sons-of-behemat"),
]

FACTION_OLD_WORLD_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Empire of Man", "empire-of-man", "empire-of-man"),
    _tag_collection_spec("Kingdom of Bretonnia", "kingdom-of-bretonnia", "bretonnia"),
    _tag_collection_spec("Dwarfen Mountain Holds", "dwarfen-mountain-holds", "dwarfs"),
    _tag_collection_spec("Tomb Kings of Khemri", "tomb-kings-of-khemri", "tomb-kings"),
    _tag_collection_spec("Orc & Goblin Tribes", "orc-and-goblin-tribes", "orc-goblin"),
    _tag_collection_spec("Warriors of Chaos", "warriors-of-chaos", "warriors-of-chaos"),
    _tag_collection_spec("Beastmen Brayherds", "beastmen-brayherds", "beastmen"),
    _tag_collection_spec("Wood Elf Realms", "wood-elf-realms", "wood-elves"),
    _tag_collection_spec("High Elf Realms", "high-elf-realms", "high-elves"),
    _tag_collection_spec("Dark Elves", "dark-elves", "dark-elves"),
    _tag_collection_spec("Lizardmen", "lizardmen", "lizardmen"),
    _tag_collection_spec("Vampire Counts", "vampire-counts", "vampire-counts"),
]

PRODUCT_TYPE_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Starter sets", "starter-sets", "starter-set"),
    _tag_collection_spec("Combat Patrols", "combat-patrols", "combat-patrol"),
    _tag_collection_spec("Army Sets", "army-sets", "army-set"),
    _tag_collection_spec("Codexes", "codexes", "codex"),
    _tag_collection_spec("Battletomes", "battletomes", "battletome"),
    _tag_collection_spec("Core Rules", "core-rules", "core-rules"),
    _tag_collection_spec("Terrain & Scenery", "terrain-and-scenery", "terrain"),
    _tag_collection_spec("Paints", "paints", "paint"),
    _tag_collection_spec("Brushes & Tools", "brushes-and-tools", "tools"),
    _tag_collection_spec("Dice & Templates", "dice-and-templates", "dice"),
    _tag_collection_spec("Card Sleeves & Storage", "card-sleeves-and-storage", "accessories"),
    _tag_collection_spec("Magazines & Partworks", "magazines-and-partworks", "partworks"),
    _tag_collection_spec("Bits & Bases", "bits-and-bases", "bits"),
]

BRAND_SPECS: list[ManagedCollectionSpec] = [
    _vendor_collection_spec("Games Workshop", "games-workshop", "Games Workshop"),
    _vendor_collection_spec("Citadel Colour", "citadel-colour", "Citadel", extra_tag="citadel"),
    _vendor_collection_spec("Forge World", "forge-world", "Forge World"),
    _vendor_collection_spec("Vallejo", "vallejo", "Vallejo"),
    _vendor_collection_spec("Army Painter", "army-painter", "Army Painter"),
    _vendor_collection_spec("Mantic Games", "mantic-games", "Mantic Games"),
    _vendor_collection_spec("Ravensburger", "ravensburger", "Ravensburger"),
    _vendor_collection_spec("Asmodee", "asmodee", "Asmodee"),
    _vendor_collection_spec("Wizards of the Coast", "wizards-of-the-coast", "Wizards of the Coast"),
    _vendor_collection_spec("Pokémon Company", "the-pokemon-company", "The Pokemon Company"),
]

UTILITY_SPECS: list[ManagedCollectionSpec] = [
    _tag_collection_spec("Latest releases", "latest-releases", "new-release"),
    _tag_collection_spec("New arrivals", "new-arrivals", INTERNAL_NEW_ARRIVAL_TAG),
    _tag_collection_spec("Pre-orders", "pre-orders", "preorder"),
    _smart_collection_spec("Back in stock", "back-in-stock", [
        ("TAG", "EQUALS", "restocked"),
        ("VARIANT_INVENTORY", "GREATER_THAN", "0"),
    ]),
    _tag_collection_spec("Coming soon", "coming-soon", "coming-soon"),
    _smart_collection_spec("Sale", "sale", [
        ("IS_PRICE_REDUCED", "IS_SET", ""),
    ]),
    _tag_collection_spec("Staff picks", "staff-picks", "staff-pick"),
    _tag_collection_spec("Gift ideas", "gift-ideas", "gift"),
    _smart_collection_spec("Under £25", "under-25", [
        ("VARIANT_PRICE", "LESS_THAN", "2500"),
    ]),
    _smart_collection_spec("Under £50", "under-50", [
        ("VARIANT_PRICE", "LESS_THAN", "5000"),
    ]),
    _tag_collection_spec("Kids & family", "kids-and-family", "family-friendly"),
    _smart_collection_spec("Solo & 2-player", "solo-and-2-player", [
        ("TAG", "EQUALS", "solo"),
        ("TAG", "EQUALS", "2-player"),
    ], applied_disjunctively=True),
]

MANAGED_COLLECTION_SPECS: list[ManagedCollectionSpec] = (
    PRIMARY_GAME_SYSTEM_SPECS
    + FACTION_40K_SPECS
    + FACTION_AOS_SPECS
    + FACTION_OLD_WORLD_SPECS
    + PRODUCT_TYPE_SPECS
    + BRAND_SPECS
    + UTILITY_SPECS
)
MANAGED_COLLECTION_SPECS_BY_HANDLE = {spec.handle: spec for spec in MANAGED_COLLECTION_SPECS}
MANAGED_COLLECTION_SPECS_BY_TITLE = {spec.title: spec for spec in MANAGED_COLLECTION_SPECS}
MANAGED_COLLECTION_HANDLES = {spec.handle for spec in MANAGED_COLLECTION_SPECS}
MANAGED_TAGS = {
    spec.rules[0].condition
    for spec in MANAGED_COLLECTION_SPECS
    if spec.kind == "tag"
}

LEGACY_MANAGED_TAG_ALIASES = {
    "warhammer 40k",
    "warhammer 40,000",
    "age of sigmar",
    "the old world",
    "kill team",
    "pokemon",
    "magic: the gathering",
    "yu-gi-oh!",
    "games workshop",
    "citadel colour",
    "forge world",
    "ravensburger",
    "asmodee",
    "starter sets",
    "combat patrols",
    "army sets",
    "codexes",
    "battletomes",
    "core rules",
    "terrain & scenery",
    "paints",
    "brushes & tools",
    "dice & templates",
    "card sleeves & storage",
    "magazines & partworks",
    "bits & bases",
    "latest releases",
    "new arrivals",
    "pre-orders",
    "preorders",
    "back in stock",
    "coming soon",
    "staff picks",
    "gift ideas",
    "kids & family",
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
        base_dir = self.image_paths[0].parent if self.image_paths else Path(".")
        for path in sorted(self.image_paths):
            rel = path.relative_to(base_dir).as_posix()
            digest.update(rel.encode("utf-8"))
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        return digest.hexdigest()


@dataclass
class ShopifyImageFile:
    id: str
    alt: str = ""
    filename: str = ""
    product_code: str = ""
    title_slug: str = ""
    file_status: str = ""

    def sort_key(self) -> tuple[str, str]:
        return (self.filename.lower(), self.id)


@dataclass
class PhotoSourceSearchResult:
    url: str
    title: str = ""


@dataclass
class PhotoSourceCandidate:
    page_url: str
    image_url: str
    page_title: str
    image_alt: str
    score: int
    reasons: list[str]
    detail_signals: list[str] = field(default_factory=list)


class _PhotoSourceHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[tuple[str, str]] = []
        self.images: list[tuple[str, str]] = []
        self.meta_images: list[str] = []
        self.title = ""
        self._title_parts: list[str] = []
        self._anchor_href = ""
        self._anchor_text_parts: list[str] = []
        self._in_title = False
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = {k.lower(): (v or "") for k, v in attrs}
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = True
            self._title_parts = []
        elif tag_name == "a":
            self._anchor_href = normalized.get("href", "").strip()
            self._anchor_text_parts = []
        elif tag_name == "img":
            src = normalized.get("src", "").strip()
            if src:
                self.images.append((src, normalize_text(normalized.get("alt", ""))))
        elif tag_name == "meta":
            prop = (normalized.get("property") or normalized.get("name") or "").strip().lower()
            content = normalized.get("content", "").strip()
            if prop in {"og:image", "twitter:image"} and content:
                self.meta_images.append(content)

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if not text:
            return
        if self._in_title:
            self._title_parts.append(text)
        if self._anchor_href:
            self._anchor_text_parts.append(text)
        self.text_chunks.append(text)

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = False
            self.title = normalize_text(" ".join(self._title_parts))
        elif tag_name == "a":
            if self._anchor_href:
                self.anchors.append((self._anchor_href, normalize_text(" ".join(self._anchor_text_parts))))
            self._anchor_href = ""
            self._anchor_text_parts = []


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


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


def _extract_asset_match_code(value: str) -> str:
    base = Path(value or "").stem.strip()
    numeric_code = _extract_product_code(base)
    if numeric_code:
        return numeric_code
    parts = [part for part in re.split(r"[-_\s]+", base) if part]
    prefix: list[str] = []
    for index, part in enumerate(parts[:-1], start=1):
        prefix.append(part)
        if re.search(r"\d", part):
            candidate = "-".join(prefix)
            tail = "-".join(parts[index:])
            if tail:
                return candidate
    return ""


def _extract_title_slug(value: str) -> str:
    base = Path(value or "").stem
    code = _extract_product_code(base)
    if code:
        _, _, tail = base.partition(code)
        base = tail.lstrip(" -_")
    else:
        asset_code = _extract_asset_match_code(base)
        if asset_code and base.lower().startswith(asset_code.lower()):
            base = base[len(asset_code):].lstrip(" -_")
    return _normalize_slug(base)


def _is_ignored_photo_asset_path(path: Path) -> bool:
    for part in path.parts:
        normalized = part.strip()
        if normalized == "__MACOSX" or normalized.startswith("._") or normalized.startswith("MACOSX-"):
            return True
    return False


def _filename_from_url(url: str) -> str:
    path = Path((url or "").split("?", 1)[0])
    return path.name


def _build_shopify_image_file(
    *,
    file_id: str,
    alt: str,
    file_status: str,
    original_source_url: str,
    image_url: str,
) -> ShopifyImageFile:
    filename = _filename_from_url(original_source_url) or _filename_from_url(image_url)
    product_code = _extract_product_code(filename) or _extract_product_code(alt)
    title_slug = _extract_title_slug(filename) or _normalize_slug(alt)
    return ShopifyImageFile(
        id=file_id,
        alt=alt,
        filename=filename,
        product_code=product_code,
        title_slug=title_slug,
        file_status=file_status,
    )


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


def require_explicit_photo_root(photo_root: Path | None, flag_name: str) -> Path:
    if photo_root is None:
        raise RuntimeError(f"{flag_name} requires --photo-root.")
    log(f"{flag_name} using explicit --photo-root override: {photo_root}")
    return photo_root


def discover_photo_asset_sets(root: Path) -> list[PhotoAssetSet]:
    if not root.exists():
        raise RuntimeError(f"Photo root not found: {root}")
    if not root.is_dir():
        raise RuntimeError(f"Photo root must be a directory: {root}")

    grouped: dict[tuple[str, str], list[Path]] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        if _is_ignored_photo_asset_path(path.relative_to(root)):
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
            product_code=_extract_asset_match_code(name_seed),
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


def build_shopify_file_indexes(
    files: list[ShopifyImageFile],
) -> tuple[dict[str, list[ShopifyImageFile]], dict[str, list[ShopifyImageFile]]]:
    by_code: dict[str, list[ShopifyImageFile]] = {}
    by_slug: dict[str, list[ShopifyImageFile]] = {}
    for file in files:
        if file.file_status and file.file_status != "READY":
            continue
        if file.product_code:
            by_code.setdefault(file.product_code, []).append(file)
        if file.title_slug:
            by_slug.setdefault(file.title_slug, []).append(file)
    for bucket in by_code.values():
        bucket.sort(key=lambda item: item.sort_key())
    for bucket in by_slug.values():
        bucket.sort(key=lambda item: item.sort_key())
    return by_code, by_slug


def build_photo_source_query(product: Product) -> str:
    parts = [product.sku, product.title, product.vendor]
    if product.barcode:
        parts.append(product.barcode)
    return " ".join(part for part in parts if part).strip()


def normalize_photo_source_redirect(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        encoded = parse_qs(parsed.query).get("uddg", [""])[0]
        if encoded:
            return encoded
    return url


def is_supported_image_url(url: str) -> bool:
    suffix = Path(urlparse(url).path.split("?", 1)[0]).suffix.lower()
    return suffix in IMAGE_SUFFIXES


def is_allowed_photo_source_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.netloc.lower()
    if any(fragment in host for fragment in PHOTO_SOURCE_BANNED_DOMAIN_FRAGMENTS):
        return False
    path = parsed.path.lower()
    return not any(marker in path for marker in PHOTO_SOURCE_DISALLOWED_URL_MARKERS)


def fetch_url_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    binary: bool = False,
) -> requests.Response:
    attempts = len(PHOTO_SOURCE_FETCH_RETRY_DELAYS_SECONDS) + 1
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                time.sleep(PHOTO_SOURCE_FETCH_RETRY_DELAYS_SECONDS[attempt])
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code} while fetching {url}")
            if binary:
                _ = response.content
            else:
                _ = response.text
            return response
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(PHOTO_SOURCE_FETCH_RETRY_DELAYS_SECONDS[attempt])
    if last_error is not None:
        raise RuntimeError(f"Network error while fetching {url}: {last_error}") from last_error
    raise RuntimeError(f"Failed to fetch {url}")


def parse_photo_source_html(html: str) -> _PhotoSourceHTMLParser:
    parser = _PhotoSourceHTMLParser()
    parser.feed(html)
    return parser


def extract_photo_source_search_results(html: str) -> list[PhotoSourceSearchResult]:
    parser = parse_photo_source_html(html)
    results: list[PhotoSourceSearchResult] = []
    seen: set[str] = set()
    for href, text in parser.anchors:
        resolved = normalize_photo_source_redirect(href)
        if not is_allowed_photo_source_url(resolved):
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        results.append(PhotoSourceSearchResult(url=resolved, title=text))
        if len(results) >= PHOTO_SOURCE_MAX_RESULT_URLS:
            break
    return results


def photo_source_title_tokens(product: Product) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", (product.title or "").lower())
    filtered = [token for token in tokens if len(token) >= 3]
    seen: set[str] = set()
    ordered: list[str] = []
    for token in filtered:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered[:8]


def photo_source_vendor_tokens(product: Product) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", (product.vendor or "").lower()) if len(token) >= 3]


def photo_source_has_sku_evidence(sku: str, raw_text: str, normalized_text: str) -> bool:
    raw_sku = (sku or "").strip().lower()
    if not raw_sku:
        return False
    escaped = re.escape(raw_sku)
    if re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", (raw_text or "").lower()):
        return True
    normalized_sku = " ".join(re.findall(r"[a-z0-9]+", raw_sku))
    return bool(normalized_sku) and f" {normalized_sku} " in normalized_text


def photo_source_detail_signals(raw_text: str, search_text: str, product: Product) -> list[str]:
    signals: list[str] = []
    if photo_source_has_sku_evidence(product.sku, raw_text, search_text):
        signals.append("sku")
    title_tokens = photo_source_title_tokens(product)
    if title_tokens and sum(1 for token in title_tokens if f" {token} " in search_text) >= min(2, len(title_tokens)):
        signals.append("title")
    vendor_tokens = photo_source_vendor_tokens(product)
    if vendor_tokens and any(f" {token} " in search_text for token in vendor_tokens):
        signals.append("vendor")
    if any(marker in search_text for marker in PHOTO_SOURCE_PRODUCT_PAGE_MARKERS):
        signals.append("product_markers")
    return signals


def score_photo_source_candidate(
    product: Product,
    *,
    page_url: str,
    page_title: str,
    page_text: str,
    image_url: str,
    image_alt: str,
) -> PhotoSourceCandidate | None:
    raw_text = " ".join([page_url, page_title, page_text, image_url, image_alt])
    search_text = _normalize_search_text(raw_text)
    detail_signals = photo_source_detail_signals(raw_text, search_text, product)
    if not detail_signals:
        return None
    score = 0
    reasons: list[str] = []
    if photo_source_has_sku_evidence(product.sku, raw_text, search_text):
        score += 55
        reasons.append("sku")
    title_tokens = photo_source_title_tokens(product)
    title_hits = sum(1 for token in title_tokens if f" {token} " in search_text)
    if title_hits >= max(2, min(4, len(title_tokens))):
        score += 25
        reasons.append("title")
    elif title_hits:
        score += 10
        reasons.append("partial_title")
    vendor_tokens = photo_source_vendor_tokens(product)
    if vendor_tokens and any(f" {token} " in search_text for token in vendor_tokens):
        score += 10
        reasons.append("vendor")
    if "product_markers" in detail_signals:
        score += 10
        reasons.append("detail_page")
    slug = _normalize_slug(product.title)
    if slug and slug in _normalize_slug(image_url):
        score += 10
        reasons.append("image_slug")
    if any(marker in search_text for marker in PHOTO_SOURCE_GENERIC_PAGE_MARKERS):
        score -= 25
        reasons.append("generic_penalty")
    score = max(0, min(100, score))
    return PhotoSourceCandidate(
        page_url=page_url,
        image_url=image_url,
        page_title=page_title,
        image_alt=image_alt,
        score=score,
        reasons=reasons,
        detail_signals=detail_signals,
    )


def extract_photo_source_candidates(product: Product, page_url: str, html: str) -> list[PhotoSourceCandidate]:
    parser = parse_photo_source_html(html)
    page_title = parser.title
    page_text = " ".join(parser.text_chunks[:200])
    candidates: list[PhotoSourceCandidate] = []
    seen: set[str] = set()
    image_pairs = [(urljoin(page_url, url), alt) for url, alt in parser.images]
    image_pairs = [(urljoin(page_url, url), "") for url in parser.meta_images] + image_pairs
    for image_url, image_alt in image_pairs:
        if not is_supported_image_url(image_url):
            continue
        if image_url in seen:
            continue
        seen.add(image_url)
        candidate = score_photo_source_candidate(
            product,
            page_url=page_url,
            page_title=page_title,
            page_text=page_text,
            image_url=image_url,
            image_alt=image_alt,
        )
        if candidate is not None:
            candidates.append(candidate)
        if len(candidates) >= PHOTO_SOURCE_MAX_IMAGE_DOWNLOADS * 2:
            break
    candidates.sort(key=lambda item: (-item.score, item.image_url))
    return candidates


def choose_photo_source_winner(candidates: list[PhotoSourceCandidate]) -> tuple[str, PhotoSourceCandidate | None, str]:
    if not candidates:
        return "missing", None, f"no candidate reached {PHOTO_SOURCE_AMBIGUOUS_THRESHOLD}"
    top = candidates[0]
    runner_up = candidates[1] if len(candidates) > 1 else None
    if top.score < PHOTO_SOURCE_AMBIGUOUS_THRESHOLD:
        return "missing", None, f"no candidate reached {PHOTO_SOURCE_AMBIGUOUS_THRESHOLD}"
    if top.score >= PHOTO_SOURCE_WINNER_THRESHOLD and runner_up and runner_up.score >= PHOTO_SOURCE_WINNER_THRESHOLD:
        return "ambiguous", None, "multiple candidates cleared the winner threshold"
    margin = top.score - (runner_up.score if runner_up else 0)
    if top.score >= PHOTO_SOURCE_WINNER_THRESHOLD and margin >= PHOTO_SOURCE_MARGIN_THRESHOLD:
        return "winner", top, ""
    if top.score >= PHOTO_SOURCE_AMBIGUOUS_THRESHOLD:
        return "ambiguous", None, f"top-vs-runner-up margin {margin} is below {PHOTO_SOURCE_MARGIN_THRESHOLD}"
    return "missing", None, f"no candidate reached {PHOTO_SOURCE_AMBIGUOUS_THRESHOLD}"


def stable_photo_source_dirname(product: Product) -> str:
    slug = _normalize_slug(product.title)
    return f"{product.sku}-{slug}" if slug else product.sku


def publish_photo_source_pack(staging_dir: Path, current_root: Path, pack_name: str) -> Path:
    current_root.mkdir(parents=True, exist_ok=True)
    final_dir = current_root / pack_name
    backup_dir = current_root / f"_{pack_name}.previous"
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    if final_dir.exists():
        final_dir.rename(backup_dir)
    try:
        staging_dir.rename(final_dir)
    except Exception:
        if backup_dir.exists() and not final_dir.exists():
            backup_dir.rename(final_dir)
        raise
    else:
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
    return final_dir


def build_photo_source_preview_row(
    product: Product,
    *,
    status: str,
    query: str,
    top_score: int = 0,
    winner: PhotoSourceCandidate | None = None,
    reason: str = "",
    staged_dir: str = "",
) -> dict[str, Any]:
    return {
        "sku": product.sku,
        "title": product.title,
        "status": status,
        "query": query,
        "top_score": top_score,
        "winner_page_url": winner.page_url if winner else "",
        "winner_image_url": winner.image_url if winner else "",
        "winner_reasons": "|".join(winner.reasons) if winner else "",
        "staged_dir": staged_dir,
        "reason": reason,
    }


def record_photo_source_non_winner(
    product: Product,
    *,
    status: str,
    query: str,
    top_score: int,
    reason: str,
    preview_rows: list[dict[str, Any]],
    log_rows: list[tuple[str, str, str]],
    dry: bool,
    manifest: dict[str, Any],
    manifest_path: Path,
) -> None:
    preview_rows.append(build_photo_source_preview_row(
        product,
        status=status,
        query=query,
        top_score=top_score,
        reason=reason,
    ))
    log_rows.append((product.sku, product.title, reason))
    if dry:
        return
    update_and_save_photo_manifest_entry(
        manifest,
        manifest_path,
        sku=product.sku,
        state=status,
        query=query,
        top_score=top_score,
        reason=reason,
        manifest_version=PHOTO_SOURCE_MANIFEST_VERSION,
    )


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(HERE))
    except ValueError:
        return str(path)


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


def resolve_existing_shopify_files(
    product: Product,
    by_code: dict[str, list[ShopifyImageFile]],
    by_slug: dict[str, list[ShopifyImageFile]],
) -> tuple[str, str, list[ShopifyImageFile], str]:
    exact_matches = by_code.get(product.sku, [])
    if exact_matches:
        return "replace", "exact", exact_matches, ""

    slug = _normalize_slug(product.title)
    slug_matches = by_slug.get(slug, [])
    if slug_matches:
        return "replace", "fallback", slug_matches, ""
    return "skip", "missing", [], "no matching Shopify image files"


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
    source_mode: str,
    photo_root: Path | None = None,
    asset_set: PhotoAssetSet | None = None,
    existing_files: list[ShopifyImageFile] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    image_paths = asset_set.image_paths if asset_set else []
    existing_files = existing_files or []
    if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
        source_paths = "|".join(str(path.relative_to(photo_root)) for path in image_paths) if photo_root else ""
        asset_label = asset_set.label if asset_set else ""
        image_count = len(image_paths)
    else:
        source_paths = "|".join(file.filename or file.id for file in existing_files)
        asset_label = "|".join(file.filename or file.id for file in existing_files)
        image_count = len(existing_files)
    return {
        "sku": product.sku,
        "title": product.title,
        "status": status,
        "match_type": match_type,
        "asset_label": asset_label,
        "image_count": image_count,
        "reason": reason,
        "source_mode": source_mode,
        "source_paths": source_paths,
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


def _normalized_tag(tag: str) -> str:
    return re.sub(r"\s+", " ", (tag or "").strip().lower())


def _record_created_age_days(record: dict[str, Any]) -> int | None:
    raw = (record.get("created_at") or "").strip()
    if not raw:
        return None
    try:
        created = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0, int((now - created).total_seconds() // 86400))


def _record_matches_phrase_map(record: dict[str, Any], phrases: Iterable[str]) -> bool:
    return any(_record_has_phrase(record, phrase) for phrase in phrases)


GAME_SYSTEM_KEYWORDS: dict[str, tuple[str, ...]] = {
    "warhammer-40k": ("warhammer 40,000", "warhammer 40k", "warhammer 40000", " 40k ", " combat patrol ", " codex "),
    "age-of-sigmar": ("age of sigmar", " battletome ", " spearhead "),
    "old-world": ("the old world", " old world ", " forces of fantasy ", " ravening hordes "),
    "horus-heresy": ("horus heresy", "the horus heresy", "age of darkness"),
    "kill-team": ("kill team",),
    "necromunda": ("necromunda",),
    "warcry": ("warcry",),
    "blood-bowl": ("blood bowl",),
    "middle-earth": ("middle earth", "middle-earth", "strategy battle game"),
    "lorcana": ("lorcana", " disney lorcana "),
    "pokemon": ("pokemon", "pokémon", "elite trainer box", "booster bundle", "booster box"),
    "magic": ("magic the gathering", "magic: the gathering", "mtg", "play booster", "collector booster"),
    "yugioh": ("yu-gi-oh", "yugioh", "yu gi oh"),
    "flesh-and-blood": ("flesh and blood",),
    "one-piece": ("one piece card game", "one piece tcg"),
    "d-and-d": ("dungeons dragons", "dungeons & dragons", "player's handbook", "players handbook", "essentials kit"),
    "pathfinder": ("pathfinder", "beginner box"),
}

FACTION_KEYWORDS: dict[str, tuple[str, ...]] = {
    "space-marines": ("space marines",),
    "dark-angels": ("dark angels",),
    "blood-angels": ("blood angels",),
    "space-wolves": ("space wolves",),
    "black-templars": ("black templars",),
    "deathwatch": ("deathwatch",),
    "grey-knights": ("grey knights",),
    "sisters-of-battle": ("adepta sororitas", "sisters of battle"),
    "custodes": ("adeptus custodes", "custodes"),
    "astra-militarum": ("astra militarum", "imperial guard"),
    "ad-mech": ("adeptus mechanicus", "ad mech", "admech"),
    "imperial-knights": ("imperial knights",),
    "agents": ("agents of the imperium", "agents imperium"),
    "chaos-space-marines": ("chaos space marines",),
    "world-eaters": ("world eaters",),
    "death-guard": ("death guard",),
    "thousand-sons": ("thousand sons",),
    "chaos-daemons": ("chaos daemons", "chaos demons"),
    "chaos-knights": ("chaos knights",),
    "tyranids": ("tyranids", "tyranid"),
    "genestealer-cults": ("genestealer cults", "genestealer cult"),
    "aeldari": ("aeldari", "craftworld", "asuryani"),
    "drukhari": ("drukhari", "dark eldar"),
    "harlequins": ("harlequins",),
    "ynnari": ("ynnari",),
    "tau": ("t'au", "tau empire", "tau "),
    "necrons": ("necrons", "necron"),
    "orks": ("orks", "ork "),
    "votann": ("leagues of votann", "votann"),
    "stormcast-eternals": ("stormcast eternals", "stormcast"),
    "cities-of-sigmar": ("cities of sigmar",),
    "daughters-of-khaine": ("daughters of khaine",),
    "fyreslayers": ("fyreslayers",),
    "idoneth-deepkin": ("idoneth deepkin",),
    "kharadron-overlords": ("kharadron overlords",),
    "lumineth": ("lumineth realm", "lumineth"),
    "seraphon": ("seraphon",),
    "sylvaneth": ("sylvaneth",),
    "slaves-to-darkness": ("slaves to darkness",),
    "blades-of-khorne": ("blades of khorne",),
    "disciples-of-tzeentch": ("disciples of tzeentch",),
    "hedonites-of-slaanesh": ("hedonites of slaanesh",),
    "maggotkin-of-nurgle": ("maggotkin of nurgle",),
    "skaven": ("skaven",),
    "beasts-of-chaos": ("beasts of chaos",),
    "flesh-eater-courts": ("flesh eater courts",),
    "nighthaunt": ("nighthaunt",),
    "ossiarch-bonereapers": ("ossiarch bonereapers",),
    "soulblight-gravelords": ("soulblight gravelords",),
    "gloomspite-gitz": ("gloomspite gitz",),
    "kruleboyz": ("kruleboyz",),
    "ironjawz": ("ironjawz",),
    "bonesplitterz": ("bonesplitterz",),
    "ogor-mawtribes": ("ogor mawtribes",),
    "sons-of-behemat": ("sons of behemat",),
    "empire-of-man": ("empire of man",),
    "bretonnia": ("bretonnia", "bretonnian"),
    "dwarfs": ("dwarfen mountain holds", "dwarf", "dwarfs"),
    "tomb-kings": ("tomb kings", "khemri"),
    "orc-goblin": ("orc & goblin", "orc and goblin", "orc goblin"),
    "warriors-of-chaos": ("warriors of chaos",),
    "beastmen": ("beastmen brayherds", "beastmen"),
    "wood-elves": ("wood elf", "wood elves"),
    "high-elves": ("high elf", "high elves"),
    "dark-elves": ("dark elves",),
    "lizardmen": ("lizardmen",),
    "vampire-counts": ("vampire counts",),
}

PRODUCT_TYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("combat-patrol", ("combat patrol",)),
    ("starter-set", ("starter set", "starter box", "beginner box", "battle academy")),
    ("army-set", ("army set", "spearhead", "vanguard", "battleforce")),
    ("codex", (" codex ", "codex:", "codex supplement")),
    ("battletome", ("battletome",)),
    ("core-rules", ("core rules", "core book", "rulebook", "rules manual")),
    ("terrain", ("terrain", "scenery", "ruins", "killzone", "sector imperialis")),
    ("paint", ("paint", "contrast", "shade", "technical", "layer", "base ", " air ", "spray")),
    ("tools", ("brush", "clippers", "files", "glue", "tool", "hobby knife")),
    ("dice", ("dice", "template", "ruler")),
    ("accessories", ("sleeve", "deck box", "binder", "storage", "organiser", "organizer", "case")),
    ("partworks", ("white dwarf", "stormbringer", "combat patrol magazine", "partwork")),
    ("bits", ("bits", "bases", "base pack", "sprue")),
]

SPECIAL_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "preorder": ("preorder", "pre-order", "pre order"),
    "coming-soon": ("coming soon",),
    "restocked": ("restocked", "back in stock"),
    "staff-pick": ("staff pick", "staff-pick"),
    "gift": ("gift", "giftable"),
    "family-friendly": ("family friendly", "family-friendly"),
    "solo": ("solo", "1-player", "1 player"),
    "2-player": ("2-player", "two-player", "2 player"),
    "exclusive": ("exclusive",),
    "limited-edition": ("limited edition", "limited-edition"),
    "citadel": ("citadel",),
    "contrast": ("contrast",),
    "base": (" base ",),
    "layer": (" layer ",),
    "shade": (" shade ",),
    "technical": ("technical",),
    "dry": (" dry ",),
    "air": (" air ",),
}

LEGACY_MANAGED_NORMALIZED_TAGS = {
    _normalized_tag(tag)
    for tag in (
        *MANAGED_TAGS,
        INTERNAL_NEW_ARRIVAL_TAG,
        *LEGACY_MANAGED_TAG_ALIASES,
    )
}
LEGACY_MANAGED_NORMALIZED_TAGS |= {
    _normalized_tag(collection_marker_tag_from_handle(handle))
    for _, handle in WAYLAND_COLLECTION_SPECS
}

FACTION_TO_SYSTEM = {
    **{tag: "warhammer-40k" for tag in [spec.rules[0].condition for spec in FACTION_40K_SPECS]},
    **{tag: "age-of-sigmar" for tag in [spec.rules[0].condition for spec in FACTION_AOS_SPECS]},
    **{tag: "old-world" for tag in [spec.rules[0].condition for spec in FACTION_OLD_WORLD_SPECS]},
}


def managed_collection_signature(collection: dict[str, Any]) -> tuple[bool, tuple[tuple[str, str, str], ...]]:
    applied = bool(collection.get("applied_disjunctively"))
    rules = tuple(
        sorted(
            (
                (rule.get("column") or "").upper(),
                (rule.get("relation") or "").upper(),
                rule.get("condition") or "",
            )
            for rule in collection.get("rules") or []
        )
    )
    return applied, rules


def is_managed_collection(collection: dict[str, Any], expected_spec: ManagedCollectionSpec | None = None) -> bool:
    handle = (collection.get("handle") or "").strip()
    spec = expected_spec or MANAGED_COLLECTION_SPECS_BY_HANDLE.get(handle)
    if spec is None or collection.get("collection_type") != "smart":
        return False
    expected_rules = tuple(sorted((rule.column, rule.relation, rule.condition) for rule in spec.rules))
    actual_applied, actual_rules = managed_collection_signature(collection)
    return actual_applied == spec.applied_disjunctively and actual_rules == expected_rules


def desired_collection_tags_for_record(record: dict[str, Any]) -> set[str]:
    matches: set[str] = set()
    search_text = record.get("search_text", " ")
    for tag, phrases in GAME_SYSTEM_KEYWORDS.items():
        if any(f" {phrase.strip()} " in search_text for phrase in map(_normalize_search_text, phrases)):
            matches.add(tag)
    for tag, phrases in FACTION_KEYWORDS.items():
        if any(f" {phrase.strip()} " in search_text for phrase in map(_normalize_search_text, phrases)):
            matches.add(tag)
            system = FACTION_TO_SYSTEM.get(tag)
            if system:
                matches.add(system)
    for tag, phrases in PRODUCT_TYPE_RULES:
        if any(f" {phrase.strip()} " in search_text for phrase in map(_normalize_search_text, phrases)):
            matches.add(tag)
            break
    for tag, phrases in SPECIAL_TAG_KEYWORDS.items():
        if any(f" {phrase.strip()} " in search_text for phrase in map(_normalize_search_text, phrases)):
            matches.add(tag)

    vendor = (record.get("vendor") or "").strip().lower()
    if vendor in {"asmodee", "ravensburger"} and "board-game" not in matches and not matches.intersection({"pokemon", "magic", "yugioh", "lorcana", "one-piece", "flesh-and-blood", "d-and-d", "pathfinder"}):
        matches.add("board-game")
    if "combat-patrol" in matches or "codex" in matches:
        matches.add("warhammer-40k")
    if "battletome" in matches:
        matches.add("age-of-sigmar")

    age_days = _record_created_age_days(record)
    if age_days is not None:
        if age_days <= 30:
            matches.add("new-release")
        if age_days <= 45:
            matches.add(INTERNAL_NEW_ARRIVAL_TAG)
    return matches


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
            product.description_html,
            *product.tags,
            product.sku,
        ])),
    }
    return sorted(desired_collection_tags_for_record(record) - {INTERNAL_NEW_ARRIVAL_TAG})


def _record_matches_collection_spec(
    record: dict[str, Any],
    desired_tags: set[str],
    spec: ManagedCollectionSpec,
) -> bool:
    evaluations: list[bool] = []
    for rule in spec.rules:
        if rule.column == "TAG" and rule.relation == "EQUALS":
            evaluations.append(rule.condition in desired_tags)
        elif rule.column == "VENDOR" and rule.relation == "EQUALS":
            evaluations.append((record.get("vendor") or "").strip() == rule.condition)
        elif rule.column == "VARIANT_INVENTORY" and rule.relation == "GREATER_THAN":
            evaluations.append(int(record.get("total_inventory") or 0) > int(rule.condition))
        elif rule.column == "VARIANT_PRICE" and rule.relation == "LESS_THAN":
            evaluations.append(int(record.get("min_price_cents") or 0) <= int(rule.condition))
        elif rule.column == "IS_PRICE_REDUCED" and rule.relation == "IS_SET":
            evaluations.append(bool(record.get("is_price_reduced")))
        else:
            evaluations.append(False)
    if not evaluations:
        return False
    return any(evaluations) if spec.applied_disjunctively else all(evaluations)


def build_collection_matches(
    products: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, set[str]]]:
    by_collection: dict[str, list[dict[str, Any]]] = {spec.title: [] for spec in MANAGED_COLLECTION_SPECS}
    desired_tags_by_product = {
        record["id"]: desired_collection_tags_for_record(record)
        for record in products
    }
    unmatched: list[dict[str, Any]] = []
    for record in products:
        product_matches = []
        desired_tags = desired_tags_by_product[record["id"]]
        for spec in MANAGED_COLLECTION_SPECS:
            if _record_matches_collection_spec(record, desired_tags, spec):
                by_collection[spec.title].append(record)
                product_matches.append(spec.title)
        if not product_matches:
            unmatched.append(record)
    return by_collection, unmatched, desired_tags_by_product


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
                    "applied_disjunctively": bool((node.get("ruleSet") or {}).get("appliedDisjunctively")),
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
                    description
                    tags
                    createdAt
                    totalInventory
                    priceRangeV2 {
                      minVariantPrice { amount }
                    }
                    compareAtPriceRange {
                      minVariantCompareAtPrice { amount }
                    }
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
                    node.get("description") or "",
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
                    "description": node.get("description") or "",
                    "total_inventory": int(node.get("totalInventory") or 0),
                    "min_price_cents": int(round((_safe_float((((node.get("priceRangeV2") or {}).get("minVariantPrice") or {}).get("amount")) or 0) or 0) * 100)),
                    "is_price_reduced": _safe_float((((node.get("compareAtPriceRange") or {}).get("minVariantCompareAtPrice") or {}).get("amount")) or 0) not in (None, 0.0),
                    "skus": skus,
                    "search_text": _normalize_search_text(" ".join(search_parts)),
                }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def get_collection_image(self, collection_id: str) -> dict[str, str]:
        """Return current image info for a collection (empty dict if no image)."""
        q = """
            query($id: ID!) {
              collection(id: $id) {
                image { url altText }
              }
            }
        """
        data = self.gql(q, {"id": collection_id})
        node = (data.get("collection") or {})
        image = node.get("image") or {}
        return {
            "url": image.get("url") or "",
            "alt_text": image.get("altText") or "",
        }

    def find_first_alphabetical_product_with_image(self, collection_id: str) -> dict[str, str]:
        """Return {'product_id', 'product_title', 'image_url', 'image_alt'} for the
        first product (alphabetically by title) in the collection that has a
        featuredImage. Returns an empty dict if none found.

        Uses sortKey TITLE so the first page already starts at the alphabetical top.
        Pages forward only as needed to find an imaged product.
        """
        cursor = None
        page_q = """
            query($id: ID!, $cursor: String) {
              collection(id: $id) {
                products(first: 50, after: $cursor, sortKey: TITLE) {
                  edges {
                    node {
                      id
                      title
                      featuredImage { url altText }
                    }
                  }
                  pageInfo { hasNextPage endCursor }
                }
              }
            }
        """
        while True:
            data = self.gql(page_q, {"id": collection_id, "cursor": cursor})
            collection = data.get("collection") or {}
            products = collection.get("products") or {}
            for edge in products.get("edges", []) or []:
                node = edge.get("node") or {}
                image = node.get("featuredImage") or {}
                url = image.get("url") or ""
                if url:
                    return {
                        "product_id": node.get("id") or "",
                        "product_title": node.get("title") or "",
                        "image_url": url,
                        "image_alt": image.get("altText") or "",
                    }
            page_info = products.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return {}
            cursor = page_info.get("endCursor")

    def update_collection_image(self, collection_id: str, image_src: str, alt_text: str = "") -> str:
        """Set a collection's image to the given URL. Returns the resulting image URL."""
        q = """
            mutation($input: CollectionInput!) {
              collectionUpdate(input: $input) {
                collection { id image { url } }
                userErrors { field message }
              }
            }
        """
        image_input: dict[str, Any] = {"src": image_src}
        if alt_text:
            image_input["altText"] = alt_text
        data = self.gql(q, {
            "input": {
                "id": collection_id,
                "image": image_input,
            }
        })
        errs = data["collectionUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"collectionUpdate (image) errors for {collection_id}: {errs}")
        result_image = ((data["collectionUpdate"].get("collection") or {}).get("image") or {})
        return result_image.get("url") or ""

    def create_smart_collection(
        self,
        title: str,
        handle: str,
        rules: list[CollectionRuleSpec],
        *,
        applied_disjunctively: bool = False,
        description_html: str = "",
    ) -> dict[str, Any]:
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
                        "appliedDisjunctively": applied_disjunctively,
                        "rules": [
                            {
                                "column": rule.column,
                                "relation": rule.relation,
                                "condition": rule.condition,
                            }
                            for rule in rules
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

    def update_smart_collection(
        self,
        collection_id: str,
        title: str,
        handle: str,
        rules: list[CollectionRuleSpec],
        *,
        applied_disjunctively: bool = False,
    ) -> None:
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
                    "appliedDisjunctively": applied_disjunctively,
                    "rules": [
                        {
                            "column": rule.column,
                            "relation": rule.relation,
                            "condition": rule.condition,
                        }
                        for rule in rules
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
                  availablePublicationsCount {
                    count
                  }
                  resourcePublicationsCount {
                    count
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

    def get_publication_id_by_name(self, publication_name: str) -> str:
        target = (publication_name or "").strip().lower()
        if not target:
            raise RuntimeError("Publication name cannot be blank.")
        for publication in self.iter_publications():
            if (publication.get("name") or "").strip().lower() == target:
                return publication["id"]
        raise RuntimeError(f"Could not find Shopify publication named {publication_name!r}.")

    def publish_to_publication(self, resource_id: str, publication_id: str) -> None:
        q = """
            mutation($id: ID!, $publicationId: ID!) {
              publishablePublish(id: $id, input: {publicationId: $publicationId}) {
                publishable {
                  publishedOnPublication(publicationId: $publicationId)
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"id": resource_id, "publicationId": publication_id})
        errs = data["publishablePublish"]["userErrors"]
        if errs:
            raise RuntimeError(f"publishablePublish errors: {errs}")
        if not data["publishablePublish"]["publishable"]["publishedOnPublication"]:
            raise RuntimeError(
                f"publishablePublish did not confirm publication for resource {resource_id} "
                f"on publication {publication_id}"
            )

    def unpublish_from_publication(self, resource_id: str, publication_id: str) -> None:
        q = """
            mutation($id: ID!, $publicationId: ID!) {
              publishableUnpublish(id: $id, input: {publicationId: $publicationId}) {
                publishable {
                  publishedOnPublication(publicationId: $publicationId)
                }
                userErrors { field message }
              }
            }
        """
        data = self.gql(q, {"id": resource_id, "publicationId": publication_id})
        errs = data["publishableUnpublish"]["userErrors"]
        if errs:
            raise RuntimeError(f"publishableUnpublish errors: {errs}")
        if data["publishableUnpublish"]["publishable"]["publishedOnPublication"]:
            raise RuntimeError(
                f"publishableUnpublish did not confirm unpublication for resource {resource_id} "
                f"on publication {publication_id}"
            )

    def publish_to_online_store(self, resource_id: str) -> None:
        publication_id = self.get_publication_id_by_name(ONLINE_STORE_PUBLICATION_NAME)
        self.publish_to_publication(resource_id, publication_id)

    def iter_products_unpublished_on_publication(self, publication_id: str) -> Iterable[dict[str, Any]]:
        cursor = None
        page_q = """
            query($cursor: String, $publicationId: ID!) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    publishedOnPublication(publicationId: $publicationId)
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
            data = self.gql(page_q, {"cursor": cursor, "publicationId": publication_id})
            for edge in data["products"]["edges"]:
                node = edge["node"]
                if node.get("publishedOnPublication"):
                    continue
                yield {
                    "id": node["id"],
                    "title": node.get("title") or "",
                    "published_on_publication": False,
                    "publication_id": publication_id,
                    "skus": [
                        (variant_edge.get("node") or {}).get("sku") or ""
                        for variant_edge in (node.get("variants") or {}).get("edges") or []
                        if ((variant_edge.get("node") or {}).get("sku") or "").strip()
                    ],
                }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

    def iter_products_for_online_store_image_visibility(self, publication_id: str) -> Iterable[dict[str, Any]]:
        # Per the clarified workflow contract, any attached Shopify product media
        # counts for visibility, not only MediaImage entries.
        cursor = None
        page_q = """
            query($cursor: String, $publicationId: ID!) {
              products(first: 100, after: $cursor) {
                edges {
                  cursor
                  node {
                    id
                    title
                    publishedOnPublication(publicationId: $publicationId)
                    variants(first: 10) {
                      edges {
                        node {
                          sku
                        }
                      }
                    }
                    media(first: 1) {
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
            data = self.gql(page_q, {"cursor": cursor, "publicationId": publication_id})
            for edge in data["products"]["edges"]:
                node = edge["node"]
                yield {
                    "id": node["id"],
                    "title": node.get("title") or "",
                    "published_on_publication": bool(node.get("publishedOnPublication")),
                    "publication_id": publication_id,
                    "has_media": bool(((node.get("media") or {}).get("edges") or [])),
                    "skus": [
                        (variant_edge.get("node") or {}).get("sku") or ""
                        for variant_edge in (node.get("variants") or {}).get("edges") or []
                        if ((variant_edge.get("node") or {}).get("sku") or "").strip()
                    ],
                }
            if not data["products"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["products"]["pageInfo"]["endCursor"]

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

    def get_product_metafield_definition(self, namespace: str, key: str) -> dict[str, Any] | None:
        q = """
            query($namespace: String!, $key: String!) {
              metafieldDefinitions(first: 2, ownerType: PRODUCT, namespace: $namespace, key: $key) {
                nodes {
                  id
                  namespace
                  key
                  ownerType
                  type { name }
                  capabilities {
                    adminFilterable {
                      eligible
                      enabled
                      status
                    }
                  }
                }
              }
            }
        """
        data = self.gql(q, {"namespace": namespace, "key": key})
        matches = data["metafieldDefinitions"].get("nodes") or []
        if not matches:
            return None
        if len(matches) > 1:
            raise RuntimeError(
                f"Multiple product metafield definitions matched {namespace}.{key}; resolve duplicates before continuing."
            )
        return matches[0]

    def create_product_metafield_definition(
        self,
        namespace: str,
        key: str,
        name: str,
        metafield_type: str,
    ) -> dict[str, Any]:
        q = """
            mutation($definition: MetafieldDefinitionInput!) {
              metafieldDefinitionCreate(definition: $definition) {
                createdDefinition {
                  id
                  namespace
                  key
                  ownerType
                  type { name }
                  capabilities {
                    adminFilterable {
                      eligible
                      enabled
                      status
                    }
                  }
                }
                userErrors { field message code }
              }
            }
        """
        data = self.gql(q, {
            "definition": {
                "name": name,
                "namespace": namespace,
                "key": key,
                "ownerType": "PRODUCT",
                "type": metafield_type,
                "access": {"admin": FALLBACK_IMAGE_METAFIELD_ADMIN_ACCESS},
                "capabilities": {
                    "adminFilterable": {"enabled": True},
                },
            }
        })
        errs = data["metafieldDefinitionCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldDefinitionCreate errors for {namespace}.{key}: {errs}")
        return data["metafieldDefinitionCreate"]["createdDefinition"]

    def update_product_metafield_definition_admin_filterable(self, namespace: str, key: str) -> dict[str, Any]:
        q = """
            mutation($definition: MetafieldDefinitionUpdateInput!) {
              metafieldDefinitionUpdate(definition: $definition) {
                updatedDefinition {
                  id
                  namespace
                  key
                  ownerType
                  type { name }
                  capabilities {
                    adminFilterable {
                      eligible
                      enabled
                      status
                    }
                  }
                }
                userErrors { field message code }
              }
            }
        """
        data = self.gql(q, {
            "definition": {
                "namespace": namespace,
                "key": key,
                "ownerType": "PRODUCT",
                "capabilities": {
                    "adminFilterable": {"enabled": True},
                },
            }
        })
        errs = data["metafieldDefinitionUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldDefinitionUpdate errors for {namespace}.{key}: {errs}")
        return data["metafieldDefinitionUpdate"]["updatedDefinition"]

    def ensure_fallback_image_metafield_definition(self) -> None:
        definition = self.get_product_metafield_definition(
            FALLBACK_IMAGE_METAFIELD_NAMESPACE,
            FALLBACK_IMAGE_METAFIELD_KEY,
        )
        if definition is None:
            definition = self.create_product_metafield_definition(
                FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                FALLBACK_IMAGE_METAFIELD_KEY,
                FALLBACK_IMAGE_METAFIELD_NAME,
                FALLBACK_IMAGE_METAFIELD_TYPE,
            )
        else:
            definition_type = (((definition.get("type") or {}).get("name")) or "").strip().lower()
            if definition_type != FALLBACK_IMAGE_METAFIELD_TYPE:
                raise RuntimeError(
                    f"Fallback image metafield definition {FALLBACK_IMAGE_METAFIELD_NAMESPACE}.{FALLBACK_IMAGE_METAFIELD_KEY} "
                    f"already exists with incompatible type {definition_type!r}."
                )
            admin_filterable = ((definition.get("capabilities") or {}).get("adminFilterable") or {})
            if not admin_filterable.get("eligible", False):
                raise RuntimeError(
                    f"Fallback image metafield definition {FALLBACK_IMAGE_METAFIELD_NAMESPACE}.{FALLBACK_IMAGE_METAFIELD_KEY} "
                    "is not eligible for admin filtering."
                )
            if not admin_filterable.get("enabled", False):
                definition = self.update_product_metafield_definition_admin_filterable(
                    FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                    FALLBACK_IMAGE_METAFIELD_KEY,
                )
        admin_filterable = ((definition.get("capabilities") or {}).get("adminFilterable") or {})
        status = (admin_filterable.get("status") or "").strip().upper()
        if not admin_filterable.get("enabled", False):
            raise RuntimeError(
                f"Fallback image metafield definition {FALLBACK_IMAGE_METAFIELD_NAMESPACE}.{FALLBACK_IMAGE_METAFIELD_KEY} "
                "did not end in an enabled admin-filterable state."
            )
        if status in {"FAILED", "NOT_FILTERABLE"}:
            raise RuntimeError(
                f"Fallback image metafield definition {FALLBACK_IMAGE_METAFIELD_NAMESPACE}.{FALLBACK_IMAGE_METAFIELD_KEY} "
                f"reported non-queryable admin filter status {status!r}."
            )

    def set_product_fallback_image_used(self, product_id: str) -> None:
        q = """
            mutation($metafields: [MetafieldsSetInput!]!) {
              metafieldsSet(metafields: $metafields) {
                metafields {
                  namespace
                  key
                  value
                }
                userErrors { field message code }
              }
            }
        """
        data = self.gql(q, {
            "metafields": [
                {
                    "ownerId": product_id,
                    "namespace": FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                    "key": FALLBACK_IMAGE_METAFIELD_KEY,
                    "type": FALLBACK_IMAGE_METAFIELD_TYPE,
                    "value": "true",
                }
            ]
        })
        errs = data["metafieldsSet"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldsSet errors for {product_id}: {errs}")

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

    def iter_shopify_image_files_for_photo_sync(self) -> Iterable[ShopifyImageFile]:
        cursor = None
        page_q = """
            query($cursor: String) {
              files(first: 100, after: $cursor, query: "media_type:IMAGE") {
                edges {
                  node {
                    ... on MediaImage {
                      id
                      alt
                      fileStatus
                      image {
                        url
                      }
                      originalSource {
                        url
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
            for edge in data["files"]["edges"]:
                node = edge.get("node") or {}
                file_id = node.get("id")
                if not file_id:
                    continue
                yield _build_shopify_image_file(
                    file_id=file_id,
                    alt=node.get("alt") or "",
                    file_status=node.get("fileStatus") or "",
                    original_source_url=((node.get("originalSource") or {}).get("url") or ""),
                    image_url=((node.get("image") or {}).get("url") or ""),
                )
            if not data["files"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["files"]["pageInfo"]["endCursor"]

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

    def wait_for_files_ready(
        self,
        file_ids: list[str],
        timeout_seconds: int = 240,
        file_labels: dict[str, str] | None = None,
    ) -> list[str]:
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
                details = statuses
                if file_labels:
                    details = {
                        file_id: {
                            "status": status,
                            "source": file_labels.get(file_id, ""),
                        }
                        for file_id, status in statuses.items()
                    }
                raise RuntimeError(f"File processing failed: {details}")
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
              job(id: $id) {
                id
                done
              }
            }
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            data = self.gql(q, {"id": job_id})
            job = data.get("job")
            if job and job.get("done"):
                return
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for job completion: {job_id}")

    def reorder_product_media(self, product_id: str, media_ids: list[str]) -> None:
        if not media_ids:
            return
        moves = [{"id": media_id, "newPosition": str(index)} for index, media_id in enumerate(media_ids)]
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
    log("=== DELETE COLLECTIONS phase: removing all Shopify collections ===")
    deleted = 0
    for collection in client.iter_all_collections():
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
    desired_tags_by_product: dict[str, set[str]],
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
        "desired_tags",
    ]
    with COLLECTION_GENERATION_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=preview_cols)
        writer.writeheader()
        for spec in MANAGED_COLLECTION_SPECS:
            for record in by_collection[spec.title]:
                writer.writerow({
                    "collection_title": spec.title,
                    "collection_handle": spec.handle,
                    "product_id": record["id"],
                    "title": record["title"],
                    "vendor": record["vendor"],
                    "product_type": record["product_type"],
                    "skus": "|".join(record["skus"]),
                    "tags": "|".join(record["tags"]),
                    "desired_tags": "|".join(sorted(desired_tags_by_product[record["id"]] - {INTERNAL_NEW_ARRIVAL_TAG})),
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
    log("=== GENERATE COLLECTIONS phase: rebuilding the managed storefront collection taxonomy ===")
    products = list(client.iter_existing_for_collection_generation())
    existing_collections = list(client.iter_all_collections())
    by_collection, unmatched, desired_tags_by_product = build_collection_matches(products)
    write_collection_generation_preview(by_collection, unmatched, desired_tags_by_product)
    log(
        f"  wrote collection preview: {COLLECTION_GENERATION_PREVIEW_CSV} "
        f"and unmatched report: {COLLECTION_GENERATION_UNMATCHED_CSV}"
    )
    matched_count = sum(len(items) for items in by_collection.values())
    non_empty = sum(1 for items in by_collection.values() if items)
    log(
        f"  planned memberships={matched_count} across {non_empty}/{len(MANAGED_COLLECTION_SPECS)} "
        f"collections; unmatched_products={len(unmatched)}"
    )
    for spec in MANAGED_COLLECTION_SPECS:
        if by_collection[spec.title]:
            log(f"  match count: {spec.title} -> {len(by_collection[spec.title])}")

    if dry:
        raise RuntimeError("--generate-collections now performs a live rebuild only and cannot be used with --dry-run.")

    updated_products = 0
    created = 0
    deleted = 0
    published = 0
    images_updated = 0
    images_skipped = 0

    for collection in existing_collections:
        client.delete_collection(collection["id"])
        deleted += 1

    for record in products:
        desired_tags = desired_tags_by_product[record["id"]] - {INTERNAL_NEW_ARRIVAL_TAG}
        preserved_tags = [
            tag for tag in record["tags"]
            if _normalized_tag(tag) not in LEGACY_MANAGED_NORMALIZED_TAGS
        ]
        next_tags = sorted(set(preserved_tags) | desired_tags)
        if next_tags != sorted(set(record["tags"])):
            client.update_product_tags(record["id"], next_tags)
            updated_products += 1

    for spec in MANAGED_COLLECTION_SPECS:
        members = by_collection[spec.title]
        if not members:
            continue
        collection = client.create_smart_collection(
            spec.title,
            spec.handle,
            list(spec.rules),
            applied_disjunctively=spec.applied_disjunctively,
        )
        created += 1
        try:
            published += client.publish_to_all_channels(collection["id"])
        except Exception as e:
            log(
                f"  warn: could not publish collection {spec.title!r} to all sales channels: {e}. "
                "The collection may remain hidden until the app has publication scopes."
            )
        picked = client.find_first_alphabetical_product_with_image(collection["id"])
        if not picked:
            images_skipped += 1
            continue
        current = client.get_collection_image(collection["id"])
        if current.get("url") == picked["image_url"]:
            images_skipped += 1
            continue
        client.update_collection_image(
            collection["id"],
            picked["image_url"],
            alt_text=picked.get("image_alt") or spec.title,
        )
        images_updated += 1

    log(
        f"GENERATE COLLECTIONS summary: deleted={deleted} tagged_products={updated_products} created={created} "
        f"publication_links={published} images_updated={images_updated} images_skipped={images_skipped} "
        f"unmatched_products={len(unmatched)}"
    )


def phase_update_collection_images(client: Shopify, dry: bool) -> None:
    """For each managed smart collection, set the collection image to the
    image of the first product (alphabetically by title) in that collection that
    has a featuredImage. Idempotent: skips collections whose image already matches.
    """
    log("=== UPDATE COLLECTION IMAGES phase: copying first-alphabetical product image to each collection ===")
    existing_collections = {item["handle"]: item for item in client.iter_all_collections()}
    rows: list[dict[str, str]] = []
    actions = {"updated": 0, "already_set": 0, "no_products_with_image": 0, "missing_collection": 0, "skip_unmanaged": 0, "errors": 0}

    for spec in MANAGED_COLLECTION_SPECS:
        title = spec.title
        handle = spec.handle
        collection = existing_collections.get(handle)
        if not collection:
            log(f"  skip (no collection): {title!r} handle={handle!r}")
            rows.append({
                "collection_title": title,
                "collection_handle": handle,
                "collection_id": "",
                "current_image_url": "",
                "picked_product_id": "",
                "picked_product_title": "",
                "picked_image_url": "",
                "status": "missing_collection",
            })
            actions["missing_collection"] += 1
            continue

        if not is_managed_collection(collection, expected_spec=spec):
            log(
                f"  skip (unmanaged collection): {title!r} handle={handle!r} "
                f"id={collection['id']} -- rule set does not match the managed taxonomy"
            )
            rows.append({
                "collection_title": title,
                "collection_handle": handle,
                "collection_id": collection["id"],
                "current_image_url": "",
                "picked_product_id": "",
                "picked_product_title": "",
                "picked_image_url": "",
                "status": "skip_unmanaged",
            })
            actions["skip_unmanaged"] += 1
            continue

        try:
            current = client.get_collection_image(collection["id"])
        except Exception as e:
            log(f"  ERROR reading current image for {title!r}: {e}")
            current = {"url": "", "alt_text": ""}

        try:
            picked = client.find_first_alphabetical_product_with_image(collection["id"])
        except Exception as e:
            log(f"  ERROR querying products for {title!r}: {e}")
            actions["errors"] += 1
            rows.append({
                "collection_title": title,
                "collection_handle": handle,
                "collection_id": collection["id"],
                "current_image_url": current.get("url", ""),
                "picked_product_id": "",
                "picked_product_title": "",
                "picked_image_url": "",
                "status": f"error: {e}",
            })
            continue

        if not picked:
            log(f"  no imaged product found in {title!r} (handle={handle!r})")
            actions["no_products_with_image"] += 1
            rows.append({
                "collection_title": title,
                "collection_handle": handle,
                "collection_id": collection["id"],
                "current_image_url": current.get("url", ""),
                "picked_product_id": "",
                "picked_product_title": "",
                "picked_image_url": "",
                "status": "no_products_with_image",
            })
            continue

        picked_url = picked["image_url"]
        already = current.get("url", "") == picked_url
        status = "already_set" if already else ("dry_run_would_update" if dry else "updated")
        log(
            f"  {title!r}: pick={picked['product_title']!r} "
            f"image={picked_url} status={status}"
        )

        if not already and not dry:
            try:
                # Use product title as alt text fallback for screen readers.
                client.update_collection_image(
                    collection["id"],
                    picked_url,
                    alt_text=picked.get("image_alt") or title,
                )
                actions["updated"] += 1
            except Exception as e:
                log(f"  ERROR updating image for {title!r}: {e}")
                status = f"error: {e}"
                actions["errors"] += 1
        elif already:
            actions["already_set"] += 1

        rows.append({
            "collection_title": title,
            "collection_handle": handle,
            "collection_id": collection["id"],
            "current_image_url": current.get("url", ""),
            "picked_product_id": picked["product_id"],
            "picked_product_title": picked["product_title"],
            "picked_image_url": picked_url,
            "status": status,
        })

    cols = [
        "collection_title",
        "collection_handle",
        "collection_id",
        "current_image_url",
        "picked_product_id",
        "picked_product_title",
        "picked_image_url",
        "status",
    ]
    with COLLECTION_IMAGE_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"  wrote preview: {COLLECTION_IMAGE_PREVIEW_CSV}")
    log(
        "UPDATE COLLECTION IMAGES summary: "
        f"updated={actions['updated']} already_set={actions['already_set']} "
        f"no_products_with_image={actions['no_products_with_image']} "
        f"missing_collection={actions['missing_collection']} "
        f"skip_unmanaged={actions['skip_unmanaged']} errors={actions['errors']} "
        f"dry_run={dry}"
    )


def phase_import(client: Shopify, products: list[Product], location_id: str, dry: bool,
                 start_at: int = 0) -> None:
    log(f"=== IMPORT phase: creating {len(products)} products (starting at index {start_at}) ===")
    if dry:
        log("(dry-run: not contacting Shopify)")
        return
    created = 0
    create_failed = 0
    publish_failed = 0
    published = 0
    for i, p in enumerate(products):
        if i < start_at:
            continue
        try:
            pid = client.create_product(p, location_id)
            created += 1
            try:
                client.publish_to_online_store(pid)
                published += 1
            except Exception as e:
                publish_failed += 1
                log(f"  FAILED publish {p.sku} ({p.title!r}): {e}")
                with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                    fh.write(f"import_publish\t{p.sku}\t{p.title}\t{e}\n")
            if (created % 25) == 0:
                log(f"  progress: created {created}/{len(products) - start_at}  (last: {p.sku})")
        except Exception as e:
            create_failed += 1
            log(f"  FAILED {p.sku} ({p.title!r}): {e}")
            with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                fh.write(f"{i}\t{p.sku}\t{p.title}\t{e}\n")
    log(
        f"IMPORT summary: created={created}, published={published}, "
        f"create_failed={create_failed}, publish_failed={publish_failed}"
    )


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
    write_failed = 0
    publish_failed = 0
    published = 0

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
            write_failed += 1
            log(f"  FAILED update {p.sku} ({p.title!r}): {e}")
            with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                fh.write(f"update_write\t{p.sku}\t{p.title}\t{e}\n")
            continue

        try:
            client.publish_to_online_store(existing["product_id"])
            published += 1
        except Exception as e:
            publish_failed += 1
            log(f"  FAILED publish {p.sku} ({p.title!r}): {e}")
            with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                fh.write(f"update_publish\t{p.sku}\t{p.title}\t{e}\n")

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
        f"cost_updates={cost_updates} qty_updates={qty_updates} published={published} "
        f"write_failed={write_failed} publish_failed={publish_failed} dry_run={dry}"
    )


def phase_publish_online_store_backfill(client: Shopify, dry: bool) -> None:
    log("=== ONLINE STORE BACKFILL phase: publishing currently-unpublished Shopify products ===")
    publication_id = client.get_publication_id_by_name(ONLINE_STORE_PUBLICATION_NAME)
    candidates = list(client.iter_products_unpublished_on_publication(publication_id))
    rows: list[dict[str, str]] = []
    published = 0
    publish_failed = 0

    for candidate in candidates:
        status = "dry_run_candidate" if dry else "published"
        if not dry:
            try:
                client.publish_to_publication(candidate["id"], publication_id)
                published += 1
            except Exception as e:
                publish_failed += 1
                status = f"publish_failed: {e}"
                log(f"  FAILED publish {candidate['title']!r} ({candidate['id']}): {e}")
                with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                    fh.write(
                        f"backfill_publish\t{'|'.join(candidate['skus'])}\t"
                        f"{candidate['title']}\t{e}\n"
                    )
        rows.append({
            "product_id": candidate["id"],
            "title": candidate["title"],
            "skus": "|".join(candidate["skus"]),
            "publication_name": ONLINE_STORE_PUBLICATION_NAME,
            "publication_id": candidate["publication_id"],
            "published_on_publication": "false",
            "status": status,
        })

    cols = [
        "product_id",
        "title",
        "skus",
        "publication_name",
        "publication_id",
        "published_on_publication",
        "status",
    ]
    with ONLINE_STORE_BACKFILL_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"  wrote preview: {ONLINE_STORE_BACKFILL_PREVIEW_CSV}")
    log(
        f"ONLINE STORE BACKFILL summary: candidates={len(candidates)} published={published} "
        f"publish_failed={publish_failed} dry_run={dry}"
    )


def phase_reconcile_online_store_image_visibility(client: Shopify, dry: bool) -> None:
    log("=== ONLINE STORE IMAGE VISIBILITY phase: reconciling publication against any product media ===")
    publication_id = client.get_publication_id_by_name(ONLINE_STORE_PUBLICATION_NAME)
    candidates = list(client.iter_products_for_online_store_image_visibility(publication_id))
    rows: list[dict[str, str]] = []
    published = 0
    unpublished = 0
    publish_failed = 0
    unpublish_failed = 0
    unchanged = 0

    for candidate in candidates:
        has_media = candidate["has_media"]
        is_published = candidate["published_on_publication"]
        if has_media and not is_published:
            status = "dry_run_publish" if dry else "published"
            if not dry:
                try:
                    client.publish_to_publication(candidate["id"], publication_id)
                    published += 1
                except Exception as e:
                    publish_failed += 1
                    status = f"publish_failed: {e}"
                    log(f"  FAILED publish {candidate['title']!r} ({candidate['id']}): {e}")
                    with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                        fh.write(
                            f"image_visibility_publish\t{_safe_spreadsheet_cell('|'.join(candidate['skus']))}\t"
                            f"{_safe_spreadsheet_cell(candidate['title'])}\t{_safe_spreadsheet_cell(e)}\n"
                        )
            rows.append({
                "product_id": candidate["id"],
                "title": _safe_spreadsheet_cell(candidate["title"]),
                "skus": _safe_spreadsheet_cell("|".join(candidate["skus"])),
                "publication_name": ONLINE_STORE_PUBLICATION_NAME,
                "publication_id": candidate["publication_id"],
                "published_on_publication": "false",
                "has_media": "true",
                "desired_published_on_publication": "true",
                "status": status,
            })
            continue
        if not has_media and is_published:
            status = "dry_run_unpublish" if dry else "unpublished"
            if not dry:
                try:
                    client.unpublish_from_publication(candidate["id"], publication_id)
                    unpublished += 1
                except Exception as e:
                    unpublish_failed += 1
                    status = f"unpublish_failed: {e}"
                    log(f"  FAILED unpublish {candidate['title']!r} ({candidate['id']}): {e}")
                    with (HERE / "failures.tsv").open("a", encoding="utf-8") as fh:
                        fh.write(
                            f"image_visibility_unpublish\t{_safe_spreadsheet_cell('|'.join(candidate['skus']))}\t"
                            f"{_safe_spreadsheet_cell(candidate['title'])}\t{_safe_spreadsheet_cell(e)}\n"
                        )
            rows.append({
                "product_id": candidate["id"],
                "title": _safe_spreadsheet_cell(candidate["title"]),
                "skus": _safe_spreadsheet_cell("|".join(candidate["skus"])),
                "publication_name": ONLINE_STORE_PUBLICATION_NAME,
                "publication_id": candidate["publication_id"],
                "published_on_publication": "true",
                "has_media": "false",
                "desired_published_on_publication": "false",
                "status": status,
            })
            continue
        unchanged += 1

    cols = [
        "product_id",
        "title",
        "skus",
        "publication_name",
        "publication_id",
        "published_on_publication",
        "has_media",
        "desired_published_on_publication",
        "status",
    ]
    with ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"  wrote preview: {ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV}")
    log(
        "ONLINE STORE IMAGE VISIBILITY summary: "
        f"candidates={len(candidates)} actions={len(rows)} published={published} "
        f"unpublished={unpublished} unchanged={unchanged} "
        f"publish_failed={publish_failed} unpublish_failed={unpublish_failed} dry_run={dry}"
    )


def _graphql_error_code(err: Any) -> str | None:
    if isinstance(err, dict):
        return err.get("extensions", {}).get("code")
    return None


def _safe_spreadsheet_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", " ").replace("\n", " ")
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


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


def phase_photo_source_web_all(
    client: Shopify,
    products: list[Product],
    *,
    dry: bool,
    manifest_path: Path = PHOTO_SOURCE_MANIFEST_JSON,
    cache_root: Path = PHOTO_SOURCE_CACHE_CURRENT,
) -> None:
    log("=== PHOTO SOURCE phase: discovering zero-media catalog products and staging web winners ===")
    manifest = load_photo_manifest(manifest_path)
    session = getattr(client, "session", None)
    if session is None:
        session = requests.Session()
    headers = getattr(session, "headers", None)
    if headers is None:
        session.headers = {}
    session.headers.setdefault("User-Agent", PHOTO_SOURCE_USER_AGENT)

    local_by_sku = {product.sku: product for product in products}
    by_sku: dict[str, dict[str, Any]] = {}
    duplicate_shopify_skus: set[str] = set()
    unmapped_rows: list[tuple[str, str, str]] = []
    for rec in client.iter_existing_for_photo_sync():
        if rec["sku"] in by_sku:
            duplicate_shopify_skus.add(rec["sku"])
            continue
        by_sku[rec["sku"]] = rec
        if not rec["media_ids"] and rec["sku"] not in local_by_sku:
            unmapped_rows.append((rec["sku"], rec["title"], "zero-media Shopify SKU not present in local catalog"))

    preview_rows: list[dict[str, Any]] = []
    missing_rows: list[tuple[str, str, str]] = []
    ambiguous_rows: list[tuple[str, str, str]] = []
    failure_rows: list[tuple[str, str, str]] = []
    current_root = cache_root

    for product in products:
        existing = by_sku.get(product.sku)
        if not existing:
            continue
        if existing["media_ids"]:
            continue
        if product.sku in duplicate_shopify_skus:
            preview_rows.append(build_photo_source_preview_row(
                product,
                status="skip_ambiguous_shopify",
                query=build_photo_source_query(product),
                reason=PHOTO_SOURCE_DUPLICATE_SKU_REASON,
            ))
            ambiguous_rows.append((product.sku, product.title, PHOTO_SOURCE_DUPLICATE_SKU_REASON))
            continue

        query = build_photo_source_query(product)
        pack_name = stable_photo_source_dirname(product)
        prior_entry = dict(manifest.get(product.sku, {}))
        if (
            not dry
            and prior_entry.get("state") == "completed"
            and prior_entry.get("query") == query
            and (current_root / pack_name).exists()
        ):
            preview_rows.append(build_photo_source_preview_row(
                product,
                status="resume_completed",
                query=query,
                top_score=int(prior_entry.get("top_score") or 0),
                reason="existing staged winner reused",
                staged_dir=display_path(current_root / pack_name),
            ))
            continue

        try:
            search_url = f"{PHOTO_SOURCE_SEARCH_URL}?q={quote_plus(query)}"
            search_response = fetch_url_with_retries(
                session,
                search_url,
                timeout=PHOTO_SOURCE_HTML_TIMEOUT_SECONDS,
            )
            result_urls = extract_photo_source_search_results(search_response.text)
            candidates: list[PhotoSourceCandidate] = []
            for result in result_urls[:PHOTO_SOURCE_MAX_CANDIDATE_PAGES]:
                if not is_allowed_photo_source_url(result.url):
                    continue
                page_response = fetch_url_with_retries(
                    session,
                    result.url,
                    timeout=PHOTO_SOURCE_HTML_TIMEOUT_SECONDS,
                )
                candidates.extend(extract_photo_source_candidates(product, result.url, page_response.text))
            candidates.sort(key=lambda item: (-item.score, item.image_url))
            outcome, winner, reason = choose_photo_source_winner(candidates)
            top_score = candidates[0].score if candidates else 0
            if outcome == "missing":
                record_photo_source_non_winner(
                    product,
                    status="missing",
                    query=query,
                    top_score=top_score,
                    reason=reason,
                    preview_rows=preview_rows,
                    log_rows=missing_rows,
                    dry=dry,
                    manifest=manifest,
                    manifest_path=manifest_path,
                )
                continue
            if outcome == "ambiguous":
                record_photo_source_non_winner(
                    product,
                    status="ambiguous",
                    query=query,
                    top_score=top_score,
                    reason=reason,
                    preview_rows=preview_rows,
                    log_rows=ambiguous_rows,
                    dry=dry,
                    manifest=manifest,
                    manifest_path=manifest_path,
                )
                continue

            assert winner is not None
            staged_dir_display = display_path(current_root / pack_name)
            if not dry:
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="downloading",
                    query=query,
                    top_score=winner.score,
                    winner_page_url=winner.page_url,
                    winner_image_url=winner.image_url,
                    winner_reasons=winner.reasons,
                    staged_dir=staged_dir_display,
                    manifest_version=PHOTO_SOURCE_MANIFEST_VERSION,
                )
                PHOTO_SOURCE_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
                with tempfile.TemporaryDirectory(dir=PHOTO_SOURCE_CACHE_ROOT) as tmp:
                    temp_root = Path(tmp)
                    pack_dir = temp_root / pack_name
                    pack_dir.mkdir(parents=True, exist_ok=True)
                    image_response = fetch_url_with_retries(
                        session,
                        winner.image_url,
                        timeout=PHOTO_SOURCE_IMAGE_TIMEOUT_SECONDS,
                        binary=True,
                    )
                    image_bytes = bytes(image_response.content)
                    filename = Path(urlparse(winner.image_url).path).name or "01.jpg"
                    if Path(filename).suffix.lower() not in IMAGE_SUFFIXES:
                        filename = f"{pack_name}.jpg"
                    (pack_dir / filename).write_bytes(image_bytes)
                    metadata = {
                        "sku": product.sku,
                        "title": product.title,
                        "query": query,
                        "score": winner.score,
                        "page_url": winner.page_url,
                        "image_url": winner.image_url,
                        "reasons": winner.reasons,
                        "detail_signals": winner.detail_signals,
                        "image_sha256": hashlib.sha256(image_bytes).hexdigest(),
                    }
                    (pack_dir / "_source.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
                    publish_photo_source_pack(pack_dir, current_root, pack_name)
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="completed",
                    query=query,
                    top_score=winner.score,
                    winner_page_url=winner.page_url,
                    winner_image_url=winner.image_url,
                    winner_reasons=winner.reasons,
                    staged_dir=staged_dir_display,
                    manifest_version=PHOTO_SOURCE_MANIFEST_VERSION,
                    reason="",
                )
            preview_rows.append(build_photo_source_preview_row(
                product,
                status="winner",
                query=query,
                top_score=winner.score,
                winner=winner,
                staged_dir=staged_dir_display if not dry else "",
            ))
        except Exception as exc:
            detail = str(exc)
            record_photo_source_non_winner(
                product,
                status="failed",
                query=query,
                top_score=0,
                reason=detail,
                preview_rows=preview_rows,
                log_rows=failure_rows,
                dry=dry,
                manifest=manifest,
                manifest_path=manifest_path,
            )

    cols = [
        "sku",
        "title",
        "status",
        "query",
        "top_score",
        "winner_page_url",
        "winner_image_url",
        "winner_reasons",
        "staged_dir",
        "reason",
    ]
    with PHOTO_SOURCE_PREVIEW_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in preview_rows:
            writer.writerow(row)
    if not dry:
        append_photo_log(PHOTO_SOURCE_UNMAPPED_SHOPIFY_TSV, unmapped_rows)
    append_photo_log(PHOTO_SOURCE_MISSING_TSV, missing_rows)
    append_photo_log(PHOTO_SOURCE_AMBIGUOUS_TSV, ambiguous_rows)
    append_photo_log(PHOTO_SOURCE_FAILURES_TSV, failure_rows)
    log(
        f"PHOTO SOURCE summary: candidates={len(preview_rows)} "
        f"missing={len(missing_rows)} ambiguous={len(ambiguous_rows)} failed={len(failure_rows)} "
        f"unmapped={len(unmapped_rows)} dry_run={dry}"
    )


def phase_photo_sync(
    client: Shopify,
    products: list[Product],
    photo_root: Path | None,
    dry: bool,
    manifest_path: Path = PHOTO_SYNC_MANIFEST_JSON,
    source_mode: str = PHOTO_SYNC_SOURCE_STAGED_LOCAL,
    product_scope: str = PHOTO_SYNC_SCOPE_GW,
    fallback_audit: bool = False,
) -> None:
    if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
        if product_scope == PHOTO_SYNC_SCOPE_ALL:
            log("=== PHOTO SYNC phase: matching all catalog products to staged local image folders ===")
        else:
            log("=== PHOTO SYNC phase: matching GW products to staged local image folders ===")
        if photo_root is None:
            raise RuntimeError("Photo sync requires a local photo root.")
        asset_sets = discover_photo_asset_sets(photo_root)
        asset_sets_by_code, asset_sets_by_slug = build_photo_indexes(asset_sets)
        shopify_files_by_code: dict[str, list[ShopifyImageFile]] = {}
        shopify_files_by_slug: dict[str, list[ShopifyImageFile]] = {}
    elif source_mode == PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING:
        log("=== PHOTO SYNC phase: matching GW products to existing Shopify Files ===")
        image_files = list(client.iter_shopify_image_files_for_photo_sync())
        shopify_files_by_code, shopify_files_by_slug = build_shopify_file_indexes(image_files)
        log(f"  indexed {len(image_files)} Shopify image files for attachment matching")
        asset_sets_by_code = {}
        asset_sets_by_slug = {}
    else:
        raise RuntimeError(f"Unsupported photo sync source mode: {source_mode}")
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

    if product_scope == PHOTO_SYNC_SCOPE_GW:
        scoped_products = [p for p in products if p.source == "GW"]
    elif product_scope == PHOTO_SYNC_SCOPE_ALL:
        scoped_products = list(products)
    else:
        raise RuntimeError(f"Unsupported photo sync product scope: {product_scope}")

    if fallback_audit:
        if source_mode != PHOTO_SYNC_SOURCE_STAGED_LOCAL or product_scope != PHOTO_SYNC_SCOPE_ALL:
            raise RuntimeError("Fallback audit is only supported for all-catalog staged-local photo sync.")
        if not dry:
            client.ensure_fallback_image_metafield_definition()

    for product in scoped_products:
        existing = by_sku.get(product.sku)
        if not existing:
            reason = "SKU not found in Shopify"
            preview_rows.append(build_photo_sync_preview_row(
                product,
                status="skip_missing_shopify",
                match_type="",
                source_mode=source_mode,
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
                source_mode=source_mode,
                photo_root=photo_root,
                reason=reason,
            ))
            ambiguous_rows.append((product.sku, product.title, reason))
            continue
        if product_scope == PHOTO_SYNC_SCOPE_GW and not _is_games_workshop_record(existing):
            reason = "Shopify product failed Games Workshop identity gate"
            preview_rows.append(build_photo_sync_preview_row(
                product,
                status="skip_non_gw",
                match_type="",
                source_mode=source_mode,
                photo_root=photo_root,
                reason=reason,
            ))
            continue

        asset_set: PhotoAssetSet | None = None
        matched_files: list[ShopifyImageFile] = []
        if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
            status, match_type, asset_set, reason = resolve_photo_asset(
                product,
                asset_sets_by_code,
                asset_sets_by_slug,
            )
        else:
            status, match_type, matched_files, reason = resolve_existing_shopify_files(
                product,
                shopify_files_by_code,
                shopify_files_by_slug,
            )

        preview_rows.append(build_photo_sync_preview_row(
            product,
            status=status,
            match_type=match_type,
            source_mode=source_mode,
            photo_root=photo_root,
            asset_set=asset_set,
            existing_files=matched_files,
            reason=reason,
        ))
        if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL and not asset_set:
            if match_type == "ambiguous":
                ambiguous_rows.append((product.sku, product.title, reason))
            else:
                missing_rows.append((product.sku, product.title, reason))
            continue
        if source_mode == PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING and not matched_files:
            missing_rows.append((product.sku, product.title, reason))
            continue
        if dry:
            continue

        if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
            assert asset_set is not None
            fingerprint = asset_set.fingerprint()
            source_paths = [str(path) for path in asset_set.image_paths]
        else:
            fingerprint = hashlib.sha1(
                "|".join(f"{file.id}:{file.filename}:{file.file_status}" for file in matched_files).encode("utf-8")
            ).hexdigest()
            source_paths = [file.filename or file.id for file in matched_files]
        asset_label = asset_set.label if asset_set else "|".join(file.filename or file.id for file in matched_files)
        prior_entry = dict(manifest.get(product.sku, {}))
        prior_state = prior_entry.get("state")
        prior_source_mode = prior_entry.get("source_mode") or PHOTO_SYNC_SOURCE_STAGED_LOCAL
        prior_audit_version = prior_entry.get("fallback_audit_version")
        reset_resume_state = (
            prior_entry.get("asset_fingerprint") != fingerprint
            or prior_entry.get("product_id") != existing["product_id"]
            or prior_source_mode != source_mode
        )
        legacy_audit_upgrade = (
            fallback_audit
            and prior_state == "completed"
            and prior_entry.get("asset_fingerprint") == fingerprint
            and prior_entry.get("product_id") == existing["product_id"]
            and prior_source_mode == source_mode
            and prior_audit_version != PHOTO_SYNC_AUDIT_VERSION
        )
        if (
            prior_state == "completed"
            and prior_entry.get("asset_fingerprint") == fingerprint
            and prior_entry.get("product_id") == existing["product_id"]
            and prior_source_mode == source_mode
            and not legacy_audit_upgrade
        ):
            log(f"  resume: skipping already-synced photo set for {product.sku}")
            continue

        effective_prior_state: str | None = None
        try:
            if reset_resume_state:
                old_media_ids = existing["media_ids"]
                file_ids: list[str] = []
                file_labels: dict[str, str] = {}
                effective_prior_state = None
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="preparing",
                    product_id=existing["product_id"],
                    source_mode=source_mode,
                    asset_label=asset_label,
                    asset_fingerprint=fingerprint,
                    old_media_ids=old_media_ids,
                    detached_old_media=False,
                    new_file_ids=file_ids,
                    error="",
                    source_paths=source_paths,
                )
            elif legacy_audit_upgrade:
                old_media_ids = prior_entry.get("old_media_ids", existing["media_ids"])
                file_ids = prior_entry.get("new_file_ids") or []
                file_labels = prior_entry.get("file_labels") or {}
                effective_prior_state = PHOTO_SYNC_STATE_MEDIA_APPLIED
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state=PHOTO_SYNC_STATE_MEDIA_APPLIED,
                    product_id=existing["product_id"],
                    source_mode=source_mode,
                    asset_label=asset_label,
                    asset_fingerprint=fingerprint,
                    old_media_ids=old_media_ids,
                    detached_old_media=prior_entry.get("detached_old_media", True),
                    new_file_ids=file_ids,
                    error="",
                    source_paths=source_paths,
                )
            elif fallback_audit and prior_state in {PHOTO_SYNC_STATE_MEDIA_APPLIED, PHOTO_SYNC_STATE_AUDIT_PENDING}:
                old_media_ids = prior_entry.get("old_media_ids", existing["media_ids"])
                file_ids = prior_entry.get("new_file_ids") or []
                file_labels = prior_entry.get("file_labels") or {}
                effective_prior_state = prior_state
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state=prior_state,
                    product_id=existing["product_id"],
                    source_mode=source_mode,
                    asset_label=asset_label,
                    asset_fingerprint=fingerprint,
                    old_media_ids=old_media_ids,
                    detached_old_media=prior_entry.get("detached_old_media", True),
                    new_file_ids=file_ids,
                    error=prior_entry.get("error", ""),
                    source_paths=source_paths,
                    fallback_audit_version=PHOTO_SYNC_AUDIT_VERSION,
                )
            else:
                old_media_ids = prior_entry.get("old_media_ids", existing["media_ids"])
                file_ids = prior_entry.get("new_file_ids") or []
                file_labels = prior_entry.get("file_labels") or {}
                effective_prior_state = prior_state
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="preparing",
                    product_id=existing["product_id"],
                    source_mode=source_mode,
                    asset_label=asset_label,
                    asset_fingerprint=fingerprint,
                    old_media_ids=old_media_ids,
                    detached_old_media=prior_entry.get("detached_old_media", False),
                    new_file_ids=file_ids,
                    error="",
                    source_paths=source_paths,
                    fallback_audit_version=prior_audit_version,
                )

            if (
                not file_ids
                or effective_prior_state == "failed"
            ):
                if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
                    assert asset_set is not None
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
                    file_labels = {
                        item["id"]: str(path)
                        for path, item in zip(asset_set.image_paths, created_files)
                    }
                else:
                    file_ids = [file.id for file in matched_files]
                    file_labels = {file.id: (file.filename or file.id) for file in matched_files}
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="files_created" if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL else "files_resolved",
                    new_file_ids=file_ids,
                    old_media_ids=[media_id for media_id in old_media_ids if media_id not in set(file_ids)],
                    file_labels=file_labels,
                )
                old_media_ids = entry.get("old_media_ids") or []
            if source_mode == PHOTO_SYNC_SOURCE_STAGED_LOCAL:
                if effective_prior_state not in {
                    "files_ready",
                    "associated",
                    "reordered",
                    PHOTO_SYNC_STATE_MEDIA_APPLIED,
                    PHOTO_SYNC_STATE_AUDIT_PENDING,
                    "completed",
                }:
                    client.wait_for_files_ready(file_ids, file_labels=file_labels)
                    entry = update_and_save_photo_manifest_entry(
                        manifest,
                        manifest_path,
                        sku=product.sku,
                        state="files_ready",
                    )
                    effective_prior_state = "files_ready"
            else:
                effective_prior_state = effective_prior_state or "files_ready"

            if effective_prior_state not in {
                "associated",
                "reordered",
                PHOTO_SYNC_STATE_MEDIA_APPLIED,
                PHOTO_SYNC_STATE_AUDIT_PENDING,
                "completed",
            }:
                client.attach_files_to_product(file_ids, existing["product_id"])
                entry = update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="associated",
                )
                effective_prior_state = "associated"

            if effective_prior_state not in {
                "reordered",
                PHOTO_SYNC_STATE_MEDIA_APPLIED,
                PHOTO_SYNC_STATE_AUDIT_PENDING,
                "completed",
            }:
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

            if fallback_audit:
                if effective_prior_state not in {PHOTO_SYNC_STATE_MEDIA_APPLIED, PHOTO_SYNC_STATE_AUDIT_PENDING}:
                    entry = update_and_save_photo_manifest_entry(
                        manifest,
                        manifest_path,
                        sku=product.sku,
                        state=PHOTO_SYNC_STATE_MEDIA_APPLIED,
                        fallback_audit_version=PHOTO_SYNC_AUDIT_VERSION,
                        error="",
                    )
                    effective_prior_state = PHOTO_SYNC_STATE_MEDIA_APPLIED
                if effective_prior_state == PHOTO_SYNC_STATE_MEDIA_APPLIED:
                    entry = update_and_save_photo_manifest_entry(
                        manifest,
                        manifest_path,
                        sku=product.sku,
                        state=PHOTO_SYNC_STATE_AUDIT_PENDING,
                        fallback_audit_version=PHOTO_SYNC_AUDIT_VERSION,
                        error="",
                    )
                    effective_prior_state = PHOTO_SYNC_STATE_AUDIT_PENDING
                if effective_prior_state == PHOTO_SYNC_STATE_AUDIT_PENDING:
                    client.set_product_fallback_image_used(existing["product_id"])
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="completed",
                    fallback_audit_version=PHOTO_SYNC_AUDIT_VERSION,
                    error="",
                )
            else:
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="completed",
                )
        except Exception as e:
            detail = str(e)
            if fallback_audit and effective_prior_state == PHOTO_SYNC_STATE_AUDIT_PENDING:
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state=PHOTO_SYNC_STATE_AUDIT_PENDING,
                    fallback_audit_version=PHOTO_SYNC_AUDIT_VERSION,
                    error=detail,
                )
            else:
                update_and_save_photo_manifest_entry(
                    manifest,
                    manifest_path,
                    sku=product.sku,
                    state="failed",
                    error=detail,
                )
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
        f"PHOTO SYNC summary: candidates={len(scoped_products)} "
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
                        help="Delete all Shopify collections.")
    parser.add_argument("--generate-collections", dest="do_generate_collections", action="store_true",
                        help="Delete all collections, retag products into the managed taxonomy, recreate populated smart collections, and set collection images.")
    parser.add_argument("--update-collection-images", dest="do_update_collection_images", action="store_true",
                        help="Set each managed collection's image to the first alphabetical product's image.")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Run import phase.")
    parser.add_argument("--update", dest="do_update", action="store_true",
                        help="Update existing products in place (matched by SKU).")
    parser.add_argument("--publish-online-store-backfill", dest="do_publish_online_store_backfill", action="store_true",
                        help="Publish existing Shopify products that are not on the current publication.")
    parser.add_argument("--reconcile-online-store-image-visibility", dest="do_reconcile_online_store_image_visibility", action="store_true",
                        help="Publish products with any Shopify media to Online Store and unpublish products with none.")
    parser.add_argument("--photo-sync", dest="do_photo_sync", action="store_true",
                        help="Replace GW product media from the repo-local cache or an explicit local photo root.")
    parser.add_argument("--photo-sync-existing-files", dest="do_photo_sync_existing_files", action="store_true",
                        help="Attach matching existing Shopify Files to GW products without uploading new media.")
    parser.add_argument("--photo-sync-existing-files-all", dest="do_photo_sync_existing_files_all", action="store_true",
                        help="Attach matching existing Shopify Files to all catalog products without uploading new media.")
    parser.add_argument("--photo-source-web-all", dest="do_photo_source_web_all", action="store_true",
                        help="Search public web sources for zero-media catalog products and stage high-confidence winners locally.")
    parser.add_argument("--photo-sync-staged-local-all", dest="do_photo_sync_staged_local_all", action="store_true",
                        help="Apply staged local fallback images to all catalog products and write the fallback audit metafield.")
    parser.add_argument("--photo-root", type=Path,
                        help="Root folder containing staged GW image folders/files for --photo-sync.")
    parser.add_argument("--all", action="store_true", help="Run delete then import.")
    parser.add_argument("--start-at", type=int, default=0,
                        help="Resume import from this product index (after a partial run).")
    args = parser.parse_args()

    if not (args.dry_run or args.preflight or args.delete or args.do_delete_collections
            or args.do_generate_collections or args.do_update_collection_images
            or args.do_gw_refresh_cache or args.do_import or args.do_update or args.do_publish_online_store_backfill
            or args.do_reconcile_online_store_image_visibility
            or args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
            or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all or args.all):
        parser.print_help()
        return 1

    if args.do_gw_refresh_cache:
        invalid_refresh_combo = (
            args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
            or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all or args.do_update or args.do_import or args.delete
            or args.do_delete_collections or args.do_generate_collections or args.do_update_collection_images
            or args.do_publish_online_store_backfill or args.do_reconcile_online_store_image_visibility
            or args.all or args.preflight
            or args.start_at != 0 or args.photo_root is not None
        )
        if invalid_refresh_combo:
            raise RuntimeError(
                "--gw-refresh-cache must run separately and cannot be combined with "
                "--photo-sync, --photo-sync-existing-files, --photo-sync-existing-files-all, --photo-source-web-all, --photo-sync-staged-local-all, --update, --import, --delete, --delete-collections, "
                "--generate-collections, --update-collection-images, --publish-online-store-backfill, "
                "--reconcile-online-store-image-visibility, --all, --preflight, --start-at, or --photo-root."
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
    if args.do_photo_sync and args.do_photo_sync_existing_files:
        raise RuntimeError("--photo-sync and --photo-sync-existing-files must run separately.")
    if args.do_photo_sync and args.do_photo_sync_existing_files_all:
        raise RuntimeError("--photo-sync and --photo-sync-existing-files-all must run separately.")
    if args.do_photo_sync_existing_files and (args.delete or args.do_import or args.do_update or args.all):
        raise RuntimeError("--photo-sync-existing-files must run separately from delete/import/update/all phases.")
    if args.do_photo_sync_existing_files and args.preflight:
        raise RuntimeError("--photo-sync-existing-files cannot be combined with --preflight.")
    if args.do_photo_sync_existing_files and args.start_at != 0:
        raise RuntimeError("--start-at is only valid with --import/--all, not --photo-sync-existing-files.")
    if args.do_photo_sync_existing_files and args.photo_root is not None:
        raise RuntimeError("--photo-sync-existing-files does not use --photo-root.")
    if args.do_photo_sync_existing_files and args.do_photo_sync_existing_files_all:
        raise RuntimeError("--photo-sync-existing-files and --photo-sync-existing-files-all must run separately.")
    if args.do_photo_sync_existing_files_all and (args.delete or args.do_import or args.do_update or args.all):
        raise RuntimeError("--photo-sync-existing-files-all must run separately from delete/import/update/all phases.")
    if args.do_photo_sync_existing_files_all and args.preflight:
        raise RuntimeError("--photo-sync-existing-files-all cannot be combined with --preflight.")
    if args.do_photo_sync_existing_files_all and args.start_at != 0:
        raise RuntimeError("--start-at is only valid with --import/--all, not --photo-sync-existing-files-all.")
    if args.do_photo_sync_existing_files_all and args.photo_root is not None:
        raise RuntimeError("--photo-sync-existing-files-all does not use --photo-root.")
    if args.do_photo_sync_staged_local_all and (args.delete or args.do_import or args.do_update or args.all):
        raise RuntimeError("--photo-sync-staged-local-all must run separately from delete/import/update/all phases.")
    if args.do_photo_sync_staged_local_all and args.preflight:
        raise RuntimeError("--photo-sync-staged-local-all cannot be combined with --preflight.")
    if args.do_photo_sync_staged_local_all and args.start_at != 0:
        raise RuntimeError("--start-at is only valid with --import/--all, not --photo-sync-staged-local-all.")
    if args.do_photo_sync_staged_local_all and args.photo_root is None:
        raise RuntimeError("--photo-sync-staged-local-all requires --photo-root.")
    if args.do_photo_sync_staged_local_all and args.do_photo_sync:
        raise RuntimeError("--photo-sync and --photo-sync-staged-local-all must run separately.")
    if args.do_photo_sync_staged_local_all and args.do_photo_sync_existing_files:
        raise RuntimeError("--photo-sync-existing-files and --photo-sync-staged-local-all must run separately.")
    if args.do_photo_sync_staged_local_all and args.do_photo_sync_existing_files_all:
        raise RuntimeError("--photo-sync-existing-files-all and --photo-sync-staged-local-all must run separately.")
    if args.do_photo_source_web_all and (args.delete or args.do_import or args.do_update or args.all):
        raise RuntimeError("--photo-source-web-all must run separately from delete/import/update/all phases.")
    if args.do_photo_source_web_all and args.preflight:
        raise RuntimeError("--photo-source-web-all cannot be combined with --preflight.")
    if args.do_photo_source_web_all and args.start_at != 0:
        raise RuntimeError("--start-at is only valid with --import/--all, not --photo-source-web-all.")
    if args.do_photo_source_web_all and args.photo_root is not None:
        raise RuntimeError("--photo-source-web-all does not use --photo-root.")
    if args.do_photo_source_web_all and args.do_photo_sync:
        raise RuntimeError("--photo-source-web-all and --photo-sync must run separately.")
    if args.do_photo_source_web_all and args.do_photo_sync_existing_files:
        raise RuntimeError("--photo-source-web-all and --photo-sync-existing-files must run separately.")
    if args.do_photo_source_web_all and args.do_photo_sync_existing_files_all:
        raise RuntimeError("--photo-source-web-all and --photo-sync-existing-files-all must run separately.")
    if args.do_photo_source_web_all and args.do_photo_sync_staged_local_all:
        raise RuntimeError("--photo-source-web-all and --photo-sync-staged-local-all must run separately.")
    if args.do_delete_collections and (
        args.preflight or args.delete or args.do_import or args.do_update
        or args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
        or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all or args.do_generate_collections
        or args.all or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--delete-collections must run separately from preflight/delete/import/update/"
            "photo-sync/photo-sync-existing-files/photo-sync-existing-files-all/all and cannot be combined with --start-at or --photo-root."
        )
    if args.do_generate_collections and (
        args.preflight or args.delete or args.do_delete_collections or args.do_import
        or args.do_update or args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
        or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all or args.all
        or args.do_update_collection_images
        or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--generate-collections must run separately from preflight/delete/delete-collections/"
            "import/update/photo-sync/photo-sync-existing-files/photo-sync-existing-files-all/update-collection-images/all "
            "and cannot be combined with --start-at or --photo-root."
        )
    if args.do_generate_collections and args.dry_run:
        raise RuntimeError("--generate-collections always applies live for the managed collection rebuild and cannot be combined with --dry-run.")
    if args.do_update_collection_images and (
        args.preflight or args.delete or args.do_delete_collections or args.do_generate_collections
        or args.do_import or args.do_update or args.do_photo_sync or args.do_photo_sync_existing_files
        or args.do_photo_sync_existing_files_all or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all or args.all
        or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--update-collection-images must run separately from preflight/delete/delete-collections/"
            "generate-collections/import/update/photo-sync/photo-sync-existing-files/photo-sync-existing-files-all/all "
            "and cannot be combined with --start-at or --photo-root."
        )
    if args.do_publish_online_store_backfill and (
        args.preflight or args.delete or args.do_delete_collections or args.do_generate_collections
        or args.do_update_collection_images or args.do_import or args.do_update
        or args.do_reconcile_online_store_image_visibility
        or args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
        or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all
        or args.all or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--publish-online-store-backfill must run separately from preflight/delete/delete-collections/"
            "generate-collections/update-collection-images/import/update/reconcile-online-store-image-visibility/photo-sync/photo-sync-existing-files/"
            "photo-sync-existing-files-all/all and cannot be combined with --start-at or --photo-root."
        )
    if args.do_reconcile_online_store_image_visibility and (
        args.preflight or args.delete or args.do_delete_collections or args.do_generate_collections
        or args.do_update_collection_images or args.do_import or args.do_update
        or args.do_publish_online_store_backfill
        or args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all
        or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all
        or args.all or args.start_at != 0 or args.photo_root is not None
    ):
        raise RuntimeError(
            "--reconcile-online-store-image-visibility must run separately from preflight/delete/delete-collections/"
            "generate-collections/update-collection-images/import/update/publish-online-store-backfill/photo-sync/"
            "photo-sync-existing-files/photo-sync-existing-files-all/all and cannot be combined with --start-at or --photo-root."
        )

    # Plain --dry-run (without --update / --photo-sync) means: read sheets,
    # write preview.csv, don't contact Shopify. --update/--photo-sync dry-runs
    # are handled below since they need Shopify reads.
    if (
        args.dry_run
        and not args.do_update
        and not args.do_photo_sync
        and not args.do_photo_sync_existing_files
        and not args.do_photo_sync_existing_files_all
        and not args.do_photo_source_web_all
        and not args.do_photo_sync_staged_local_all
        and not args.do_generate_collections
        and not args.do_delete_collections
        and not args.do_update_collection_images
        and not args.do_publish_online_store_backfill
        and not args.do_reconcile_online_store_image_visibility
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
    if args.do_update_collection_images:
        phase_update_collection_images(client, dry=args.dry_run)
        if args.dry_run:
            log(
                "Collection image update dry-run complete. Review "
                "collection_image_preview.csv, then re-run with "
                "--update-collection-images (no --dry-run) to apply."
            )
        return 0
    if args.do_publish_online_store_backfill:
        phase_publish_online_store_backfill(client, dry=args.dry_run)
        if args.dry_run:
            log(
                "Online Store backfill dry-run complete. Review "
                "online_store_backfill_preview.csv, then re-run with "
                "--publish-online-store-backfill (no --dry-run) to apply."
            )
        return 0
    if args.do_reconcile_online_store_image_visibility:
        phase_reconcile_online_store_image_visibility(client, dry=args.dry_run)
        if args.dry_run:
            log(
                "Online Store image-visibility dry-run complete. Review "
                "online_store_image_visibility_preview.csv, then re-run with "
                "--reconcile-online-store-image-visibility (no --dry-run) to apply."
            )
        return 0

    if args.delete and not args.all and not args.do_import:
        phase_delete(client, dry=False)
        return 0

    if args.do_photo_sync or args.do_photo_sync_existing_files:
        products = build_gw_product_list(strict=True)
    elif args.do_photo_sync_existing_files_all or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all:
        products = build_product_list(strict=True)
    else:
        products = prepare_products_for_import()

    if args.do_photo_sync or args.do_photo_sync_existing_files or args.do_photo_sync_existing_files_all or args.do_photo_source_web_all or args.do_photo_sync_staged_local_all:
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
        phase_photo_sync(
            client,
            products,
            resolve_photo_sync_root(args.photo_root),
            dry=args.dry_run,
            source_mode=PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=PHOTO_SYNC_SCOPE_GW,
        )
        if args.dry_run:
            log(
                "Photo-sync dry-run complete. Review photo_sync_preview.csv, then re-run "
                "with --photo-sync (no --dry-run) to apply."
            )
    if args.do_photo_sync_existing_files:
        phase_photo_sync(
            client,
            products,
            None,
            dry=args.dry_run,
            source_mode=PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            product_scope=PHOTO_SYNC_SCOPE_GW,
        )
        if args.dry_run:
            log(
                "Photo-sync existing-files dry-run complete. Review photo_sync_preview.csv, then re-run "
                "with --photo-sync-existing-files (no --dry-run) to apply."
            )
    if args.do_photo_sync_existing_files_all:
        phase_photo_sync(
            client,
            products,
            None,
            dry=args.dry_run,
            source_mode=PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            product_scope=PHOTO_SYNC_SCOPE_ALL,
        )
        if args.dry_run:
            log(
                "Photo-sync existing-files-all dry-run complete. Review photo_sync_preview.csv, then re-run "
                "with --photo-sync-existing-files-all (no --dry-run) to apply."
            )
    if args.do_photo_source_web_all:
        phase_photo_source_web_all(
            client,
            products,
            dry=args.dry_run,
        )
        if args.dry_run:
            log(
                "Photo-source web-all dry-run complete. Review photo_source_preview.csv, then re-run "
                "with --photo-source-web-all (no --dry-run) to stage winners locally."
            )
    if args.do_photo_sync_staged_local_all:
        phase_photo_sync(
            client,
            products,
            require_explicit_photo_root(args.photo_root, "--photo-sync-staged-local-all"),
            dry=args.dry_run,
            source_mode=PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=PHOTO_SYNC_SCOPE_ALL,
            fallback_audit=True,
        )
        if args.dry_run:
            log(
                "Photo-sync staged-local-all dry-run complete. Review photo_sync_preview.csv, then re-run "
                "with --photo-sync-staged-local-all (no --dry-run) to apply."
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
