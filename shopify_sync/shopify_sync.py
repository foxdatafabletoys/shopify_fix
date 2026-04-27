#!/usr/bin/env python3
"""
Shopify bulk delete + import script for Telemachus Foxfable.

Reads two spreadsheets:
  - "Games Workshop Store List.xlsx"  (master GW catalog, 1,974 items)
  - "everything else.xlsx"             (general inventory; only non-GW rows used)

What it does:
  --delete   Deletes existing Shopify products that have NO SKU on any variant.
             (Products that already have a Product Code/SKU are kept.)
  --import   Creates new Shopify products from the spreadsheets.
  --all      Runs --delete then --import.
  --dry-run  Reads sheets, builds the product list, writes preview.csv,
             and makes ZERO API calls. Always run this first.

Pricing logic:
  - Games Workshop:    price = UKR * 0.79   compare_at = UKR    (21% off)
  - Funko (FUNKO mfg): price = SRP * 0.89   compare_at = SRP    (11% off)
  - Everything else:   price = SRP                              (no discount)

Cost-per-item is set to GBD (GW) or Unit Cost (others) when present.
Inventory tracking is enabled; quantities come from Store Quantity (GW) or Available (others).

Required env vars (or .env file in same folder):
  SHOPIFY_STORE      e.g. "telemachus-foxfable"  (the part before .myshopify.com)
  SHOPIFY_TOKEN      Admin API access token starting with "shpat_..."
  SHOPIFY_LOCATION   (optional) numeric location ID; auto-detected if not set

Usage:
  python shopify_sync.py --dry-run
  python shopify_sync.py --delete
  python shopify_sync.py --import
  python shopify_sync.py --all
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

API_VERSION = "2025-01"
HERE = Path(__file__).resolve().parent
SHEET_DIR = HERE.parent  # the "cleaning up inventory" folder
GW_FILE = SHEET_DIR / "Games Workshop Store List.xlsx"
INV_FILE = SHEET_DIR / "everything else.xlsx"
PREVIEW_CSV = HERE / "preview.csv"
LOG_FILE = HERE / "sync.log"


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


def build_product_list() -> list[Product]:
    if not GW_FILE.exists():
        log(f"WARNING: GW file not found: {GW_FILE}")
        gw: list[Product] = []
    else:
        gw = parse_gw(GW_FILE)
        log(f"Parsed {len(gw)} products from {GW_FILE.name}")
    if not INV_FILE.exists():
        log(f"WARNING: Inventory file not found: {INV_FILE}")
        inv: list[Product] = []
    else:
        inv = parse_inventory(INV_FILE, skip_gw=True)
        log(f"Parsed {len(inv)} non-GW products from {INV_FILE.name}")

    # final dedupe across both sheets by SKU
    seen: set[str] = set()
    merged: list[Product] = []
    for p in gw + inv:
        if p.sku in seen:
            continue
        seen.add(p.sku)
        merged.append(p)
    log(f"Total unique products to import: {len(merged)}")
    return merged


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
            try:
                data = r.json()
            except Exception as e:
                raise RuntimeError(f"Bad JSON from Shopify: {e}\n{r.text[:500]}")
            if "errors" in data:
                # If throttled, sleep based on cost info
                throttled = any(
                    err.get("extensions", {}).get("code") == "THROTTLED"
                    for err in data.get("errors", [])
                )
                if throttled:
                    time.sleep(1.5)
                    continue
                raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'])[:1000]}")
            # Respect cost throttle
            cost = data.get("extensions", {}).get("cost", {})
            ts = cost.get("throttleStatus", {})
            avail = ts.get("currentlyAvailable", 1000)
            req = ts.get("restoreRate", 50)
            if avail < 200:
                time.sleep(max(0.5, (250 - avail) / max(req, 1)))
            return data["data"]
        raise RuntimeError("Exceeded max retries on GraphQL request")

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------
    def get_primary_location_id(self) -> str:
        data = self.gql("""
            query {
              shop { name }
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
    # Product create (with variant + inventory)
    # ------------------------------------------------------------------
    def create_product(self, p: Product, location_id: str) -> str:
        # Step 1: productCreate with title/vendor/etc and one variant
        create_q = """
            mutation($input: ProductInput!, $media: [CreateMediaInput!]) {
              productCreate(input: $input, media: $media) {
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
        variant: dict[str, Any] = {
            "price": f"{p.price:.2f}",
            "sku": p.sku,
            "inventoryManagement": "SHOPIFY",
            "inventoryPolicy": "DENY",
            "requiresShipping": True,
            "taxable": True,
        }
        if p.compare_at_price is not None:
            variant["compareAtPrice"] = f"{p.compare_at_price:.2f}"
        if p.barcode:
            variant["barcode"] = p.barcode
        if p.weight_grams is not None and p.weight_grams > 0:
            variant["weight"] = round(p.weight_grams, 2)
            variant["weightUnit"] = "GRAMS"

        product_input: dict[str, Any] = {
            "title": p.title,
            "vendor": p.vendor or "Foxfable",
            "productType": p.product_type or "",
            "tags": p.tags,
            "descriptionHtml": p.description_html,
            "status": "ACTIVE",
            "variants": [variant],
        }

        data = self.gql(create_q, {"input": product_input, "media": []})
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

        # Step 2: cost-per-item on the inventory item (separate mutation)
        if p.cost is not None and p.cost > 0:
            cost_q = """
                mutation($id: ID!, $input: InventoryItemInput!) {
                  inventoryItemUpdate(id: $id, input: $input) {
                    inventoryItem { id unitCost { amount } }
                    userErrors { field message }
                  }
                }
            """
            cost_data = self.gql(cost_q, {
                "id": inventory_item_id,
                "input": {"cost": f"{p.cost:.2f}", "tracked": True},
            })
            cost_errs = cost_data["inventoryItemUpdate"]["userErrors"]
            if cost_errs:
                log(f"  warn: failed to set cost on {p.sku}: {cost_errs}")

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


# ---------------------------------------------------------------------------
# Phases: delete / import
# ---------------------------------------------------------------------------

def phase_delete(client: Shopify, products_to_keep_skus: set[str], dry: bool) -> None:
    log("=== DELETE phase: removing existing products with no SKU ===")
    deleted = 0
    kept = 0
    skipped_with_sku = 0
    for prod in client.iter_all_products():
        skus_on_variants = [
            (v["node"].get("sku") or "").strip()
            for v in prod["variants"]["edges"]
        ]
        any_sku = any(s for s in skus_on_variants)
        if any_sku:
            skipped_with_sku += 1
            continue
        kept += 0
        log(f"  delete: {prod['title']!r}  ({prod['id']})")
        if not dry:
            try:
                client.delete_product(prod["id"])
                deleted += 1
            except Exception as e:
                log(f"  ERROR deleting {prod['id']}: {e}")
    log(f"DELETE summary: deleted={deleted}, kept(with SKU)={skipped_with_sku}, dry_run={dry}")


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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Shopify bulk delete + import")
    parser.add_argument("--dry-run", action="store_true", help="Don't call Shopify; just preview.")
    parser.add_argument("--delete", action="store_true", help="Run delete phase.")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Run import phase.")
    parser.add_argument("--all", action="store_true", help="Run delete then import.")
    parser.add_argument("--start-at", type=int, default=0,
                        help="Resume import from this product index (after a partial run).")
    args = parser.parse_args()

    if not (args.dry_run or args.delete or args.do_import or args.all):
        parser.print_help()
        return 1

    products = build_product_list()
    write_preview(products)

    if args.dry_run:
        log("Dry run complete. Review preview.csv, then re-run with --delete and/or --import.")
        return 0

    env = load_env()
    store = env.get("SHOPIFY_STORE")
    token = env.get("SHOPIFY_TOKEN")
    if not store or not token:
        log("ERROR: SHOPIFY_STORE and SHOPIFY_TOKEN must be set in env or .env file.")
        log("See SETUP.md for instructions.")
        return 2

    client = Shopify(store, token)
    location_id = env.get("SHOPIFY_LOCATION") or client.get_primary_location_id()
    log(f"Using location: {location_id}")

    sheet_skus = {p.sku for p in products}

    if args.delete or args.all:
        phase_delete(client, sheet_skus, dry=False)
    if args.do_import or args.all:
        phase_import(client, products, location_id, dry=False, start_at=args.start_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
