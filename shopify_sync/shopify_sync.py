#!/usr/bin/env python3
"""
Shopify bulk delete + import script for Telemachus Foxfable.

Reads two spreadsheets:
  - "Games Workshop Store List.xlsx"  (master GW catalog, 1,974 items)
  - "everything else.xlsx"             (general inventory; only non-GW rows used)

What it does:
  --delete     Deletes existing Shopify products that have NO SKU on any variant.
               (Products that already have a Product Code/SKU are kept.)
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
  python shopify_sync.py --import
  python shopify_sync.py --update --dry-run
  python shopify_sync.py --update
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
SHEET_DIR = HERE
GW_FILE = SHEET_DIR / "Games Workshop Store List.xlsx"
INV_FILE = SHEET_DIR / "everything else.xlsx"
PREVIEW_CSV = HERE / "preview.csv"
UPDATE_PREVIEW_CSV = HERE / "update_preview.csv"
LOG_FILE = HERE / "sync.log"

# Tolerance for treating two money values as equal (Shopify rounds to 2 dp).
MONEY_EPSILON = 0.005


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
        product_input: dict[str, Any] = {
            "title": p.title,
            "vendor": p.vendor or "Foxfable",
            "productType": p.product_type or "",
            "tags": p.tags,
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Shopify bulk delete + import")
    parser.add_argument("--dry-run", action="store_true", help="Don't call Shopify; just preview.")
    parser.add_argument("--preflight", action="store_true",
                        help="Validate auth and location readiness without delete/import side effects.")
    parser.add_argument("--delete", action="store_true", help="Run delete phase.")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Run import phase.")
    parser.add_argument("--update", dest="do_update", action="store_true",
                        help="Update existing products in place (matched by SKU).")
    parser.add_argument("--all", action="store_true", help="Run delete then import.")
    parser.add_argument("--start-at", type=int, default=0,
                        help="Resume import from this product index (after a partial run).")
    args = parser.parse_args()

    if not (args.dry_run or args.preflight or args.delete
            or args.do_import or args.do_update or args.all):
        parser.print_help()
        return 1

    # Plain --dry-run (without --update) means: read sheets, write preview.csv,
    # don't contact Shopify. --update --dry-run is handled below since it needs
    # to read from Shopify.
    if args.dry_run and not args.do_update:
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

    if args.delete and not args.all and not args.do_import:
        phase_delete(client, dry=False)
        return 0

    products = prepare_products_for_import()
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
