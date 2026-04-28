import tempfile
import unittest
from pathlib import Path
from unittest import mock

import shopify_sync


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class ShopifyGraphQLTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_gql_raises_string_graphql_errors(self):
        self.client.session.post = mock.Mock(
            return_value=FakeResponse(payload={"errors": ["plain string error"]})
        )

        with self.assertRaisesRegex(RuntimeError, "plain string error"):
            self.client.gql("query { shop { name } }")

    def test_gql_raises_dict_graphql_errors(self):
        self.client.session.post = mock.Mock(
            return_value=FakeResponse(
                payload={"errors": [{"message": "bad field", "extensions": {"code": "BAD_REQUEST"}}]}
            )
        )

        with self.assertRaisesRegex(RuntimeError, "bad field"):
            self.client.gql("query { shop { name } }")

    def test_gql_raises_mixed_graphql_errors(self):
        self.client.session.post = mock.Mock(
            return_value=FakeResponse(
                payload={"errors": ["plain", {"message": "structured", "extensions": {"code": "BAD_REQUEST"}}]}
            )
        )

        with self.assertRaisesRegex(RuntimeError, "plain .*structured"):
            self.client.gql("query { shop { name } }")

    def test_gql_retries_when_graphql_error_is_throttled(self):
        self.client.session.post = mock.Mock(
            side_effect=[
                FakeResponse(
                    payload={"errors": [{"message": "slow down", "extensions": {"code": "THROTTLED"}}]}
                ),
                FakeResponse(payload={"data": {"shop": {"name": "Foxfable"}}}),
            ]
        )

        with mock.patch("shopify_sync.time.sleep") as sleep:
            result = self.client.gql("query { shop { name } }")

        self.assertEqual(result, {"shop": {"name": "Foxfable"}})
        sleep.assert_called_once_with(1.5)
        self.assertEqual(self.client.session.post.call_count, 2)

    def test_gql_raises_http_auth_errors_with_status(self):
        self.client.session.post = mock.Mock(
            return_value=FakeResponse(
                status_code=401,
                payload={"errors": "[API] Invalid API key or access token"},
            )
        )

        with self.assertRaisesRegex(RuntimeError, "Shopify HTTP 401"):
            self.client.gql("query { shop { name } }")


class LocationResolutionTests(unittest.TestCase):
    def test_normalize_location_id_accepts_numeric_value(self):
        self.assertEqual(
            shopify_sync.normalize_location_id("12345"),
            "gid://shopify/Location/12345",
        )

    def test_normalize_location_id_preserves_gid_value(self):
        gid = "gid://shopify/Location/98765"
        self.assertEqual(shopify_sync.normalize_location_id(gid), gid)

    def test_run_preflight_uses_auth_query_and_blank_location_auto_detect(self):
        client = mock.Mock()
        client.get_shop_name.return_value = "Foxfable"
        client.get_primary_location_id.return_value = "gid://shopify/Location/111"

        location_id = shopify_sync.run_preflight(client, {"SHOPIFY_LOCATION": "   "})

        self.assertEqual(location_id, "gid://shopify/Location/111")
        client.get_shop_name.assert_called_once_with()
        client.get_primary_location_id.assert_called_once_with()
        client.validate_location_id.assert_not_called()

    def test_resolve_location_for_import_validates_explicit_location(self):
        client = mock.Mock()
        client.validate_location_id.return_value = "gid://shopify/Location/222"

        location_id = shopify_sync.resolve_location_for_import(client, {"SHOPIFY_LOCATION": "222"})

        self.assertEqual(location_id, "gid://shopify/Location/222")
        client.validate_location_id.assert_called_once_with("222")
        client.get_primary_location_id.assert_not_called()


class ProductCreateTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_create_product_uses_current_product_and_variant_mutations(self):
        product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            barcode="5011921149063",
            vendor="Games Workshop",
            product_type="Warhammer 40,000",
            tags=["Games Workshop", "Warhammer 40,000", "40K - Generic"],
            description_html="SS Code: 39-13",
            price=82.95,
            compare_at_price=105.00,
            cost=65.40,
            weight_grams=730.0,
            quantity=12,
        )
        self.client.gql = mock.Mock(side_effect=[
            {
                "productCreate": {
                    "product": {
                        "id": "gid://shopify/Product/1",
                        "variants": {
                            "edges": [
                                {
                                    "node": {
                                        "id": "gid://shopify/ProductVariant/2",
                                        "inventoryItem": {"id": "gid://shopify/InventoryItem/3"},
                                    }
                                }
                            ]
                        },
                    },
                    "userErrors": [],
                }
            },
            {
                "productVariantsBulkUpdate": {
                    "productVariants": [
                        {
                            "id": "gid://shopify/ProductVariant/2",
                            "inventoryItem": {"id": "gid://shopify/InventoryItem/3"},
                        }
                    ],
                    "userErrors": [],
                }
            },
            {
                "inventorySetOnHandQuantities": {
                    "inventoryAdjustmentGroup": {"id": "gid://shopify/InventoryAdjustmentGroup/4"},
                    "userErrors": [],
                }
            },
        ])

        product_id = self.client.create_product(product, "gid://shopify/Location/9")

        self.assertEqual(product_id, "gid://shopify/Product/1")
        self.assertEqual(self.client.gql.call_count, 3)

        create_query, create_vars = self.client.gql.call_args_list[0].args
        self.assertIn("productCreate(product: $product", create_query)
        self.assertEqual(
            create_vars,
            {
                "product": {
                    "title": "ARMAGEDDON BATTALION: DEATHWATCH",
                    "vendor": "Games Workshop",
                    "productType": "Warhammer 40,000",
                    "tags": ["Games Workshop", "Warhammer 40,000", "40K - Generic"],
                    "descriptionHtml": "SS Code: 39-13",
                    "status": "ACTIVE",
                },
                "media": [],
            },
        )

        update_query, update_vars = self.client.gql.call_args_list[1].args
        self.assertIn("productVariantsBulkUpdate", update_query)
        self.assertEqual(
            update_vars,
            {
                "productId": "gid://shopify/Product/1",
                "variants": [
                    {
                        "id": "gid://shopify/ProductVariant/2",
                        "price": "82.95",
                        "compareAtPrice": "105.00",
                        "barcode": "5011921149063",
                        "inventoryPolicy": "DENY",
                        "taxable": True,
                        "inventoryItem": {
                            "sku": "99120109017",
                            "tracked": True,
                            "requiresShipping": True,
                            "cost": "65.40",
                            "measurement": {
                                "weight": {
                                    "value": 730.0,
                                    "unit": "GRAMS",
                                }
                            },
                        },
                    }
                ],
            },
        )

        inv_query, inv_vars = self.client.gql.call_args_list[2].args
        self.assertIn("inventorySetOnHandQuantities", inv_query)
        self.assertEqual(
            inv_vars,
            {
                "input": {
                    "reason": "correction",
                    "referenceDocumentUri": "logistics://foxfable/initial-load",
                    "setQuantities": [
                        {
                            "inventoryItemId": "gid://shopify/InventoryItem/3",
                            "locationId": "gid://shopify/Location/9",
                            "quantity": 12,
                        }
                    ],
                }
            },
        )

    def test_create_product_omits_optional_variant_fields_when_missing(self):
        product = shopify_sync.Product(
            title="Generic Product",
            sku="SKU-1",
            price=9.50,
        )
        self.client.gql = mock.Mock(side_effect=[
            {
                "productCreate": {
                    "product": {
                        "id": "gid://shopify/Product/10",
                        "variants": {
                            "edges": [
                                {
                                    "node": {
                                        "id": "gid://shopify/ProductVariant/11",
                                        "inventoryItem": {"id": "gid://shopify/InventoryItem/12"},
                                    }
                                }
                            ]
                        },
                    },
                    "userErrors": [],
                }
            },
            {
                "productVariantsBulkUpdate": {
                    "productVariants": [{"id": "gid://shopify/ProductVariant/11"}],
                    "userErrors": [],
                }
            },
            {
                "inventorySetOnHandQuantities": {
                    "inventoryAdjustmentGroup": {"id": "gid://shopify/InventoryAdjustmentGroup/13"},
                    "userErrors": [],
                }
            },
        ])

        self.client.create_product(product, "gid://shopify/Location/9")

        update_vars = self.client.gql.call_args_list[1].args[1]
        variant = update_vars["variants"][0]
        self.assertNotIn("compareAtPrice", variant)
        self.assertNotIn("barcode", variant)
        self.assertEqual(
            variant["inventoryItem"],
            {
                "sku": "SKU-1",
                "tracked": True,
                "requiresShipping": True,
            },
        )


class MainFlowTests(unittest.TestCase):
    def test_preflight_flag_does_not_call_delete_or_import(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--preflight"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.run_preflight", return_value="gid://shopify/Location/1") as run_preflight, \
             mock.patch("shopify_sync.phase_delete") as phase_delete, \
             mock.patch("shopify_sync.phase_import") as phase_import:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        run_preflight.assert_called_once_with(client, {
            "SHOPIFY_STORE": "example-store",
            "SHOPIFY_TOKEN": "shpat_test",
        })
        phase_delete.assert_not_called()
        phase_import.assert_not_called()

    def test_delete_flag_without_all_does_not_call_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--delete"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_delete") as phase_delete, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products, \
             mock.patch("shopify_sync.phase_import") as phase_import:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_delete.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()
        phase_import.assert_not_called()
        client.get_primary_location_id.assert_not_called()
        client.validate_location_id.assert_not_called()

    def test_all_flag_runs_prepare_then_preflight_then_delete_then_import(self):
        events = []
        client = mock.Mock()
        products = [mock.sentinel.product]

        def mark(name, value=None):
            def _marker(*args, **kwargs):
                events.append(name)
                return value
            return _marker

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--all"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.prepare_products_for_import", side_effect=mark("prepare", products)), \
             mock.patch("shopify_sync.run_preflight", side_effect=mark("preflight", "gid://shopify/Location/9")), \
             mock.patch("shopify_sync.phase_delete", side_effect=mark("delete")) as phase_delete, \
             mock.patch("shopify_sync.phase_import", side_effect=mark("import")) as phase_import:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        self.assertEqual(events, ["prepare", "preflight", "delete", "import"])
        phase_delete.assert_called_once_with(client, dry=False)
        phase_import.assert_called_once_with(
            client,
            products,
            "gid://shopify/Location/9",
            dry=False,
            start_at=0,
        )

    def test_all_flag_aborts_before_delete_when_prepare_fails(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--all"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.prepare_products_for_import", side_effect=RuntimeError("local gate failed")), \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.phase_delete") as phase_delete, \
             mock.patch("shopify_sync.phase_import") as phase_import:
            with self.assertRaisesRegex(RuntimeError, "local gate failed"):
                shopify_sync.main()

        run_preflight.assert_not_called()
        phase_delete.assert_not_called()
        phase_import.assert_not_called()

    def test_all_flag_aborts_before_delete_when_preflight_fails(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--all"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.prepare_products_for_import", return_value=products), \
             mock.patch("shopify_sync.run_preflight", side_effect=RuntimeError("auth failed")), \
             mock.patch("shopify_sync.phase_delete") as phase_delete, \
             mock.patch("shopify_sync.phase_import") as phase_import:
            with self.assertRaisesRegex(RuntimeError, "auth failed"):
                shopify_sync.main()

        phase_delete.assert_not_called()
        phase_import.assert_not_called()


class PhaseUpdateTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")
        self.location = "gid://shopify/Location/9"

    def _make_product(self, sku, price, compare, cost, qty, title="t"):
        return shopify_sync.Product(
            title=title,
            sku=sku,
            price=price,
            compare_at_price=compare,
            cost=cost,
            quantity=qty,
            source="GW",
        )

    def _existing_record(self, sku, price, compare, cost, on_hand,
                         variant_id="gid://v/1", product_id="gid://p/1",
                         inventory_item_id="gid://i/1"):
        return {
            "product_id": product_id,
            "title": "t",
            "variant_id": variant_id,
            "sku": sku,
            "price": price,
            "compare_at_price": compare,
            "cost": cost,
            "inventory_item_id": inventory_item_id,
            "on_hand": on_hand,
        }

    def test_dry_run_writes_diff_and_makes_no_writes(self):
        sheet = [
            self._make_product("A", price=8.00, compare=10.00, cost=5.00, qty=4),
            # unchanged row
            self._make_product("B", price=8.00, compare=10.00, cost=5.00, qty=4),
        ]
        existing = [
            self._existing_record("A", price=7.50, compare=10.00, cost=5.00, on_hand=2),
            self._existing_record("B", price=8.00, compare=10.00, cost=5.00, on_hand=4),
        ]
        with mock.patch.object(self.client, "iter_existing_for_update",
                               return_value=iter(existing)), \
             mock.patch.object(self.client, "update_variant_fields") as upd, \
             mock.patch.object(self.client, "set_on_hand") as set_qty, \
             mock.patch("shopify_sync.UPDATE_PREVIEW_CSV",
                        new=Path(tempfile.gettempdir()) / "_tmp_update_preview.csv"):
            shopify_sync.phase_update(self.client, sheet, self.location, dry=True)
            upd.assert_not_called()
            set_qty.assert_not_called()

    def test_live_run_pushes_only_changed_fields(self):
        sheet = [
            # price + qty change
            self._make_product("A", price=9.99, compare=12.00, cost=5.00, qty=7),
            # cost-only change
            self._make_product("B", price=8.00, compare=10.00, cost=4.50, qty=2),
            # nothing changed
            self._make_product("C", price=8.00, compare=10.00, cost=5.00, qty=2),
            # missing in shopify -> skipped
            self._make_product("Z", price=1.00, compare=2.00, cost=0.5, qty=1),
        ]
        existing = [
            self._existing_record("A", price=8.00, compare=12.00, cost=5.00, on_hand=2,
                                  variant_id="v-A", inventory_item_id="i-A"),
            self._existing_record("B", price=8.00, compare=10.00, cost=5.00, on_hand=2,
                                  variant_id="v-B", inventory_item_id="i-B"),
            self._existing_record("C", price=8.00, compare=10.00, cost=5.00, on_hand=2,
                                  variant_id="v-C", inventory_item_id="i-C"),
        ]
        with mock.patch.object(self.client, "iter_existing_for_update",
                               return_value=iter(existing)), \
             mock.patch.object(self.client, "update_variant_fields") as upd, \
             mock.patch.object(self.client, "set_on_hand") as set_qty, \
             mock.patch("shopify_sync.UPDATE_PREVIEW_CSV",
                        new=Path(tempfile.gettempdir()) / "_tmp_update_preview.csv"):
            shopify_sync.phase_update(self.client, sheet, self.location, dry=False)

        # A: variant update (price) AND inventory set
        # B: variant update (cost via inventoryItem)
        # C: nothing
        self.assertEqual(upd.call_count, 2)
        self.assertEqual(set_qty.call_count, 1)
        # A's inventory was set
        set_qty.assert_called_once_with("i-A", self.location, 7)


if __name__ == "__main__":
    unittest.main()
