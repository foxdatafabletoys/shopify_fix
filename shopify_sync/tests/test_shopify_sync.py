import csv
from contextlib import contextmanager
import io
import json
import os
import requests
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gw_cache_refresh
import shopify_sync


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text="", content=b"", url=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text
        self.content = content
        self.url = url or ""

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeWaylandScraper:
    def __init__(self, pages=None, error=None):
        self.pages = pages or {}
        self.error = error
        self.fetches = []
        self.closed = False

    def fetch_html(self, url):
        self.fetches.append(url)
        if self.error is not None:
            raise self.error
        return self.pages.get(url, "")

    def close(self):
        self.closed = True


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

    def test_gql_http_auth_error_includes_sanitized_auth_context(self):
        self.client.session.post = mock.Mock(
            return_value=FakeResponse(
                status_code=401,
                payload={"errors": "[API] Invalid API key or access token"},
            )
        )

        with self.assertRaises(RuntimeError) as ctx:
            self.client.gql("query { shop { name } }")

        message = str(ctx.exception)
        self.assertIn("store='example-store'", message)
        self.assertIn("endpoint='https://example-store.myshopify.com/admin/api/", message)
        self.assertIn("token_len=", message)
        self.assertIn("token_prefix='shpat_'", message)
        self.assertIn("token_fp=", message)
        self.assertNotIn("shpat_test", message)

    def test_staged_uploads_create_uses_put_image_payload(self):
        self.client.gql = mock.Mock(return_value={
            "stagedUploadsCreate": {
                "stagedTargets": [{"url": "https://upload", "resourceUrl": "https://resource", "parameters": []}],
                "userErrors": [],
            }
        })
        file_size = None
        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp) / "test.jpg"
            image.write_bytes(b"image")
            file_size = image.stat().st_size
            targets = self.client.staged_uploads_create([image])

        self.assertEqual(len(targets), 1)
        query, variables = self.client.gql.call_args.args
        self.assertIn("stagedUploadsCreate", query)
        self.assertEqual(
            variables["input"][0],
            {
                "filename": "test.jpg",
                "mimeType": "image/jpeg",
                "resource": "IMAGE",
                "httpMethod": "PUT",
                "fileSize": str(file_size),
            },
        )

    def test_upload_file_to_staged_target_uses_put_headers(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.requests.put") as put:
            image = Path(tmp) / "test.jpg"
            image.write_bytes(b"image")
            put.return_value = mock.Mock(status_code=200)

            resource_url = self.client.upload_file_to_staged_target(
                image,
                {
                    "url": "https://upload",
                    "resourceUrl": "https://resource",
                    "parameters": [{"name": "x-amz-acl", "value": "private"}],
                },
            )

        self.assertEqual(resource_url, "https://resource")
        args, kwargs = put.call_args
        self.assertEqual(args[0], "https://upload")
        self.assertEqual(kwargs["headers"]["x-amz-acl"], "private")
        self.assertEqual(kwargs["headers"]["Content-Type"], "image/jpeg")

    def test_wait_for_files_ready_uses_node_file_query(self):
        self.client.gql = mock.Mock(side_effect=[
            {"node": {"id": "gid://shopify/MediaImage/1", "fileStatus": "PROCESSING"}},
            {"node": {"id": "gid://shopify/MediaImage/1", "fileStatus": "READY"}},
        ])

        with mock.patch("shopify_sync.time.sleep") as sleep:
            result = self.client.wait_for_files_ready(["gid://shopify/MediaImage/1"], timeout_seconds=1)

        self.assertEqual(result, ["gid://shopify/MediaImage/1"])
        query, variables = self.client.gql.call_args_list[0].args
        self.assertIn("node(id: $id)", query)
        self.assertIn("... on File", query)
        self.assertEqual(variables, {"id": "gid://shopify/MediaImage/1"})
        sleep.assert_called_once_with(2)

    def test_wait_for_files_ready_includes_source_labels_on_failure(self):
        self.client.gql = mock.Mock(return_value={
            "node": {"id": "gid://shopify/MediaImage/1", "fileStatus": "FAILED"},
        })

        with self.assertRaisesRegex(RuntimeError, "bad-image.jpg"):
            self.client.wait_for_files_ready(
                ["gid://shopify/MediaImage/1"],
                timeout_seconds=1,
                file_labels={"gid://shopify/MediaImage/1": "bad-image.jpg"},
            )

    def test_reorder_product_media_encodes_positions_as_strings(self):
        self.client.gql = mock.Mock(return_value={
            "productReorderMedia": {
                "job": {"id": None, "done": True},
                "mediaUserErrors": [],
            }
        })

        self.client.reorder_product_media(
            "gid://shopify/Product/1",
            ["gid://shopify/MediaImage/1", "gid://shopify/MediaImage/2"],
        )

        query, variables = self.client.gql.call_args.args
        self.assertIn("productReorderMedia", query)
        self.assertEqual(
            variables["moves"],
            [
                {"id": "gid://shopify/MediaImage/1", "newPosition": "0"},
                {"id": "gid://shopify/MediaImage/2", "newPosition": "1"},
            ],
        )

    def test_wait_for_job_uses_top_level_job_query(self):
        self.client.gql = mock.Mock(side_effect=[
            {"job": {"id": "gid://shopify/Job/1", "done": False}},
            {"job": {"id": "gid://shopify/Job/1", "done": True}},
        ])

        with mock.patch("shopify_sync.time.sleep") as sleep:
            self.client.wait_for_job("gid://shopify/Job/1", timeout_seconds=1)

        query, variables = self.client.gql.call_args_list[0].args
        self.assertIn("job(id: $id)", query)
        self.assertEqual(variables, {"id": "gid://shopify/Job/1"})
        sleep.assert_called_once_with(2)

    def test_get_product_metafield_definition_queries_reserved_namespace_and_key(self):
        self.client.gql = mock.Mock(return_value={
            "metafieldDefinitions": {
                "nodes": [
                    {
                        "id": "gid://shopify/MetafieldDefinition/1",
                        "namespace": "app--123456",
                        "key": shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
                        "ownerType": "PRODUCT",
                        "type": {"name": shopify_sync.FALLBACK_IMAGE_METAFIELD_TYPE},
                        "capabilities": {
                            "adminFilterable": {
                                "eligible": True,
                                "enabled": True,
                                "status": "ENABLED",
                            }
                        },
                    }
                ]
            }
        })

        definition = self.client.get_product_metafield_definition(
            shopify_sync.FALLBACK_IMAGE_METAFIELD_NAMESPACE,
            shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
        )

        query, variables = self.client.gql.call_args.args
        self.assertIn("metafieldDefinitions(first: 2, ownerType: PRODUCT, namespace: $namespace, key: $key)", query)
        self.assertEqual(
            variables,
            {
                "namespace": shopify_sync.FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                "key": shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
            },
        )
        self.assertEqual(definition["namespace"], "app--123456")


class ShopifyDescriptionBackfillTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_iter_existing_for_description_backfill_returns_flattened_catalog_records(self):
        self.client.gql = mock.Mock(return_value={
            "products": {
                "edges": [
                    {
                        "cursor": "cursor-1",
                        "node": {
                            "id": "gid://shopify/Product/1",
                            "title": "Space Marines Captain",
                            "vendor": "Games Workshop",
                            "productType": "Warhammer 40,000",
                            "description": "Lead the charge.",
                            "descriptionHtml": "<p>Lead the charge.</p>",
                            "variants": {
                                "edges": [
                                    {"node": {"sku": "SKU-1"}},
                                    {"node": {"sku": " "}},
                                    {"node": {"sku": "SKU-2"}},
                                ]
                            },
                        },
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

        records = list(self.client.iter_existing_for_description_backfill())

        self.assertEqual(
            records,
            [
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Space Marines Captain",
                    "vendor": "Games Workshop",
                    "product_type": "Warhammer 40,000",
                    "description": "Lead the charge.",
                    "description_html": "<p>Lead the charge.</p>",
                    "skus": ["SKU-1", "SKU-2"],
                }
            ],
        )
        query, variables = self.client.gql.call_args.args
        self.assertIn("descriptionHtml", query)
        self.assertEqual(variables, {"cursor": None})

    def test_update_product_description_sends_only_id_and_description_html(self):
        self.client.gql = mock.Mock(return_value={
            "productUpdate": {
                "product": {"id": "gid://shopify/Product/1"},
                "userErrors": [],
            }
        })

        self.client.update_product_description("gid://shopify/Product/1", "<p>Fresh copy</p>")

        query, variables = self.client.gql.call_args.args
        self.assertIn("productUpdate(product: $product)", query)
        self.assertEqual(
            variables,
            {
                "product": {
                    "id": "gid://shopify/Product/1",
                    "descriptionHtml": "<p>Fresh copy</p>",
                }
            },
        )

    def test_update_product_description_raises_user_errors(self):
        self.client.gql = mock.Mock(return_value={
            "productUpdate": {
                "product": None,
                "userErrors": [{"field": ["descriptionHtml"], "message": "Too long"}],
            }
        })

        with self.assertRaisesRegex(RuntimeError, "productUpdate errors for gid://shopify/Product/1"):
            self.client.update_product_description("gid://shopify/Product/1", "<p>Fresh copy</p>")


class ShopifyCountryOfOriginBackfillTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_iter_existing_for_country_of_origin_backfill_returns_all_variants_without_sku_filtering(self):
        self.client.gql = mock.Mock(side_effect=[
            {
                "products": {
                    "edges": [
                        {
                            "cursor": "product-cursor-1",
                            "node": {
                                "id": "gid://shopify/Product/1",
                                "title": "Starter Set",
                                "variants": {
                                    "edges": [
                                        {
                                            "node": {
                                                "id": "gid://shopify/ProductVariant/1",
                                                "sku": "SKU-1",
                                                "inventoryItem": {
                                                    "id": "gid://shopify/InventoryItem/1",
                                                    "countryCodeOfOrigin": "US",
                                                },
                                            }
                                        },
                                        {
                                            "node": {
                                                "id": "gid://shopify/ProductVariant/2",
                                                "sku": "",
                                                "inventoryItem": {
                                                    "id": "gid://shopify/InventoryItem/2",
                                                    "countryCodeOfOrigin": None,
                                                },
                                            }
                                        },
                                    ],
                                    "pageInfo": {"hasNextPage": True, "endCursor": "variant-cursor-2"},
                                },
                            },
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            },
            {
                "product": {
                    "variants": {
                        "edges": [
                            {
                                "node": {
                                    "id": "gid://shopify/ProductVariant/3",
                                    "sku": "SKU-3",
                                    "inventoryItem": {
                                        "id": "gid://shopify/InventoryItem/3",
                                        "countryCodeOfOrigin": "GB",
                                    },
                                }
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            },
        ])

        records = list(self.client.iter_existing_for_country_of_origin_backfill())

        self.assertEqual(
            records,
            [
                {
                    "product_id": "gid://shopify/Product/1",
                    "title": "Starter Set",
                    "variant_id": "gid://shopify/ProductVariant/1",
                    "sku": "SKU-1",
                    "inventory_item_id": "gid://shopify/InventoryItem/1",
                    "country_of_origin": "US",
                },
                {
                    "product_id": "gid://shopify/Product/1",
                    "title": "Starter Set",
                    "variant_id": "gid://shopify/ProductVariant/2",
                    "sku": "",
                    "inventory_item_id": "gid://shopify/InventoryItem/2",
                    "country_of_origin": "",
                },
                {
                    "product_id": "gid://shopify/Product/1",
                    "title": "Starter Set",
                    "variant_id": "gid://shopify/ProductVariant/3",
                    "sku": "SKU-3",
                    "inventory_item_id": "gid://shopify/InventoryItem/3",
                    "country_of_origin": "GB",
                },
            ],
        )
        first_query, first_variables = self.client.gql.call_args_list[0].args
        self.assertIn("countryCodeOfOrigin", first_query)
        self.assertEqual(first_variables, {"cursor": None})
        second_query, second_variables = self.client.gql.call_args_list[1].args
        self.assertIn("product(id: $productId)", second_query)
        self.assertEqual(
            second_variables,
            {"productId": "gid://shopify/Product/1", "cursor": "variant-cursor-2"},
        )

    def test_update_inventory_item_country_of_origin_sends_inventory_item_update(self):
        self.client.gql = mock.Mock(return_value={
            "inventoryItemUpdate": {
                "inventoryItem": {
                    "id": "gid://shopify/InventoryItem/1",
                    "countryCodeOfOrigin": "GB",
                },
                "userErrors": [],
            }
        })

        self.client.update_inventory_item_country_of_origin(
            "gid://shopify/InventoryItem/1",
            "GB",
        )

        query, variables = self.client.gql.call_args.args
        self.assertIn("inventoryItemUpdate", query)
        self.assertEqual(
            variables,
            {
                "id": "gid://shopify/InventoryItem/1",
                "input": {"countryCodeOfOrigin": "GB"},
            },
        )


class DescriptionBackfillHelperTests(unittest.TestCase):
    def test_require_openrouter_config_rejects_missing_api_key(self):
        with self.assertRaisesRegex(RuntimeError, "OPENROUTER_API_KEY must be set"):
            shopify_sync.description_backfill.require_openrouter_config({})

    def test_require_openrouter_config_uses_default_model_when_env_is_unset(self):
        config = shopify_sync.description_backfill.require_openrouter_config(
            {"OPENROUTER_API_KEY": "test-key"}
        )

        self.assertEqual(
            config,
            {
                "api_key": "test-key",
                "model": shopify_sync.description_backfill.OPENROUTER_DEFAULT_MODEL,
            },
        )

    def test_current_description_backfill_policy_version_changes_when_preview_columns_change(self):
        baseline = shopify_sync.description_backfill.current_description_backfill_policy_version(
            preview_columns=["sku", "title"],
            review_columns=["sku", "title", "reason"],
        )

        changed = shopify_sync.description_backfill.current_description_backfill_policy_version(
            preview_columns=["sku", "title", "scope_reason"],
            review_columns=["sku", "title", "reason"],
        )

        self.assertNotEqual(changed, baseline)

    def test_resolve_wayland_source_accepts_unique_title_and_sku_match(self):
        session = requests.Session()
        search_url = "https://www.waylandgames.co.uk/search?s=SKU-1+Space+Marines+Captain"
        guessed_url = "https://www.waylandgames.co.uk/space-marines-captain-sku-1"
        search_html = '<html><body><a href="/space-marines-captain">Space Marines Captain</a></body></html>'
        product_html = """
        <html>
          <head><title>Space Marines Captain</title></head>
          <body>
            <div>SKU SKU-1</div>
            <p>Lead a veteran strike force into battle with this heavily armoured commander.</p>
          </body>
        </html>
        """

        scraper = FakeWaylandScraper({
            search_url: search_html,
            guessed_url: product_html,
            "https://www.waylandgames.co.uk/space-marines-captain": "<html><body>listing page</body></html>",
        })
        with mock.patch.object(shopify_sync.description_backfill, "_get_wayland_scraper", return_value=scraper):
            result = shopify_sync.description_backfill.resolve_wayland_source(
                session,
                title="Space Marines Captain",
                sku="SKU-1",
            )

        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.reason, "unique_title_sku_match")
        self.assertEqual(result.candidate.page_url, guessed_url)

    def test_resolve_wayland_source_accepts_direct_slug_guess_when_search_results_are_weak(self):
        session = requests.Session()
        search_url = "https://www.waylandgames.co.uk/search?s=99120109017+ARMAGEDDON+BATTALION%3A+DEATHWATCH"
        guessed_url = "https://www.waylandgames.co.uk/armageddon-battalion-deathwatch-99120109017"
        search_html = '<html><body><a href="/gift-card">Gift Card</a></body></html>'
        product_html = """
        <html>
          <head><title>ARMAGEDDON BATTALION: DEATHWATCH</title></head>
          <body>
            <div>SKU 99120109017</div>
            <p>Assemble an elite Deathwatch strike force for Armageddon with veteran operatives and specialist wargear.</p>
          </body>
        </html>
        """

        scraper = FakeWaylandScraper({
            search_url: search_html,
            guessed_url: product_html,
            "https://www.waylandgames.co.uk/gift-card": "<html><body>Gift Card</body></html>",
        })
        with mock.patch.object(shopify_sync.description_backfill, "_get_wayland_scraper", return_value=scraper):
            result = shopify_sync.description_backfill.resolve_wayland_source(
                session,
                title="ARMAGEDDON BATTALION: DEATHWATCH",
                sku="99120109017",
            )

        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.candidate.page_url, guessed_url)

    def test_resolve_wayland_source_reviews_when_no_confident_match_is_found(self):
        session = requests.Session()
        search_url = "https://www.waylandgames.co.uk/search?s=SKU-1+Space+Marines+Captain"
        search_html = '<html><body><a href="/gift-card">Gift Card</a></body></html>'
        product_html = """
        <html>
          <head><title>Gift Card</title></head>
          <body>
            <p>This is a very generic product page without the requested SKU anywhere in the content.</p>
          </body>
        </html>
        """

        scraper = FakeWaylandScraper({
            search_url: search_html,
            "https://www.waylandgames.co.uk/gift-card": product_html,
        })
        with mock.patch.object(shopify_sync.description_backfill, "_get_wayland_scraper", return_value=scraper):
            result = shopify_sync.description_backfill.resolve_wayland_source(
                session,
                title="Space Marines Captain",
                sku="SKU-1",
            )

        self.assertEqual(result.status, "review")
        self.assertEqual(result.reason, "no_confident_wayland_match")

    def test_resolve_wayland_source_raises_when_playwright_lookup_fails(self):
        session = requests.Session()
        scraper = FakeWaylandScraper(error=RuntimeError("browser challenge"))

        with mock.patch.object(shopify_sync.description_backfill, "_get_wayland_scraper", return_value=scraper):
            with self.assertRaisesRegex(RuntimeError, "browser challenge"):
                shopify_sync.description_backfill.resolve_wayland_source(
                    session,
                    title="Space Marines Captain",
                    sku="SKU-1",
                )

    def test_close_wayland_scraper_closes_and_detaches_session_scraper(self):
        session = requests.Session()
        scraper = FakeWaylandScraper()
        setattr(session, shopify_sync.description_backfill.WAYLAND_PLAYWRIGHT_SESSION_ATTR, scraper)

        shopify_sync.description_backfill.close_wayland_scraper(session)

        self.assertTrue(scraper.closed)
        self.assertFalse(hasattr(session, shopify_sync.description_backfill.WAYLAND_PLAYWRIGHT_SESSION_ATTR))

    def test_wayland_scraper_close_suppresses_keyboard_interrupt_from_resource_close(self):
        scraper = object.__new__(shopify_sync.description_backfill.WaylandPlaywrightScraper)

        class InterruptingCloser:
            def close(self):
                raise KeyboardInterrupt()

        scraper._page = InterruptingCloser()
        scraper._context = None
        scraper._browser = None
        scraper._playwright_manager = None

        shopify_sync.description_backfill.WaylandPlaywrightScraper.close(scraper)

        self.assertIsNone(scraper._page)

    def test_resolve_preferred_source_falls_back_to_games_workshop_when_wayland_reviews(self):
        session = requests.Session()
        search_html = """
        <html><body>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.warhammer.com%2Fen-GB%2Fshop%2Farmageddon-battalion-deathwatch">
            Armageddon Battalion: Deathwatch
          </a>
        </body></html>
        """
        product_html = """
        <html>
          <head>
            <title>Armageddon Battalion: Deathwatch</title>
            <meta name="description" content="Assemble an elite Deathwatch strike force for Armageddon with veteran operatives and specialist wargear.">
          </head>
          <body>
            <div>99120109017</div>
          </body>
        </html>
        """
        session.get = mock.Mock(side_effect=[
            FakeResponse(text=search_html, url="https://html.duckduckgo.com/html/?q=deathwatch"),
            FakeResponse(text=product_html, url="https://www.warhammer.com/en-GB/shop/armageddon-battalion-deathwatch"),
        ])

        with mock.patch.object(
            shopify_sync.description_backfill,
            "resolve_wayland_source",
            return_value=shopify_sync.description_backfill.SourceResolution(
                status="review",
                reason="no_confident_wayland_match",
                search_url="https://www.waylandgames.co.uk/search?query=99120109017",
            ),
        ):
            result = shopify_sync.description_backfill.resolve_preferred_source(
                session,
                title="ARMAGEDDON BATTALION: DEATHWATCH",
                sku="99120109017",
            )

        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.source_site, "games_workshop")
        self.assertEqual(result.reason, "games_workshop_title_sku_match")
        self.assertEqual(
            result.candidate.page_url,
            "https://www.warhammer.com/en-GB/shop/armageddon-battalion-deathwatch",
        )

    def test_require_playwright_raises_clear_runtime_error_when_missing(self):
        session = requests.Session()
        with mock.patch.object(
            shopify_sync.description_backfill,
            "WaylandPlaywrightScraper",
            side_effect=RuntimeError("Playwright is required for Wayland description scraping."),
        ):
            with self.assertRaisesRegex(RuntimeError, "Playwright is required"):
                shopify_sync.description_backfill.resolve_wayland_source(
                    session,
                    title="Space Marines Captain",
                    sku="SKU-1",
                )

    def test_challenge_page_detector_flags_cloudflare_interstitial(self):
        html_text = """
        <html><head><title>Just a moment...</title></head>
        <body><script>window._cf_chl_opt = {};</script></body></html>
        """

        self.assertTrue(
            shopify_sync.description_backfill.WaylandPlaywrightScraper._looks_like_challenge_page(html_text)
        )

    def test_phase_description_backfill_closes_wayland_scraper_session(self):
        client = mock.Mock()
        client.iter_existing_for_description_backfill.return_value = iter([])
        env = {"OPENROUTER_API_KEY": "test-key"}
        session_scraper = FakeWaylandScraper()

        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "description_backfill_manifest.json"
            with mock.patch.object(shopify_sync.description_backfill, "close_wayland_scraper") as close_scraper, \
                 mock.patch.object(shopify_sync.description_backfill, "load_manifest", return_value={}), \
                 mock.patch.object(shopify_sync.description_backfill, "save_manifest"):
                shopify_sync.phase_backfill_descriptions(
                    client,
                    env,
                    dry=True,
                    target_skus=[],
                    limit=None,
                    manifest_path=manifest_path,
                )

        close_scraper.assert_called_once()

    def test_rewrite_with_openrouter_retries_transient_http_errors(self):
        session = requests.Session()
        session.post = mock.Mock(side_effect=[
            FakeResponse(status_code=503, text="temporary"),
            FakeResponse(payload={"choices": [{"message": {"content": "Fresh copy SKU: SKU-1."}}]}),
        ])

        with mock.patch("shopify_sync.description_backfill.time.sleep") as sleep:
            rewritten = shopify_sync.description_backfill.rewrite_with_openrouter(
                session,
                {"OPENROUTER_API_KEY": "test-key"},
                source_text="Lead a veteran strike force into battle.",
                sku="SKU-1",
            )

        self.assertEqual(rewritten, "Fresh copy SKU: SKU-1.")
        sleep.assert_called_once_with(shopify_sync.description_backfill.OPENROUTER_RETRY_DELAYS_SECONDS[0])
        self.assertEqual(session.post.call_count, 2)

    def test_evaluate_rewrite_rejects_exact_copy(self):
        result = shopify_sync.description_backfill.evaluate_rewrite(
            "Lead a veteran strike force into battle with this armoured commander. SKU: SKU-1.",
            "Lead a veteran strike force into battle with this armoured commander. SKU: SKU-1.",
            "SKU-1",
        )

        self.assertEqual(result.status, "review")
        self.assertEqual(result.reason, "rewrite_equals_source")

    def test_evaluate_rewrite_repairs_missing_sku_and_accepts_distinct_copy(self):
        result = shopify_sync.description_backfill.evaluate_rewrite(
            "Lead a veteran strike force into battle with this armoured commander.",
            "Command the battlefield with a decorated hero who anchors elite assaults.",
            "SKU-1",
        )

        self.assertEqual(result.status, "accepted")
        self.assertTrue(result.repaired_for_sku)
        self.assertIn("SKU: SKU-1.", result.rewritten_text)

    def test_sanitize_description_html_wraps_single_safe_paragraph(self):
        html_output = shopify_sync.description_backfill.sanitize_description_html(
            'Commander <strong>ready</strong> & "armed"'
        )

        self.assertEqual(
            html_output,
            '<p>Commander &lt;strong&gt;ready&lt;/strong&gt; &amp; "armed"</p>',
        )


class DescriptionBackfillUtilityTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_select_description_backfill_sku_returns_unique_product_sku(self):
        sku, reason = shopify_sync._select_description_backfill_sku({"skus": ["SKU-1", "SKU-1", " SKU-1 "]})

        self.assertEqual(sku, "SKU-1")
        self.assertEqual(reason, "unique_product_sku")

    def test_select_description_backfill_sku_rejects_multi_variant_records(self):
        sku, reason = shopify_sync._select_description_backfill_sku({"skus": ["SKU-1", "SKU-2"]})

        self.assertIsNone(sku)
        self.assertEqual(reason, "ambiguous_multi_variant_skus")

    def test_scope_description_backfill_records_applies_sku_filter_before_limit(self):
        records = [
            {"id": "1", "skus": ["SKU-1"]},
            {"id": "2", "skus": ["SKU-2"]},
            {"id": "3", "skus": ["SKU-3"]},
        ]

        scoped = shopify_sync._scope_description_backfill_records(
            records,
            target_skus={"SKU-2", "SKU-3"},
            limit=1,
        )

        self.assertEqual([record["id"] for record in scoped], ["2"])
        self.assertEqual(scoped[0]["scope_reason"], "sku_filter:limit")

    def test_append_description_backfill_failure_sanitizes_formula_like_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            failures_path = Path(tmp) / "description_backfill_failures.tsv"
            with mock.patch("shopify_sync.DESCRIPTION_BACKFILL_FAILURES_TSV", new=failures_path):
                shopify_sync._append_description_backfill_failure(
                    "gid://shopify/Product/1",
                    "@SKU-1",
                    "=Danger Title",
                    "-boom",
                )

            failure_row = failures_path.read_text(encoding="utf-8")

        self.assertIn("'@SKU-1", failure_row)
        self.assertIn("'=Danger Title", failure_row)
        self.assertIn("'-boom", failure_row)

    def test_create_product_metafield_definition_uses_documented_app_owned_admin_access(self):
        self.client.gql = mock.Mock(return_value={
            "metafieldDefinitionCreate": {
                "createdDefinition": {
                    "id": "gid://shopify/MetafieldDefinition/1",
                    "namespace": "app--123456",
                    "key": shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
                    "ownerType": "PRODUCT",
                    "type": {"name": shopify_sync.FALLBACK_IMAGE_METAFIELD_TYPE},
                    "capabilities": {
                        "adminFilterable": {
                            "eligible": True,
                            "enabled": True,
                            "status": "ENABLED",
                        }
                    },
                },
                "userErrors": [],
            }
        })

        self.client.create_product_metafield_definition(
            shopify_sync.FALLBACK_IMAGE_METAFIELD_NAMESPACE,
            shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
            shopify_sync.FALLBACK_IMAGE_METAFIELD_NAME,
            shopify_sync.FALLBACK_IMAGE_METAFIELD_TYPE,
        )

        query, variables = self.client.gql.call_args.args
        self.assertIn("metafieldDefinitionCreate", query)
        self.assertEqual(
            variables["definition"]["access"]["admin"],
            shopify_sync.FALLBACK_IMAGE_METAFIELD_ADMIN_ACCESS,
        )

    def test_set_product_fallback_image_used_uses_reserved_namespace_key_and_boolean_type(self):
        self.client.gql = mock.Mock(return_value={
            "metafieldsSet": {
                "metafields": [
                    {
                        "namespace": shopify_sync.FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                        "key": shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
                        "value": "true",
                    }
                ],
                "userErrors": [],
            }
        })

        self.client.set_product_fallback_image_used("gid://shopify/Product/2")

        query, variables = self.client.gql.call_args.args
        self.assertIn("metafieldsSet", query)
        self.assertEqual(
            variables["metafields"][0],
            {
                "ownerId": "gid://shopify/Product/2",
                "namespace": shopify_sync.FALLBACK_IMAGE_METAFIELD_NAMESPACE,
                "key": shopify_sync.FALLBACK_IMAGE_METAFIELD_KEY,
                "type": shopify_sync.FALLBACK_IMAGE_METAFIELD_TYPE,
                "value": "true",
            },
        )


class CliRuntimeHandlingTests(unittest.TestCase):
    def test_run_cli_returns_2_and_logs_guidance_for_shopify_401(self):
        with mock.patch("shopify_sync.main", side_effect=RuntimeError(
            "Shopify HTTP 401: {\"errors\":\"[API] Invalid API key or access token\"} "
            "[store='kviv0f-15' endpoint='https://kviv0f-15.myshopify.com/admin/api/2025-01/graphql.json' "
            "token_len=38 token_prefix='shpat_' token_fp=9a2c714e5d]"
        )), mock.patch("shopify_sync.log") as log:
            result = shopify_sync.run_cli()

        self.assertEqual(result, 2)
        logged = "\n".join(call.args[0] for call in log.call_args_list)
        self.assertIn("Shopify Admin API authentication failed", logged)
        self.assertIn("rotate/reinstall the custom-app Admin API token", logged)
        self.assertIn("store='kviv0f-15'", logged)

    def test_run_cli_reraises_non_auth_runtime_errors(self):
        with mock.patch("shopify_sync.main", side_effect=RuntimeError("local gate failed")):
            with self.assertRaisesRegex(RuntimeError, "local gate failed"):
                shopify_sync.run_cli()


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
                    "tags": [
                        "40K - Generic",
                        "Games Workshop",
                        "Warhammer 40,000",
                        "deathwatch",
                        "warhammer-40k",
                    ],
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


class CollectionManagementTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def _collection(self, collection_id, title, handle, rule_set=None):
        return {
            "id": collection_id,
            "title": title,
            "handle": handle,
            "productsCount": {"count": 0},
            "ruleSet": rule_set,
        }

    def test_iter_all_collections_marks_custom_and_smart(self):
        self.client.gql = mock.Mock(side_effect=[
            {
                "collections": {
                    "edges": [
                        {
                            "cursor": "cur-1",
                            "node": self._collection(
                                "gid://shopify/Collection/1",
                                "Wargames",
                                "wargames",
                            ),
                        },
                        {
                            "cursor": "cur-2",
                            "node": self._collection(
                                "gid://shopify/Collection/2",
                                "Plush Figures",
                                "plush-figures",
                                {"appliedDisjunctively": False},
                            ),
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        ])

        result = list(self.client.iter_all_collections())

        self.assertEqual(
            result,
            [
                {
                    "id": "gid://shopify/Collection/1",
                    "title": "Wargames",
                    "handle": "wargames",
                    "products_count": 0,
                    "collection_type": "custom",
                    "applied_disjunctively": False,
                    "rules": [],
                },
                {
                    "id": "gid://shopify/Collection/2",
                    "title": "Plush Figures",
                    "handle": "plush-figures",
                    "products_count": 0,
                    "collection_type": "smart",
                    "applied_disjunctively": False,
                    "rules": [],
                },
            ],
        )

    def test_is_managed_collection_matches_expected_rule_signature(self):
        spec = shopify_sync.MANAGED_COLLECTION_SPECS_BY_HANDLE["games-workshop"]
        collection = {
            "id": "gid://shopify/Collection/1",
            "title": "Games Workshop",
            "handle": "games-workshop",
            "collection_type": "smart",
            "applied_disjunctively": False,
            "rules": [
                {
                    "column": "VENDOR",
                    "relation": "EQUALS",
                    "condition": "Games Workshop",
                }
            ],
        }

        self.assertTrue(shopify_sync.is_managed_collection(collection, expected_spec=spec))

    def test_delete_collection_uses_collection_delete_mutation(self):
        self.client.gql = mock.Mock(return_value={
            "collectionDelete": {
                "deletedCollectionId": "gid://shopify/Collection/2",
                "userErrors": [],
            }
        })

        self.client.delete_collection("gid://shopify/Collection/2")

        query, variables = self.client.gql.call_args.args
        self.assertIn("collectionDelete", query)
        self.assertEqual(
            variables,
            {"input": {"id": "gid://shopify/Collection/2"}},
        )

    def test_create_smart_collection_uses_rule_input(self):
        self.client.gql = mock.Mock(return_value={
            "collectionCreate": {
                "collection": {
                    "id": "gid://shopify/Collection/3",
                    "title": "Games Workshop",
                    "handle": "games-workshop",
                },
                "userErrors": [],
            }
        })

        result = self.client.create_smart_collection(
            "Games Workshop",
            "games-workshop",
            [shopify_sync.CollectionRuleSpec("VENDOR", "EQUALS", "Games Workshop")],
        )

        self.assertEqual(
            result,
            {
                "id": "gid://shopify/Collection/3",
                "title": "Games Workshop",
                "handle": "games-workshop",
            },
        )
        query, variables = self.client.gql.call_args.args
        self.assertIn("collectionCreate", query)
        self.assertEqual(
            variables,
            {
                "input": {
                    "title": "Games Workshop",
                    "handle": "games-workshop",
                    "descriptionHtml": "",
                    "ruleSet": {
                        "appliedDisjunctively": False,
                        "rules": [
                            {
                                "column": "VENDOR",
                                "relation": "EQUALS",
                                "condition": "Games Workshop",
                            }
                        ],
                    },
                }
            },
        )

    def test_get_collection_image_returns_url_and_alt(self):
        self.client.gql = mock.Mock(return_value={
            "collection": {
                "image": {
                    "url": "https://cdn.shopify.com/s/files/1/0001/img.jpg",
                    "altText": "An image",
                }
            }
        })
        result = self.client.get_collection_image("gid://shopify/Collection/1")
        self.assertEqual(
            result,
            {"url": "https://cdn.shopify.com/s/files/1/0001/img.jpg", "alt_text": "An image"},
        )

    def test_get_collection_image_handles_no_image(self):
        self.client.gql = mock.Mock(return_value={"collection": {"image": None}})
        result = self.client.get_collection_image("gid://shopify/Collection/1")
        self.assertEqual(result, {"url": "", "alt_text": ""})

    def test_find_first_alphabetical_product_with_image_skips_imageless(self):
        # First page: two products without an image and one with.
        page1 = {
            "collection": {
                "products": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Product/100", "title": "Aaa", "featuredImage": None}},
                        {"node": {"id": "gid://shopify/Product/101", "title": "Bbb", "featuredImage": {"url": "", "altText": ""}}},
                        {"node": {"id": "gid://shopify/Product/102", "title": "Ccc",
                                  "featuredImage": {"url": "https://cdn.shopify.com/img.jpg", "altText": "alt"}}},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
        self.client.gql = mock.Mock(return_value=page1)
        result = self.client.find_first_alphabetical_product_with_image("gid://shopify/Collection/1")
        self.assertEqual(result, {
            "product_id": "gid://shopify/Product/102",
            "product_title": "Ccc",
            "image_url": "https://cdn.shopify.com/img.jpg",
            "image_alt": "alt",
        })
        query, variables = self.client.gql.call_args.args
        self.assertIn("sortKey: TITLE", query)
        self.assertEqual(variables, {"id": "gid://shopify/Collection/1", "cursor": None})

    def test_find_first_alphabetical_product_with_image_pages_when_needed(self):
        page1 = {
            "collection": {
                "products": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Product/100", "title": "Aaa", "featuredImage": None}},
                    ],
                    "pageInfo": {"hasNextPage": True, "endCursor": "CURSOR1"},
                }
            }
        }
        page2 = {
            "collection": {
                "products": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Product/200", "title": "Zzz",
                                  "featuredImage": {"url": "https://cdn.shopify.com/late.jpg", "altText": ""}}},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
        self.client.gql = mock.Mock(side_effect=[page1, page2])
        result = self.client.find_first_alphabetical_product_with_image("gid://shopify/Collection/1")
        self.assertEqual(result["product_id"], "gid://shopify/Product/200")
        self.assertEqual(result["image_url"], "https://cdn.shopify.com/late.jpg")
        self.assertEqual(self.client.gql.call_count, 2)

    def test_find_first_alphabetical_product_with_image_returns_empty_when_none(self):
        self.client.gql = mock.Mock(return_value={
            "collection": {
                "products": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Product/100", "title": "Aaa", "featuredImage": None}},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        })
        result = self.client.find_first_alphabetical_product_with_image("gid://shopify/Collection/1")
        self.assertEqual(result, {})

    def test_update_collection_image_uses_collection_update_with_image_src(self):
        self.client.gql = mock.Mock(return_value={
            "collectionUpdate": {
                "collection": {
                    "id": "gid://shopify/Collection/1",
                    "image": {"url": "https://cdn.shopify.com/new.jpg"},
                },
                "userErrors": [],
            }
        })
        result = self.client.update_collection_image(
            "gid://shopify/Collection/1",
            "https://cdn.shopify.com/source.jpg",
            alt_text="Some alt",
        )
        self.assertEqual(result, "https://cdn.shopify.com/new.jpg")
        query, variables = self.client.gql.call_args.args
        self.assertIn("collectionUpdate", query)
        self.assertEqual(variables, {
            "input": {
                "id": "gid://shopify/Collection/1",
                "image": {"src": "https://cdn.shopify.com/source.jpg", "altText": "Some alt"},
            }
        })

    def test_update_collection_image_raises_on_user_errors(self):
        self.client.gql = mock.Mock(return_value={
            "collectionUpdate": {
                "collection": None,
                "userErrors": [{"field": ["image", "src"], "message": "Image is invalid"}],
            }
        })
        with self.assertRaisesRegex(RuntimeError, "Image is invalid"):
            self.client.update_collection_image(
                "gid://shopify/Collection/1",
                "https://bad.example.com/img.jpg",
            )

    def test_publish_to_all_channels_uses_publications_query_and_publishable_publish(self):
        self.client.gql = mock.Mock(side_effect=[
            {
                "publications": {
                    "edges": [
                        {"node": {"id": "gid://shopify/Publication/1", "name": "Online Store"}},
                        {"node": {"id": "gid://shopify/Publication/2", "name": "Shop"}},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            },
            {
                "publishablePublish": {
                    "publishable": {"id": "gid://shopify/Collection/3"},
                    "userErrors": [],
                }
            },
        ])

        count = self.client.publish_to_all_channels("gid://shopify/Collection/3")

        self.assertEqual(count, 2)
        publish_query, publish_vars = self.client.gql.call_args_list[1].args
        self.assertIn("publishablePublish", publish_query)
        self.assertEqual(
            publish_vars,
            {
                "id": "gid://shopify/Collection/3",
                "input": [
                    {"publicationId": "gid://shopify/Publication/1"},
                    {"publicationId": "gid://shopify/Publication/2"},
                ],
            },
        )


class ProductPublicationTests(unittest.TestCase):
    def setUp(self):
        self.client = shopify_sync.Shopify("example-store", "shpat_test")

    def test_get_publication_id_by_name_matches_online_store(self):
        self.client.gql = mock.Mock(return_value={
            "publications": {
                "edges": [
                    {"node": {"id": "gid://shopify/Publication/1", "name": "Shop"}},
                    {"node": {"id": "gid://shopify/Publication/2", "name": "Online Store"}},
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

        publication_id = self.client.get_publication_id_by_name("Online Store")

        self.assertEqual(publication_id, "gid://shopify/Publication/2")

    def test_publish_to_publication_uses_publishable_publish(self):
        self.client.gql = mock.Mock(return_value={
            "publishablePublish": {
                "publishable": {
                    "publishedOnPublication": True,
                },
                "userErrors": [],
            }
        })

        self.client.publish_to_publication("gid://shopify/Product/9", "gid://shopify/Publication/2")

        query, variables = self.client.gql.call_args.args
        self.assertIn("publishablePublish", query)
        self.assertIn("publishedOnPublication", query)
        self.assertEqual(
            variables,
            {"id": "gid://shopify/Product/9", "publicationId": "gid://shopify/Publication/2"},
        )

    def test_unpublish_from_publication_uses_publishable_unpublish(self):
        self.client.gql = mock.Mock(return_value={
            "publishableUnpublish": {
                "publishable": {
                    "publishedOnPublication": False,
                },
                "userErrors": [],
            }
        })

        self.client.unpublish_from_publication("gid://shopify/Product/9", "gid://shopify/Publication/2")

        query, variables = self.client.gql.call_args.args
        self.assertIn("publishableUnpublish", query)
        self.assertIn("publishedOnPublication", query)
        self.assertEqual(
            variables,
            {"id": "gid://shopify/Product/9", "publicationId": "gid://shopify/Publication/2"},
        )

    def test_iter_products_unpublished_on_publication_filters_published_products(self):
        self.client.gql = mock.Mock(return_value={
            "products": {
                "edges": [
                    {
                        "cursor": "cur-1",
                        "node": {
                            "id": "gid://shopify/Product/1",
                            "title": "Published",
                            "publishedOnPublication": True,
                            "variants": {"edges": [{"node": {"sku": "PUB-1"}}]},
                        },
                    },
                    {
                        "cursor": "cur-2",
                        "node": {
                            "id": "gid://shopify/Product/2",
                            "title": "Needs Publish",
                            "publishedOnPublication": False,
                            "variants": {"edges": [{"node": {"sku": "NP-1"}}, {"node": {"sku": ""}}]},
                        },
                    },
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

        rows = list(self.client.iter_products_unpublished_on_publication("gid://shopify/Publication/2"))

        self.assertEqual(
            rows,
            [
                {
                    "id": "gid://shopify/Product/2",
                    "title": "Needs Publish",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/2",
                    "skus": ["NP-1"],
                }
            ],
        )
        query, variables = self.client.gql.call_args.args
        self.assertIn("publishedOnPublication", query)
        self.assertEqual(variables, {"cursor": None, "publicationId": "gid://shopify/Publication/2"})

    def test_iter_products_for_online_store_image_visibility_treats_any_media_as_present(self):
        self.client.gql = mock.Mock(return_value={
            "products": {
                "edges": [
                    {
                        "cursor": "cur-1",
                        "node": {
                            "id": "gid://shopify/Product/1",
                            "title": "Visible With Video Only",
                            "publishedOnPublication": True,
                            "variants": {"edges": [{"node": {"sku": "VIS-1"}}]},
                            "media": {"edges": [{"node": {"id": "gid://shopify/Video/1"}}]},
                        },
                    },
                    {
                        "cursor": "cur-2",
                        "node": {
                            "id": "gid://shopify/Product/2",
                            "title": "Hidden Without Media",
                            "publishedOnPublication": False,
                            "variants": {"edges": [{"node": {"sku": "HID-1"}}]},
                            "media": {"edges": []},
                        },
                    },
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

        rows = list(self.client.iter_products_for_online_store_image_visibility("gid://shopify/Publication/2"))

        self.assertEqual(
            rows,
            [
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Visible With Video Only",
                    "status": "",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/2",
                    "has_media": True,
                    "skus": ["VIS-1"],
                },
                {
                    "id": "gid://shopify/Product/2",
                    "title": "Hidden Without Media",
                    "status": "",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/2",
                    "has_media": False,
                    "skus": ["HID-1"],
                },
            ],
        )
        query, variables = self.client.gql.call_args.args
        self.assertIn("publishedOnPublication", query)
        self.assertIn("media(first: 1)", query)
        self.assertEqual(variables, {"cursor": None, "publicationId": "gid://shopify/Publication/2"})

    def test_iter_products_for_online_store_image_visibility_excludes_draft_and_archived_but_keeps_active_zero_inventory(self):
        self.client.gql = mock.Mock(return_value={
            "products": {
                "edges": [
                    {
                        "cursor": "cur-1",
                        "node": {
                            "id": "gid://shopify/Product/1",
                            "title": "Active Zero Inventory",
                            "status": "ACTIVE",
                            "totalInventory": 0,
                            "publishedOnPublication": False,
                            "variants": {"edges": [{"node": {"sku": "ZERO-1"}}]},
                            "media": {"edges": []},
                        },
                    },
                    {
                        "cursor": "cur-2",
                        "node": {
                            "id": "gid://shopify/Product/2",
                            "title": "Draft Product",
                            "status": "DRAFT",
                            "totalInventory": 5,
                            "publishedOnPublication": True,
                            "variants": {"edges": [{"node": {"sku": "DRAFT-1"}}]},
                            "media": {"edges": [{"node": {"id": "gid://shopify/MediaImage/2"}}]},
                        },
                    },
                    {
                        "cursor": "cur-3",
                        "node": {
                            "id": "gid://shopify/Product/3",
                            "title": "Archived Product",
                            "status": "ARCHIVED",
                            "totalInventory": 2,
                            "publishedOnPublication": False,
                            "variants": {"edges": [{"node": {"sku": "ARCH-1"}}]},
                            "media": {"edges": []},
                        },
                    },
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

        rows = list(self.client.iter_products_for_online_store_image_visibility("gid://shopify/Publication/2"))

        self.assertEqual(
            rows,
            [
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Active Zero Inventory",
                    "status": "ACTIVE",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/2",
                    "has_media": False,
                    "skus": ["ZERO-1"],
                }
            ],
        )
        query, variables = self.client.gql.call_args.args
        self.assertIn("status", query)
        self.assertEqual(variables, {"cursor": None, "publicationId": "gid://shopify/Publication/2"})


class MainFlowTests(unittest.TestCase):
    def test_help_text_explains_jobs_in_plain_english(self):
        help_text = shopify_sync.build_parser().format_help()

        self.assertIn("Run one Shopify catalog maintenance job at a time.", help_text)
        self.assertIn("safety and setup", help_text)
        self.assertIn("catalog import and cleanup", help_text)
        self.assertIn("collections", help_text)
        self.assertIn("store visibility", help_text)
        self.assertIn("photos and media", help_text)
        self.assertIn("Run this before any live delete, import, update, or media job.", help_text)
        self.assertIn("Run this when prices, stock, or costs changed in the sheets.", help_text)
        self.assertIn("Google & YouTube", help_text)
        self.assertIn("Draft and archived products are ignored.", help_text)
        self.assertIn("Run this when you want a conservative one-command recovery for missing images.", help_text)
        self.assertIn("Most job flags must be run by themselves.", help_text)

    def test_top_level_usage_text_mentions_two_publication_visibility_rule(self):
        self.assertIn("`Online Store` and `Google & YouTube` visibility", shopify_sync.__doc__)
        self.assertIn("for active products only", shopify_sync.__doc__)

    def test_gw_refresh_cache_runs_without_shopify_credentials(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--gw-refresh-cache"]), \
             mock.patch("shopify_sync.refresh_gw_cache") as refresh, \
             mock.patch("shopify_sync.load_env") as load_env:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        refresh.assert_called_once()
        load_env.assert_not_called()

    def test_gw_refresh_cache_rejects_invalid_combinations(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--gw-refresh-cache", "--preflight"]):
            with self.assertRaisesRegex(RuntimeError, "--gw-refresh-cache must run separately"):
                shopify_sync.main()

    def test_gw_build_archive_index_runs_without_shopify_credentials(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--gw-build-archive-index"]), \
             mock.patch("shopify_sync.warm_gw_official_archive_index") as warm, \
             mock.patch("shopify_sync.load_env") as load_env:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        warm.assert_called_once()
        load_env.assert_not_called()

    def test_gw_build_archive_index_rejects_invalid_combinations(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--gw-build-archive-index", "--preflight"]):
            with self.assertRaisesRegex(RuntimeError, "--gw-build-archive-index must run separately"):
                shopify_sync.main()

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

    def test_delete_collections_flag_runs_without_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--delete-collections"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_delete_collections") as phase_delete_collections, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_delete_collections.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()

    def test_delete_collections_rejects_import_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--delete-collections", "--import"]):
            with self.assertRaisesRegex(RuntimeError, "--delete-collections must run separately"):
                shopify_sync.main()

    def test_generate_collections_flag_runs_without_preflight(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--generate-collections"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_generate_collections") as phase_generate_collections, \
             mock.patch("shopify_sync.run_preflight") as run_preflight:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_generate_collections.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()

    def test_generate_collections_rejects_delete_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--generate-collections", "--delete"]):
            with self.assertRaisesRegex(RuntimeError, "--generate-collections must run separately"):
                shopify_sync.main()

    def test_generate_collections_rejects_dry_run(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--generate-collections", "--dry-run"]):
            with self.assertRaisesRegex(RuntimeError, "--generate-collections always applies live"):
                shopify_sync.main()

    def test_help_text_includes_description_backfill_flags(self):
        help_text = shopify_sync.build_parser().format_help()

        self.assertIn("--backfill-descriptions", help_text)
        self.assertIn("--backfill-descriptions-sku", help_text)
        self.assertIn("--backfill-descriptions-limit", help_text)
        self.assertIn("Rewrite existing Shopify product descriptions", help_text)
        self.assertIn("--backfill-country-of-origin", help_text)

    def test_help_text_describes_fixed_publications_for_image_visibility_reconcile(self):
        help_text = shopify_sync.build_parser().format_help()

        self.assertIn("--reconcile-online-store-image-visibility", help_text)
        self.assertIn("Online Store", help_text)
        self.assertIn("Google & YouTube", help_text)

    def test_backfill_descriptions_runs_without_prepare_or_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-descriptions"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
                 "OPENROUTER_API_KEY": "test-key",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_backfill_descriptions") as phase_backfill, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(
            client,
            {
                "SHOPIFY_STORE": "example-store",
                "SHOPIFY_TOKEN": "shpat_test",
                "OPENROUTER_API_KEY": "test-key",
            },
            dry=False,
            target_skus=[],
            limit=None,
        )
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()

    def test_backfill_descriptions_dry_run_bypasses_plain_preview_flow(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-descriptions", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
                 "OPENROUTER_API_KEY": "test-key",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_backfill_descriptions") as phase_backfill, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products, \
             mock.patch("shopify_sync.run_preflight") as run_preflight:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(
            client,
            {
                "SHOPIFY_STORE": "example-store",
                "SHOPIFY_TOKEN": "shpat_test",
                "OPENROUTER_API_KEY": "test-key",
            },
            dry=True,
            target_skus=[],
            limit=None,
        )
        prepare_products.assert_not_called()
        run_preflight.assert_not_called()

    def test_backfill_descriptions_passes_scope_flags_to_phase(self):
        client = mock.Mock()

        with mock.patch(
            "shopify_sync.sys.argv",
            [
                "shopify_sync.py",
                "--backfill-descriptions",
                "--backfill-descriptions-sku",
                "SKU-1",
                "--backfill-descriptions-sku",
                "SKU-2",
                "--backfill-descriptions-limit",
                "3",
            ],
        ), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
                 "OPENROUTER_API_KEY": "test-key",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_backfill_descriptions") as phase_backfill:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(
            client,
            {
                "SHOPIFY_STORE": "example-store",
                "SHOPIFY_TOKEN": "shpat_test",
                "OPENROUTER_API_KEY": "test-key",
            },
            dry=False,
            target_skus=["SKU-1", "SKU-2"],
            limit=3,
        )

    def test_backfill_descriptions_rejects_update_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-descriptions", "--update"]):
            with self.assertRaisesRegex(RuntimeError, "--backfill-descriptions must run separately"):
                shopify_sync.main()

    def test_backfill_descriptions_scope_flag_without_job_prints_help(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-descriptions-sku", "SKU-1"]), \
             mock.patch("shopify_sync.load_env") as load_env:
            result = shopify_sync.main()

        self.assertEqual(result, 1)
        load_env.assert_not_called()

    def test_backfill_descriptions_limit_must_be_positive(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-descriptions", "--backfill-descriptions-limit", "0"]):
            with self.assertRaisesRegex(RuntimeError, "--backfill-descriptions-limit must be greater than zero"):
                shopify_sync.main()

    def test_publish_online_store_backfill_runs_without_prepare_or_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--publish-online-store-backfill"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_publish_online_store_backfill") as phase_backfill, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()

    def test_publish_online_store_backfill_dry_run_bypasses_plain_preview_flow(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--publish-online-store-backfill", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_publish_online_store_backfill") as phase_backfill, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products, \
             mock.patch("shopify_sync.run_preflight") as run_preflight:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(client, dry=True)
        prepare_products.assert_not_called()
        run_preflight.assert_not_called()

    def test_publish_online_store_backfill_rejects_update_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--publish-online-store-backfill", "--update"]):
            with self.assertRaisesRegex(RuntimeError, "--publish-online-store-backfill must run separately"):
                shopify_sync.main()

    def test_backfill_country_of_origin_runs_without_prepare_or_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-country-of-origin"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_backfill_country_of_origin") as phase_backfill, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()

    def test_backfill_country_of_origin_dry_run_bypasses_plain_preview_flow(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-country-of-origin", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_backfill_country_of_origin") as phase_backfill, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products, \
             mock.patch("shopify_sync.run_preflight") as run_preflight:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_backfill.assert_called_once_with(client, dry=True)
        prepare_products.assert_not_called()
        run_preflight.assert_not_called()

    def test_backfill_country_of_origin_rejects_update_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--backfill-country-of-origin", "--update"]):
            with self.assertRaisesRegex(RuntimeError, "--backfill-country-of-origin must run separately"):
                shopify_sync.main()

    def test_reconcile_online_store_image_visibility_runs_without_prepare_or_location_lookup(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--reconcile-online-store-image-visibility"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_reconcile_online_store_image_visibility") as phase_reconcile, \
             mock.patch("shopify_sync.run_preflight") as run_preflight, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_reconcile.assert_called_once_with(client, dry=False)
        run_preflight.assert_not_called()
        prepare_products.assert_not_called()

    def test_reconcile_online_store_image_visibility_dry_run_bypasses_plain_preview_flow(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--reconcile-online-store-image-visibility", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_reconcile_online_store_image_visibility") as phase_reconcile, \
             mock.patch("shopify_sync.prepare_products_for_import") as prepare_products, \
             mock.patch("shopify_sync.run_preflight") as run_preflight:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_reconcile.assert_called_once_with(client, dry=True)
        prepare_products.assert_not_called()
        run_preflight.assert_not_called()

    def test_reconcile_online_store_image_visibility_dry_run_logs_two_publication_message(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--reconcile-online-store-image-visibility", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_reconcile_online_store_image_visibility"), \
             mock.patch("shopify_sync.log") as log_mock:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        logged_messages = [call.args[0] for call in log_mock.call_args_list]
        self.assertTrue(
            any(
                "Store image-visibility dry-run complete for Online Store and Google & YouTube." in message
                for message in logged_messages
            )
        )

    def test_reconcile_online_store_image_visibility_dry_run_logs_preview_message(self):
        client = mock.Mock()

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--reconcile-online-store-image-visibility", "--dry-run"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.phase_reconcile_online_store_image_visibility"), \
             mock.patch("shopify_sync.log") as log_mock:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        logged_messages = [call.args[0] for call in log_mock.call_args_list]
        self.assertTrue(
            any(
                "Store image-visibility dry-run complete for Online Store and Google & YouTube. Review "
                "online_store_image_visibility_preview.csv" in message
                for message in logged_messages
            )
        )

    def test_reconcile_online_store_image_visibility_rejects_backfill_combination(self):
        with mock.patch(
            "shopify_sync.sys.argv",
            ["shopify_sync.py", "--reconcile-online-store-image-visibility", "--publish-online-store-backfill"],
        ):
            with self.assertRaisesRegex(RuntimeError, "--publish-online-store-backfill must run separately"):
                shopify_sync.main()

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
        self.failures_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.failures_dir.cleanup)
        self.failures_patcher = mock.patch(
            "shopify_sync.GENERAL_FAILURES_TSV",
            new=Path(self.failures_dir.name) / "failures.tsv",
        )
        self.failures_patcher.start()
        self.addCleanup(self.failures_patcher.stop)

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
             mock.patch.object(self.client, "publish_to_online_store") as publish, \
             mock.patch("shopify_sync.UPDATE_PREVIEW_CSV",
                        new=Path(tempfile.gettempdir()) / "_tmp_update_preview.csv"):
            shopify_sync.phase_update(self.client, sheet, self.location, dry=True)
            upd.assert_not_called()
            set_qty.assert_not_called()
            publish.assert_not_called()

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
             mock.patch.object(self.client, "publish_to_online_store") as publish, \
             mock.patch("shopify_sync.UPDATE_PREVIEW_CSV",
                        new=Path(tempfile.gettempdir()) / "_tmp_update_preview.csv"):
            shopify_sync.phase_update(self.client, sheet, self.location, dry=False)

        # A: variant update (price) AND inventory set
        # B: variant update (cost via inventoryItem)
        # C: nothing
        self.assertEqual(upd.call_count, 2)
        self.assertEqual(set_qty.call_count, 1)
        self.assertEqual(publish.call_count, 2)
        # A's inventory was set
        set_qty.assert_called_once_with("i-A", self.location, 7)

    def test_live_run_does_not_publish_when_write_fails(self):
        sheet = [self._make_product("A", price=9.99, compare=12.00, cost=5.00, qty=7)]
        existing = [
            self._existing_record("A", price=8.00, compare=12.00, cost=5.00, on_hand=2,
                                  variant_id="v-A", inventory_item_id="i-A"),
        ]
        with mock.patch.object(self.client, "iter_existing_for_update",
                               return_value=iter(existing)), \
             mock.patch.object(self.client, "update_variant_fields", side_effect=RuntimeError("write failed")) as upd, \
             mock.patch.object(self.client, "set_on_hand") as set_qty, \
             mock.patch.object(self.client, "publish_to_online_store") as publish, \
             mock.patch("shopify_sync.UPDATE_PREVIEW_CSV",
                        new=Path(tempfile.gettempdir()) / "_tmp_update_preview.csv"):
            shopify_sync.phase_update(self.client, sheet, self.location, dry=False)

        upd.assert_called_once()
        set_qty.assert_not_called()
        publish.assert_not_called()


class PhaseImportTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.location = "gid://shopify/Location/9"
        self.failures_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.failures_dir.cleanup)
        self.failures_patcher = mock.patch(
            "shopify_sync.GENERAL_FAILURES_TSV",
            new=Path(self.failures_dir.name) / "failures.tsv",
        )
        self.failures_patcher.start()
        self.addCleanup(self.failures_patcher.stop)

    def _make_product(self, sku="SKU-1", title="Title 1"):
        return shopify_sync.Product(title=title, sku=sku, price=9.99, source="GW")

    def test_live_run_publishes_created_products(self):
        products = [self._make_product()]
        self.client.create_product.return_value = "gid://shopify/Product/7"

        shopify_sync.phase_import(self.client, products, self.location, dry=False)

        self.client.create_product.assert_called_once_with(products[0], self.location)
        self.client.publish_to_online_store.assert_called_once_with("gid://shopify/Product/7")

    def test_live_run_skips_publish_when_create_raises(self):
        products = [self._make_product()]
        self.client.create_product.side_effect = RuntimeError("create failed")

        shopify_sync.phase_import(self.client, products, self.location, dry=False)

        self.client.publish_to_online_store.assert_not_called()


class PhaseOnlineStoreBackfillTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.preview_path = Path(tempfile.gettempdir()) / "_tmp_online_store_backfill_preview.csv"
        self.failures_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.failures_dir.cleanup)
        self.failures_patcher = mock.patch(
            "shopify_sync.GENERAL_FAILURES_TSV",
            new=Path(self.failures_dir.name) / "failures.tsv",
        )
        self.failures_patcher.start()
        self.addCleanup(self.failures_patcher.stop)

    def test_dry_run_queries_candidates_without_publishing(self):
        self.client.get_publication_id_by_name.return_value = "gid://shopify/Publication/2"
        self.client.iter_products_unpublished_on_publication.return_value = iter([
            {
                "id": "gid://shopify/Product/1",
                "title": "Needs Publish",
                "published_on_publication": False,
                "publication_id": "gid://shopify/Publication/2",
                "skus": ["SKU-1"],
            }
        ])

        with mock.patch("shopify_sync.ONLINE_STORE_BACKFILL_PREVIEW_CSV", new=self.preview_path):
            shopify_sync.phase_publish_online_store_backfill(self.client, dry=True)

        self.client.publish_to_publication.assert_not_called()
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("dry_run_candidate", preview)
        self.assertIn("Needs Publish", preview)
        self.assertIn("Online Store", preview)

    def test_live_run_publishes_each_candidate(self):
        self.client.get_publication_id_by_name.return_value = "gid://shopify/Publication/2"
        self.client.iter_products_unpublished_on_publication.return_value = iter([
            {
                "id": "gid://shopify/Product/1",
                "title": "Needs Publish",
                "published_on_publication": False,
                "publication_id": "gid://shopify/Publication/2",
                "skus": ["SKU-1"],
            }
        ])

        with mock.patch("shopify_sync.ONLINE_STORE_BACKFILL_PREVIEW_CSV", new=self.preview_path):
            shopify_sync.phase_publish_online_store_backfill(self.client, dry=False)

        self.client.publish_to_publication.assert_called_once_with(
            "gid://shopify/Product/1",
            "gid://shopify/Publication/2",
        )
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("published", preview)


class PhaseCountryOfOriginBackfillTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.outputs_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.outputs_dir.cleanup)
        root = Path(self.outputs_dir.name)
        self.preview_path = root / "country_of_origin_backfill_preview.csv"
        self.verification_path = root / "country_of_origin_verification.csv"
        self.failures_path = root / "failures.tsv"
        self.output_patchers = [
            mock.patch("shopify_sync.COUNTRY_OF_ORIGIN_BACKFILL_PREVIEW_CSV", new=self.preview_path),
            mock.patch("shopify_sync.COUNTRY_OF_ORIGIN_VERIFICATION_CSV", new=self.verification_path),
            mock.patch("shopify_sync.GENERAL_FAILURES_TSV", new=self.failures_path),
        ]
        for patcher in self.output_patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _record(
        self,
        *,
        product_id="gid://shopify/Product/1",
        title="Captain",
        variant_id="gid://shopify/ProductVariant/1",
        inventory_item_id="gid://shopify/InventoryItem/1",
        sku="SKU-1",
        origin="US",
    ):
        return {
            "product_id": product_id,
            "title": title,
            "variant_id": variant_id,
            "inventory_item_id": inventory_item_id,
            "sku": sku,
            "country_of_origin": origin,
        }

    def test_dry_run_writes_preview_without_shopify_writes(self):
        self.client.iter_existing_for_country_of_origin_backfill.return_value = iter([
            self._record(origin="US"),
            self._record(
                product_id="gid://shopify/Product/2",
                title="No SKU Variant",
                variant_id="gid://shopify/ProductVariant/2",
                inventory_item_id="gid://shopify/InventoryItem/2",
                sku="",
                origin="",
            ),
            self._record(
                product_id="gid://shopify/Product/3",
                title="Missing Inventory",
                variant_id="gid://shopify/ProductVariant/3",
                inventory_item_id="",
                sku="SKU-3",
                origin="",
            ),
            self._record(
                product_id="gid://shopify/Product/4",
                title="Already UK",
                variant_id="gid://shopify/ProductVariant/4",
                inventory_item_id="gid://shopify/InventoryItem/4",
                sku="SKU-4",
                origin="GB",
            ),
        ])

        shopify_sync.phase_backfill_country_of_origin(self.client, dry=True)

        self.client.update_inventory_item_country_of_origin.assert_not_called()
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("dry_run_candidate", preview)
        self.assertIn("missing_inventory_item", preview)
        self.assertIn("already_uk", preview)
        self.assertIn("No SKU Variant", preview)
        self.assertFalse(self.verification_path.exists())

    def test_live_run_updates_candidates_and_writes_verification_artifact(self):
        initial_records = [
            self._record(origin="US"),
            self._record(
                product_id="gid://shopify/Product/2",
                title="Already UK",
                variant_id="gid://shopify/ProductVariant/2",
                inventory_item_id="gid://shopify/InventoryItem/2",
                sku="SKU-2",
                origin="GB",
            ),
        ]
        final_records = [
            self._record(origin="GB"),
            self._record(
                product_id="gid://shopify/Product/2",
                title="Already UK",
                variant_id="gid://shopify/ProductVariant/2",
                inventory_item_id="gid://shopify/InventoryItem/2",
                sku="SKU-2",
                origin="GB",
            ),
        ]
        self.client.iter_existing_for_country_of_origin_backfill.side_effect = [
            iter(initial_records),
            iter(final_records),
        ]

        shopify_sync.phase_backfill_country_of_origin(self.client, dry=False)

        self.client.update_inventory_item_country_of_origin.assert_called_once_with(
            "gid://shopify/InventoryItem/1",
            "GB",
        )
        preview = self.preview_path.read_text(encoding="utf-8")
        verification = self.verification_path.read_text(encoding="utf-8")
        self.assertIn("updated", preview)
        self.assertIn("verified_uk", verification)
        self.assertIn("after_origin", verification)

    def test_live_run_raises_when_non_uk_survivor_remains_after_verification(self):
        initial_records = [self._record(origin="US")]
        final_records = [self._record(origin="US")]
        self.client.iter_existing_for_country_of_origin_backfill.side_effect = [
            iter(initial_records),
            iter(final_records),
        ]

        with self.assertRaisesRegex(RuntimeError, "verification_failed=1"):
            shopify_sync.phase_backfill_country_of_origin(self.client, dry=False)

        verification = self.verification_path.read_text(encoding="utf-8")
        self.assertIn("verification_failed_non_uk", verification)


class PhaseDescriptionBackfillTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.env = {
            "SHOPIFY_STORE": "example-store",
            "SHOPIFY_TOKEN": "shpat_test",
            "OPENROUTER_API_KEY": "test-key",
        }
        self.outputs_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.outputs_dir.cleanup)
        root = Path(self.outputs_dir.name)
        self.preview_path = root / "description_backfill_preview.csv"
        self.review_path = root / "description_backfill_review.csv"
        self.failures_path = root / "description_backfill_failures.tsv"
        self.manifest_path = root / "description_backfill_manifest.json"
        self.output_patchers = [
            mock.patch("shopify_sync.DESCRIPTION_BACKFILL_PREVIEW_CSV", new=self.preview_path),
            mock.patch("shopify_sync.DESCRIPTION_BACKFILL_REVIEW_CSV", new=self.review_path),
            mock.patch("shopify_sync.DESCRIPTION_BACKFILL_FAILURES_TSV", new=self.failures_path),
        ]
        for patcher in self.output_patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _record(self, product_id="gid://shopify/Product/1", title="Captain", skus=None):
        return {
            "id": product_id,
            "title": title,
            "vendor": "Games Workshop",
            "product_type": "Warhammer 40,000",
            "description": "Old description",
            "description_html": "<p>Old description</p>",
            "skus": skus or ["SKU-1"],
        }

    def _candidate(self, url="https://www.waylandgames.co.uk/captain"):
        return shopify_sync.description_backfill.WaylandCandidate(
            page_url=url,
            title="Captain",
            description_text="Lead a veteran strike force into battle with this armoured commander.",
            sku_text="SKU-1",
            title_score=1.0,
        )

    def test_dry_run_scopes_records_before_network_work_and_writes_preview_only(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([
            self._record(product_id="gid://shopify/Product/1", title="Captain", skus=["SKU-1"]),
            self._record(product_id="gid://shopify/Product/2", title="Chaplain", skus=["SKU-2"]),
        ])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="unique_title_sku_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=self._candidate(),
             )) as resolve_source, \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=True,
                target_skus=["SKU-1"],
                limit=1,
                manifest_path=self.manifest_path,
            )

        self.client.update_product_description.assert_not_called()
        self.assertEqual(resolve_source.call_count, 1)
        preview = self.preview_path.read_text(encoding="utf-8")
        review = self.review_path.read_text(encoding="utf-8")
        self.assertIn("dry_run_candidate", preview)
        self.assertIn("sku_filter:limit", preview)
        self.assertIn("Captain", preview)
        self.assertNotIn("Chaplain", preview)
        self.assertEqual(review.strip().splitlines(), [",".join(shopify_sync.DESCRIPTION_BACKFILL_REVIEW_COLUMNS)])

    def test_live_run_updates_description_for_accepted_record(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="unique_title_sku_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=self._candidate(),
             )), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=False,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        self.client.update_product_description.assert_called_once_with(
            "gid://shopify/Product/1",
            "<p>Fresh copy for collectors. SKU: SKU-1.</p>",
        )
        preview = self.preview_path.read_text(encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertIn("updated", preview)
        self.assertEqual(manifest["gid://shopify/Product/1"]["state"], "completed")
        self.client.update_product_tags.assert_not_called()

    def test_missing_source_routes_record_to_review_without_shopify_write(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="review",
                 reason="no_confident_wayland_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=None,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=True,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        self.client.update_product_description.assert_not_called()
        review = self.review_path.read_text(encoding="utf-8")
        self.assertIn("no_confident_wayland_match", review)

    def test_rewrite_rejection_routes_record_to_review_without_shopify_write(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="unique_title_sku_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=self._candidate(),
             )), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Copied text"), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="review",
                 reason="rewrite_similarity_too_high",
                 source_text="source",
                 rewritten_text="Copied text SKU: SKU-1.",
                 similarity=0.98,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=False,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        self.client.update_product_description.assert_not_called()
        review = self.review_path.read_text(encoding="utf-8")
        self.assertIn("rewrite_similarity_too_high", review)

    def test_live_run_logs_update_failure_and_continues_to_next_record(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([
            self._record(product_id="gid://shopify/Product/1", title="Captain", skus=["SKU-1"]),
            self._record(product_id="gid://shopify/Product/2", title="Chaplain", skus=["SKU-2"]),
        ])
        self.client.update_product_description.side_effect = [RuntimeError("write broke"), None]

        source_resolutions = [
            shopify_sync.description_backfill.SourceResolution(
                status="accepted",
                reason="unique_title_sku_match",
                search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                candidate=self._candidate("https://www.waylandgames.co.uk/captain"),
            ),
            shopify_sync.description_backfill.SourceResolution(
                status="accepted",
                reason="unique_title_sku_match",
                search_url="https://www.waylandgames.co.uk/search?s=SKU-2",
                candidate=shopify_sync.description_backfill.WaylandCandidate(
                    page_url="https://www.waylandgames.co.uk/chaplain",
                    title="Chaplain",
                    description_text="Inspire the faithful with a veteran zealot who leads from the front line.",
                    sku_text="SKU-2",
                    title_score=1.0,
                ),
            ),
        ]
        rewrite_results = [
            shopify_sync.description_backfill.RewriteResult(
                status="accepted",
                reason="rewrite_passed",
                source_text="source",
                rewritten_text="Fresh copy one. SKU: SKU-1.",
                similarity=0.21,
                repaired_for_sku=False,
            ),
            shopify_sync.description_backfill.RewriteResult(
                status="accepted",
                reason="rewrite_passed",
                source_text="source",
                rewritten_text="Fresh copy two. SKU: SKU-2.",
                similarity=0.18,
                repaired_for_sku=False,
            ),
        ]

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", side_effect=source_resolutions), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", side_effect=["Fresh copy one.", "Fresh copy two."]), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", side_effect=rewrite_results):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=False,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        self.assertEqual(self.client.update_product_description.call_count, 2)
        failures = self.failures_path.read_text(encoding="utf-8")
        review = self.review_path.read_text(encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertIn("description_backfill\tgid://shopify/Product/1\tSKU-1\tCaptain\twrite broke", failures)
        self.assertIn("shopify_update_failed", review)
        self.assertEqual(manifest["gid://shopify/Product/1"]["state"], "failed")
        self.assertEqual(manifest["gid://shopify/Product/2"]["state"], "completed")

    def test_resume_completed_manifest_entry_skips_rewrite_and_shopify_update(self):
        policy_version = shopify_sync.description_backfill.current_description_backfill_policy_version(
            preview_columns=shopify_sync.DESCRIPTION_BACKFILL_PREVIEW_COLUMNS,
            review_columns=shopify_sync.DESCRIPTION_BACKFILL_REVIEW_COLUMNS,
        )
        self.manifest_path.write_text(json.dumps({
            "gid://shopify/Product/1": {
                "state": "completed",
                "policy_version": policy_version,
                "sku": "SKU-1",
                "source_url": "https://www.waylandgames.co.uk/captain",
                "source_reason": "unique_title_sku_match",
                "rewrite_reason": "rewrite_passed",
                "similarity": 0.22,
                "repaired_for_sku": False,
                "description_html": "<p>Fresh copy for collectors. SKU: SKU-1.</p>",
            }
        }), encoding="utf-8")
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source") as resolve_source, \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter") as rewrite:
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=False,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        resolve_source.assert_not_called()
        rewrite.assert_not_called()
        self.client.update_product_description.assert_not_called()
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("resume_completed", preview)

    def test_policy_version_mismatch_recomputes_stale_manifest_entry(self):
        self.manifest_path.write_text(json.dumps({
            "gid://shopify/Product/1": {
                "state": "completed",
                "policy_version": "dbv1-stale",
                "sku": "SKU-1",
                "source_url": "https://www.waylandgames.co.uk/captain",
            }
        }), encoding="utf-8")
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="unique_title_sku_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=self._candidate(),
             )) as resolve_source, \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=True,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        self.assertEqual(resolve_source.call_count, 1)
        preview = self.preview_path.read_text(encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertIn("dry_run_candidate", preview)
        self.assertNotIn("resume_completed", preview)
        self.assertNotEqual(manifest["gid://shopify/Product/1"]["policy_version"], "dbv1-stale")

    def test_games_workshop_fallback_is_recorded_in_preview_and_manifest(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="games_workshop_title_only_match",
                 search_url="https://html.duckduckgo.com/html/?q=Captain",
                 source_site="games_workshop",
                 candidate=shopify_sync.description_backfill.WaylandCandidate(
                     page_url="https://www.warhammer.com/en-GB/shop/space-marines-captain",
                     title="Space Marines Captain",
                     description_text="Lead a veteran strike force into battle with this heavily armoured commander.",
                     sku_text="SKU-1",
                     title_score=1.0,
                     source_site="games_workshop",
                 ),
             )), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=True,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        preview = self.preview_path.read_text(encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertIn("games_workshop", preview)
        self.assertEqual(manifest["gid://shopify/Product/1"]["source_site"], "games_workshop")

    def test_interrupt_still_flushes_completed_backfill_checkpoint(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([
            self._record(product_id="gid://shopify/Product/1", title="Captain", skus=["SKU-1"]),
            self._record(product_id="gid://shopify/Product/2", title="Chaplain", skus=["SKU-2"]),
        ])

        with mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", side_effect=[
                 shopify_sync.description_backfill.SourceResolution(
                     status="accepted",
                     reason="unique_title_sku_match",
                     search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                     candidate=self._candidate(),
                 ),
                 KeyboardInterrupt(),
             ]), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            with self.assertRaises(KeyboardInterrupt):
                shopify_sync.phase_backfill_descriptions(
                    self.client,
                    self.env,
                    dry=False,
                    target_skus=[],
                    limit=None,
                    manifest_path=self.manifest_path,
                )

        preview = self.preview_path.read_text(encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertIn("updated", preview)
        self.assertEqual(manifest["gid://shopify/Product/1"]["state"], "completed")
        self.assertNotIn("gid://shopify/Product/2", manifest)

    def test_backfill_logs_progress_when_checkpoint_boundary_is_hit(self):
        self.client.iter_existing_for_description_backfill.return_value = iter([self._record()])

        with mock.patch("shopify_sync.DESCRIPTION_BACKFILL_CHECKPOINT_EVERY", 1), \
             mock.patch("shopify_sync.log") as log_mock, \
             mock.patch("shopify_sync.description_backfill.require_openrouter_config"), \
             mock.patch("shopify_sync.description_backfill.resolve_preferred_source", return_value=shopify_sync.description_backfill.SourceResolution(
                 status="accepted",
                 reason="unique_title_sku_match",
                 search_url="https://www.waylandgames.co.uk/search?s=SKU-1",
                 candidate=self._candidate(),
             )), \
             mock.patch("shopify_sync.description_backfill.rewrite_with_openrouter", return_value="Fresh copy for collectors."), \
             mock.patch("shopify_sync.description_backfill.evaluate_rewrite", return_value=shopify_sync.description_backfill.RewriteResult(
                 status="accepted",
                 reason="rewrite_passed",
                 source_text="source",
                 rewritten_text="Fresh copy for collectors. SKU: SKU-1.",
                 similarity=0.22,
                 repaired_for_sku=True,
             )):
            shopify_sync.phase_backfill_descriptions(
                self.client,
                self.env,
                dry=True,
                target_skus=[],
                limit=None,
                manifest_path=self.manifest_path,
            )

        logged_messages = [call.args[0] for call in log_mock.call_args_list]
        self.assertTrue(
            any(message.startswith("DESCRIPTION BACKFILL progress: processed=1/1") for message in logged_messages)
        )


class PhaseOnlineStoreImageVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.preview_path = Path(tempfile.gettempdir()) / "_tmp_online_store_image_visibility_preview.csv"
        self.failures_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.failures_dir.cleanup)
        self.failures_patcher = mock.patch(
            "shopify_sync.GENERAL_FAILURES_TSV",
            new=Path(self.failures_dir.name) / "failures.tsv",
        )
        self.failures_patcher.start()
        self.addCleanup(self.failures_patcher.stop)

    def test_dry_run_reports_publish_and_unpublish_candidates_only(self):
        self.client.get_publication_id_by_name.return_value = "gid://shopify/Publication/2"
        self.client.iter_products_for_online_store_image_visibility.return_value = iter([
            {
                "id": "gid://shopify/Product/1",
                "title": "Needs Publish",
                "published_on_publication": False,
                "publication_id": "gid://shopify/Publication/2",
                "has_media": True,
                "skus": ["PUB-1"],
            },
            {
                "id": "gid://shopify/Product/2",
                "title": "Needs Unpublish",
                "published_on_publication": True,
                "publication_id": "gid://shopify/Publication/2",
                "has_media": False,
                "skus": ["UNPUB-1"],
            },
            {
                "id": "gid://shopify/Product/3",
                "title": "Already Hidden",
                "published_on_publication": False,
                "publication_id": "gid://shopify/Publication/2",
                "has_media": False,
                "skus": ["SKIP-1"],
            },
        ])

        with mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=self.preview_path):
            shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=True)

        self.client.publish_to_publication.assert_not_called()
        self.client.unpublish_from_publication.assert_not_called()
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("dry_run_publish", preview)
        self.assertIn("dry_run_unpublish", preview)
        self.assertNotIn("Already Hidden", preview)

    def test_dry_run_looks_up_both_fixed_publications_before_any_iteration_or_preview_write(self):
        preview_path = Path(self.failures_dir.name) / "image_visibility_preview.csv"

        def lookup(name):
            if name == "Online Store":
                return "gid://shopify/Publication/online"
            if name == "Google & YouTube":
                raise RuntimeError("missing Google & YouTube publication")
            raise AssertionError(f"unexpected publication lookup: {name}")

        self.client.get_publication_id_by_name.side_effect = lookup

        with self.assertRaisesRegex(RuntimeError, "missing Google & YouTube publication"), \
             mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=preview_path):
            shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=True)

        self.client.iter_products_for_online_store_image_visibility.assert_not_called()
        self.client.publish_to_publication.assert_not_called()
        self.client.unpublish_from_publication.assert_not_called()
        self.assertFalse(preview_path.exists())

    def test_dry_run_writes_only_action_rows_across_fixed_publication_pairs(self):
        self.client.get_publication_id_by_name.side_effect = [
            "gid://shopify/Publication/online",
            "gid://shopify/Publication/google",
        ]
        self.client.iter_products_for_online_store_image_visibility.side_effect = [
            iter([
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Online Needs Publish",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/online",
                    "has_media": True,
                    "skus": ["PAIR-PUB-1"],
                },
                {
                    "id": "gid://shopify/Product/2",
                    "title": "Online Already Hidden",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/online",
                    "has_media": False,
                    "skus": ["PAIR-SKIP-1"],
                },
            ]),
            iter([
                {
                    "id": "gid://shopify/Product/3",
                    "title": "Google Needs Unpublish",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/google",
                    "has_media": False,
                    "skus": ["PAIR-UNPUB-1"],
                },
                {
                    "id": "gid://shopify/Product/4",
                    "title": "Google Already Visible",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/google",
                    "has_media": True,
                    "skus": ["PAIR-SKIP-2"],
                },
            ]),
        ]

        with mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=self.preview_path), \
             mock.patch("shopify_sync.log") as log_mock:
            shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=True)

        self.assertEqual(
            self.client.iter_products_for_online_store_image_visibility.call_args_list,
            [
                mock.call("gid://shopify/Publication/online"),
                mock.call("gid://shopify/Publication/google"),
            ],
        )
        with self.preview_path.open(encoding="utf-8") as fh:
            preview_rows = list(csv.DictReader(fh))

        self.assertEqual(len(preview_rows), 2)
        self.assertEqual(
            {row["title"] for row in preview_rows},
            {"Online Needs Publish", "Google Needs Unpublish"},
        )
        self.assertEqual(
            {row["publication_name"] for row in preview_rows},
            {"Online Store", "Google & YouTube"},
        )
        self.assertEqual(
            {row["status"] for row in preview_rows},
            {"dry_run_publish", "dry_run_unpublish"},
        )
        logged_messages = [call.args[0] for call in log_mock.call_args_list]
        self.assertTrue(
            any(
                "STORE IMAGE VISIBILITY summary:" in message
                and "candidates=4" in message
                and "actions=2" in message
                and "unchanged=2" in message
                for message in logged_messages
            )
        )

    def test_live_run_publishes_and_unpublishes_mismatches(self):
        self.client.get_publication_id_by_name.side_effect = [
            "gid://shopify/Publication/online",
            "gid://shopify/Publication/google",
        ]
        self.client.iter_products_for_online_store_image_visibility.side_effect = [
            iter([
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Needs Publish",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/online",
                    "has_media": True,
                    "skus": ["PUB-1"],
                },
                {
                    "id": "gid://shopify/Product/2",
                    "title": "Needs Unpublish",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/online",
                    "has_media": False,
                    "skus": ["UNPUB-1"],
                },
            ]),
            iter([
                {
                    "id": "gid://shopify/Product/3",
                    "title": "Needs Google Publish",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/google",
                    "has_media": True,
                    "skus": ["GOOG-1"],
                },
                {
                    "id": "gid://shopify/Product/4",
                    "title": "Needs Google Unpublish",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/google",
                    "has_media": False,
                    "skus": ["GOOG-2"],
                },
            ]),
        ]

        with mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=self.preview_path):
            shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=False)

        self.assertEqual(
            self.client.publish_to_publication.call_args_list,
            [
                mock.call("gid://shopify/Product/1", "gid://shopify/Publication/online"),
                mock.call("gid://shopify/Product/3", "gid://shopify/Publication/google"),
            ],
        )
        self.assertEqual(
            self.client.unpublish_from_publication.call_args_list,
            [
                mock.call("gid://shopify/Product/2", "gid://shopify/Publication/online"),
                mock.call("gid://shopify/Product/4", "gid://shopify/Publication/google"),
            ],
        )
        preview = self.preview_path.read_text(encoding="utf-8")
        self.assertIn("published", preview)
        self.assertIn("unpublished", preview)
        self.assertIn("Google & YouTube", preview)

    def test_live_run_continues_after_publish_and_unpublish_failures(self):
        self.client.get_publication_id_by_name.side_effect = [
            "gid://shopify/Publication/online",
            "gid://shopify/Publication/google",
        ]
        self.client.iter_products_for_online_store_image_visibility.side_effect = [
            iter([
                {
                    "id": "gid://shopify/Product/1",
                    "title": "Publish Failure",
                    "published_on_publication": False,
                    "publication_id": "gid://shopify/Publication/online",
                    "has_media": True,
                    "skus": ["PUB-FAIL"],
                },
            ]),
            iter([
                {
                    "id": "gid://shopify/Product/2",
                    "title": "Unpublish Failure",
                    "published_on_publication": True,
                    "publication_id": "gid://shopify/Publication/google",
                    "has_media": False,
                    "skus": ["UNPUB-FAIL"],
                },
            ]),
        ]
        self.client.publish_to_publication.side_effect = RuntimeError("publish broke")
        self.client.unpublish_from_publication.side_effect = RuntimeError("unpublish broke")

        with tempfile.TemporaryDirectory() as tmp:
            preview_path = Path(tmp) / "preview.csv"
            failures_path = Path(tmp) / "failures.tsv"
            with mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=preview_path), \
                 mock.patch("shopify_sync.GENERAL_FAILURES_TSV", new=failures_path):
                shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=False)

            with preview_path.open(encoding="utf-8") as fh:
                preview_rows = list(csv.DictReader(fh))
            failures = failures_path.read_text(encoding="utf-8")

        self.assertEqual(
            {row["status"] for row in preview_rows},
            {"publish_failed: publish broke", "unpublish_failed: unpublish broke"},
        )
        self.assertIn("image_visibility_publish:Online Store\tPUB-FAIL\tPublish Failure\tpublish broke", failures)
        self.assertIn(
            "image_visibility_unpublish:Google & YouTube\tUNPUB-FAIL\tUnpublish Failure\tunpublish broke",
            failures,
        )

    def test_exports_sanitize_formula_like_titles_and_skus(self):
        self.client.get_publication_id_by_name.return_value = "gid://shopify/Publication/2"
        self.client.iter_products_for_online_store_image_visibility.return_value = iter([
            {
                "id": "gid://shopify/Product/1",
                "title": "=cmd",
                "published_on_publication": False,
                "publication_id": "gid://shopify/Publication/2",
                "has_media": True,
                "skus": ["@SKU"],
            },
        ])
        self.client.publish_to_publication.side_effect = RuntimeError("-boom")

        with tempfile.TemporaryDirectory() as tmp:
            preview_path = Path(tmp) / "preview.csv"
            failures_path = Path(tmp) / "failures.tsv"
            with mock.patch("shopify_sync.ONLINE_STORE_IMAGE_VISIBILITY_PREVIEW_CSV", new=preview_path), \
                 mock.patch("shopify_sync.GENERAL_FAILURES_TSV", new=failures_path):
                shopify_sync.phase_reconcile_online_store_image_visibility(self.client, dry=False)

            preview = preview_path.read_text(encoding="utf-8")
            failures = failures_path.read_text(encoding="utf-8")

        self.assertIn("'=cmd", preview)
        self.assertIn("'@SKU", preview)
        self.assertIn("'@SKU", failures)
        self.assertIn("'=cmd", failures)
        self.assertIn("'-boom", failures)


class PhaseDeleteCollectionsTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()

    def _collection(self, collection_id, title, handle, collection_type):
        return {
            "id": collection_id,
            "title": title,
            "handle": handle,
            "collection_type": collection_type,
        }

    def test_dry_run_logs_collections_without_deleting(self):
        self.client.iter_all_collections.return_value = iter([
            self._collection("gid://shopify/Collection/1", "Wargames", "wargames", "custom"),
            self._collection("gid://shopify/Collection/2", "Plush Figures", "plush-figures", "smart"),
        ])

        shopify_sync.phase_delete_collections(self.client, dry=True)

        self.client.delete_collection.assert_not_called()

    def test_live_run_deletes_each_collection(self):
        self.client.iter_all_collections.return_value = iter([
            {
                "id": "gid://shopify/Collection/1",
                "title": "Games Workshop",
                "handle": "games-workshop",
                "products_count": 10,
                "collection_type": "smart",
                "rules": [
                    {
                        "column": "TAG",
                        "relation": "EQUALS",
                        "condition": "AUTO_COLLECTION::games-workshop",
                    }
                ],
            },
            {
                "id": "gid://shopify/Collection/2",
                "title": "Wargames",
                "handle": "wargames",
                "products_count": 4,
                "collection_type": "custom",
                "rules": [],
            },
        ])

        shopify_sync.phase_delete_collections(self.client, dry=False)

        self.assertEqual(
            [call.args[0] for call in self.client.delete_collection.call_args_list],
            ["gid://shopify/Collection/1", "gid://shopify/Collection/2"],
        )


class CollectionClassificationTests(unittest.TestCase):
    def _record(self, product_id, title, vendor, product_type="", tags=None, created_at="2026-01-01T00:00:00Z"):
        tags = tags or []
        return {
            "id": product_id,
            "title": title,
            "vendor": vendor,
            "product_type": product_type,
            "tags": tags,
            "created_at": created_at,
            "skus": [product_id],
            "search_text": shopify_sync._normalize_search_text(" ".join([title, vendor, product_type, *tags, product_id])),
        }

    def test_build_collection_matches_assigns_expected_buckets(self):
        products = [
            self._record(
                "gw-1",
                "Tyranid Combat Patrol",
                "Games Workshop",
                "Generic",
                ["Games Workshop"],
                created_at="2026-04-28T10:00:00Z",
            ),
            self._record(
                "board-1",
                "Disney Lorcana Starter Deck",
                "Ravensburger",
                "Ravensburger",
                ["Ravensburger"],
            ),
            self._record(
                "book-1",
                "General Fiction Book",
                "Simon & Schuster",
                "Simon & Schuster",
                ["Simon & Schuster"],
            ),
        ]
        products[0]["description"] = ""
        products[0]["total_inventory"] = 3
        products[0]["min_price_cents"] = 14000
        products[0]["is_price_reduced"] = False
        products[1]["description"] = ""
        products[1]["total_inventory"] = 6
        products[1]["min_price_cents"] = 1699
        products[1]["is_price_reduced"] = False
        products[2]["description"] = ""
        products[2]["total_inventory"] = 0
        products[2]["min_price_cents"] = 7000
        products[2]["is_price_reduced"] = False

        by_collection, unmatched, desired_tags_by_product = shopify_sync.build_collection_matches(products)

        self.assertEqual(desired_tags_by_product["gw-1"], {"warhammer-40k", "tyranids", "combat-patrol", "new-release", "new-arrival"})
        self.assertEqual([item["id"] for item in by_collection["Warhammer 40,000"]], ["gw-1"])
        self.assertEqual([item["id"] for item in by_collection["Tyranids"]], ["gw-1"])
        self.assertEqual([item["id"] for item in by_collection["Combat Patrols"]], ["gw-1"])
        self.assertEqual([item["id"] for item in by_collection["Disney Lorcana"]], ["board-1"])
        self.assertEqual([item["id"] for item in by_collection["Ravensburger"]], ["board-1"])
        self.assertEqual([item["id"] for item in unmatched], ["book-1"])


class PhaseGenerateCollectionsTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()

    def test_dry_run_is_rejected(self):
        products = [
            {
                "id": "gw-1",
                "title": "Tyranid Combat Patrol",
                "vendor": "Games Workshop",
                "product_type": "Generic",
                "tags": ["Games Workshop"],
                "created_at": "2026-04-29T10:00:00Z",
                "description": "",
                "total_inventory": 2,
                "min_price_cents": 14000,
                "is_price_reduced": False,
                "skus": ["gw-1"],
                "search_text": shopify_sync._normalize_search_text("Tyranid Combat Patrol Games Workshop Generic"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([])

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            with self.assertRaisesRegex(RuntimeError, "cannot be used with --dry-run"):
                shopify_sync.phase_generate_collections(self.client, dry=True)

        self.client.create_smart_collection.assert_not_called()
        self.client.update_product_tags.assert_not_called()

    def test_live_run_rebuilds_taxonomy_and_sets_images(self):
        products = [
            {
                "id": "gw-1",
                "title": "Tyranid Combat Patrol",
                "vendor": "Games Workshop",
                "product_type": "Generic",
                "tags": ["Games Workshop", "AUTO_COLLECTION::games-workshop", "Warhammer 40K", "keep-me"],
                "created_at": "2026-04-29T10:00:00Z",
                "description": "",
                "total_inventory": 2,
                "min_price_cents": 14000,
                "is_price_reduced": False,
                "skus": ["gw-1"],
                "search_text": shopify_sync._normalize_search_text("Tyranid Combat Patrol Games Workshop Generic"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([
            {
                "id": "gid://shopify/Collection/1",
                "title": "Legacy",
                "handle": "legacy",
                "products_count": 1,
                "collection_type": "smart",
                "rules": [],
            }
        ])
        self.client.create_smart_collection.side_effect = lambda title, handle, rules, **kwargs: {
            "id": f"gid://shopify/Collection/{handle}",
            "title": title,
            "handle": handle,
        }
        self.client.publish_to_all_channels.return_value = 2
        self.client.find_first_alphabetical_product_with_image.return_value = {
            "product_id": "gid://shopify/Product/100",
            "product_title": "Tyranid Combat Patrol",
            "image_url": "https://cdn.shopify.com/tyranid.jpg",
            "image_alt": "",
        }
        self.client.get_collection_image.return_value = {"url": "", "alt_text": ""}

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            shopify_sync.phase_generate_collections(self.client, dry=False)

        self.client.update_product_tags.assert_called_once_with(
            "gw-1",
            ["combat-patrol", "keep-me", "new-release", "tyranids", "warhammer-40k"],
        )
        self.client.delete_collection.assert_called_once_with("gid://shopify/Collection/1")
        self.assertFalse(self.client.update_smart_collection.called)
        self.client.update_collection_image.assert_any_call(
            "gid://shopify/Collection/warhammer-40k",
            "https://cdn.shopify.com/tyranid.jpg",
            alt_text="Warhammer 40,000",
        )
        created_titles = {call.args[0] for call in self.client.create_smart_collection.call_args_list}
        self.assertIn("Warhammer 40,000", created_titles)
        self.assertIn("Tyranids", created_titles)
        self.assertIn("Combat Patrols", created_titles)
        self.assertIn("Latest releases", created_titles)
        self.assertIn("Games Workshop", created_titles)

    def test_live_run_skips_empty_and_manual_collections(self):
        products = [
            {
                "id": "board-1",
                "title": "Disney Lorcana Starter Deck",
                "vendor": "Ravensburger",
                "product_type": "Card Game",
                "tags": ["Ravensburger"],
                "created_at": "2026-02-01T10:00:00Z",
                "description": "",
                "total_inventory": 1,
                "min_price_cents": 1699,
                "is_price_reduced": False,
                "skus": ["board-1"],
                "search_text": shopify_sync._normalize_search_text("Disney Lorcana Starter Deck Ravensburger Card Game"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([])
        self.client.create_smart_collection.side_effect = lambda title, handle, rules, **kwargs: {
            "id": f"gid://shopify/Collection/{handle}",
            "title": title,
            "handle": handle,
        }
        self.client.publish_to_all_channels.return_value = 1
        self.client.find_first_alphabetical_product_with_image.return_value = {}

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            shopify_sync.phase_generate_collections(self.client, dry=False)

        created_titles = {call.args[0] for call in self.client.create_smart_collection.call_args_list}
        self.assertIn("Disney Lorcana", created_titles)
        self.assertIn("Ravensburger", created_titles)
        self.assertNotIn("Warhammer 40,000", created_titles)
        self.assertNotIn("Bestsellers", created_titles)


class PhotoAssetMatchingTests(unittest.TestCase):
    def test_extract_asset_match_code_handles_split_leading_codes(self):
        self.assertEqual(
            shopify_sync._extract_asset_match_code("985-47709-Pop-Vinyl-Batman-1989-Joker-w-Hat-w-Chase"),
            "985 47709",
        )
        self.assertEqual(
            shopify_sync._extract_asset_match_code("SMX 220 My First Safari Animals"),
            "SMX 220",
        )
        self.assertEqual(
            shopify_sync._extract_asset_match_code("77771115TQ2-robo-alive-dino-fossil-find-egg"),
            "77771115TQ2",
        )

    def test_photo_source_is_book_product_uses_expanded_vendor_allowlist(self):
        self.assertTrue(
            shopify_sync.photo_source_is_book_product(
                shopify_sync.Product(title="Demon Slayer #1", sku="978-1974700523", vendor="VIZ Media LLC")
            )
        )
        self.assertTrue(
            shopify_sync.photo_source_is_book_product(
                shopify_sync.Product(title="Dog Man", sku="978-1-338-23064-2", vendor="Scholastic Inc.")
            )
        )
        self.assertTrue(
            shopify_sync.photo_source_is_book_product(
                shopify_sync.Product(title="Neuromancer", sku="978-1473217386", vendor="Orion Books")
            )
        )
        self.assertTrue(
            shopify_sync.photo_source_is_book_product(
                shopify_sync.Product(title="Never Lie", sku="978-1464221361", vendor="Poisoned Pen Press")
            )
        )
        self.assertFalse(
            shopify_sync.photo_source_is_book_product(
                shopify_sync.Product(title="Plus-Plus BIG Basic / 100 pcs", sku="5710410000000", vendor="PlusPlus")
            )
        )

    def test_resolve_photo_asset_prefers_exact_code_then_slug_fallback(self):
        exact = shopify_sync.PhotoAssetSet(
            key="dir:exact",
            label="TR-39-13-99120109017-Armageddon-Battalion-Deathwatch",
            product_code="99120109017",
            title_slug="armageddon-battalion-deathwatch",
        )
        fallback = shopify_sync.PhotoAssetSet(
            key="dir:fallback",
            label="Armageddon-Battalion-Deathwatch",
            title_slug="armageddon-battalion-deathwatch",
        )
        product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            source="GW",
        )

        status, match_type, asset_set, reason = shopify_sync.resolve_photo_asset(
            product,
            {"99120109017": [exact]},
            {"armageddon-battalion-deathwatch": [fallback]},
        )

        self.assertEqual((status, match_type, asset_set, reason), ("replace", "exact", exact, ""))

    def test_resolve_photo_asset_marks_ambiguous_slug_fallback(self):
        product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            source="GW",
        )
        options = [
            shopify_sync.PhotoAssetSet(key="a", label="A", title_slug="armageddon-battalion-deathwatch"),
            shopify_sync.PhotoAssetSet(key="b", label="B", title_slug="armageddon-battalion-deathwatch"),
        ]

        status, match_type, asset_set, reason = shopify_sync.resolve_photo_asset(
            product,
            {},
            {"armageddon-battalion-deathwatch": options},
        )

        self.assertEqual(status, "skip")
        self.assertEqual(match_type, "ambiguous")
        self.assertIsNone(asset_set)
        self.assertIn("multiple title-slug matches", reason)

    def test_resolve_photo_asset_prefers_best_exact_match_when_duplicate_codes_exist(self):
        product = shopify_sync.Product(
            title="GETTING STARTED WITH WARHAMMER 40K (ENG)",
            sku="60040199169",
            source="GW",
        )
        better = shopify_sync.PhotoAssetSet(
            key="better",
            label="60040199169-Gtting-Started-With-Warhammer-40k-ENG",
            product_code="60040199169",
            title_slug="gtting-started-with-warhammer-40k-eng",
        )
        worse = shopify_sync.PhotoAssetSet(
            key="worse",
            label="60040199169-EngWH40KGettingStartedWithTenthEdition",
            product_code="60040199169",
            title_slug="engwh40kgettingstartedwithtenthedition",
        )

        status, match_type, asset_set, reason = shopify_sync.resolve_photo_asset(
            product,
            {"60040199169": [worse, better]},
            {},
        )

        self.assertEqual(status, "replace")
        self.assertEqual(match_type, "exact_best")
        self.assertEqual(asset_set, better)
        self.assertEqual(reason, "")

    def test_resolve_photo_asset_matches_split_code_variants(self):
        product = shopify_sync.Product(
            title="Pop! Vinyl - Batman 1989 - Joker w/Hat w/Chase",
            sku="985 47709",
            source="INV",
        )
        asset_set = shopify_sync.PhotoAssetSet(
            key="dir:funko",
            label="985-47709-Pop-Vinyl-Batman-1989-Joker-w-Hat-w-Chase",
            product_code="985 47709",
            title_slug="pop-vinyl-batman-1989-joker-w-hat-w-chase",
        )

        status, match_type, matched, reason = shopify_sync.resolve_photo_asset(
            product,
            shopify_sync.build_photo_indexes([asset_set])[0],
            {},
        )

        self.assertEqual((status, match_type, matched, reason), ("replace", "exact", asset_set, ""))

    def test_discover_photo_asset_sets_skips_macosx_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "99120109017-ArmageddonBattalionDeathwatch"
            folder.mkdir()
            (folder / "real.jpg").write_bytes(b"image")
            (folder / "MACOSX-real.jpg").write_bytes(b"artifact")
            hidden = root / "__MACOSX"
            hidden.mkdir()
            (hidden / "ignored.jpg").write_bytes(b"artifact")

            asset_sets = shopify_sync.discover_photo_asset_sets(root)

        self.assertEqual(len(asset_sets), 1)
        self.assertEqual([path.name for path in asset_sets[0].image_paths], ["real.jpg"])

    def test_discover_photo_asset_sets_indexes_sku_prefixed_staged_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "PKM-001-pokemon-booster-box"
            folder.mkdir()
            (folder / "01.jpg").write_bytes(b"image")

            asset_set = shopify_sync.discover_photo_asset_sets(root)[0]

        self.assertEqual(
            shopify_sync._canonical_match_code(asset_set.product_code),
            shopify_sync._canonical_match_code("PKM-001"),
        )
        self.assertEqual(asset_set.title_slug, "pokemon-booster-box")

    def test_photo_asset_fingerprint_ignores_mtime_for_same_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "99120109017-ArmageddonBattalionDeathwatch"
            folder.mkdir()
            image = folder / "01.jpg"
            image.write_bytes(b"same-bytes")
            asset_set = shopify_sync.discover_photo_asset_sets(root)[0]
            first = asset_set.fingerprint()
            os.utime(image, (image.stat().st_atime, image.stat().st_mtime + 60))
            second = shopify_sync.discover_photo_asset_sets(root)[0].fingerprint()

        self.assertEqual(first, second)

    def test_photo_asset_fingerprint_changes_when_bytes_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "99120109017-ArmageddonBattalionDeathwatch"
            folder.mkdir()
            image = folder / "01.jpg"
            image.write_bytes(b"same-bytes")
            first = shopify_sync.discover_photo_asset_sets(root)[0].fingerprint()
            image.write_bytes(b"changed-bytes")
            second = shopify_sync.discover_photo_asset_sets(root)[0].fingerprint()

        self.assertNotEqual(first, second)


class PhotoSyncPhaseTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            source="GW",
        )
        self.existing = {
            "product_id": "gid://shopify/Product/1",
            "title": self.product.title,
            "vendor": "Games Workshop",
            "tags": ["Games Workshop"],
            "sku": self.product.sku,
            "media_ids": ["gid://shopify/MediaImage/old1", "gid://shopify/MediaImage/old2"],
        }
        self.existing_files = [
            shopify_sync.ShopifyImageFile(
                id="gid://shopify/MediaImage/new1",
                filename="99120109017_cover_a.jpg",
                product_code=self.product.sku,
                title_slug="armageddon-battalion-deathwatch",
                file_status="READY",
            ),
            shopify_sync.ShopifyImageFile(
                id="gid://shopify/MediaImage/new2",
                filename="99120109017_back_b.jpg",
                product_code=self.product.sku,
                title_slug="armageddon-battalion-deathwatch",
                file_status="READY",
            ),
        ]

    def _make_photo_root(self, folder_name: str = "TR-39-13-99120109017-Armageddon-Battalion-Deathwatch") -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        folder = root / folder_name
        folder.mkdir()
        (folder / "01.jpg").write_bytes(b"image-1")
        (folder / "02.jpg").write_bytes(b"image-2")
        return root

    def _make_non_gw_photo_sync_fixture(self) -> tuple[shopify_sync.Product, dict[str, object], Path]:
        product = shopify_sync.Product(
            title="Pokemon Booster Box",
            sku="PKM-001",
            vendor="Pokemon",
            source="INV",
        )
        existing = {
            "product_id": "gid://shopify/Product/2",
            "title": product.title,
            "vendor": "Pokemon",
            "tags": ["Pokemon"],
            "sku": product.sku,
            "media_ids": ["gid://shopify/MediaImage/old9"],
        }
        return product, existing, self._make_photo_root("Pokemon-Booster-Box")

    @contextmanager
    def _patched_photo_sync_outputs(self, photo_root: Path):
        with mock.patch("shopify_sync.PHOTO_SYNC_PREVIEW_CSV", new=photo_root / "preview.csv"), \
             mock.patch("shopify_sync.PHOTO_SYNC_MISSING_TSV", new=photo_root / "missing.tsv"), \
             mock.patch("shopify_sync.PHOTO_SYNC_AMBIGUOUS_TSV", new=photo_root / "ambiguous.tsv"), \
             mock.patch("shopify_sync.PHOTO_SYNC_FAILURES_TSV", new=photo_root / "failures.tsv"):
            yield

    def test_photo_sync_dry_run_writes_preview_and_makes_no_writes(self):
        photo_root = self._make_photo_root()
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=True,
                manifest_path=photo_root / "manifest.json",
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.file_create.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        self.client.reorder_product_media.assert_not_called()
        self.client.detach_files_from_product.assert_not_called()

    def test_photo_sync_existing_files_dry_run_writes_preview_and_makes_no_writes(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        self.client.iter_shopify_image_files_for_photo_sync.return_value = iter(self.existing_files)

        with tempfile.TemporaryDirectory() as tmp, self._patched_photo_sync_outputs(Path(tmp)):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                None,
                dry=True,
                manifest_path=Path(tmp) / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.file_create.assert_not_called()
        self.client.wait_for_files_ready.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        self.client.reorder_product_media.assert_not_called()
        self.client.detach_files_from_product.assert_not_called()

    def test_photo_sync_live_run_uses_file_first_sequence(self):
        photo_root = self._make_photo_root()
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        self.client.staged_uploads_create.return_value = [
            {"url": "https://upload/1", "resourceUrl": "https://resource/1", "parameters": []},
            {"url": "https://upload/2", "resourceUrl": "https://resource/2", "parameters": []},
        ]
        self.client.file_create.return_value = [
            {"id": "gid://shopify/MediaImage/new1", "fileStatus": "UPLOADED"},
            {"id": "gid://shopify/MediaImage/new2", "fileStatus": "UPLOADED"},
        ]

        calls = []
        self.client.upload_file_to_staged_target.side_effect = lambda path, target: calls.append(("upload", path.name)) or target["resourceUrl"]
        self.client.wait_for_files_ready.side_effect = lambda ids, **kwargs: calls.append(("ready", tuple(ids))) or ids
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append(("detach", tuple(ids), pid))

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
            )

        self.assertEqual(
            calls,
            [
                ("upload", "01.jpg"),
                ("upload", "02.jpg"),
                ("ready", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2")),
                ("attach", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2"), "gid://shopify/Product/1"),
                ("reorder", "gid://shopify/Product/1", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2")),
                ("detach", ("gid://shopify/MediaImage/old1", "gid://shopify/MediaImage/old2"), "gid://shopify/Product/1"),
            ],
        )
        manifest = json.loads((photo_root / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest[self.product.sku]["state"], "completed")

    def test_photo_sync_existing_files_live_run_attaches_without_uploading(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        self.client.iter_shopify_image_files_for_photo_sync.return_value = iter(self.existing_files)

        calls = []
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append(("detach", tuple(ids), pid))

        with tempfile.TemporaryDirectory() as tmp, self._patched_photo_sync_outputs(Path(tmp)):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                None,
                dry=False,
                manifest_path=Path(tmp) / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            )

            manifest = json.loads((Path(tmp) / "manifest.json").read_text(encoding="utf-8"))

        self.client.staged_uploads_create.assert_not_called()
        self.client.file_create.assert_not_called()
        self.client.wait_for_files_ready.assert_not_called()
        self.assertEqual(
            calls,
            [
                ("attach", ("gid://shopify/MediaImage/new2", "gid://shopify/MediaImage/new1"), "gid://shopify/Product/1"),
                ("reorder", "gid://shopify/Product/1", ("gid://shopify/MediaImage/new2", "gid://shopify/MediaImage/new1")),
                ("detach", ("gid://shopify/MediaImage/old1", "gid://shopify/MediaImage/old2"), "gid://shopify/Product/1"),
            ],
        )
        self.assertEqual(manifest[self.product.sku]["state"], "completed")
        self.assertEqual(manifest[self.product.sku]["source_mode"], shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING)

    def test_photo_sync_existing_files_all_allows_non_gw_products(self):
        non_gw_product = shopify_sync.Product(
            title="Pokemon Booster Box",
            sku="PKM-001",
            vendor="Pokemon",
            source="INV",
        )
        non_gw_existing = {
            "product_id": "gid://shopify/Product/2",
            "title": non_gw_product.title,
            "vendor": "Pokemon",
            "tags": ["Pokemon"],
            "sku": non_gw_product.sku,
            "media_ids": ["gid://shopify/MediaImage/old9"],
        }
        non_gw_files = [
            shopify_sync.ShopifyImageFile(
                id="gid://shopify/MediaImage/pkm1",
                filename="PKM-001-front.jpg",
                product_code=non_gw_product.sku,
                title_slug="pokemon-booster-box",
                file_status="READY",
            ),
        ]
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        self.client.iter_shopify_image_files_for_photo_sync.return_value = iter(non_gw_files)

        calls = []
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append(("detach", tuple(ids), pid))

        with tempfile.TemporaryDirectory() as tmp, self._patched_photo_sync_outputs(Path(tmp)):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                None,
                dry=False,
                manifest_path=Path(tmp) / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
            )

        self.assertEqual(
            calls,
            [
                ("attach", ("gid://shopify/MediaImage/pkm1",), "gid://shopify/Product/2"),
                ("reorder", "gid://shopify/Product/2", ("gid://shopify/MediaImage/pkm1",)),
                ("detach", ("gid://shopify/MediaImage/old9",), "gid://shopify/Product/2"),
            ],
        )

    def test_photo_sync_staged_local_all_live_run_writes_fallback_metafield_after_detach(self):
        non_gw_product, non_gw_existing, photo_root = self._make_non_gw_photo_sync_fixture()
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        self.client.staged_uploads_create.return_value = [
            {"url": "https://upload/1", "resourceUrl": "https://resource/1", "parameters": []},
            {"url": "https://upload/2", "resourceUrl": "https://resource/2", "parameters": []},
        ]
        self.client.file_create.return_value = [
            {"id": "gid://shopify/MediaImage/new1", "fileStatus": "UPLOADED"},
            {"id": "gid://shopify/MediaImage/new2", "fileStatus": "UPLOADED"},
        ]

        calls = []
        self.client.ensure_fallback_image_metafield_definition.side_effect = lambda: calls.append(("ensure",))
        self.client.upload_file_to_staged_target.side_effect = lambda path, target: calls.append(("upload", path.name)) or target["resourceUrl"]
        self.client.wait_for_files_ready.side_effect = lambda ids, **kwargs: calls.append(("ready", tuple(ids))) or ids
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append(("detach", tuple(ids), pid))
        self.client.set_product_fallback_image_used.side_effect = lambda pid: calls.append(("audit", pid))

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
                fallback_audit=True,
            )

        self.assertEqual(
            calls,
            [
                ("ensure",),
                ("upload", "01.jpg"),
                ("upload", "02.jpg"),
                ("ready", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2")),
                ("attach", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2"), "gid://shopify/Product/2"),
                ("reorder", "gid://shopify/Product/2", ("gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2")),
                ("detach", ("gid://shopify/MediaImage/old9",), "gid://shopify/Product/2"),
                ("audit", "gid://shopify/Product/2"),
            ],
        )
        manifest = json.loads((photo_root / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest[non_gw_product.sku]["state"], "completed")
        self.assertEqual(manifest[non_gw_product.sku]["fallback_audit_version"], shopify_sync.PHOTO_SYNC_AUDIT_VERSION)

    def test_photo_sync_staged_local_all_metafield_failure_stays_audit_pending(self):
        non_gw_product, non_gw_existing, photo_root = self._make_non_gw_photo_sync_fixture()
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        self.client.staged_uploads_create.return_value = [
            {"url": "https://upload/1", "resourceUrl": "https://resource/1", "parameters": []},
            {"url": "https://upload/2", "resourceUrl": "https://resource/2", "parameters": []},
        ]
        self.client.file_create.return_value = [
            {"id": "gid://shopify/MediaImage/new1", "fileStatus": "UPLOADED"},
            {"id": "gid://shopify/MediaImage/new2", "fileStatus": "UPLOADED"},
        ]
        self.client.upload_file_to_staged_target.side_effect = lambda path, target: target["resourceUrl"]
        self.client.wait_for_files_ready.side_effect = lambda ids, **kwargs: ids
        self.client.set_product_fallback_image_used.side_effect = RuntimeError("audit write failed")

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
                fallback_audit=True,
            )

        manifest = json.loads((photo_root / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest[non_gw_product.sku]["state"], shopify_sync.PHOTO_SYNC_STATE_AUDIT_PENDING)
        self.assertEqual(manifest[non_gw_product.sku]["error"], "audit write failed")
        failures = (photo_root / "failures.tsv").read_text(encoding="utf-8")
        self.assertIn("audit write failed", failures)

    def test_photo_sync_staged_local_all_fails_when_metafield_definition_shape_is_wrong(self):
        non_gw_product, non_gw_existing, photo_root = self._make_non_gw_photo_sync_fixture()
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        self.client.ensure_fallback_image_metafield_definition.side_effect = RuntimeError("wrong metafield definition shape")

        with self._patched_photo_sync_outputs(photo_root), self.assertRaisesRegex(RuntimeError, "wrong metafield definition shape"):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
                fallback_audit=True,
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()

    def test_photo_sync_staged_local_all_resume_from_audit_pending_skips_media_reapply(self):
        non_gw_product, non_gw_existing, photo_root = self._make_non_gw_photo_sync_fixture()
        non_gw_existing["media_ids"] = [
            "gid://shopify/MediaImage/old9",
            "gid://shopify/MediaImage/new1",
            "gid://shopify/MediaImage/new2",
        ]
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        manifest_path = photo_root / "manifest.json"
        manifest_path.write_text(json.dumps({
            non_gw_product.sku: {
                "state": shopify_sync.PHOTO_SYNC_STATE_AUDIT_PENDING,
                "product_id": "gid://shopify/Product/2",
                "asset_fingerprint": shopify_sync.discover_photo_asset_sets(photo_root)[0].fingerprint(),
                "old_media_ids": ["gid://shopify/MediaImage/old9"],
                "new_file_ids": ["gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2"],
                "detached_old_media": True,
                "file_labels": {
                    "gid://shopify/MediaImage/new1": "01.jpg",
                    "gid://shopify/MediaImage/new2": "02.jpg",
                },
                "error": "audit write failed",
                "fallback_audit_version": shopify_sync.PHOTO_SYNC_AUDIT_VERSION,
            }
        }), encoding="utf-8")

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                photo_root,
                dry=False,
                manifest_path=manifest_path,
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
                fallback_audit=True,
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.file_create.assert_not_called()
        self.client.wait_for_files_ready.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        self.client.reorder_product_media.assert_not_called()
        self.client.detach_files_from_product.assert_not_called()
        self.client.set_product_fallback_image_used.assert_called_once_with("gid://shopify/Product/2")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest[non_gw_product.sku]["state"], "completed")

    def test_photo_sync_staged_local_all_legacy_completed_entry_resumes_audit_only(self):
        non_gw_product, non_gw_existing, photo_root = self._make_non_gw_photo_sync_fixture()
        non_gw_existing["media_ids"] = [
            "gid://shopify/MediaImage/old9",
            "gid://shopify/MediaImage/new1",
            "gid://shopify/MediaImage/new2",
        ]
        self.client.iter_existing_for_photo_sync.return_value = iter([non_gw_existing])
        manifest_path = photo_root / "manifest.json"
        manifest_path.write_text(json.dumps({
            non_gw_product.sku: {
                "state": "completed",
                "product_id": "gid://shopify/Product/2",
                "source_mode": shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                "asset_fingerprint": shopify_sync.discover_photo_asset_sets(photo_root)[0].fingerprint(),
                "old_media_ids": ["gid://shopify/MediaImage/old9"],
                "new_file_ids": ["gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2"],
                "detached_old_media": True,
                "file_labels": {
                    "gid://shopify/MediaImage/new1": "01.jpg",
                    "gid://shopify/MediaImage/new2": "02.jpg",
                }
            }
        }), encoding="utf-8")

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [non_gw_product],
                photo_root,
                dry=False,
                manifest_path=manifest_path,
                source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
                product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
                fallback_audit=True,
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        self.client.reorder_product_media.assert_not_called()
        self.client.detach_files_from_product.assert_not_called()
        self.client.set_product_fallback_image_used.assert_called_once_with("gid://shopify/Product/2")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest[non_gw_product.sku]["state"], "completed")
        self.assertEqual(manifest[non_gw_product.sku]["fallback_audit_version"], shopify_sync.PHOTO_SYNC_AUDIT_VERSION)

    def test_photo_sync_skips_duplicate_shopify_skus_as_ambiguous(self):
        photo_root = self._make_photo_root()
        duplicate = dict(self.existing)
        duplicate["product_id"] = "gid://shopify/Product/2"
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing, duplicate])

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
            )

        self.client.staged_uploads_create.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        ambiguous_log = (photo_root / "ambiguous.tsv").read_text(encoding="utf-8")
        self.assertIn("multiple Shopify products share this SKU", ambiguous_log)

    def test_photo_sync_reuses_manifest_old_media_snapshot_on_retry(self):
        photo_root = self._make_photo_root()
        retry_existing = dict(self.existing)
        retry_existing["media_ids"] = [
            "gid://shopify/MediaImage/old1",
            "gid://shopify/MediaImage/old2",
            "gid://shopify/MediaImage/new1",
            "gid://shopify/MediaImage/new2",
        ]
        self.client.iter_existing_for_photo_sync.return_value = iter([retry_existing])
        manifest_path = photo_root / "manifest.json"
        manifest_path.write_text(json.dumps({
            self.product.sku: {
                "state": "reordered",
                "product_id": "gid://shopify/Product/1",
                "asset_fingerprint": shopify_sync.discover_photo_asset_sets(photo_root)[0].fingerprint(),
                "old_media_ids": ["gid://shopify/MediaImage/old1", "gid://shopify/MediaImage/old2"],
                "new_file_ids": ["gid://shopify/MediaImage/new1", "gid://shopify/MediaImage/new2"],
            }
        }), encoding="utf-8")

        calls = []
        self.client.wait_for_files_ready.side_effect = lambda ids, **kwargs: calls.append(("ready", tuple(ids)))
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append((tuple(ids), pid))

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=False,
                manifest_path=manifest_path,
            )

        self.assertEqual(
            calls,
            [(("gid://shopify/MediaImage/old1", "gid://shopify/MediaImage/old2"), "gid://shopify/Product/1")],
        )
        self.client.wait_for_files_ready.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        self.client.reorder_product_media.assert_not_called()

    def test_photo_sync_fails_when_staged_target_count_is_short(self):
        photo_root = self._make_photo_root()
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        self.client.staged_uploads_create.return_value = [
            {"url": "https://upload/1", "resourceUrl": "https://resource/1", "parameters": []},
        ]

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=False,
                manifest_path=photo_root / "manifest.json",
            )

        self.client.upload_file_to_staged_target.assert_not_called()
        self.client.attach_files_to_product.assert_not_called()
        failures = (photo_root / "failures.tsv").read_text(encoding="utf-8")
        self.assertIn("unexpected number of targets", failures)

    def test_photo_sync_changed_fingerprint_resets_detach_state_and_old_media_snapshot(self):
        photo_root = self._make_photo_root()
        existing = dict(self.existing)
        existing["media_ids"] = ["gid://shopify/MediaImage/current1", "gid://shopify/MediaImage/current2"]
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])
        self.client.staged_uploads_create.return_value = [
            {"url": "https://upload/1", "resourceUrl": "https://resource/1", "parameters": []},
            {"url": "https://upload/2", "resourceUrl": "https://resource/2", "parameters": []},
        ]
        self.client.file_create.return_value = [
            {"id": "gid://shopify/MediaImage/new1", "fileStatus": "UPLOADED"},
            {"id": "gid://shopify/MediaImage/new2", "fileStatus": "UPLOADED"},
        ]
        manifest_path = photo_root / "manifest.json"
        manifest_path.write_text(json.dumps({
            self.product.sku: {
                "state": "completed",
                "product_id": "gid://shopify/Product/1",
                "asset_fingerprint": "old-fingerprint",
                "old_media_ids": ["gid://shopify/MediaImage/very-old1", "gid://shopify/MediaImage/very-old2"],
                "new_file_ids": ["gid://shopify/MediaImage/prior1", "gid://shopify/MediaImage/prior2"],
                "detached_old_media": True,
                "error": "old error",
            }
        }), encoding="utf-8")

        calls = []
        self.client.upload_file_to_staged_target.side_effect = lambda path, target: target["resourceUrl"]
        self.client.wait_for_files_ready.side_effect = lambda ids, **kwargs: calls.append(("ready", tuple(ids))) or ids
        self.client.attach_files_to_product.side_effect = lambda ids, pid: calls.append(("attach", tuple(ids), pid))
        self.client.reorder_product_media.side_effect = lambda pid, ids: calls.append(("reorder", pid, tuple(ids)))
        self.client.detach_files_from_product.side_effect = lambda ids, pid: calls.append(("detach", tuple(ids), pid))

        with self._patched_photo_sync_outputs(photo_root):
            shopify_sync.phase_photo_sync(
                self.client,
                [self.product],
                photo_root,
                dry=False,
                manifest_path=manifest_path,
            )

        self.assertIn(("detach", ("gid://shopify/MediaImage/current1", "gid://shopify/MediaImage/current2"), "gid://shopify/Product/1"), calls)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest[self.product.sku]["old_media_ids"], ["gid://shopify/MediaImage/current1", "gid://shopify/MediaImage/current2"])
        self.assertEqual(manifest[self.product.sku]["error"], "")


class PhotoSourcePhaseTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.client.session = mock.Mock(headers={})
        self.product = shopify_sync.Product(
            title="Pokemon Booster Box",
            sku="PKM-001",
            vendor="Pokemon",
            source="INV",
        )
        self.existing = {
            "product_id": "gid://shopify/Product/2",
            "title": self.product.title,
            "vendor": "Pokemon",
            "tags": ["Pokemon"],
            "sku": self.product.sku,
            "media_ids": [],
        }

    @contextmanager
    def _patched_photo_source_outputs(self, root: Path):
        with mock.patch("shopify_sync.PHOTO_SOURCE_PREVIEW_CSV", new=root / "preview.csv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_REVIEW_CSV", new=root / "review.csv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_MISSING_TSV", new=root / "missing.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_AMBIGUOUS_TSV", new=root / "ambiguous.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_FAILURES_TSV", new=root / "failures.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_UNMAPPED_SHOPIFY_TSV", new=root / "unmapped.tsv"), \
             mock.patch("shopify_sync.GW_TRADE_FEED_INDEX_JSON", new=root / "gw_trade_feed_index.json"):
            yield

    def test_photo_source_web_all_live_run_stages_high_confidence_winner(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([
            self.existing,
            {
                "product_id": "gid://shopify/Product/999",
                "title": "Shopify Only",
                "vendor": "Other",
                "tags": [],
                "sku": "SHOP-ONLY",
                "media_ids": [],
            },
        ])
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """
        image_bytes = b"winner-image"
        responses = [
            FakeResponse(text=search_html, url=f"{shopify_sync.PHOTO_SOURCE_SEARCH_URL}?q=PKM"),
            FakeResponse(text=candidate_html, url="https://example.com/pkm-001-product"),
            FakeResponse(content=image_bytes, url="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg"),
        ]
        self.client.session.get.side_effect = responses

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "PKM-001-pokemon-booster-box"
            preview = (root / "preview.csv").read_text(encoding="utf-8")
            unmapped = (root / "unmapped.tsv").read_text(encoding="utf-8")
            self.assertEqual(manifest[self.product.sku]["state"], "completed")
            self.assertTrue(staged_dir.exists())
            self.assertTrue((staged_dir / "_source.json").exists())
            self.assertIn("winner", preview)
            self.assertIn("SHOP-ONLY", unmapped)

    def test_photo_source_outputs_include_product_metadata_columns(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        self.product.barcode = "5012345678901"
        self.product.product_type = "Trading Cards"
        self.product.tags = ["Pokemon", "Distributor: ABGee", "ASIN: B08B3XP7DZ"]
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """
        self.client.session.get.side_effect = [
            FakeResponse(text=search_html, url=f"{shopify_sync.PHOTO_SOURCE_SEARCH_URL}?q=PKM"),
            FakeResponse(text=candidate_html, url="https://example.com/pkm-001-product"),
            FakeResponse(content=b"winner-image", url="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg"),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            with (root / "preview.csv").open(encoding="utf-8") as fh:
                preview_rows = list(csv.DictReader(fh))

        self.assertEqual(preview_rows[0]["source"], "INV")
        self.assertEqual(preview_rows[0]["barcode"], "5012345678901")
        self.assertEqual(preview_rows[0]["vendor"], "Pokemon")
        self.assertEqual(preview_rows[0]["product_type"], "Trading Cards")
        self.assertIn("Distributor: ABGee", preview_rows[0]["tags"])

    def test_photo_source_web_all_marks_equal_high_score_candidates_for_review(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        search_html = """
        <html><body>
          <a href="https://example.com/pkm-001-a">A</a>
          <a href="https://example.com/pkm-001-b">B</a>
        </body></html>
        """
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body><div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div></body></html>
        """
        self.client.session.get.side_effect = [
            FakeResponse(text=search_html, url="https://search"),
            FakeResponse(text=candidate_html, url="https://example.com/pkm-001-a"),
            FakeResponse(text=candidate_html, url="https://example.com/pkm-001-b"),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )
            review = (root / "review.csv").read_text(encoding="utf-8")
            ambiguous = (root / "ambiguous.tsv").read_text(encoding="utf-8")
            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("winner_tie", review)
        self.assertIn("winner_tie", ambiguous)
        self.assertIn("review", preview)

    def test_photo_source_policy_version_mismatch_invalidates_completed_entry_reuse(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "cache" / "current"
            pack_dir = cache_root / "PKM-001-pokemon-booster-box"
            pack_dir.mkdir(parents=True)
            (pack_dir / "01.jpg").write_bytes(b"existing")
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({
                self.product.sku: {
                    "state": "completed",
                    "query": shopify_sync.build_photo_source_query(self.product),
                    "policy_version": "psv1-stale",
                }
            }), encoding="utf-8")
            self.client.session.get.side_effect = [
                FakeResponse(text=search_html, url="https://search"),
                FakeResponse(text=candidate_html, url="https://example.com/pkm-001-product"),
                FakeResponse(content=b"new-image", url="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg"),
            ]
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest[self.product.sku]["state"], "completed")
        self.assertEqual(manifest[self.product.sku]["policy_version"], shopify_sync.current_photo_source_policy_version())
        self.assertEqual(self.client.session.get.call_count, 3)

    def test_photo_source_policy_version_changes_when_book_vendor_policy_changes(self):
        baseline = shopify_sync.current_photo_source_policy_version()

        with mock.patch.object(
            shopify_sync,
            "BOOK_PHOTO_SOURCE_VENDORS",
            set(shopify_sync.BOOK_PHOTO_SOURCE_VENDORS) | {"new vendor imprint"},
        ):
            changed = shopify_sync.current_photo_source_policy_version()

        self.assertNotEqual(changed, baseline)

    def test_photo_source_policy_version_changes_when_output_schema_changes(self):
        baseline = shopify_sync.current_photo_source_policy_version()

        with mock.patch.object(
            shopify_sync,
            "PHOTO_SOURCE_PREVIEW_COLUMNS",
            list(shopify_sync.PHOTO_SOURCE_PREVIEW_COLUMNS) + ["debug_column"],
        ):
            changed = shopify_sync.current_photo_source_policy_version()

        self.assertNotEqual(changed, baseline)

    def test_photo_source_web_all_dry_run_recomputes_preview_without_mutating_stale_manifest(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({
                self.product.sku: {
                    "state": "missing",
                    "query": shopify_sync.build_photo_source_query(self.product),
                    "policy_version": "psv1-stale",
                    "reason": "old missing result",
                }
            }), encoding="utf-8")
            before = manifest_path.read_text(encoding="utf-8")
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.fetch_url_with_retries", side_effect=[
                     FakeResponse(text=search_html, url="https://search"),
                     FakeResponse(text=candidate_html, url="https://example.com/pkm-001-product"),
                 ]):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=True,
                    manifest_path=manifest_path,
                    cache_root=root / "cache" / "current",
                )

            after = manifest_path.read_text(encoding="utf-8")
            with (root / "preview.csv").open(encoding="utf-8") as fh:
                preview_rows = list(csv.DictReader(fh))

        self.assertEqual(preview_rows[0]["status"], "winner")
        self.assertEqual(before, after)

    def test_photo_source_web_all_skips_products_with_existing_media(self):
        existing = dict(self.existing)
        existing["media_ids"] = ["gid://shopify/MediaImage/1"]
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )
            preview_rows = (root / "preview.csv").read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(preview_rows), 1)
        self.client.session.get.assert_not_called()

    def test_photo_source_web_all_prioritizes_non_gw_before_gw(self):
        gw_product = shopify_sync.Product(
            title="L: TUSKGOR FUR 12ML ROW X6",
            sku="9918995134406",
            vendor="Games Workshop",
            source="GW",
        )
        inv_product = shopify_sync.Product(
            title="Attached - Book",
            sku="978-1529032178",
            barcode="9781529032178",
            vendor="bluebird",
            source="INV",
        )
        gw_existing = dict(self.existing)
        gw_existing["sku"] = gw_product.sku
        gw_existing["title"] = gw_product.title
        gw_existing["vendor"] = gw_product.vendor
        inv_existing = dict(self.existing)
        inv_existing["sku"] = inv_product.sku
        inv_existing["title"] = inv_product.title
        inv_existing["vendor"] = inv_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([gw_existing, inv_existing])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.fetch_book_photo_source_candidates", return_value=([], 0, [])), \
                 mock.patch("shopify_sync.fetch_amazon_photo_source_candidates", return_value=([], 0, [])), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates", return_value=([], 0, [])):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product, inv_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            with (root / "preview.csv").open(encoding="utf-8") as fh:
                preview_rows = list(csv.DictReader(fh))

        self.assertEqual(preview_rows[0]["sku"], inv_product.sku)
        self.assertEqual(preview_rows[1]["sku"], gw_product.sku)

    def test_photo_source_web_all_preserves_existing_session_auth_headers(self):
        session = requests.Session()
        session.headers.update({"X-Shopify-Access-Token": "secret-token"})
        session.get = mock.Mock()
        self.client.session = session
        self.client.iter_existing_for_photo_sync.return_value = iter([])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

        self.assertEqual(self.client.session.headers["X-Shopify-Access-Token"], "secret-token")
        self.assertTrue(self.client.session.headers.get("User-Agent"))

    def test_photo_source_web_all_uses_open_library_for_book_vendors_before_search(self):
        book = shopify_sync.Product(
            title="A Great Book",
            sku="9781234567890",
            barcode="9781234567890",
            vendor="Simon & Schuster",
            source="INV",
        )
        existing = dict(self.existing)
        existing["sku"] = book.sku
        existing["title"] = book.title
        existing["vendor"] = book.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])
        self.client.session.get.side_effect = [
            FakeResponse(content=b"cover-bytes", headers={"Content-Type": "image/jpeg"}),
            FakeResponse(payload={"items": []}),
            FakeResponse(content=b"cover-bytes", headers={"Content-Type": "image/jpeg"}),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [book],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "9781234567890-a-great-book"
            self.assertIn("openlibrary", preview)
            self.assertEqual(manifest[book.sku]["state"], "completed")
            self.assertTrue(staged_dir.exists())

        self.assertEqual(self.client.session.get.call_count, 3)

    def test_photo_source_web_all_treats_viz_media_llc_as_book_vendor(self):
        book = shopify_sync.Product(
            title="Demon Slayer #1",
            sku="9781974700523",
            barcode="9781974700523",
            vendor="VIZ Media LLC",
            source="INV",
        )
        existing = dict(self.existing)
        existing["sku"] = book.sku
        existing["title"] = book.title
        existing["vendor"] = book.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])
        self.client.session.get.side_effect = [
            FakeResponse(content=b"cover-bytes", headers={"Content-Type": "image/jpeg"}),
            FakeResponse(payload={"items": []}),
            FakeResponse(content=b"cover-bytes", headers={"Content-Type": "image/jpeg"}),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates") as search_mock:
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [book],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "9781974700523-demon-slayer-1"
            self.assertIn("openlibrary", preview)
            self.assertEqual(manifest[book.sku]["state"], "completed")
            self.assertTrue(staged_dir.exists())

        search_mock.assert_not_called()
        self.assertEqual(self.client.session.get.call_count, 3)

    def test_photo_source_web_all_stages_supplier_folder_matches_without_network(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            supplier_root = root / "supplier"
            pack = supplier_root / "PKM-001-Pokemon-Booster-Box"
            pack.mkdir(parents=True)
            (pack / "01.jpg").write_bytes(b"supplier-image")
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"

            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.load_env", return_value={shopify_sync.PHOTO_SOURCE_SUPPLIER_ROOTS_ENV: str(supplier_root)}):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "PKM-001-pokemon-booster-box"
            self.assertIn("supplier_local", preview)
            self.assertEqual(manifest[self.product.sku]["state"], "completed")
            self.assertTrue((staged_dir / "01.jpg").exists())

        self.client.session.get.assert_not_called()

    def test_photo_source_web_all_stages_split_code_supplier_folder_without_network(self):
        product = shopify_sync.Product(
            title="Pop! Vinyl - Batman 1989 - Joker w/Hat w/Chase",
            sku="985 47709",
            vendor="FUNKO",
            product_type="FUNKO",
            source="INV",
        )
        existing = dict(self.existing)
        existing["sku"] = product.sku
        existing["title"] = product.title
        existing["vendor"] = product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            supplier_root = root / "supplier"
            pack = supplier_root / "985-47709-Pop-Vinyl-Batman-1989-Joker-w-Hat-w-Chase"
            pack.mkdir(parents=True)
            (pack / "01.jpg").write_bytes(b"supplier-image")
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"

            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.load_env", return_value={shopify_sync.PHOTO_SOURCE_SUPPLIER_ROOTS_ENV: str(supplier_root)}):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / shopify_sync.stable_photo_source_dirname(product)
            self.assertIn("supplier_local", preview)
            self.assertEqual(manifest[product.sku]["state"], "completed")
            self.assertTrue((staged_dir / "01.jpg").exists())

        self.client.session.get.assert_not_called()

    def test_photo_source_web_all_uses_gw_cache_before_web_search(self):
        gw_product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            pack = gw_cache_root / "TR-39-13-99120109017-Armageddon-Battalion-Deathwatch"
            pack.mkdir(parents=True)
            (pack / "01.jpg").write_bytes(b"gw-cache-image")
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"

            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "99120109017-armageddon-battalion-deathwatch"
            self.assertIn("gw_cache", preview)
            self.assertEqual(manifest[gw_product.sku]["state"], "completed")
            self.assertTrue((staged_dir / "01.jpg").exists())

        self.client.session.get.assert_not_called()

    def test_photo_source_web_all_uses_gw_official_pack_before_generic_search(self):
        gw_product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        packs = [
            gw_cache_refresh.ResourcePack(
                label="99120109017-Armageddon-Battalion-Deathwatch",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/99120109017.jpg", filename="99120109017.jpg")],
                archives=[],
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("gw_official", preview)
        self.client.session.get.assert_not_called()

    def test_photo_source_web_all_stages_gw_official_direct_image_pack(self):
        gw_product = shopify_sync.Product(
            title="ARMAGEDDON BATTALION: DEATHWATCH",
            sku="99120109017",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        packs = [
            gw_cache_refresh.ResourcePack(
                label="99120109017-Armageddon-Battalion-Deathwatch",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/99120109017.jpg", filename="99120109017.jpg")],
                archives=[],
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", return_value=(b"gw-official-image", "https://trade.games-workshop.com/resources/99120109017.jpg")):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            staged_dir = cache_root / "99120109017-armageddon-battalion-deathwatch"
            self.assertIn("gw_official", preview)
            self.assertEqual(manifest[gw_product.sku]["state"], "completed")
            self.assertTrue((staged_dir / "resources-99120109017.jpg").exists())
            self.assertTrue((staged_dir / "_source.json").exists())

    def test_photo_source_web_all_uses_gw_official_archive_member_before_missing(self):
        gw_product = shopify_sync.Product(
            title="L: TUSKGOR FUR 12ML ROW X6",
            sku="9918995134406",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        malformed_packs = [
            gw_cache_refresh.ResourcePack(
                label="Articles",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/articles.zip"],
            ),
        ]
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w") as zf:
            zf.writestr("Products/9918995134406-Tuskgor-Fur.jpg", b"match")
            zf.writestr("Products/9999999999999-Other-Product.jpg", b"other")
        archive_bytes = archive_buffer.getvalue()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            archive_cache_path = root / "gw_official_archive_index.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.GW_OFFICIAL_ARCHIVE_INDEX_JSON", new=archive_cache_path), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(malformed_packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", return_value=(archive_bytes, malformed_packs[0].archives[0])), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates") as search:
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        search.assert_not_called()
        self.assertIn("winner", preview)
        self.assertIn("gw_official", preview)

    def test_photo_source_web_all_stages_only_matching_gw_official_archive_members(self):
        gw_product = shopify_sync.Product(
            title="L: TUSKGOR FUR 12ML ROW X6",
            sku="9918995134406",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        malformed_packs = [
            gw_cache_refresh.ResourcePack(
                label="Articles",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/articles.zip"],
            ),
        ]
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w") as zf:
            zf.writestr("Products/9918995134406-Tuskgor-Fur.jpg", b"match")
            zf.writestr("Products/9999999999999-Other-Product.jpg", b"other")
        archive_bytes = archive_buffer.getvalue()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            cache_root = root / "photo_source_cache" / "current"
            manifest_path = root / "photo_source_manifest.json"
            archive_cache_path = root / "gw_official_archive_index.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.GW_OFFICIAL_ARCHIVE_INDEX_JSON", new=archive_cache_path), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(malformed_packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", return_value=(archive_bytes, malformed_packs[0].archives[0])):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=False,
                    manifest_path=manifest_path,
                    cache_root=cache_root,
                )

            staged_dir = cache_root / "9918995134406-l-tuskgor-fur-12ml-row-x6"
            files = sorted(path.name for path in staged_dir.iterdir() if path.is_file() and path.name != "_source.json")

        self.assertEqual(files, ["Products-9918995134406-Tuskgor-Fur.jpg"])

    def test_build_gw_official_resource_pack_indexes_reuses_archive_index_cache(self):
        packs = [
            gw_cache_refresh.ResourcePack(
                label="Articles",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/articles.zip"],
            ),
        ]
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w") as zf:
            zf.writestr("Products/9918995134406-Tuskgor-Fur.jpg", b"match")
        archive_bytes = archive_buffer.getvalue()
        session = requests.Session()

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "gw_official_archive_index.json"
            with mock.patch("shopify_sync.GW_OFFICIAL_ARCHIVE_INDEX_JSON", new=cache_path), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", return_value=(archive_bytes, packs[0].archives[0])) as fetch_binary:
                by_code, _ = shopify_sync.build_gw_official_resource_pack_indexes(packs, session)
                self.assertIn("9918995134406", by_code)
                self.assertEqual(fetch_binary.call_count, 1)

                by_code_again, _ = shopify_sync.build_gw_official_resource_pack_indexes(packs, session)
                self.assertIn("9918995134406", by_code_again)
                self.assertEqual(fetch_binary.call_count, 1)

    def test_gw_trade_feed_index_cache_round_trip(self):
        packs = [
            gw_cache_refresh.ResourcePack(
                label="BSF-21-03-99189950267-MEPHISTON RED 12ML ROW__1.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/mephiston.jpg", filename="mephiston.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "gw_trade_feed_index.json"
            shopify_sync.save_gw_trade_feed_index_cache(packs, cache_path)
            loaded = shopify_sync.load_gw_trade_feed_index_cache(cache_path)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].label, packs[0].label)
        self.assertEqual(loaded[0].images[0].url, packs[0].images[0].url)
        self.assertEqual(loaded[0].source_label, gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL)

    def test_load_or_discover_gw_trade_feed_packs_reuses_cache(self):
        packs = [
            gw_cache_refresh.ResourcePack(
                label="BSF-21-03-99189950267-MEPHISTON RED 12ML ROW__1.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/mephiston.jpg", filename="mephiston.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]
        session = requests.Session()

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "gw_trade_feed_index.json"
            shopify_sync.save_gw_trade_feed_index_cache(packs, cache_path)
            with mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs") as discover:
                loaded, stats = shopify_sync.load_or_discover_gw_trade_feed_packs(
                    session,
                    logger=lambda _msg: None,
                    cache_path=cache_path,
                )

        discover.assert_not_called()
        self.assertEqual(len(loaded), 1)
        self.assertTrue(stats.get("cached"))

    def test_load_or_discover_gw_trade_feed_packs_saves_checkpoints(self):
        packs = [
            gw_cache_refresh.ResourcePack(
                label="BSF-21-03-99189950267-MEPHISTON RED 12ML ROW__1.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/mephiston.jpg", filename="mephiston.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]
        session = requests.Session()

        def fake_discover_trade_feed_packs(_session, *, logger=None, on_progress=None, **_kwargs):
            if on_progress is not None:
                on_progress(
                    packs,
                    {
                        "request_count": 10,
                        "image_count": 1,
                    },
                )
            return packs, "GW Trade Feed", {"request_count": 11, "image_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "gw_trade_feed_index.json"
            with mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", side_effect=fake_discover_trade_feed_packs), \
                 mock.patch("shopify_sync.save_gw_trade_feed_index_cache", wraps=shopify_sync.save_gw_trade_feed_index_cache) as save:
                loaded, stats = shopify_sync.load_or_discover_gw_trade_feed_packs(
                    session,
                    logger=lambda _msg: None,
                    cache_path=cache_path,
                )

        self.assertEqual(len(loaded), 1)
        self.assertFalse(stats.get("cached"))
        self.assertGreaterEqual(save.call_count, 2)

    def test_resolve_gw_official_resource_pack_uses_trade_feed_prefix_code(self):
        product = shopify_sync.Product(
            title="B: MEPHISTON RED 12ML ROW X6",
            sku="9918995026706",
            vendor="Games Workshop",
            source="GW",
        )
        packs = [
            gw_cache_refresh.ResourcePack(
                label="BSF-21-03-99189950267-MEPHISTON RED 12ML ROW__1.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/mephiston.jpg", filename="mephiston.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]

        by_code, by_slug = shopify_sync.build_gw_official_resource_pack_indexes(packs, None)
        action, match_type, ref, reason = shopify_sync.resolve_gw_official_resource_pack(product, by_code, by_slug)

        self.assertEqual(action, "replace")
        self.assertEqual(match_type, "trade_feed_prefix")
        self.assertIsNotNone(ref)
        self.assertEqual(ref.label, packs[0].label)
        self.assertEqual(reason, "")

    def test_photo_source_web_all_uses_trade_feed_prefix_match_for_gw(self):
        gw_product = shopify_sync.Product(
            title="B: MEPHISTON RED 12ML ROW X6",
            sku="9918995026706",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        trade_feed_packs = [
            gw_cache_refresh.ResourcePack(
                label="BSF-21-03-99189950267-MEPHISTON RED 12ML ROW__1.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/mephiston.jpg", filename="mephiston.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=([], "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=(trade_feed_packs, "GW Trade Feed", {"image_count": 1, "request_count": 1})):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("winner", preview)
        self.assertIn("gw_official", preview)
        self.assertIn("trade_feed_prefix", preview)

    def test_photo_source_web_all_rejects_trade_feed_prefix_without_title_agreement(self):
        gw_product = shopify_sync.Product(
            title="SPACE MARINES STORMRAVEN GUNSHIP",
            sku="99120101339",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        trade_feed_packs = [
            gw_cache_refresh.ResourcePack(
                label="TR-41-25-9912010133-Combat Patrol Blood Angels.jpg",
                images=[gw_cache_refresh.ImageTarget(url="https://trade.games-workshop.com/resources/blood-angels.jpg", filename="blood-angels.jpg")],
                archives=[],
                source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=([], "Product Images")), \
                 mock.patch("shopify_sync.load_or_discover_gw_trade_feed_packs", return_value=(trade_feed_packs, {"cached": True, "pack_count": 1})):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("missing", preview)
        self.assertIn("title agreement was too weak", preview)

    def test_choose_best_gw_official_pack_prefers_non_box_variant(self):
        product = shopify_sync.Product(
            title="WARHAMMER QUEST: DARKWATER (ENGLISH)",
            sku="60010799029",
            vendor="Games Workshop",
            source="GW",
        )
        refs = [
            shopify_sync.GWOfficialResourcePackRef(
                key="a",
                label="60010799029-ENGWHQuestCoreGame",
                pack=gw_cache_refresh.ResourcePack(label="a", images=[], archives=["x.zip"]),
                product_code="60010799029",
                archive_url="x.zip",
                archive_members=["60010799029_ENGWHQuestCoreGame1.jpg"],
            ),
            shopify_sync.GWOfficialResourcePackRef(
                key="b",
                label="60010799029-ENGWHQuestCoreGameBOX",
                pack=gw_cache_refresh.ResourcePack(label="b", images=[], archives=["x.zip"]),
                product_code="60010799029",
                archive_url="x.zip",
                archive_members=["60010799029_ENGWHQuestCoreGameBOX.jpg"],
            ),
        ]

        best = shopify_sync._choose_best_gw_official_pack(product, refs)

        self.assertIsNotNone(best)
        self.assertEqual(best.label, "60010799029-ENGWHQuestCoreGame")

    def test_choose_best_gw_official_pack_prefers_duplicate_free_variant(self):
        product = shopify_sync.Product(
            title="SPEARHEAD: SERAPHON SUNBLOODED PROWLERS",
            sku="99120208046",
            vendor="Games Workshop",
            source="GW",
        )
        refs = [
            shopify_sync.GWOfficialResourcePackRef(
                key="a",
                label="99120208046-SeraphonSunbloodedProwlersSpearhead",
                pack=gw_cache_refresh.ResourcePack(label="a", images=[], archives=["x.zip"]),
                product_code="99120208046",
                archive_url="x.zip",
                archive_members=["99120208046_SeraphonSunbloodedProwlersSpearhead17.jpg"],
            ),
            shopify_sync.GWOfficialResourcePackRef(
                key="b",
                label="99120208046-SeraphonSunbloodedProwlersSpearhead17-1",
                pack=gw_cache_refresh.ResourcePack(label="b", images=[], archives=["x.zip"]),
                product_code="99120208046",
                archive_url="x.zip",
                archive_members=["99120208046_SeraphonSunbloodedProwlersSpearhead17 (1).jpg"],
            ),
        ]

        best = shopify_sync._choose_best_gw_official_pack(product, refs)

        self.assertIsNotNone(best)
        self.assertEqual(best.label, "99120208046-SeraphonSunbloodedProwlersSpearhead")

    def test_choose_best_gw_official_pack_uses_title_initials_and_tokens(self):
        product = shopify_sync.Product(
            title="COMBAT PATROL: ADEPTA SORORITAS",
            sku="99120108100",
            vendor="Games Workshop",
            source="GW",
        )
        refs = [
            shopify_sync.GWOfficialResourcePackRef(
                key="a",
                label="99120108100-ASCombatPatrol",
                pack=gw_cache_refresh.ResourcePack(label="a", images=[], archives=["x.zip"]),
                product_code="99120108100",
                archive_url="x.zip",
                archive_members=["99120108100_ASCombatPatrol05.jpg"],
            ),
            shopify_sync.GWOfficialResourcePackRef(
                key="b",
                label="99120108100-GSCCP",
                pack=gw_cache_refresh.ResourcePack(label="b", images=[], archives=["x.zip"]),
                product_code="99120108100",
                archive_url="x.zip",
                archive_members=["99120108100_GSCCP6.jpg"],
            ),
        ]

        best = shopify_sync._choose_best_gw_official_pack(product, refs)

        self.assertIsNotNone(best)
        self.assertEqual(best.label, "99120108100-ASCombatPatrol")

    def test_choose_best_gw_official_pack_dedupes_identical_refs(self):
        product = shopify_sync.Product(
            title="NECROMUNDA: SQUAT PROSPECTORS EXO-KYN",
            sku="99120599073",
            vendor="Games Workshop",
            source="GW",
        )
        refs = [
            shopify_sync.GWOfficialResourcePackRef(
                key="a",
                label="99120599073-NECIronheadProspectorsExoKyn",
                pack=gw_cache_refresh.ResourcePack(label="a", images=[], archives=["x.zip"]),
                product_code="99120599073",
                archive_url="x.zip",
                archive_members=["99120599073_NECIronheadProspectorsExoKyn03.jpg"],
            ),
            shopify_sync.GWOfficialResourcePackRef(
                key="b",
                label="99120599073-NECIronheadProspectorsExoKyn",
                pack=gw_cache_refresh.ResourcePack(label="b", images=[], archives=["x.zip"]),
                product_code="99120599073",
                archive_url="x.zip",
                archive_members=["99120599073_NECIronheadProspectorsExoKyn03.jpg"],
            ),
            shopify_sync.GWOfficialResourcePackRef(
                key="c",
                label="99120599073-NECIronheadProspectorsExoKyn03-1",
                pack=gw_cache_refresh.ResourcePack(label="c", images=[], archives=["x.zip"]),
                product_code="99120599073",
                archive_url="x.zip",
                archive_members=["99120599073_NECIronheadProspectorsExoKyn03 (1).jpg"],
            ),
        ]

        best = shopify_sync._choose_best_gw_official_pack(product, refs)

        self.assertIsNotNone(best)
        self.assertEqual(best.label, "99120599073-NECIronheadProspectorsExoKyn")

    def test_photo_source_web_all_skips_malformed_gw_official_feed(self):
        gw_product = shopify_sync.Product(
            title="L: TUSKGOR FUR 12ML ROW X6",
            sku="9918995134406",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        malformed_packs = [
            gw_cache_refresh.ResourcePack(
                label="const homeUrl = 'https://trade.games-workshop.com'; window.currentLanguage = 'en';",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/junk.zip"],
            ),
            gw_cache_refresh.ResourcePack(
                label="Articles",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/articles.zip"],
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            archive_cache_path = root / "gw_official_archive_index.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.GW_OFFICIAL_ARCHIVE_INDEX_JSON", new=archive_cache_path), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(malformed_packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", side_effect=RuntimeError("zip unavailable")), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates", return_value=([], 0, [])), \
                 mock.patch("shopify_sync.log") as log:
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("missing", preview)
        logged = "\n".join(call.args[0] for call in log.call_args_list)
        self.assertIn("official GW resource feed did not expose product-code-labelled packs", logged)

    def test_photo_source_web_all_does_not_fall_back_to_generic_search_for_gw(self):
        gw_product = shopify_sync.Product(
            title="L: TUSKGOR FUR 12ML ROW X6",
            sku="9918995134406",
            vendor="Games Workshop",
            source="GW",
        )
        existing = dict(self.existing)
        existing["sku"] = gw_product.sku
        existing["title"] = gw_product.title
        existing["vendor"] = gw_product.vendor
        self.client.iter_existing_for_photo_sync.return_value = iter([existing])

        malformed_packs = [
            gw_cache_refresh.ResourcePack(
                label="const homeUrl = 'https://trade.games-workshop.com'; window.currentLanguage = 'en';",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/junk.zip"],
            ),
            gw_cache_refresh.ResourcePack(
                label="Articles",
                images=[],
                archives=["https://trade.games-workshop.com/wp-content/uploads/2026/04/articles.zip"],
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gw_cache_root = root / "gw_photo_cache" / "current"
            gw_cache_root.mkdir(parents=True)
            archive_cache_path = root / "gw_official_archive_index.json"
            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=gw_cache_root), \
                 mock.patch("shopify_sync.GW_OFFICIAL_ARCHIVE_INDEX_JSON", new=archive_cache_path), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_resource_packs", return_value=(malformed_packs, "Product Images")), \
                 mock.patch("shopify_sync.gw_cache_refresh.discover_trade_feed_packs", return_value=([], "GW Trade Feed", {"image_count": 0, "request_count": 0})), \
                 mock.patch("shopify_sync.gw_cache_refresh.fetch_binary", side_effect=RuntimeError("zip unavailable")), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates") as search:
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [gw_product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")

        search.assert_not_called()
        self.assertIn("missing", preview)
        self.assertIn("no matching Games Workshop resource pack", preview)

    def test_photo_source_web_all_flushes_preview_and_review_incrementally(self):
        first = shopify_sync.Product(
            title="Pokemon Booster Box",
            sku="PKM-001",
            vendor="Pokemon",
            source="INV",
        )
        second = shopify_sync.Product(
            title="Pokemon Elite Trainer Box",
            sku="PKM-002",
            vendor="Pokemon",
            source="INV",
        )
        self.client.iter_existing_for_photo_sync.return_value = iter([
            {
                "product_id": "gid://shopify/Product/2",
                "title": first.title,
                "vendor": first.vendor,
                "tags": ["Pokemon"],
                "sku": first.sku,
                "media_ids": [],
            },
            {
                "product_id": "gid://shopify/Product/3",
                "title": second.title,
                "vendor": second.vendor,
                "tags": ["Pokemon"],
                "sku": second.sku,
                "media_ids": [],
            },
        ])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            first_candidate = shopify_sync._build_direct_photo_source_candidate(
                first,
                page_url="https://example.com/pkm-001",
                image_url="https://cdn.example.com/pkm-001.jpg",
                score=100,
                reasons=["sku", "title", "detail_page"],
            )

            def fake_search(session, product, query):
                if product.sku == first.sku:
                    return [first_candidate], 1, []
                preview = (root / "preview.csv").read_text(encoding="utf-8")
                review = (root / "review.csv").read_text(encoding="utf-8")
                self.assertIn(first.sku, preview)
                self.assertEqual(review.strip().splitlines(), ["sku,title,outcome,top_score,runner_up_score,score_margin,top_candidate_page_url,top_candidate_image_url,runner_up_page_url,runner_up_image_url,source_class,reasons,disqualifiers,policy_version,notes"])
                return [], 0, ["blocked"]

            with self._patched_photo_source_outputs(root), \
                 mock.patch("shopify_sync.fetch_search_page_photo_source_candidates", side_effect=fake_search), \
                 mock.patch("shopify_sync.fetch_amazon_photo_source_candidates", return_value=([], 0, [])), \
                 mock.patch("shopify_sync.fetch_url_with_retries", return_value=FakeResponse(content=b"winner-image")):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [first, second],
                    dry=False,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )

            preview = (root / "preview.csv").read_text(encoding="utf-8")
            self.assertIn(first.sku, preview)
            self.assertIn(second.sku, preview)

    def test_photo_source_web_all_falls_back_to_lite_search_when_html_search_is_blocked(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """
        self.client.session.get.side_effect = [
            FakeResponse(status_code=403),
            FakeResponse(text=search_html),
            FakeResponse(text=candidate_html),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )
            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("winner", preview)
        self.assertEqual(self.client.session.get.call_count, 3)

    def test_photo_source_web_all_falls_back_past_duckduckgo_to_next_search_provider(self):
        self.client.iter_existing_for_photo_sync.return_value = iter([self.existing])
        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """
        self.client.session.get.side_effect = [
            FakeResponse(status_code=403),
            FakeResponse(status_code=403),
            FakeResponse(text=search_html),
            FakeResponse(text=candidate_html),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._patched_photo_source_outputs(root):
                shopify_sync.phase_photo_source_web_all(
                    self.client,
                    [self.product],
                    dry=True,
                    manifest_path=root / "manifest.json",
                    cache_root=root / "cache" / "current",
                )
            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("winner", preview)
        self.assertEqual(self.client.session.get.call_count, 4)

    def test_fetch_photo_source_search_results_uses_provider_specific_query_params(self):
        session = mock.Mock()
        session.get.side_effect = [
            FakeResponse(status_code=403),
            FakeResponse(status_code=403),
            FakeResponse(status_code=403),
            FakeResponse(text='<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'),
        ]

        results, successes, errors = shopify_sync.fetch_photo_source_search_results(session, "PKM-001 Pokemon Booster Box")

        self.assertEqual(len(results), 1)
        self.assertEqual(successes, 1)
        self.assertEqual(len(errors), 3)
        called_urls = [call.args[0] for call in session.get.call_args_list]
        self.assertIn("https://search.yahoo.com/search?p=PKM-001+Pokemon+Booster+Box", called_urls)
        called_timeouts = [call.kwargs["timeout"] for call in session.get.call_args_list]
        self.assertTrue(all(timeout == shopify_sync.PHOTO_SOURCE_SEARCH_TIMEOUT_SECONDS for timeout in called_timeouts))

    def test_fetch_photo_source_search_results_disables_hard_failed_provider_for_rest_of_run(self):
        session = mock.Mock()
        provider_state: dict[str, dict[str, object]] = {}
        session.get.side_effect = [
            FakeResponse(status_code=403),
            FakeResponse(text='<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'),
            FakeResponse(text='<html><body><a href="https://example.com/pkm-002-product">Pokemon Elite Trainer Box</a></body></html>'),
        ]

        with mock.patch("shopify_sync.PHOTO_SOURCE_SEARCH_PROVIDERS", new=(
            ("https://html.duckduckgo.com/html/", "q"),
            ("https://search.yahoo.com/search", "p"),
        )):
            first_results, first_successes, first_errors = shopify_sync.fetch_photo_source_search_results(
                session,
                "PKM-001 Pokemon Booster Box",
                provider_state=provider_state,
            )
            second_results, second_successes, second_errors = shopify_sync.fetch_photo_source_search_results(
                session,
                "PKM-002 Pokemon Elite Trainer Box",
                provider_state=provider_state,
            )

        self.assertEqual(len(first_results), 1)
        self.assertEqual(len(second_results), 1)
        self.assertEqual(first_successes, 1)
        self.assertEqual(second_successes, 1)
        self.assertEqual(len(first_errors), 1)
        self.assertEqual(second_errors, [])
        called_urls = [call.args[0] for call in session.get.call_args_list]
        self.assertEqual(called_urls, [
            "https://html.duckduckgo.com/html/?q=PKM-001+Pokemon+Booster+Box",
            "https://search.yahoo.com/search?p=PKM-001+Pokemon+Booster+Box",
            "https://search.yahoo.com/search?p=PKM-002+Pokemon+Elite+Trainer+Box",
        ])
        self.assertTrue(provider_state["https://html.duckduckgo.com/html/"]["disabled"])

    def test_fetch_photo_source_search_results_disables_timed_out_provider_without_retries(self):
        session = mock.Mock()
        provider_state: dict[str, dict[str, object]] = {}
        session.get.side_effect = [
            requests.exceptions.ConnectTimeout("timed out"),
            FakeResponse(text='<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'),
            FakeResponse(text='<html><body><a href="https://example.com/pkm-002-product">Pokemon Elite Trainer Box</a></body></html>'),
        ]

        with mock.patch("shopify_sync.PHOTO_SOURCE_SEARCH_PROVIDERS", new=(
            ("https://html.duckduckgo.com/html/", "q"),
            ("https://search.yahoo.com/search", "p"),
        )), mock.patch("shopify_sync.time.sleep") as sleep_mock:
            first_results, first_successes, first_errors = shopify_sync.fetch_photo_source_search_results(
                session,
                "PKM-001 Pokemon Booster Box",
                provider_state=provider_state,
            )
            second_results, second_successes, second_errors = shopify_sync.fetch_photo_source_search_results(
                session,
                "PKM-002 Pokemon Elite Trainer Box",
                provider_state=provider_state,
            )

        self.assertEqual(len(first_results), 1)
        self.assertEqual(len(second_results), 1)
        self.assertEqual(first_successes, 1)
        self.assertEqual(second_successes, 1)
        self.assertEqual(len(first_errors), 1)
        self.assertEqual(second_errors, [])
        self.assertEqual(session.get.call_count, 3)
        sleep_mock.assert_not_called()
        self.assertTrue(provider_state["https://html.duckduckgo.com/html/"]["disabled"])

    def test_photo_source_candidate_rejects_adjacent_sku_false_positive(self):
        candidate = shopify_sync.score_photo_source_candidate(
            self.product,
            page_url="https://example.com/products/pkm-0012",
            page_title="PKM-0012 Pokemon Booster Box",
            page_text="Pokemon Booster Box SKU PKM-0012 Add to cart",
            image_url="https://cdn.example.com/pokemon-booster-box-pkm-0012-front.jpg",
            image_alt="Pokemon Booster Box PKM-0012",
        )

        self.assertIsNotNone(candidate)
        self.assertNotIn("sku", candidate.reasons)
        self.assertLess(candidate.score, shopify_sync.PHOTO_SOURCE_WINNER_THRESHOLD)


class PhotoSyncMainFlowTests(unittest.TestCase):
    def test_photo_sync_without_photo_root_uses_default_cache(self):
        client = mock.Mock()
        products = [mock.sentinel.product]
        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp)
            pack = cache_root / "TR-39-13-99120109017-Armageddon-Battalion-Deathwatch"
            pack.mkdir()
            (pack / "01.jpg").write_bytes(b"image")

            with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync"]), \
                 mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=cache_root), \
                 mock.patch("shopify_sync.load_env", return_value={
                     "SHOPIFY_STORE": "example-store",
                     "SHOPIFY_TOKEN": "shpat_test",
                 }), \
                 mock.patch("shopify_sync.Shopify", return_value=client), \
                 mock.patch("shopify_sync.build_gw_product_list", return_value=products), \
                 mock.patch("shopify_sync.run_photo_sync_preflight"), \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            cache_root,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_GW,
        )

    def test_photo_sync_without_photo_root_errors_when_default_cache_missing(self):
        client = mock.Mock()

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync"]), \
             mock.patch("shopify_sync.GW_PHOTO_CACHE_CURRENT", new=Path(tmp) / "missing"), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.build_gw_product_list", return_value=[mock.sentinel.product]), \
             mock.patch("shopify_sync.run_photo_sync_preflight"):
            with self.assertRaisesRegex(RuntimeError, "Run --gw-refresh-cache first"):
                shopify_sync.main()

    def test_photo_sync_with_explicit_photo_root_preserves_local_folder_routing(self):
        client = mock.Mock()
        products = [mock.sentinel.product]
        with tempfile.TemporaryDirectory() as tmp:
            photo_root = Path(tmp)
            folder = photo_root / "TR-39-13-99120109017-Armageddon-Battalion-Deathwatch"
            folder.mkdir()
            (folder / "01.jpg").write_bytes(b"image")

            with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync", "--photo-root", str(photo_root)]), \
                 mock.patch("shopify_sync.load_env", return_value={
                     "SHOPIFY_STORE": "example-store",
                     "SHOPIFY_TOKEN": "shpat_test",
                 }), \
                 mock.patch("shopify_sync.Shopify", return_value=client), \
                 mock.patch("shopify_sync.build_gw_product_list", return_value=products), \
                 mock.patch("shopify_sync.run_photo_sync_preflight"), \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            photo_root,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_GW,
        )

    def test_photo_sync_existing_files_routes_without_photo_root(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-existing-files"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.build_gw_product_list", return_value=products), \
             mock.patch("shopify_sync.run_photo_sync_preflight"), \
             mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            None,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_GW,
        )

    def test_photo_sync_existing_files_all_routes_with_full_product_list(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-existing-files-all"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.build_product_list", return_value=products), \
             mock.patch("shopify_sync.run_photo_sync_preflight"), \
             mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            None,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_SHOPIFY_EXISTING,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
        )

    def test_photo_sync_staged_local_all_routes_with_full_product_list(self):
        client = mock.Mock()
        products = [mock.sentinel.product]
        with tempfile.TemporaryDirectory() as tmp:
            photo_root = Path(tmp)
            folder = photo_root / "PKM-001-Pokemon-Booster-Box"
            folder.mkdir()
            (folder / "01.jpg").write_bytes(b"image")

            with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-staged-local-all", "--photo-root", str(photo_root)]), \
                 mock.patch("shopify_sync.load_env", return_value={
                     "SHOPIFY_STORE": "example-store",
                     "SHOPIFY_TOKEN": "shpat_test",
                 }), \
                 mock.patch("shopify_sync.Shopify", return_value=client), \
                 mock.patch("shopify_sync.build_product_list", return_value=products), \
                 mock.patch("shopify_sync.run_photo_sync_preflight"), \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            photo_root,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
            fallback_audit=True,
        )

    def test_photo_sync_rejects_preflight_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync", "--preflight"]):
            with self.assertRaisesRegex(RuntimeError, "--photo-sync cannot be combined with --preflight"):
                shopify_sync.main()

    def test_photo_sync_existing_files_rejects_photo_root_combination(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-existing-files", "--photo-root", tmp]):
            with self.assertRaisesRegex(RuntimeError, "does not use --photo-root"):
                shopify_sync.main()

    def test_photo_sync_existing_files_all_rejects_photo_root_combination(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-existing-files-all", "--photo-root", tmp]):
            with self.assertRaisesRegex(RuntimeError, "does not use --photo-root"):
                shopify_sync.main()

    def test_photo_sync_staged_local_all_requires_photo_root(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-staged-local-all"]):
            with self.assertRaisesRegex(RuntimeError, "requires --photo-root"):
                shopify_sync.main()

    def test_photo_sync_staged_local_all_rejects_preflight_combination(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-sync-staged-local-all", "--photo-root", tmp, "--preflight"]):
            with self.assertRaisesRegex(RuntimeError, "--photo-sync-staged-local-all cannot be combined with --preflight"):
                shopify_sync.main()

    def test_photo_source_web_all_routes_with_full_product_list(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-source-web-all"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.build_product_list", return_value=products), \
             mock.patch("shopify_sync.run_photo_sync_preflight"), \
             mock.patch("shopify_sync.phase_photo_source_web_all") as phase_photo_source_web_all:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        phase_photo_source_web_all.assert_called_once_with(
            client,
            products,
            dry=False,
        )

    def test_photo_source_web_all_rejects_photo_root_combination(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-source-web-all", "--photo-root", tmp]):
            with self.assertRaisesRegex(RuntimeError, "does not use --photo-root"):
                shopify_sync.main()

    def test_photo_source_web_all_rejects_preflight_combination(self):
        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--photo-source-web-all", "--preflight"]):
            with self.assertRaisesRegex(RuntimeError, "--photo-source-web-all cannot be combined with --preflight"):
                shopify_sync.main()

    def test_recover_zero_media_images_routes_with_full_product_list(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--recover-zero-media-images"]), \
             mock.patch("shopify_sync.load_env", return_value={
                 "SHOPIFY_STORE": "example-store",
                 "SHOPIFY_TOKEN": "shpat_test",
             }), \
             mock.patch("shopify_sync.Shopify", return_value=client), \
             mock.patch("shopify_sync.build_product_list", return_value=products), \
             mock.patch("shopify_sync.run_photo_sync_preflight"), \
             mock.patch("shopify_sync.recover_zero_media_images") as recover_zero_media_images:
            result = shopify_sync.main()

        self.assertEqual(result, 0)
        recover_zero_media_images.assert_called_once_with(
            client,
            products,
            dry=False,
        )

    def test_recover_zero_media_images_rejects_photo_root_combination(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.sys.argv", ["shopify_sync.py", "--recover-zero-media-images", "--photo-root", tmp]):
            with self.assertRaisesRegex(RuntimeError, "does not use --photo-root"):
                shopify_sync.main()


class RecoverZeroMediaImagesTests(unittest.TestCase):
    def test_recover_zero_media_images_dry_run_writes_preview_review_and_manifest_without_apply(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "photo_source_cache"
            manifest_path = Path(tmp) / "photo_source_manifest.json"
            with mock.patch("shopify_sync.PHOTO_SOURCE_CACHE_ROOT", new=cache_root), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_RECOVERY_RUNS_ROOT", new=cache_root / "recovery_runs"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_MANIFEST_JSON", new=manifest_path), \
                 mock.patch("shopify_sync.phase_photo_source_web_all") as phase_photo_source_web_all, \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                shopify_sync.recover_zero_media_images(client, products, dry=True)

        phase_photo_source_web_all.assert_called_once()
        called_kwargs = phase_photo_source_web_all.call_args.kwargs
        self.assertEqual(called_kwargs["cache_root"].name, "winners")
        self.assertIn("recovery_runs", str(called_kwargs["cache_root"]))
        phase_photo_sync.assert_not_called()

    def test_recover_zero_media_images_dry_run_recomputes_preview_without_mutating_stale_manifest(self):
        product = shopify_sync.Product(
            title="Pokemon Booster Box",
            sku="PKM-001",
            vendor="Pokemon",
            source="INV",
        )
        existing = {
            "sku": product.sku,
            "title": product.title,
            "vendor": product.vendor,
            "media_ids": [],
        }

        class Client:
            def __init__(self):
                import requests
                self.session = requests.Session()

            def iter_existing_for_photo_sync(self):
                yield existing

        search_html = '<html><body><a href="https://example.com/pkm-001-product">Pokemon Booster Box</a></body></html>'
        candidate_html = """
        <html><head>
          <title>PKM-001 Pokemon Booster Box</title>
          <meta property="og:image" content="https://cdn.example.com/pokemon-booster-box-pkm-001-front.jpg">
        </head><body>
          <div>Pokemon Booster Box</div><div>SKU PKM-001</div><div>Add to cart</div>
        </body></html>
        """

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            preview = root / "photo_source_preview.csv"
            review = root / "photo_source_review.csv"
            manifest_path = root / "photo_source_manifest.json"
            cache_root = root / "photo_source_cache"
            manifest_path.write_text(json.dumps({
                product.sku: {
                    "state": "missing",
                    "query": shopify_sync.build_photo_source_query(product),
                    "policy_version": "psv1-stale",
                    "reason": "old missing result",
                }
            }), encoding="utf-8")
            before = manifest_path.read_text(encoding="utf-8")
            client = Client()
            with mock.patch("shopify_sync.PHOTO_SOURCE_PREVIEW_CSV", new=preview), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_REVIEW_CSV", new=review), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_MISSING_TSV", new=root / "photo_source_missing.tsv"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_AMBIGUOUS_TSV", new=root / "photo_source_ambiguous.tsv"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_FAILURES_TSV", new=root / "photo_source_failures.tsv"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_UNMAPPED_SHOPIFY_TSV", new=root / "photo_source_unmapped.tsv"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_CACHE_ROOT", new=cache_root), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_RECOVERY_RUNS_ROOT", new=cache_root / "recovery_runs"), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_MANIFEST_JSON", new=manifest_path), \
                 mock.patch("shopify_sync.fetch_url_with_retries", side_effect=[
                     FakeResponse(text=search_html, url="https://search"),
                     FakeResponse(text=candidate_html, url="https://example.com/pkm-001-product"),
                 ]), \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                shopify_sync.recover_zero_media_images(client, [product], dry=True)

            after = manifest_path.read_text(encoding="utf-8")
            with preview.open(encoding="utf-8") as fh:
                preview_rows = list(csv.DictReader(fh))
            winner_roots = list((cache_root / "recovery_runs").glob("*/winners"))

        self.assertEqual(preview_rows[0]["status"], "winner")
        self.assertEqual(before, after)
        self.assertEqual(len(winner_roots), 1)
        phase_photo_sync.assert_not_called()

    def test_recover_zero_media_images_live_applies_only_current_run_winner_root(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "photo_source_cache"
            current_run_root = cache_root / "recovery_runs" / "20260501T095242Z" / "winners"
            prior_run_root = cache_root / "recovery_runs" / "20260430T095242Z" / "winners"
            current_pack = current_run_root / "PKM-001-pokemon-booster-box"
            prior_pack = prior_run_root / "STALE-001-old-pack"
            current_pack.mkdir(parents=True)
            prior_pack.mkdir(parents=True)
            (current_pack / "01.jpg").write_bytes(b"winner")
            (prior_pack / "01.jpg").write_bytes(b"stale")
            with mock.patch("shopify_sync.PHOTO_SOURCE_CACHE_ROOT", new=cache_root), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_RECOVERY_RUNS_ROOT", new=cache_root / "recovery_runs"), \
                 mock.patch("shopify_sync.create_photo_source_recovery_run_id", return_value="20260501T095242Z"), \
                 mock.patch("shopify_sync.phase_photo_source_web_all") as phase_photo_source_web_all, \
                 mock.patch("shopify_sync.phase_photo_sync") as phase_photo_sync:
                shopify_sync.recover_zero_media_images(client, products, dry=False)

        phase_photo_source_web_all.assert_called_once()
        phase_photo_sync.assert_called_once_with(
            client,
            products,
            current_run_root,
            dry=False,
            source_mode=shopify_sync.PHOTO_SYNC_SOURCE_STAGED_LOCAL,
            product_scope=shopify_sync.PHOTO_SYNC_SCOPE_ALL,
            fallback_audit=True,
        )

    def test_recover_zero_media_images_creates_and_logs_run_root_before_source_phase(self):
        client = mock.Mock()
        products = [mock.sentinel.product]

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "photo_source_cache"
            winner_root = cache_root / "recovery_runs" / "20260501T140500Z" / "winners"
            with mock.patch("shopify_sync.PHOTO_SOURCE_CACHE_ROOT", new=cache_root), \
                 mock.patch("shopify_sync.PHOTO_SOURCE_RECOVERY_RUNS_ROOT", new=cache_root / "recovery_runs"), \
                 mock.patch("shopify_sync.create_photo_source_recovery_run_id", return_value="20260501T140500Z"), \
                 mock.patch("shopify_sync.phase_photo_source_web_all") as phase_photo_source_web_all, \
                 mock.patch("shopify_sync.log") as log:
                shopify_sync.recover_zero_media_images(client, products, dry=True)
                self.assertTrue(winner_root.exists())
                phase_photo_source_web_all.assert_called_once_with(
                    client,
                    products,
                    dry=True,
                    cache_root=winner_root,
                )
                self.assertTrue(any("run_id=20260501T140500Z" in str(call.args[0]) for call in log.call_args_list))


class GWCacheRefreshTests(unittest.TestCase):
    class FakeSession:
        def __init__(self, mapping):
            self.mapping = mapping

        def get(self, url, timeout=60):
            response = self.mapping[url]
            if isinstance(response, list):
                if not response:
                    raise AssertionError(f"No more queued responses for {url}")
                response = response.pop(0)
            if isinstance(response, Exception):
                raise response
            response.url = url
            return response

    def _resources_page(self):
        return """
        <html><body>
          <div>TR-39-13-99120109017-Armageddon-Battalion-Deathwatch</div>
          <a href="https://trade.games-workshop.com/resource/deathwatch.html">Download jpg</a>
          <div>TR-39-13-99120109017-Armageddon-Battalion-Deathwatch</div>
          <a href="https://trade.games-workshop.com/resource/deathwatch-alt.html">Download jpg</a>
          <div>TR-50-72-99120103128-Orks-Wazdakka-Gutsmek</div>
          <a href="https://www.games-workshop.com/some-blocked-page">Download jpg</a>
        </body></html>
        """

    def _detail_page(self):
        return """
        <html><body>
          <a href="https://trade.games-workshop.com/images/folder/01.jpg">One</a>
          <a href="https://trade.games-workshop.com/images/folder/sub/01.jpg">Two</a>
        </body></html>
        """

    def _alt_detail_page(self):
        return """
        <html><body>
          <a href="https://trade.games-workshop.com/images/alt/01.jpg">One</a>
          <a href="https://trade.games-workshop.com/images/alt/sub/01.jpg">Two</a>
        </body></html>
        """

    def _archive_only_resources_page(self):
        return """
        <html><body>
          <div>TR-39-13-99120109017-Armageddon-Battalion-Deathwatch</div>
          <a href="https://trade.games-workshop.com/downloads/deathwatch-pack.zip">Download jpg</a>
        </body></html>
        """

    def _zip_bytes(self, files):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for name, data in files.items():
                zf.writestr(name, data)
        return buf.getvalue()

    def test_refresh_dry_run_does_not_create_status_file(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            result = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=True,
                logger=lambda msg: None,
                session=session,
            )

        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(status_path.exists())
        self.assertFalse(cache_root.exists())

    def test_refresh_dry_run_does_not_mutate_existing_status_file(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            original = '{"status":"published","published_fingerprint":"abc"}'
            status_path.write_text(original, encoding="utf-8")
            gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=True,
                logger=lambda msg: None,
                session=session,
            )
            self.assertEqual(status_path.read_text(encoding="utf-8"), original)

    def test_refresh_publishes_cache_with_collision_safe_names(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
            "https://trade.games-workshop.com/images/folder/01.jpg": FakeResponse(content=b"one"),
            "https://trade.games-workshop.com/images/folder/sub/01.jpg": FakeResponse(content=b"two"),
            "https://trade.games-workshop.com/images/alt/01.jpg": FakeResponse(content=b"three"),
            "https://trade.games-workshop.com/images/alt/sub/01.jpg": FakeResponse(content=b"four"),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )
            current = cache_root / "current"
            pack_dirs = sorted(path.name for path in current.iterdir() if path.is_dir())
            image_names = sorted(path.name for path in current.rglob("*") if path.is_file())

        self.assertEqual(status["status"], "published")
        self.assertGreaterEqual(len(pack_dirs), 1)
        self.assertEqual(len(image_names), 4)
        self.assertEqual(len(set(image_names)), 4)

    def test_refresh_skips_external_html_pages_that_would_403(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
            "https://trade.games-workshop.com/images/folder/01.jpg": FakeResponse(content=b"one"),
            "https://trade.games-workshop.com/images/folder/sub/01.jpg": FakeResponse(content=b"two"),
            "https://trade.games-workshop.com/images/alt/01.jpg": FakeResponse(content=b"three"),
            "https://trade.games-workshop.com/images/alt/sub/01.jpg": FakeResponse(content=b"four"),
            "https://www.games-workshop.com/some-blocked-page": FakeResponse(status_code=403),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )

        self.assertEqual(status["status"], "published")

    def test_refresh_extracts_images_from_zip_only_pack(self):
        archive_bytes = self._zip_bytes({
            "nested/99120109017-ArmageddonBattalionDeathwatch01.jpg": b"one",
            "nested/sub/99120109017-ArmageddonBattalionDeathwatch02.jpg": b"two",
            "notes/readme.txt": b"skip",
        })
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._archive_only_resources_page()),
            "https://trade.games-workshop.com/downloads/deathwatch-pack.zip": FakeResponse(content=archive_bytes),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )
            current = cache_root / "current"
            pack_dirs = sorted(path.name for path in current.iterdir())
            pack_dir = next(current.iterdir())
            image_names = sorted(path.name for path in pack_dir.iterdir())

        self.assertEqual(status["status"], "published")
        self.assertEqual(status["archive_count"], 1)
        self.assertEqual(status["image_count"], 2)
        self.assertEqual(pack_dirs, ["99120109017-ArmageddonBattalionDeathwatch"])
        self.assertEqual(
            image_names,
            [
                "nested-99120109017-ArmageddonBattalionDeathwatch01.jpg",
                "sub-99120109017-ArmageddonBattalionDeathwatch02.jpg",
            ],
        )

    def test_refresh_ignores_macosx_zip_artifacts(self):
        archive_bytes = self._zip_bytes({
            "__MACOSX/nested/99120109017-ArmageddonBattalionDeathwatch01.jpg": b"artifact",
            "nested/99120109017-ArmageddonBattalionDeathwatch01.jpg": b"real",
            "nested/._99120109017-ArmageddonBattalionDeathwatch02.jpg": b"artifact",
        })
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._archive_only_resources_page()),
            "https://trade.games-workshop.com/downloads/deathwatch-pack.zip": FakeResponse(content=archive_bytes),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )
            current = cache_root / "current"
            image_names = sorted(path.name for path in current.rglob("*") if path.is_file())

        self.assertEqual(status["status"], "published")
        self.assertEqual(image_names, ["nested-99120109017-ArmageddonBattalionDeathwatch01.jpg"])

    def test_refresh_rejects_unsupported_archive_only_sources(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text="""
                <html><body>
                  <div>TR-39-13-99120109017-Armageddon-Battalion-Deathwatch</div>
                  <a href="https://trade.games-workshop.com/downloads/deathwatch-pack.7z">Download jpg</a>
                </body></html>
            """),
        })

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "Archive type '.7z' is not supported"):
                gw_cache_refresh.refresh_gw_cache(
                    resources_url="https://trade.games-workshop.com/resources/",
                    cache_root=Path(tmp) / "gw_photo_cache",
                    status_path=Path(tmp) / "gw_photo_cache_status.json",
                    dry=False,
                    logger=lambda msg: None,
                    session=session,
                )

    def test_refresh_retries_transient_archive_download_reset(self):
        archive_bytes = self._zip_bytes({
            "nested/99120109017-ArmageddonBattalionDeathwatch01.jpg": b"one",
        })
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._archive_only_resources_page()),
            "https://trade.games-workshop.com/downloads/deathwatch-pack.zip": [
                requests.exceptions.ConnectionError("connection reset by peer"),
                FakeResponse(content=archive_bytes),
            ],
        })

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("gw_cache_refresh.time.sleep") as sleep:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )

        self.assertEqual(status["status"], "published")
        sleep.assert_called_once_with(1.0)

    def test_refresh_extracts_archive_only_pack_with_fallback_label(self):
        archive_bytes = self._zip_bytes({
            "nested/box-art.jpg": b"one",
            "nested/sub/rear-shot.jpg": b"two",
        })
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._archive_only_resources_page()),
            "https://trade.games-workshop.com/downloads/deathwatch-pack.zip": FakeResponse(content=archive_bytes),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"
            status = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=session,
            )
            current = cache_root / "current"
            pack_dirs = sorted(path.name for path in current.iterdir())
            image_names = sorted(path.name for path in next(current.iterdir()).iterdir())

        self.assertEqual(status["status"], "published")
        self.assertEqual(pack_dirs, ["TR-39-13-99120109017-Armageddon-Battalion-Deathwatch"])
        self.assertEqual(image_names, ["nested-box-art.jpg", "sub-rear-shot.jpg"])

    def test_refresh_failure_preserves_current_cache_and_marks_failed(self):
        session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
            "https://trade.games-workshop.com/images/folder/01.jpg": FakeResponse(status_code=500),
            "https://trade.games-workshop.com/images/folder/sub/01.jpg": FakeResponse(content=b"two"),
            "https://trade.games-workshop.com/images/alt/01.jpg": FakeResponse(content=b"three"),
            "https://trade.games-workshop.com/images/alt/sub/01.jpg": FakeResponse(content=b"four"),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            current = cache_root / "current" / "existing-pack"
            current.mkdir(parents=True)
            existing = current / "keep.jpg"
            existing.write_bytes(b"keep")
            before = gw_cache_refresh.compute_tree_fingerprint(cache_root / "current")
            status_path = Path(tmp) / "gw_photo_cache_status.json"

            with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                gw_cache_refresh.refresh_gw_cache(
                    resources_url="https://trade.games-workshop.com/resources/",
                    cache_root=cache_root,
                    status_path=status_path,
                    dry=False,
                    logger=lambda msg: None,
                    session=session,
                )

            after = gw_cache_refresh.compute_tree_fingerprint(cache_root / "current")
            status = json.loads(status_path.read_text(encoding="utf-8"))
            preserved = existing.read_bytes()

        self.assertEqual(before, after)
        self.assertEqual(preserved, b"keep")
        self.assertEqual(status["status"], "failed")
        self.assertTrue(status["failure_reason"])

    def test_refresh_recovery_clears_failure_reason_and_preserves_last_failure_at(self):
        fail_session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
            "https://trade.games-workshop.com/images/folder/01.jpg": FakeResponse(status_code=500),
            "https://trade.games-workshop.com/images/folder/sub/01.jpg": FakeResponse(content=b"two"),
            "https://trade.games-workshop.com/images/alt/01.jpg": FakeResponse(content=b"three"),
            "https://trade.games-workshop.com/images/alt/sub/01.jpg": FakeResponse(content=b"four"),
        })
        success_session = self.FakeSession({
            "https://trade.games-workshop.com/resources/": FakeResponse(text=self._resources_page()),
            "https://trade.games-workshop.com/resource/deathwatch.html": FakeResponse(text=self._detail_page()),
            "https://trade.games-workshop.com/resource/deathwatch-alt.html": FakeResponse(text=self._alt_detail_page()),
            "https://trade.games-workshop.com/images/folder/01.jpg": FakeResponse(content=b"one"),
            "https://trade.games-workshop.com/images/folder/sub/01.jpg": FakeResponse(content=b"two"),
            "https://trade.games-workshop.com/images/alt/01.jpg": FakeResponse(content=b"three"),
            "https://trade.games-workshop.com/images/alt/sub/01.jpg": FakeResponse(content=b"four"),
        })

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "gw_photo_cache_status.json"

            with self.assertRaises(RuntimeError):
                gw_cache_refresh.refresh_gw_cache(
                    resources_url="https://trade.games-workshop.com/resources/",
                    cache_root=cache_root,
                    status_path=status_path,
                    dry=False,
                    logger=lambda msg: None,
                    session=fail_session,
                )
            failed_status = json.loads(status_path.read_text(encoding="utf-8"))

            published = gw_cache_refresh.refresh_gw_cache(
                resources_url="https://trade.games-workshop.com/resources/",
                cache_root=cache_root,
                status_path=status_path,
                dry=False,
                logger=lambda msg: None,
                session=success_session,
            )

        self.assertEqual(published["status"], "published")
        self.assertEqual(published["failure_reason"], "")
        self.assertEqual(published["last_failure_at"], failed_status["last_failure_at"])
        self.assertTrue(published["last_success_at"])
        self.assertTrue(published["finished_at"])
        self.assertTrue(published["published_fingerprint"])

class TradeFeedDiscoveryTests(unittest.TestCase):
    FIXTURE_PATH = (
        Path(__file__).resolve().parent / "fixtures" / "gw_trade_feed_page_1.json"
    )

    @classmethod
    def setUpClass(cls):
        if not cls.FIXTURE_PATH.exists():
            raise unittest.SkipTest(f"Missing fixture: {cls.FIXTURE_PATH}")
        cls.real_page1 = json.loads(cls.FIXTURE_PATH.read_text(encoding="utf-8"))

    class StubSession:
        def __init__(self, payload_by_url):
            self.payload_by_url = dict(payload_by_url)
            self.headers: dict[str, str] = {}
            self.calls: list[str] = []

        def get(self, url, timeout=60):
            self.calls.append(url)
            payload = self.payload_by_url.get(url)
            if payload is None:
                raise AssertionError(f"Unexpected URL requested: {url}")
            return FakeResponse(status_code=200, payload=payload, url=url)

    def _build_url(self, group, page):
        return gw_cache_refresh._build_trade_feed_url(
            group=group,
            page=page,
            page_size=gw_cache_refresh.GW_TRADE_FEED_PAGE_SIZE,
            lang=gw_cache_refresh.GW_TRADE_FEED_LANG,
            country=gw_cache_refresh.GW_TRADE_FEED_COUNTRY,
        )

    def test_discover_trade_feed_packs_parses_real_fixture(self):
        single_group = (46,)
        single_page_payload = {
            "page": 1,
            "page_count": 1,
            "total_items": len(self.real_page1.get("assets", [])),
            "assets": self.real_page1["assets"],
            "nonce": self.real_page1.get("nonce", ""),
        }
        url = self._build_url(46, 1)
        session = self.StubSession({url: single_page_payload})

        packs, marker, stats = gw_cache_refresh.discover_trade_feed_packs(
            session,
            groups=single_group,
            request_delay_seconds=0,
        )

        self.assertEqual(marker, "GW Trade Feed")
        # Every pack must be tagged as trade-feed
        for pack in packs:
            self.assertEqual(pack.source_label, "trade-feed")
            self.assertEqual(len(pack.images), 1)
            self.assertEqual(pack.archives, [])

        # Each of the captured target SKUs must appear with the expected file_name
        labels = {pack.label: pack for pack in packs}
        target_skus = {
            "99122720012": "99122720012_PeasantLevyBOX.jpg",
            "99122720011": "99122720011_IronHailCraneGunnersBOX.jpg",
            "60043005001": "60043005001_JOURNALTACTICABATTLEOFTALLARNPartOne1.jpg",
        }
        for sku, expected_filename in target_skus.items():
            self.assertIn(expected_filename, labels, f"missing {sku} in fixture-derived packs")
            pack = labels[expected_filename]
            extracted_sku = shopify_sync._extract_asset_match_code(pack.label)
            self.assertEqual(extracted_sku, sku)
            # file_url and file_name preserved verbatim
            self.assertTrue(pack.images[0].url.endswith(expected_filename))
            self.assertEqual(pack.images[0].filename, expected_filename)

        # Stats reflect the page count
        self.assertEqual(stats["page_count_by_group"], {"46": 1})
        self.assertEqual(stats["request_count"], 1)
        self.assertEqual(stats["image_count"], len(packs))
        # Session headers were set
        self.assertEqual(session.headers.get("User-Agent"), gw_cache_refresh.GW_TRADE_FEED_USER_AGENT)
        self.assertEqual(session.headers.get("Referer"), gw_cache_refresh.GW_TRADE_FEED_REFERER)

    def test_discover_trade_feed_packs_walks_all_pages(self):
        page1 = {
            "page": 1,
            "page_count": 2,
            "total_items": 2,
            "assets": [
                {
                    "id": 100,
                    "title": "T-1",
                    "file_name": "11111111_a.jpg",
                    "file_url": "https://example.test/a.jpg",
                    "mime_type": "image/jpeg",
                }
            ],
        }
        page2 = {
            "page": 2,
            "page_count": 2,
            "total_items": 2,
            "assets": [
                {
                    "id": 101,
                    "title": "T-2",
                    "file_name": "22222222_b.jpg",
                    "file_url": "https://example.test/b.jpg",
                    "mime_type": "image/jpeg",
                }
            ],
        }
        u1 = self._build_url(46, 1)
        u2 = self._build_url(46, 2)
        session = self.StubSession({u1: page1, u2: page2})

        packs, _, stats = gw_cache_refresh.discover_trade_feed_packs(
            session,
            groups=(46,),
            request_delay_seconds=0,
        )

        self.assertEqual(stats["page_count_by_group"], {"46": 2})
        self.assertEqual(stats["request_count"], 2)
        self.assertEqual(len(packs), 2)
        self.assertEqual(session.calls, [u1, u2])

    def test_discover_trade_feed_packs_filters_non_image_mime_types(self):
        page1 = {
            "page": 1,
            "page_count": 1,
            "total_items": 3,
            "assets": [
                {
                    "id": 1,
                    "file_name": "11111111_x.jpg",
                    "file_url": "https://example.test/x.jpg",
                    "mime_type": "image/jpeg",
                },
                {
                    "id": 2,
                    "file_name": "22222222_y.pdf",
                    "file_url": "https://example.test/y.pdf",
                    "mime_type": "application/pdf",
                },
                {
                    "id": 3,
                    "file_name": "33333333_z.zip",
                    "file_url": "https://example.test/z.zip",
                    "mime_type": "application/x-zip-compressed",
                },
            ],
        }
        u1 = self._build_url(46, 1)
        session = self.StubSession({u1: page1})
        packs, _, stats = gw_cache_refresh.discover_trade_feed_packs(
            session,
            groups=(46,),
            request_delay_seconds=0,
        )
        self.assertEqual(len(packs), 1)
        self.assertEqual(packs[0].label, "11111111_x.jpg")
        self.assertEqual(stats["image_count"], 1)

    def test_discover_trade_feed_packs_dedupes_by_id(self):
        same_asset = {
            "id": 555,
            "file_name": "99999999_repeat.jpg",
            "file_url": "https://example.test/repeat.jpg",
            "mime_type": "image/jpeg",
        }
        page1 = {"page": 1, "page_count": 2, "total_items": 2, "assets": [same_asset]}
        page2 = {"page": 2, "page_count": 2, "total_items": 2, "assets": [dict(same_asset)]}
        u1 = self._build_url(46, 1)
        u2 = self._build_url(46, 2)
        session = self.StubSession({u1: page1, u2: page2})
        packs, _, stats = gw_cache_refresh.discover_trade_feed_packs(
            session,
            groups=(46,),
            request_delay_seconds=0,
        )
        self.assertEqual(len(packs), 1)
        self.assertEqual(stats["image_count"], 1)

    def test_refresh_gw_cache_merges_resources_and_trade_feed(self):
        # Patch discover_resource_packs to return a synthetic pack
        legacy_pack = gw_cache_refresh.ResourcePack(
            label="legacy-pack",
            images=[gw_cache_refresh.ImageTarget(url="https://example.test/legacy.jpg", filename="legacy.jpg")],
            archives=[],
        )
        feed_pack = gw_cache_refresh.ResourcePack(
            label="99122720012_PeasantLevyBOX.jpg",
            images=[gw_cache_refresh.ImageTarget(
                url="https://example.test/feed.jpg",
                filename="99122720012_PeasantLevyBOX.jpg",
            )],
            archives=[],
            source_label=gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL,
        )

        def fake_discover_resources(url, session):
            return [legacy_pack], "Product Images"

        def fake_discover_feed(session, **kwargs):
            return (
                [feed_pack],
                "GW Trade Feed",
                {
                    "url": gw_cache_refresh.GW_TRADE_FEED_BASE,
                    "groups": [46, 47],
                    "country": 220,
                    "lang": "en",
                    "page_size": 24,
                    "page_count_by_group": {"46": 1, "47": 1},
                    "request_count": 2,
                    "image_count": 1,
                },
            )

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "status.json"
            with mock.patch.object(gw_cache_refresh, "discover_resource_packs", fake_discover_resources):
                with mock.patch.object(gw_cache_refresh, "discover_trade_feed_packs", fake_discover_feed):
                    result = gw_cache_refresh.refresh_gw_cache(
                        resources_url="https://trade.games-workshop.com/resources/",
                        cache_root=cache_root,
                        status_path=status_path,
                        dry=True,
                        logger=lambda msg: None,
                    )

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["pack_count"], 2)
        self.assertEqual(result["image_count"], 2)
        self.assertIn("trade_feed", result)
        self.assertEqual(result["trade_feed"]["image_count"], 1)
        self.assertEqual(result["trade_feed"]["page_count_by_group"], {"46": 1, "47": 1})

    def test_refresh_gw_cache_records_trade_feed_failure_gracefully(self):
        legacy_pack = gw_cache_refresh.ResourcePack(
            label="legacy-pack",
            images=[gw_cache_refresh.ImageTarget(url="https://example.test/legacy.jpg", filename="legacy.jpg")],
            archives=[],
        )

        def fake_discover_resources(url, session):
            return [legacy_pack], "Product Images"

        def failing_discover_feed(session, **kwargs):
            raise RuntimeError("simulated cloudflare 403")

        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp) / "gw_photo_cache"
            status_path = Path(tmp) / "status.json"
            with mock.patch.object(gw_cache_refresh, "discover_resource_packs", fake_discover_resources):
                with mock.patch.object(gw_cache_refresh, "discover_trade_feed_packs", failing_discover_feed):
                    result = gw_cache_refresh.refresh_gw_cache(
                        resources_url="https://trade.games-workshop.com/resources/",
                        cache_root=cache_root,
                        status_path=status_path,
                        dry=True,
                        logger=lambda msg: None,
                    )
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["pack_count"], 1)  # legacy only
        self.assertIn("simulated cloudflare 403", result["trade_feed"]["failure_reason"])


class PhotoAssetSourcePriorityTests(unittest.TestCase):
    def _write_image(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\xff\xd8\xff\xd9")  # tiny JPEG header/footer

    def test_discover_photo_asset_sets_marks_trade_feed_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # legacy pack: no marker file
            legacy_dir = root / "legacy-pack"
            self._write_image(legacy_dir / "99122720012_legacy.jpg")
            # trade-feed pack: marker present
            tf_dir = root / "99122720012-PeasantLevyBOX-jpg"
            self._write_image(tf_dir / "99122720012_PeasantLevyBOX.jpg")
            (tf_dir / gw_cache_refresh.GW_PACK_SOURCE_MARKER_FILENAME).write_text(
                gw_cache_refresh.GW_TRADE_FEED_SOURCE_LABEL, encoding="utf-8"
            )

            sets = shopify_sync.discover_photo_asset_sets(root)
            by_label = {s.label: s for s in sets}
            self.assertEqual(by_label["legacy-pack"].source_priority, 0)
            self.assertEqual(
                by_label["99122720012-PeasantLevyBOX-jpg"].source_priority,
                shopify_sync.PHOTO_ASSET_TRADE_FEED_PRIORITY,
            )

    def test_resolve_photo_asset_prefers_trade_feed_for_ambiguous(self):
        product = shopify_sync.Product(
            title="Warhammer The Old World Grand Cathay Peasant Levy",
            sku="99122720012",
            vendor="Games Workshop",
            source="GW",
        )
        legacy_set = shopify_sync.PhotoAssetSet(
            key="dir:legacy",
            label="99122720012-Legacy-Title",
            product_code="99122720012",
            title_slug="legacy-title",
            image_paths=[Path("/tmp/legacy/legacy.jpg")],
            source_priority=0,
        )
        feed_set = shopify_sync.PhotoAssetSet(
            key="dir:trade-feed",
            label="99122720012-PeasantLevyBOX-jpg",
            product_code="99122720012",
            title_slug="peasantlevybox",
            image_paths=[Path("/tmp/trade-feed/99122720012_PeasantLevyBOX.jpg")],
            source_priority=shopify_sync.PHOTO_ASSET_TRADE_FEED_PRIORITY,
        )
        by_code = {"99122720012": [legacy_set, feed_set]}
        by_slug: dict = {}

        action, match_type, chosen, reason = shopify_sync.resolve_photo_asset(
            product, by_code, by_slug
        )

        self.assertEqual(action, "replace")
        self.assertEqual(match_type, "exact_best")
        self.assertIs(chosen, feed_set)
        self.assertEqual(chosen.source_priority, shopify_sync.PHOTO_ASSET_TRADE_FEED_PRIORITY)


if __name__ == "__main__":
    unittest.main()
