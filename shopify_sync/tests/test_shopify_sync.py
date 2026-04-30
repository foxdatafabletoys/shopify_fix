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
                        "AUTO_COLLECTION::games-workshop",
                        "AUTO_COLLECTION::warhammer-40k",
                        "Games Workshop",
                        "Warhammer 40,000",
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
                    "rules": [],
                },
                {
                    "id": "gid://shopify/Collection/2",
                    "title": "Plush Figures",
                    "handle": "plush-figures",
                    "products_count": 0,
                    "collection_type": "smart",
                    "rules": [],
                },
            ],
        )

    def test_managed_wayland_collection_tag_detects_marker_rule(self):
        collection = {
            "id": "gid://shopify/Collection/1",
            "title": "Games Workshop",
            "handle": "games-workshop",
            "collection_type": "smart",
            "rules": [
                {
                    "column": "TAG",
                    "relation": "EQUALS",
                    "condition": "AUTO_COLLECTION::games-workshop",
                }
            ],
        }

        self.assertEqual(
            shopify_sync.managed_wayland_collection_tag(collection),
            "AUTO_COLLECTION::games-workshop",
        )
        self.assertTrue(shopify_sync.is_managed_wayland_collection(collection))

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

    def test_create_smart_collection_uses_tag_rule_input(self):
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
            "AUTO_COLLECTION::games-workshop",
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
                                "column": "TAG",
                                "relation": "EQUALS",
                                "condition": "AUTO_COLLECTION::games-workshop",
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


class MainFlowTests(unittest.TestCase):
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
            ["gid://shopify/Collection/1"],
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

    def test_build_wayland_collection_matches_assigns_expected_buckets(self):
        products = [
            self._record(
                "gw-1",
                "KILL TEAM: STARTER SET",
                "Games Workshop",
                "Generic",
                ["Games Workshop", "Kill Team - Generic"],
                created_at="2026-04-28T10:00:00Z",
            ),
            self._record(
                "gw-2",
                "WHITE DWARF 512",
                "Games Workshop",
                "Generic",
                ["Games Workshop"],
                created_at="2026-04-29T10:00:00Z",
            ),
            self._record(
                "mini-1",
                "Band of Brothers Two-Player Starter Set",
                "Warlord Games",
                "Warlord Games",
                ["Warlord Games"],
            ),
            self._record(
                "puzzle-1",
                "Mediterranean View Puzzle",
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

        by_collection, unmatched, matches_by_product = shopify_sync.build_wayland_collection_matches(products)

        self.assertEqual([item["id"] for item in by_collection["Games Workshop"]], ["gw-1", "gw-2"])
        self.assertEqual([item["id"] for item in by_collection["Kill Team"]], ["gw-1"])
        self.assertEqual([item["id"] for item in by_collection["White Dwarf"]], ["gw-2"])
        self.assertIn("gw-2", [item["id"] for item in by_collection["Latest Releases"]])
        self.assertEqual([item["id"] for item in by_collection["Miniatures Games"]], ["mini-1"])
        self.assertEqual([item["id"] for item in by_collection["Two-Player Games"]], ["mini-1"])
        self.assertEqual([item["id"] for item in by_collection["Getting Started"]], ["mini-1"])
        self.assertEqual([item["id"] for item in by_collection["Jigsaws"]], ["puzzle-1"])
        self.assertEqual([item["id"] for item in unmatched], ["book-1"])
        self.assertIn("Games Workshop", matches_by_product["gw-1"])


class PhaseGenerateCollectionsTests(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()

    def test_dry_run_writes_preview_only(self):
        products = [
            {
                "id": "gw-1",
                "title": "KILL TEAM: STARTER SET",
                "vendor": "Games Workshop",
                "product_type": "Generic",
                "tags": ["Games Workshop", "Kill Team - Generic"],
                "created_at": "2026-04-29T10:00:00Z",
                "skus": ["gw-1"],
                "search_text": shopify_sync._normalize_search_text("KILL TEAM: STARTER SET Games Workshop Generic Kill Team - Generic"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([])

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            shopify_sync.phase_generate_collections(self.client, dry=True)

        self.client.create_smart_collection.assert_not_called()
        self.client.update_product_tags.assert_not_called()

    def test_live_run_retags_products_and_upserts_smart_collections(self):
        products = [
            {
                "id": "gw-1",
                "title": "WHITE DWARF 512",
                "vendor": "Games Workshop",
                "product_type": "Generic",
                "tags": ["Games Workshop"],
                "created_at": "2026-04-29T10:00:00Z",
                "skus": ["gw-1"],
                "search_text": shopify_sync._normalize_search_text("WHITE DWARF 512 Games Workshop Generic"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([
            {
                "id": "gid://shopify/Collection/1",
                "title": "Games Workshop",
                "handle": "games-workshop",
                "products_count": 1,
                "collection_type": "smart",
                "rules": [
                    {
                        "column": "TAG",
                        "relation": "EQUALS",
                        "condition": "AUTO_COLLECTION::games-workshop",
                    }
                ],
            }
        ])
        self.client.create_smart_collection.side_effect = lambda title, handle, tag: {
            "id": f"gid://shopify/Collection/{handle}",
            "title": title,
            "handle": handle,
        }
        self.client.publish_to_all_channels.return_value = 2

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            shopify_sync.phase_generate_collections(self.client, dry=False)

        self.client.update_product_tags.assert_called_once_with(
            "gw-1",
            ["AUTO_COLLECTION::games-workshop", "AUTO_COLLECTION::latest-releases", "AUTO_COLLECTION::white-dwarf", "Games Workshop"],
        )
        self.client.update_smart_collection.assert_called_once_with(
            "gid://shopify/Collection/1",
            "Games Workshop",
            "games-workshop",
            "AUTO_COLLECTION::games-workshop",
        )
        self.assertEqual(self.client.publish_to_all_channels.call_count, len(shopify_sync.WAYLAND_COLLECTION_SPECS))
        created_titles = {call.args[0] for call in self.client.create_smart_collection.call_args_list}
        self.assertIn("White Dwarf", created_titles)
        self.assertIn("Latest Releases", created_titles)

    def test_live_run_refuses_to_overwrite_unmanaged_smart_collection(self):
        products = [
            {
                "id": "gw-1",
                "title": "WHITE DWARF 512",
                "vendor": "Games Workshop",
                "product_type": "Generic",
                "tags": ["Games Workshop"],
                "created_at": "2026-04-29T10:00:00Z",
                "skus": ["gw-1"],
                "search_text": shopify_sync._normalize_search_text("WHITE DWARF 512 Games Workshop Generic"),
            }
        ]
        self.client.iter_existing_for_collection_generation.return_value = iter(products)
        self.client.iter_all_collections.return_value = iter([
            {
                "id": "gid://shopify/Collection/1",
                "title": "Games Workshop",
                "handle": "games-workshop",
                "products_count": 1,
                "collection_type": "smart",
                "rules": [
                    {
                        "column": "TITLE",
                        "relation": "CONTAINS",
                        "condition": "Workshop",
                    }
                ],
            }
        ])

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("shopify_sync.COLLECTION_GENERATION_PREVIEW_CSV", new=Path(tmp) / "preview.csv"), \
             mock.patch("shopify_sync.COLLECTION_GENERATION_UNMATCHED_CSV", new=Path(tmp) / "unmatched.csv"):
            with self.assertRaisesRegex(RuntimeError, "not managed by this script"):
                shopify_sync.phase_generate_collections(self.client, dry=False)

        self.client.update_smart_collection.assert_not_called()


class PhotoAssetMatchingTests(unittest.TestCase):
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

        self.assertEqual(asset_set.product_code, "PKM-001")
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
             mock.patch("shopify_sync.PHOTO_SOURCE_MISSING_TSV", new=root / "missing.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_AMBIGUOUS_TSV", new=root / "ambiguous.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_FAILURES_TSV", new=root / "failures.tsv"), \
             mock.patch("shopify_sync.PHOTO_SOURCE_UNMAPPED_SHOPIFY_TSV", new=root / "unmapped.tsv"):
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

    def test_photo_source_web_all_marks_equal_high_score_candidates_ambiguous(self):
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
            ambiguous = (root / "ambiguous.tsv").read_text(encoding="utf-8")
            preview = (root / "preview.csv").read_text(encoding="utf-8")

        self.assertIn("multiple candidates cleared the winner threshold", ambiguous)
        self.assertIn("ambiguous", preview)

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

if __name__ == "__main__":
    unittest.main()
