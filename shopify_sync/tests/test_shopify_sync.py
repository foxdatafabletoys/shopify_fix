import unittest
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


if __name__ == "__main__":
    unittest.main()
