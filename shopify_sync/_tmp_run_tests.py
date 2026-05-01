import unittest

import tests.test_shopify_sync as m


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromModule(m))
    raise SystemExit(0 if result.wasSuccessful() else 1)
