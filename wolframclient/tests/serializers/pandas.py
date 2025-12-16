from __future__ import absolute_import, print_function, unicode_literals

import base64
import json
import os

from wolframclient.serializers import export
from wolframclient.utils.api import numpy, pandas
from wolframclient.utils.tests import TestCase as BaseTestCase

# Load fixture data
FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "pandas_fixture.json")
with open(FIXTURE_PATH) as f:
    FIXTURE_DATA = json.load(f)


class TestCase(BaseTestCase):
    """Test pandas serialization against fixture data for all head options."""

    def test_serialization_matches_fixture(self):
        """Test that all serialization outputs match the fixture."""
        for test_name, fixture_info in FIXTURE_DATA.items():
            python_expr = fixture_info["python"]
            heads = fixture_info["heads"]

            # Evaluate the python expression with explicit globals
            data = eval(python_expr, {"pandas": pandas, "numpy": numpy})

            for head, expected in heads.items():
                with self.subTest(test_name=test_name, head=head):
                    kwargs = {"pandas_series_head": head, "pandas_dataframe_head": head}

                    # Test WL format
                    result_wl = export(data, target_format="wl", **kwargs)
                    expected_wl = expected["wl"].encode("latin-1")
                    self.assertEqual(
                        result_wl,
                        expected_wl,
                        f"WL mismatch for {test_name} with head={head}",
                    )

                    # Test WXF format
                    result_wxf = export(data, target_format="wxf", **kwargs)
                    expected_wxf = base64.b64decode(expected["wxf_b64"])
                    self.assertEqual(
                        result_wxf,
                        expected_wxf,
                        f"WXF mismatch for {test_name} with head={head}",
                    )
