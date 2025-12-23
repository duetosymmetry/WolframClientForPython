from __future__ import absolute_import, print_function, unicode_literals

from wolframclient.serializers import export
from wolframclient.utils.api import numpy, pandas
from wolframclient.utils.tests import TestCase as BaseTestCase

TEST_CASES = [
    "pandas.DataFrame.from_dict({'a': [1, 2]})",
    "pandas.DataFrame.from_dict({'a': {'x': 1}, 'b': {'x': [-1]}})",
    "pandas.DataFrame.from_dict({})",
    "pandas.Series([])",
    "pandas.Series([1, 2, 3], index=[-1, 'a', 1])",
    "pandas.Series(numpy.arange(8), index=pandas.MultiIndex(levels=[['a', 'b'], ['x', 'y'], [0]], codes=[[1, 1, 1, 1, 0, 0, 0, 0], [0, 0, 1, 1, 0, 0, 1, 1], [0, -1, 0, -1, 0, -1, 0, -1]]))",
    "pandas.Series([0, 1, 2, 3, 4], index=[0, float('nan'), 2, float('nan'), 4])",
    "pandas.Series([1, 2, 3])",
]


class TestCase(BaseTestCase):
    """Test pandas serialization works for all cases."""

    def test_serialization_works(self):
        """Test that export works for all cases."""
        for python_expr in TEST_CASES:
            with self.subTest(expr=python_expr):
                data = eval(python_expr, {"pandas": pandas, "numpy": numpy, "float": float})
                export(data, target_format="wl")
                export(data, target_format="wxf")
