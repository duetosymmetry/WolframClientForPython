from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from itertools import starmap

from wolframclient.language import wl
from wolframclient.utils.api import pandas, pyarrow
from wolframclient.utils.datastructures import Settings
from wolframclient.utils.dispatch import Dispatch
from wolframclient.utils.functional import composition, identity

encoder = Dispatch()


def arrow_to_association(o, index=None):
    return wl.AssociationThread(index or wl.Range(0, len(o) - 1), wl.Normal(o))


encoders = dict(
    tabular=lambda o, i: o,
    values=lambda o, i: wl.Normal(o),
    association=arrow_to_association,
    dataset=composition(arrow_to_association, wl.Dataset),
    list=composition(arrow_to_association, wl.Normal),
)


def internal_serialize(serializer, o, index, prop_name, default, from_function):
    head = serializer.get_property(prop_name, d=None) or default

    if not head in encoders:
        raise ValueError(
            "Invalid value for property '{}'. Expecting one of ({}), got {}.".format(
                prop_name, ", ".join(encoders.keys()), head
            )
        )

    func = composition(encoders[head], serializer.encode)

    return func(from_function(o, preserve_index = head =='tabular'), index)


@encoder.dispatch(pandas.DataFrame)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer,
        o,
        from_function = pyarrow.RecordBatch.from_pandas,
        index=o.index.tolist(),
        prop_name="pandas_dataframe_head",
        default="tabular",
    )


@encoder.dispatch(pandas.DatetimeIndex)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer,
        o,
        from_function = pyarrow.Array.from_pandas,
        index=o.index.tolist(),
        prop_name="pandas_dataframe_head",
        default="tabular",
    )


@encoder.dispatch(pandas.Series)
def encode_panda_series(serializer, o):
    if hasattr(o, "sparse"):
        o = o.sparse.to_dense()

    return internal_serialize(
        serializer,
        o,
        from_function = pyarrow.Array.from_pandas,
        index=o.index.tolist(),
        prop_name="pandas_series_head",
        default="tabular",
    )
