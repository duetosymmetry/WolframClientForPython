from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from itertools import starmap

from wolframclient.language import wl
from wolframclient.utils.api import pandas, pyarrow
from wolframclient.utils.datastructures import Settings
from wolframclient.utils.dispatch import Dispatch
from wolframclient.utils.functional import composition, identity




index_formatter = lambda number: "Index{}".format(number)
column_formatter = lambda number: "Column{}".format(number)


encoder = Dispatch()


def is_auto_index(index):
    return isinstance(index, pandas.RangeIndex)




def arrow_to_association(o, index):
    return wl.AssociationThread(index.to_list(), wl.Normal(o))


encoders = dict(
    tabular=lambda o, i: o,
    values=lambda o, i: wl.Normal(o),
    association=arrow_to_association,
    dataset=composition(arrow_to_association, wl.Dataset),
    list=composition(arrow_to_association, wl.Normal),
)


def internal_serialize(serializer, o, prop_name, default):
    head = serializer.get_property(prop_name, d=None) or default

    if not head in encoders:
        raise ValueError(
            "Invalid value for property '{}'. Expecting one of ({}), got {}.".format(
                prop_name, ", ".join(encoders.keys()), head
            )
        )

    if is_auto_index(o.columns):
        new_columns = [column_formatter(i) for i in range(len(o.columns))]
        o = o.set_axis(new_columns, axis=1)


    if not is_auto_index(o.index):
        new_names = [
            name if name is not None else index_formatter(i) for i, name in enumerate(o.index.names)
        ]
        o.index = o.index.set_names(new_names)

    func = composition(encoders[head], serializer.encode)

    return func(
        pyarrow.RecordBatch.from_pandas(
            o, preserve_index=not is_auto_index(o.index) and head == "tabular"
        ),
        o.index,
    )


@encoder.dispatch(pandas.DataFrame)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer, o, prop_name="pandas_dataframe_head", default="tabular"
    )


@encoder.dispatch(pandas.DatetimeIndex)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer,
        o.to_frame(),
        prop_name="pandas_dataframe_head",
        default="tabular",
    )


@encoder.dispatch(pandas.Series)
def encode_panda_series(serializer, o):
    if hasattr(o, "sparse"):
        o = o.sparse.to_dense()

    return internal_serialize(
        serializer,
        o.to_frame(),
        prop_name="pandas_series_head",
        default="tabular",
    )
