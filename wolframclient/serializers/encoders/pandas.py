from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from itertools import starmap
import os
from wolframclient.language import wl
from wolframclient.utils.api import pandas, pyarrow
from wolframclient.utils.datastructures import Settings
from wolframclient.utils.dispatch import Dispatch
from wolframclient.utils.functional import composition, identity
from wolframclient.utils.legacy import is_legacy_mode



index_formatter = lambda number: "Index{}".format(number)
column_formatter = lambda number: "Column{}".format(number)


encoder = Dispatch()


def is_auto_index(index):
    return isinstance(index, pandas.RangeIndex)


def index_to_columns(index, name):
    """Convert an index to columns, splitting mixed-type indices by type.

    For a simple typed index, yields (name, values).
    For a mixed-type index like [-1, 'a', 1] with name 'idx', yields:
        ('idx_int', [-1, None, 1])
        ('idx_str', [None, 'a', None])
    For a MultiIndex, recursively processes each level.
    """
    if isinstance(index, pandas.MultiIndex):
        for i, level_name in enumerate(index.names):
            level_values = index.get_level_values(i)
            yield from index_to_columns(level_values, level_name)
    elif index.dtype == object and len(index) > 0 and len(set(type(v) for v in index)) > 1:
        # Mixed-type index - split by type
        values = index.to_series().reset_index(drop=True)
        types_in_index = set(type(v) for v in values)
        for typ in types_in_index:
            col_name = f"{name}_{typ.__name__}"
            yield col_name, pandas.Series(v if isinstance(v, typ) else None for v in values)
    else:
        # Simple typed index
        yield name, index.to_series().reset_index(drop=True)



def _distribute_multikey(o):
    """Distribute MultiIndex keys into nested OrderedDicts."""
    expr_dict = OrderedDict()
    for multikey, value in o.items():
        cur_dict = expr_dict
        for key in multikey[:-1]:
            if key not in cur_dict:
                cur_dict[key] = OrderedDict()
            cur_dict = cur_dict[key]
        cur_dict[multikey[-1]] = value
    return expr_dict


def legacy_encode_series(serializer, series):
    """Encode a Series based on its index type.

    - DatetimeIndex -> TimeSeries
    - MultiIndex -> nested associations
    - Regular index -> association
    """
    length = len(series)

    if isinstance(series.index, pandas.DatetimeIndex):
        # Encode as TimeSeries
        return serializer.serialize_function(
            serializer.serialize_symbol(b"TimeSeries"),
            (
                serializer.serialize_iterable(
                    (
                        serializer.serialize_iterable(
                            (serializer.encode(idx), serializer.encode(val)), length=2
                        )
                        for idx, val in series.items()
                    ),
                    length=length,
                ),
            ),
        )
    elif isinstance(series.index, pandas.MultiIndex):
        # Encode as nested associations
        return serializer.encode(_distribute_multikey(series))
    else:
        # Encode as simple association
        return serializer.serialize_association(
                (
                    (serializer.encode(idx), serializer.encode(val))
                    for idx, val in series.items()
                ),
                length=length,
            )

def dataset(serializer, *args):
    return serializer.serialize_function(
        serializer.serialize_symbol(b"Dataset"),
        args,
    )

def legacy_encode(serializer, o):
    """Encode DataFrame as Dataset[<association of row -> association of col -> value>].

    This replicates the old default behavior before the tabular implementation.
    The old code used o.T.items() to iterate over rows (transposed columns become rows).
    """
    return dataset(serializer, serializer.serialize_association(
            (
                (serializer.encode(k), legacy_encode_series(serializer, v))
                for k, v in o.T.items()
            ),
            length=len(o.index),
        ))



def pyarrow_serialize(serializer, o):
    if is_auto_index(o.columns):
        new_columns = [column_formatter(i) for i in range(len(o.columns))]
        o = o.set_axis(new_columns, axis=1)

    if not is_auto_index(o.index):
        # Assign default names to unnamed index levels
        new_names = [
            name if name is not None else index_formatter(i)
            for i, name in enumerate(o.index.names)
        ]
        o.index = o.index.set_names(new_names)

        # Convert index to columns
        original_index = o.index
        o = o.reset_index(drop=True)
        for col_name, col_data in index_to_columns(original_index, new_names[0]):
            o[col_name] = col_data

    return serializer.encode(
        pyarrow.RecordBatch.from_pandas(o, preserve_index=False)
    )


@encoder.dispatch(pandas.DataFrame)
def encoder_panda_dataframe(serializer, o):
    if is_legacy_mode():
        return legacy_encode(serializer, o)
    return pyarrow_serialize(serializer, o)


@encoder.dispatch(pandas.DatetimeIndex)
def encoder_panda_datetimeindex(serializer, o):
    if is_legacy_mode():
        return legacy_encode_series(serializer, o.to_series())
    return pyarrow_serialize(serializer, o.to_frame())


@encoder.dispatch(pandas.Series)
def encode_panda_series(serializer, o):
    if hasattr(o, "sparse"):
        o = o.sparse.to_dense()

    if is_legacy_mode():
        return legacy_encode_series(serializer, o)
    return pyarrow_serialize(serializer, o.to_frame())
