from __future__ import absolute_import, print_function, unicode_literals

from collections import OrderedDict
from itertools import starmap

from wolframclient.utils.api import pandas, pyarrow
from wolframclient.utils.dispatch import Dispatch
from wolframclient.language import wl
from wolframclient.utils.functional import identity, composition
from wolframclient.utils.datastructures import Settings

encoder = Dispatch()


def arrow_to_association(o):
    return wl.AssociationThread(wl.Range(0, len(o) - 1), wl.Normal(o))


encoders = dict(
    tabular=identity,
    association=arrow_to_association,
    dataset=composition(arrow_to_association, wl.Dataset),
    list=composition(arrow_to_association, wl.Normal),
    values=wl.Normal,
)


def internal_serialize(serializer, o, prop_name, default, preprocessor):

    head = serializer.get_property(prop_name, d=None) or default

    if not head in encoders:
        raise ValueError(
            "Invalid value for property '{}'. Expecting one of ({}), got {}.".format(
                prop_name, ", ".join(encoders.keys()), head
            )
        )

    func = composition(preprocessor, encoders[head], serializer.encode)

    return func(o)


@encoder.dispatch(pandas.DataFrame)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer,
        o,
        prop_name="pandas_dataframe_head",
        default="dataset",
        preprocessor=pyarrow.record_batch,
    )


@encoder.dispatch(pandas.DatetimeIndex)
def encoder_panda_dataframe(serializer, o):
    return internal_serialize(
        serializer,
        o,
        prop_name="pandas_dataframe_head",
        default="values",
        preprocessor=pyarrow.array,
    )


@encoder.dispatch(pandas.Series)
def encode_panda_series(serializer, o):
    return internal_serialize(
        serializer,
        o,
        prop_name="pandas_dataframe_head",
        default="association",
        preprocessor=pyarrow.array,
    )
