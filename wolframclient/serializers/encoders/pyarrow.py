from __future__ import absolute_import, print_function, unicode_literals

from wolframclient.utils.api import pyarrow
from wolframclient.utils.dispatch import Dispatch
from wolframclient.utils.functional import first

encoder = Dispatch()


@encoder.dispatch(pyarrow.RecordBatch)
def encoder_pyarrow_table(serializer, batch):
    sink = pyarrow.BufferOutputStream()
    strm = pyarrow.ipc.new_stream(sink, batch.schema)
    strm.write_batch(batch)
    buf = sink.getvalue()
    return serializer.serialize_function(
        serializer.serialize_symbol(b"ImportByteArray"),
        (serializer.serialize_bytes(buf), serializer.serialize_string("ArrowIPC")),
    )


@encoder.dispatch(pyarrow.Table)
def encoder_pyarrow_table(serializer, o):
    return serializer.encode(
        first(o.to_batches(max_chunksize=1)) or pyarrow.RecordBatch.from_arrays([[] for _ in o.schema], schema=o.schema)
    )
