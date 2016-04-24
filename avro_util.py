import fastavro
from os import urandom, SEEK_SET
import json

SYNC_SIZE = fastavro._writer.SYNC_SIZE
MemoryIO = fastavro._writer.MemoryIO
BLOCK_WRITERS = fastavro._writer.BLOCK_WRITERS

write_header = fastavro._writer.write_header
acquaint_schema = fastavro._writer.acquaint_schema
write_data = fastavro._writer.write_data
write_long = fastavro._writer.write_long

def gen_avro(schema,
           records,
           codec='null',
           sync_interval=1000 * SYNC_SIZE,
           metadata=None):

    sync_marker = urandom(SYNC_SIZE)
    fo = MemoryIO()
    io = MemoryIO()
    block_count = 0
    metadata = metadata or {}
    metadata['avro.codec'] = codec
    metadata['avro.schema'] = json.dumps(schema)

    try:
        block_writer = BLOCK_WRITERS[codec]
    except KeyError:
        raise ValueError('unrecognized codec: %r' % codec)

    def dump():
        write_long(fo, block_count)
        block_writer(fo, io.getvalue())
        fo.write(sync_marker)
        data = fo.getvalue()
        fo.truncate(0)
        fo.seek(0, SEEK_SET)
        io.truncate(0)
        io.seek(0, SEEK_SET)
        return data

    write_header(fo, metadata, sync_marker)
    acquaint_schema(schema)

    for record in records:
        write_data(io, record, schema)
        block_count += 1
        if io.tell() >= sync_interval:
            data = dump()
            yield data
            block_count = 0

    if io.tell() or block_count > 0:
        data = dump()
        yield data

    fo.close()
    io.close()
