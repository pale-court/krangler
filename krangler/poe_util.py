import codecs
from contextlib import contextmanager
from pathlib import PurePosixPath

from atomicwrites import atomic_write
from zstandard import ZstdCompressor
import ndjson

from . import _fnv as fnv


def hash_file_path(p: PurePosixPath | str):
    if isinstance(p, PurePosixPath):
        if p.is_absolute():
            p = p.relative_to('/')
        p = str(p)
    s = (p.lower() + '++').encode('UTF-8')
    return fnv.fnv1a64(s)


def hash_dir_path(p: PurePosixPath | str | bytes | memoryview):
    if isinstance(p, PurePosixPath):
        if p.is_absolute():
            p = p.relative_to('/')
        p = str(p)
    s = (p + '++').encode('UTF-8')
    return fnv.fnv1a64(s)


@contextmanager
def atomic_compressed_ndjson_writer(path):
    with atomic_write(path, mode='wb', overwrite=True) as ifh:
        cctx = ZstdCompressor()
        with cctx.stream_writer(ifh, closefd=False, write_return_read=True) as idx_compressor:
            with codecs.getwriter('utf-8')(idx_compressor) as codec:
                yield ndjson.writer(codec)
    path.chmod(0o644)