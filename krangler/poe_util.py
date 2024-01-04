import codecs
from contextlib import contextmanager
import logging
from pathlib import PurePosixPath

from atomicwrites import atomic_write
from zstandard import ZstdCompressor
import ndjson

from . import _fnv as fnv
from . import _murmur as murmur


def normalize_path(p: PurePosixPath | str | bytes | memoryview):
    if isinstance(p, PurePosixPath):
        if p.is_absolute():
            p = p.relative_to('/')
        p = str(p)
    return p


class PathHasher:
    pass


"FNV1A hash from the first revision of bundles in 3.11.2"
class PoE_3_11_2_Hash(PathHasher):
    def __init__(self):
        super().__init__()

    def hash_file_path(self, p: PurePosixPath | str | bytes | memoryview):
        p = normalize_path(p)
        s = (p.lower() + '++').encode('UTF-8')
        return fnv.fnv1a64(s)
    
    def hash_dir_path(self, p: PurePosixPath | str | bytes | memoryview):
        p = normalize_path(p)
        s = (p + '++').encode('UTF-8')
        return fnv.fnv1a64(s)
    

"Murmur64A hash from the second revision of bundles in 3.21.2"
class PoE_3_21_2_Hash(PathHasher):
    def __init__(self, seed = 0x1337b33f):
        super().__init__()
        self.seed = seed

    def hash_file_path(self, p: PurePosixPath | str | bytes | memoryview):
        p = normalize_path(p)
        s = p.lower().encode('UTF-8')
        return murmur.murmur2_64a(s, self.seed)
    
    def hash_dir_path(self, p: PurePosixPath | str | bytes | memoryview):
        p = normalize_path(p)
        s = p.lower().encode('UTF-8')
        return murmur.murmur2_64a(s, self.seed)

@contextmanager
def compressed_ndjson_writer(ifh):
    cctx = ZstdCompressor()
    with cctx.stream_writer(ifh, closefd=False, write_return_read=True) as idx_compressor:
        with codecs.getwriter('utf-8')(idx_compressor) as codec:
            yield ndjson.writer(codec)

@contextmanager
def atomic_compressed_ndjson_writer(path):
    with (
        atomic_write(path, mode='wb', overwrite=True) as ifh,
        compressed_ndjson_writer(ifh) as writer,
    ):
        yield writer
    path.chmod(0o644)


class BatchWriter:
    def __init__(self, max_items=None, max_size=None):
        self.max_items = max_items
        self.max_size = max_size
        self.items = []
        self.total_size = 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.commit()

    def put(self, key, data):
        self.items.append((key, data))
        self.total_size += len(data)
        should_commit = False
        if self.max_size is not None and self.max_size < self.total_size:
            should_commit = True
        if self.max_items is not None and self.max_items < len(self.items):
            should_commit = True

        if should_commit:
            self.commit()
    
    def commit(self):
        if len(self.items) > 0:
            # logging.info(f"flushing commit log with {self.total_size} bytes and {len(self.items)} items")
            for fpath,slice in self.items:
                if not fpath.exists():
                    with atomic_write(fpath, mode='wb', overwrite=True) as fh:
                        if fpath.suffix == '.zst':
                            bcctx = ZstdCompressor()
                            for chunk in bcctx.read_to_iter(slice):
                                fh.write(chunk)
                        else:
                            fh.write(slice)
                    fpath.chmod(0o644)
            self.items = []
            self.total_size = 0
