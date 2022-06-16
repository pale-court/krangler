from hashlib import sha256
from pathlib import Path
import struct
from typing import List, Optional, Tuple
import lmdb

def _extent_hash_key(bundle_hash, file_offset, file_size) -> bytes:
    return bundle_hash + struct.pack('<II', file_offset, file_size)


def _unpack_extent_hash_key(key) -> Tuple[bytes, int, int]:
    bh = bytes(key[:32])
    fo, fs = struct.unpack('<II', key[32:])
    return (bh, fo, fs)


class BundledExtentMap:
    def __init__(self, path: str | Path):
        self.env = lmdb.open(str(path), map_size=2**39)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.env.close()

    def get_extent_hash(self, bundle_hash, file_offset, file_size) -> Optional[bytes]:
        with self.env.begin() as txn:
            key = _extent_hash_key(bundle_hash, file_offset, file_size)
            return txn.get(key)

    def get_extent_hashes(self, bundle_hash) -> List[Tuple[bytes, int, int, bytes]]:
        ret = []
        with self.env.begin() as txn:
            with txn.cursor() as cur:
                cur.set_range(_extent_hash_key(bundle_hash, 0, 0))
                for k, v in iter(cur):
                    bh, fo, fs = _unpack_extent_hash_key(k)
                    if bh != bundle_hash:
                        break
                    ret.append((bh, fo, fs, v))
        return ret

    def set_extent_hash(self, bundle_hash, file_offset, file_size, file_hash):
        with self.env.begin(write=True) as txn:
            key = _extent_hash_key(bundle_hash, file_offset, file_size)
            txn.put(key, file_hash)

    def set_extent_hashes(self, keys, values):
        with self.env.begin(write=True) as txn:
            for k, v in zip(keys, values):
                key = _extent_hash_key(k[0], k[1], k[2])
                txn.put(key, v)
