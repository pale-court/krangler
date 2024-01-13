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
        self.env = lmdb.open(str(path), max_dbs=4, map_size=2**39)
        self.emdb = self.env.open_db(b'_emdb')
        self.pathdb = self.env.open_db(b'_pathdb')
        self.sha1db = self.env.open_db(b'_sha1db')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.env.close()

    def emtxn(self, **kwargs):
        return self.env.begin(db=self.emdb, **kwargs)

    def pathtxn(self, **kwargs):
        return self.env.begin(db=self.pathdb, **kwargs)
    
    def sha1txn(self, **kwargs):
        return self.env.begin(db=self.sha1db, **kwargs)

    def get_extent_hash(self, bundle_hash, file_offset, file_size) -> Optional[bytes]:
        with self.emtxn() as txn:
            key = _extent_hash_key(bundle_hash, file_offset, file_size)
            return txn.get(key)

    def get_extent_hashes(self, bundle_hash) -> List[Tuple[bytes, int, int, bytes]]:
        ret = []
        with self.emtxn() as txn:
            with txn.cursor() as cur:
                cur.set_range(_extent_hash_key(bundle_hash, 0, 0))
                for k, v in iter(cur):
                    bh, fo, fs = _unpack_extent_hash_key(k)
                    if bh != bundle_hash:
                        break
                    ret.append((bh, fo, fs, v))
        return ret

    def set_extent_hash(self, bundle_hash, file_offset, file_size, file_hash):
        with self.emtxn(write=True) as txn:
            key = _extent_hash_key(bundle_hash, file_offset, file_size)
            txn.put(key, file_hash)

    def set_extent_hashes(self, keys, values):
        with self.emtxn(write=True) as txn:
            for k, v in zip(keys, values):
                key = _extent_hash_key(k[0], k[1], k[2])
                txn.put(key, v)

    def get_path(self, path_hash) -> Optional[str]:
        with self.pathtxn() as txn:
            key = struct.pack('<Q', path_hash)
            if val := txn.get(key):
                return val.decode('UTF-8')

    def has_path(self, path_hash) -> bool:
        with self.pathtxn() as txn:
            key = struct.pack('<Q', path_hash)
            with txn.cursor() as cur:
                return cur.set_key(key)

    def set_path(self, path_hash, path):
        with self.pathtxn(write=True) as txn:
            key = struct.pack('<Q', path_hash)
            val = path.encode('UTF-8')
            txn.put(key, val)

    def set_paths(self, path_by_phash):
        with self.pathtxn(write=True) as txn:
            for hash, hashed_path in path_by_phash:
                key = struct.pack('<Q', hashed_path)
                if not txn.get(key):
                    val = hashed_path.path.encode('UTF-8')
                    txn.put(key, val)

    def get_sha256_from_sha1(self, sha1: bytes) -> Optional[bytes]:
        with self.sha1txn() as txn:
            key = sha1
            if val := txn.get(key):
                return val

    def set_sha256_from_sha1(self, sha1: bytes, sha256: bytes):
        with self.sha1txn(write=True) as txn:
            key = sha1
            val = sha256
            txn.put(key, val)
