from pathlib import Path
from typing import List, Optional

from atomicwrites import atomic_write


class DataPile:
    def __init__(self, root: Path):
        self.root = root
        if not root.exists():
            root.mkdir(parents=True, exist_ok=True)
        for group in range(256):
            (root / f'{group:02x}').mkdir(exist_ok=True)

    def _item_path(self, sha256: bytes) -> Path:
        hash_hex = sha256.hex()
        return self.root / hash_hex[:2] / f'{hash_hex}.bin'

    def has_one(self, sha256: bytes) -> bool:
        return self._item_path(sha256).exists()

    def has_many(self, sha256s: List[bytes]) -> List[bool]:
        ret = []
        for sha256 in sha256s:
            ret.append(self.has_one(sha256))
        return ret

    def read_one(self, sha256: bytes) -> Optional[bytes]:
        p = self._item_path(sha256)
        return p.read_bytes()

    def write_one(self, sha256: bytes, payload: bytes):
        p = self._item_path(sha256)
        try:
            with atomic_write(p, mode='wb', overwrite=False) as writer:
                writer.write(payload)
        except (FileExistsError, PermissionError) as e:
            pass
