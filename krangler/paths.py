from pathlib import Path
from typing import Optional


def _as_path(p: Optional[Path | str]) -> Optional[Path]:
    if isinstance(p, str):
        p = Path(p)
    return p


class Paths:
    def __init__(
        self,
        root_dir: Path | str,
        cached_bundles: Optional[Path | str],
        cached_packs: Optional[Path | str],
        cached_zips: Optional[Path | str],
        separate_manifest_dir: Optional[Path | str],
    ):
        self.root = Path(root_dir)
        self.cached_bundles = _as_path(cached_bundles)
        self.cached_packs = _as_path(cached_packs)
        self.cached_zips = _as_path(cached_zips)
        self.separate_manifest_dir = _as_path(separate_manifest_dir)

    def state_path(self, depot, manifest, state) -> Path:
        return self.root / f"state/{depot}/{manifest}.{state}"

    def scratch_path(self, depot):
        return self.root / f"scratch/{depot}"

    def zip_path(self, depot: int, manifest: int) -> Path:
        return self.root / f"zips/{depot}/{manifest}.zip"

    def cached_bundle_path(self, depot, manifest) -> Optional[Path]:
        if self.cached_bundles:
            return self.cached_bundles / f"{manifest}"

    def cached_pack_path(self, depot, manifest) -> Optional[Path]:
        if self.cached_packs:
            return self.cached_packs / f"{manifest}"

    def cached_zip_path(self, depot, manifest) -> Optional[Path]:
        if self.cached_zips:
            return self.cached_zips / f"{depot}/{manifest}.zip"

    def separate_manifest_path(self) -> Optional[Path]:
        if self.separate_manifest_dir:
            return self.separate_manifest_dir

    def loose_index_path(self, depot: int, manifest: int) -> Path:
        return self.root / f"index/{depot}/{manifest}-loose.ndjson.zst"

    def bundled_index_path(self, depot: int, manifest: int) -> Path:
        return self.root / f"index/{depot}/{manifest}-bundled.ndjson.zst"

    def loose_data_tree(self):
        return self.root / "data"

    def loose_data_path(self, hash: str | bytes) -> Path:
        if isinstance(hash, bytes):
            hash = hash.hex()
        return self.root / f"data/{hash[:2]}/{hash}.bin"

    def bundled_data_tree(self) -> Path:
        return self.root / "data"

    def bundled_data_path(self, hash: str | bytes, compressed=True) -> Path:
        if isinstance(hash, bytes):
            hash = hash.hex()
        return self.root / f'data/{hash[:2]}/{hash}.bin{".zst" if compressed else ""}'

    def bundled_extent_db(self) -> str:
        return self.root / "state/bundled_extent_map.mdb"
        return f"postgresql://inya@localhost/inya"
        return f'sqlite:///{self.root / "state/bundled_extent_map.db"}'
