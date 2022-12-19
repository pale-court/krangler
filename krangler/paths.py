from pathlib import Path
from typing import Optional


class Paths:
    def __init__(self, root_dir: Path | str, remote_zips: Optional[Path | str], remote_bundles: Optional[Path | str]):
        self.root = Path(root_dir)
        self.remote_zips = Path(remote_zips) if remote_zips else None
        self.remote_bundles = Path(remote_bundles) if remote_bundles else None

    def state_path(self, depot, manifest, state) -> Path:
        return self.root / f'state/{depot}/{manifest}.{state}'

    def scratch_path(self, depot):
        return self.root / f'scratch/{depot}'

    def zip_path(self, depot: int, manifest: int) -> Path:
        return self.root / f'zips/{depot}/{manifest}.zip'

    def remote_zip_path(self, depot, manifest) -> Optional[Path]:
        if self.remote_zips:
            return self.remote_zips / f'{depot}/{manifest}.zip'

    def remote_bundle_path(self, depot, manifest) -> Optional[Path]:
        if self.remote_bundles:
            return self.remote_bundles / f'{manifest}'

    def loose_index_path(self, depot: int, manifest: int) -> Path:
        return self.root / f'index/{depot}/{manifest}-loose.ndjson.zst'

    def bundled_index_path(self, depot: int, manifest: int) -> Path:
        return self.root / f'index/{depot}/{manifest}-bundled.ndjson.zst'

    def loose_data_tree(self):
        return self.root / 'data'

    def loose_data_path(self, hash: str) -> Path:
        return self.root / f'data/{hash[:2]}/{hash}.bin'

    def bundled_data_tree(self) -> Path:
        return self.root / 'data'

    def bundled_data_path(self, hash: str, compressed=True) -> Path:
        return self.root / f'data/{hash[:2]}/{hash}.bin{".zst" if compressed else ""}'

    def bundled_extent_db(self) -> str:
        return self.root / 'state/bundled_extent_map.mdb'
        return f'postgresql://inya@localhost/inya'
        return f'sqlite:///{self.root / "state/bundled_extent_map.db"}'
