from pathlib import Path


class DataPool:
    def contains(self, depot: int, gid: int) -> bool:
        return False

def make_data_pool(pool_dir: Path) -> DataPool:
    return DataPool()