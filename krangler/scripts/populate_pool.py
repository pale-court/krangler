from dataclasses import dataclass
import os
from pathlib import Path
from typing import List
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings

from krangler.data_pool import make_data_pool
from krangler.meta_api import fetch_build_collection


class Settings(BaseSettings):
    api_url: AnyHttpUrl
    # root_dir: Path
    depot_downloader: Path
    data_pool_dir: Path
    data_scratch_dir: Path
    cached_bundle_dir: Path = None
    cached_pack_dir: Path = None
    cached_zip_dir: Path = None
    separate_manifest_dir: Path = None
    allow_downloads: bool = True
    steam_user: str
    steam_password: str
    steam_id_base: int


@dataclass
class TodoItem:
    build: int
    depot: int
    gid: int


def main():
    settings = Settings()
    data_pool = make_data_pool(settings.data_pool_dir)
    build_collection = fetch_build_collection(settings.api_url).root
    builds_asc = sorted(build_collection.items(), key=lambda x: int(x[0]))

    todos: list[TodoItem] = []
    for bid, build in builds_asc:
        bid = int(bid)
        for did, depot in build.manifests.items():
            did = int(did)
            gid = int(depot.gid)
            if not data_pool.contains(did, gid):
                todos.append(TodoItem(bid, did, gid))

    packs: dict(int, int) = {}
    for root, dirs, files in os.walk(settings.cached_pack_dir, topdown=True):
        for dir in dirs:
            print(dir)
            if all(map(lambda x: x in "0123456789", dir)):
                packs[int(dir)] = 0
        dirs[:] = []

    # print(packs)

    for item in todos:
        # TODO(LV): Probe existing disk locations for packs, bundles and bundle ZIPs.
        # Uncertain how we should handle packs 
        # target_dir: Path = stage_depot(settings.data_scratch_dir, item.depot, item.gid)
        # ingest_disk(target_dir, item.depot, item.gid)
        if (settings.cached_pack_dir / str(item.gid) / 'Content.ggpk').exists():
            print(item)
            packs[item.gid] += 1

    for gid, count in {x: packs[x] for x in packs if packs[x] > 1}.items():
        print(f"{count} {gid}")

    for gid in {x for x in packs if packs[x] == 0}:
        print(f"0 {gid}")



if __name__ == "__main__":
    main()
