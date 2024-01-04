import csv
import os
import sys
import zipfile
import zlib
from pathlib import Path, PurePath, PurePosixPath

import requests
from atomicwrites import atomic_write
from plumbum import local
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from zstandard import ZstdCompressor, ZstdDecompressor

from krangler import protos
from krangler.bundled_extent_map import BundledExtentMap
from krangler.logging import LOG
from krangler.protos import depot_downloader_pb2
from krangler.source import open_source
from .ingest_bundled import ingest_bundled
from .ingest_loose import ingest_source
from .paths import Paths
from .state_tracking import StateTracker
from .store import ArtifactStore, FilesystemStore, DatabaseStore


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="dev-env.env", extra="ignore")
    api_url: AnyHttpUrl
    appinfo_dir: Path
    root_dir: Path
    depot_downloader: Path = None
    cached_bundle_dir: Path = None
    cached_pack_dir: Path = None
    cached_zip_dir: Path = None
    separate_manifest_dir: Path = None
    allow_downloads: bool = True
    allow_zipping: bool = False
    data_db_uri: str = None
    steam_user: str
    steam_password: str
    steam_id_base: int
    molly_guard: str


settings = Settings()
paths = Paths(
    settings.root_dir,
    cached_bundles=settings.cached_bundle_dir,
    cached_packs=settings.cached_pack_dir,
    cached_zips=settings.cached_zip_dir,
    separate_manifest_dir=settings.separate_manifest_dir,
)

current_scratch_manifests = {
    "238961": None,
    "238962": None,
    "238963": None,
}


def upload_changes_from_csv(csv_path: Path):
    LOG.info("Uploading changelists from CSV")

    def make_changelist(cid: int):
        return {
            "change_id": cid,
            "branches": {},
        }

    changelists = {}
    with csv_path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cid = row["change_number"]
            if cid not in changelists:
                changelists[cid] = make_changelist(cid)
            changelist = changelists[cid]

            bobj = {
                "build_id": int(row["id"]),
                "time_updated": row["time_updated"],
                "manifests": (mfs := {}),
            }

            branch = row["branch"]
            changelist["branches"][branch] = bobj
            mfs["238961"] = {"gid": row["data_manifest"]}
            if m := row["win_manifest"]:
                mfs["238962"] = {"gid": m}
            if m := row["mac_manifest"]:
                mfs["238963"] = {"gid": m}

    for cid, changelist in changelists.items():
        try:
            url = f"{settings.api_url}changelists/{cid}"
            resp = requests.put(
                url, json=changelist, params={"molly_guard": settings.molly_guard}
            )
            if resp.status_code == 422:
                LOG.info(
                    f"Could not upload changelist {cid}, {resp.status_code}: {resp.json()}"
                )
            if resp.status_code != 200:
                LOG.info(f"Uploaded changelist {cid}, {resp.status_code}")
            else:
                LOG.info(f"Uploaded changelist {cid}")
        except requests.RequestException:
            LOG.exception(f"Could not upload changelist {cid}")
            pass


def fetch_depot_manifest_gids():
    depots = {}
    url = f"{settings.api_url}builds/public"
    res = requests.get(url)
    js = res.json()
    builds = sorted(js.items(), key=lambda x: int(x[0]))
    for build_id, build in builds:
        # if int(build_id) < 5540437:
        # continue
        for depot, manifest in build["manifests"].items():
            if depot not in depots:
                depots[depot] = {}
            gid = manifest["gid"]
            depots[depot][gid] = manifest
    return depots


def download_depot(depot, manifest):
    if current_scratch_manifests[depot] != manifest:
        dl_dir = paths.scratch_path(depot)
        LOG.info(f"Downloading depot {depot} manifest {manifest}")
        dd_path = settings.depot_downloader
        if not dd_path:
            Path(
                f'{local.env["HOME"]}/dep/DepotDownloader/DepotDownloader/bin/Release/net6.0/DepotDownloader'
            )
        dd = local[dd_path]
        dd[
            "-app",
            "238960",
            "-depot",
            depot,
            "-username",
            settings.steam_user,
            "-remember-password",
            "-password",
            settings.steam_password,
            "-loginid",
            settings.steam_id_base + int(depot) - 238000,
            "-dir",
            dl_dir,
            "-manifest",
            manifest,
        ]()
        current_scratch_manifests[depot] = manifest


def zip_depot(depot, manifest):
    LOG.info(f"Packing depot {depot} manifest {manifest}")
    dl_dir = paths.scratch_path(depot)
    zip_path = paths.zip_path(depot, manifest)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_write(zip_path, mode="wb") as fh:
        with zipfile.ZipFile(fh, "w", compression=zipfile.ZIP_STORED) as zf:
            for root, dirs, files in os.walk(dl_dir):
                for file in files:
                    fullpath = Path(root) / file
                    relpath = PurePosixPath(fullpath.relative_to(dl_dir))
                    zf.write(fullpath, relpath)
    zip_path.chmod(0o644)


def stage_and_ingest_loose(store, depot, manifest):
    if store.has_depot_fact(depot, manifest, "loose_ingested"):
        return

    existing_path = paths.cached_bundle_path(depot, manifest)
    existing_pack = paths.cached_pack_path(depot, manifest)
    existing_cached_zip = paths.cached_zip_path(depot, manifest)
    existing_zip = paths.zip_path(depot, manifest)
    scratch_path = paths.scratch_path(depot)
    scratch_config = None
    if (scratch_config_path := scratch_path / ".DepotDownloader/depot.config").exists():
        dcs = protos.depot_downloader_pb2.DepotConfigStore()
        dcs.ParseFromString(zlib.decompress(scratch_config_path.read_bytes(), -15))
        if dcs.IsInitialized():
            scratch_config = dcs

    if (
        scratch_config
        and len(scratch_config.InstalledManifestIDs) == 1
        and scratch_config.InstalledManifestIDs[int(depot)] == int(manifest)
    ):
        path = scratch_path
    elif existing_path and existing_path.exists():
        path = existing_path
    elif existing_zip and existing_zip.exists():
        path = existing_zip
    elif existing_cached_zip and existing_cached_zip.exists():
        path = existing_cached_zip
    elif existing_pack and existing_pack.exists():
        path = existing_pack
    elif settings.allow_downloads:
        LOG.info(f"Staging depot {depot} manifest {manifest}")
        download_depot(depot, manifest)
        path = paths.scratch_path(depot)
        if settings.allow_zipping:
            zip_depot(depot, manifest)
            path = paths.zip_path(depot, manifest)
    else:
        LOG.warning(f"Not allowed to stage depot {depot} manifest {manifest}")
        return

    LOG.info(f"{depot} {manifest} cache in {path}")
    ingest_loose_files(path, store, depot, manifest)


def download_zip(tracker, depot, manifest):
    if tracker.has_state("zipped"):
        return

    dl_dir = paths.scratch_path(depot)
    zip_path = paths.zip_path(depot, manifest)
    if not zip_path.exists():
        LOG.info(f"Downloading depot {depot} manifest {manifest}")
        dd = local[
            f'{local.env["HOME"]}/dep/DepotDownloader/DepotDownloader/bin/Release/net6.0/DepotDownloader'
        ]
        dd[
            "-app",
            "238960",
            "-depot",
            depot,
            "-username",
            settings.steam_user,
            "-remember-password",
            "-password",
            settings.steam_password,
            "-loginid",
            settings.steam_id_base + depot - 238000,
            "-dir",
            dl_dir,
            "-manifest",
            manifest,
        ]()

        LOG.info(f"Packing depot {depot} manifest {manifest}")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(zip_path, mode="wb") as fh:
            with zipfile.ZipFile(fh, "w", compression=zipfile.ZIP_STORED) as zf:
                for root, dirs, files in os.walk(dl_dir):
                    for file in files:
                        fullpath = Path(root) / file
                        relpath = PurePosixPath(fullpath.relative_to(dl_dir))
                        if relpath.parts[0] not in [".DepotDownloader"]:
                            zf.write(fullpath, relpath)
        zip_path.chmod(0o644)

    tracker.set_state("zipped")


def open_store() -> ArtifactStore:
    if settings.data_db_uri:
        return DatabaseStore(settings.data_db_uri)
    return FilesystemStore(paths)


def ingest_loose_files(path, store, depot, manifest):
    if store.has_depot_fact(depot, manifest, "loose_ingested"):
        return

    LOG.info(f"{depot} {manifest} loose ingest")
    with (
        open_source(path) as source,
        BundledExtentMap(paths.bundled_extent_db()) as bem,
    ):
        ingest_source(source, paths, store, bem, depot, manifest)
        store.set_depot_fact(depot, manifest, "loose_ingested")


def ingest_bundled_files(store, depot, manifest):
    if store.has_depot_fact(depot, manifest, "bundled_ingested"):
        return

    # requirements
    if not store.has_depot_fact(
        depot, manifest, "loose_ingested"
    ) or not store.has_depot_fact(depot, manifest, "has_bundles"):
        return

    LOG.info(f"{depot} {manifest} bundled ingest")
    ingest_bundled(paths, store, depot, manifest)
    store.set_depot_fact(depot, manifest, "bundled_ingested")


def archive_zip(tracker, depot, manifest):
    if tracker.has_state("archived"):
        return

    # requirements
    if not tracker.has_state("zipped"):
        return

    LOG.info(f"Archiving depot {depot} manifest {manifest}")

    local["rsync"]


def probe_depot_cache(depot, manifest):
    if (p := paths.cached_bundle_path(depot, manifest)) and p.exists() and p.is_dir():
        return p
    if (p := paths.cached_zip_path(depot, manifest)) and p.exists() and p.is_file():
        return p
    if (p := paths.cached_pack_path(depot, manifest)) and p.exists() and p.is_dir():
        return p


def main():
    # upload_appinfos()
    depots = fetch_depot_manifest_gids()

    depots = sorted(depots.items(), key=lambda x: int(x[0]), reverse=False)
    with (open_store() as store,):
        for depot, gids in depots:
            for manifest in gids:
                stage_and_ingest_loose(store, depot, manifest)
                ingest_bundled_files(store, depot, manifest)

                # stage_and_ingest_loose(tracker, depot, manifest)
                # # download_zip(tracker, depot, manifest)
                # # ingest_loose_files(tracker, depot, manifest)
                # # archive_zip(tracker, depot, manifest)
                # ingest_bundled_files(tracker, depot, manifest)


def upload_csv_main():
    upload_changes_from_csv(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
