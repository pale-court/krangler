import csv
import json
import logging
import os
import zlib
import requests
import sys
import zipfile

from atomicwrites import atomic_write
from pathlib import Path, PurePosixPath
from plumbum import local
from pydantic import BaseSettings, AnyHttpUrl
from rich.logging import RichHandler

from krangler import ggpk, protos
from krangler.source import open_source

from .ingest_loose import ingest_source
from .ingest_bundled import ingest_bundled
from .state_tracking import StateTracker
from .paths import Paths

# logging.basicConfig(format="%(asctime)s | %(message)s", level=logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
LOG = logging.getLogger("rich")


class Settings(BaseSettings):
    api_url: AnyHttpUrl
    appinfo_dir: Path
    root_dir: Path
    depot_downloader: Path = None
    cached_bundle_dir: Path = None
    cached_pack_dir: Path = None
    cached_zip_dir: Path = None
    allow_downloads: bool = True
    allow_zipping: bool = False
    steam_user: str
    steam_password: str
    steam_id_base: int
    molly_guard: str


settings = Settings()
paths = Paths(settings.root_dir,
              cached_bundles=settings.cached_bundle_dir,
              cached_packs=settings.cached_pack_dir,
              cached_zips=settings.cached_zip_dir)

current_scratch_manifests = {
    "238961": None,
    "238962": None,
    "238963": None,
}

def saved_appinfo_to_changelist(saved_appinfo):
    try:
        appinfo = saved_appinfo['appinfo']
        cl = {}
        cl['change_id'] = appinfo['_change_number']
        cl['branches'] = {}

        ai_depots = appinfo['depots']
        ai_branches = ai_depots['branches']
        for branch_name in ['public']:
            ai_branch = ai_branches[branch_name]
            branch = {
                'build_id': int(ai_branch['buildid']),
                'time_updated': ai_branch['timeupdated'],
                'manifests': {},
            }
            manifests = branch['manifests']
            for depot_id in [238961, 238962, 238963]:
                ai_depot = ai_depots[str(depot_id)]
                mf = ai_depot['manifests'][branch_name]
                if isinstance(mf, str): # best-effort map from old appinfo format from before 2023-05-30
                    mf = {
                        "gid": mf,
                        "size": ai_depot['maxsize']
                    }
                manifests[str(depot_id)] = ai_depot['manifests'][branch_name]

            cl['branches'][branch_name] = branch
        return cl
    except Exception as e:
        LOG.info(f"Could not process appinfo, {e=}")
        return None

def upload_appinfos():
    LOG.info('Uploading changelists from appinfos')
    for root, dirs, files in os.walk(settings.appinfo_dir / 'saved'):
        root = Path(root)
        for file in files:
            path = root / file
            appinfo = json.loads(path.read_bytes())
            if changelist := saved_appinfo_to_changelist(appinfo):
                cid = changelist['change_id']
                url = f'{settings.api_url}/changelists/{cid}'
                bid = changelist['branches']['public']['build_id']
                try:
                    requests.put(url, json=changelist, params={'molly_guard': settings.molly_guard})
                    LOG.info(f'Uploaded changelist {cid} for build {bid}')
                except requests.RequestException:
                    LOG.exception(f'Could not upload appinfo {file}')
                    pass


def upload_changes_from_csv(csv_path: Path):
    LOG.info("Uploading changelists from CSV")
    with csv_path.open(newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cl = {
                'change_id': row['change_number'],
                'branches': {
                    'public': {
                        'build_id': int(row['id']),
                        'time_updated': row['time_updated'],
                        'manifests': (mfs := {})
                    }
                }
            }
            mfs['238961'] = { 'gid': row['data_manifest'] }
            if (m := row['win_manifest']):
                mfs['238962'] = { 'gid': m }
            if (m := row['mac_manifest']):
                mfs['238963'] = { 'gid': m }

            changelist = cl
            cid = changelist['change_id']
            url = f'{settings.api_url}/changelists/{cid}'
            bid = changelist['branches']['public']['build_id']
            try:
                requests.put(url, json=changelist, params={'molly_guard': settings.molly_guard})
                LOG.info(f'Uploaded changelist {cid} for build {bid}')
            except requests.RequestException:
                LOG.exception(f'Could not upload changelist {cid} for build {bid}')
                pass


def fetch_depot_manifest_gids():
    depots = {}
    url = f'{settings.api_url}/builds/public'
    res = requests.get(url)
    js = res.json()
    for build_id, build in js.items():
        for depot, manifest in build["manifests"].items():
            if depot not in depots:
                depots[depot] = {}
            gid = manifest["gid"]
            depots[depot][gid] = manifest
    return depots


def download_depot(tracker, depot, manifest):
    if current_scratch_manifests[depot] != manifest:
        dl_dir = paths.scratch_path(depot)
        LOG.info(f"Downloading depot {depot} manifest {manifest}")
        dd_path = settings.depot_downloader
        if not dd_path:
            Path(f'{local.env["HOME"]}/dep/DepotDownloader/DepotDownloader/bin/Release/net6.0/DepotDownloader')
        dd = local[dd_path]
        dd['-app', '238960',
            '-depot', depot,
            '-username', settings.steam_user,
            '-remember-password',
            '-password', settings.steam_password,
            '-loginid', settings.steam_id_base + depot - 238000,
            '-dir', dl_dir,
            '-manifest', manifest]()
        current_scratch_manifests[depot] = manifest

def zip_depot(tracker, depot, manifest):
    LOG.info(f"Packing depot {depot} manifest {manifest}")
    dl_dir = paths.scratch_path(depot)
    zip_path = paths.zip_path(depot, manifest)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_write(zip_path, mode='wb') as fh:
        with zipfile.ZipFile(fh, 'w', compression=zipfile.ZIP_STORED) as zf:
            for root, dirs, files in os.walk(dl_dir):
                for file in files:
                    fullpath = Path(root) / file
                    relpath = PurePosixPath(fullpath.relative_to(dl_dir))
                    zf.write(fullpath, relpath)
    zip_path.chmod(0o644)

def stage_and_ingest_loose(tracker, depot, manifest):
    if tracker.has_state('loose_ingested'):
        return

    existing_path = paths.cached_bundle_path(depot, manifest)
    existing_pack = paths.cached_pack_path(depot, manifest)
    existing_cached_zip = paths.cached_zip_path(depot, manifest)
    existing_zip = paths.zip_path(depot, manifest)
    scratch_path = paths.scratch_path(depot)
    scratch_config = None
    if (scratch_config_path := scratch_path / '.DepotDownloader/depot.config').exists():
        dcs = protos.depot_downloader_pb2.DepotConfigStore()
        dcs.ParseFromString(zlib.decompress(scratch_config_path.read_bytes(), -15))
        if dcs.IsInitialized():
            scratch_config = dcs

    if scratch_config and len(scratch_config.InstalledManifestIDs) == 1 and scratch_config.InstalledManifestIDs[int(depot)] == int(manifest):
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
        download_depot(tracker, depot, manifest)
        path = paths.scratch_path(depot)
        if settings.allow_zipping:
            zip_depot(tracker, depot, manifest)
            path = paths.zip_path(depot, manifest)
    else:
        LOG.warning(f"Not allowed to stage depot {depot} manifest {manifest}")
        return

    LOG.info(f'{depot} {manifest} cache in {path}')
    ingest_loose_files(path, tracker, depot, manifest)


def download_zip(tracker, depot, manifest):
    if tracker.has_state('zipped'):
        return

    dl_dir = paths.scratch_path(depot)
    zip_path = paths.zip_path(depot, manifest)
    if not zip_path.exists():
        LOG.info(f"Downloading depot {depot} manifest {manifest}")
        dd = local[
            f'{local.env["HOME"]}/dep/DepotDownloader/DepotDownloader/bin/Release/net6.0/DepotDownloader']
        dd['-app', '238960',
            '-depot', depot,
            '-username', settings.steam_user,
            '-remember-password',
            '-password', settings.steam_password,
            '-loginid', settings.steam_id_base + depot - 238000,
            '-dir', dl_dir,
            '-manifest', manifest]()

        LOG.info(f"Packing depot {depot} manifest {manifest}")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(zip_path, mode='wb') as fh:
            with zipfile.ZipFile(fh, 'w', compression=zipfile.ZIP_STORED) as zf:
                for root, dirs, files in os.walk(dl_dir):
                    for file in files:
                        fullpath = Path(root) / file
                        relpath = PurePosixPath(fullpath.relative_to(dl_dir))
                        if relpath.parts[0] not in ['.DepotDownloader']:
                            zf.write(fullpath, relpath)
        zip_path.chmod(0o644)

    tracker.set_state('zipped')


def ingest_loose_files(path, tracker, depot, manifest):
    if tracker.has_state('loose_ingested'):
        return
    
    source = open_source(path)

    LOG.info(f'{depot} {manifest} loose ingest')
    ingest_source(source, paths, depot, manifest)
    tracker.set_state('loose_ingested')


def ingest_bundled_files(tracker, depot, manifest):
    if tracker.has_state('bundled_ingested'):
        return

    # requirements
    if not tracker.has_state('loose_ingested'):
        return

    LOG.info(f'{depot} {manifest} bundled ingest')
    ingest_bundled(paths, depot, manifest)
    tracker.set_state('bundled_ingested')


def archive_zip(tracker, depot, manifest):
    if tracker.has_state('archived'):
        return

    # requirements
    if not tracker.has_state('zipped'):
        return

    LOG.info(f'Archiving depot {depot} manifest {manifest}')

    local['rsync']


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

    for depot, gids in depots.items():
        for manifest in gids:
            tracker = StateTracker(paths, depot, manifest)
            mf_data = gids[manifest]
            disk_size = mf_data.get('size')
            download_size = mf_data.get('download')

            stage_and_ingest_loose(tracker, depot, manifest)
            ingest_bundled_files(tracker, depot, manifest)

            # stage_and_ingest_loose(tracker, depot, manifest)
            # # download_zip(tracker, depot, manifest)
            # # ingest_loose_files(tracker, depot, manifest)
            # # archive_zip(tracker, depot, manifest)
            # ingest_bundled_files(tracker, depot, manifest)


def upload_appinfos_main():
    upload_appinfos()


def upload_csv_main():
    upload_changes_from_csv(Path(sys.argv[1]))


if __name__ == '__main__':
    main()
