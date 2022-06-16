import json
import logging
import os
import requests
import zipfile

from atomicwrites import atomic_write
from pathlib import Path, PurePosixPath
from plumbum import local
from pydantic import BaseSettings, AnyHttpUrl

from .ingest_zip import ingest_zip
from .ingest_bundled import ingest_bundled
from .state_tracking import StateTracker
from .paths import Paths

logging.basicConfig(format="%(asctime)s | %(message)s", level=logging.INFO)
LOG = logging.getLogger()


class Settings(BaseSettings):
    api_url: AnyHttpUrl
    appinfo_dir: Path
    root_dir: Path
    steam_user: str
    steam_password: str


settings = Settings()
paths = Paths(settings.root_dir)


def upload_appinfos():
    for root, dirs, files in os.walk(settings.appinfo_dir / 'saved'):
        root = Path(root)
        for file in files:
            path = root / file
            appinfo = json.loads(path.read_bytes())
            url = f'{settings.api_url}/appinfo'
            requests.put(url, json=appinfo)


def fetch_depot_manifests():
    depots = {}
    url = f'{settings.api_url}/builds/public'
    res = requests.get(url)
    js = res.json()
    for build_id, build in js.items():
        for depot, manifest in build["manifests"].items():
            if depot not in depots:
                depots[depot] = set()
            depots[depot].add(manifest)
    return depots


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
            '-loginid', depot,
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

    tracker.set_state('zipped')


def ingest_loose_files(tracker, depot, manifest):
    if tracker.has_state('loose_ingested'):
        return

    # requirements
    if not tracker.has_state('zipped'):
        return

    LOG.info(f'Ingesting loose files from depot {depot} manifest {manifest}')
    ingest_zip(paths, depot, manifest)
    tracker.set_state('loose_ingested')


def ingest_bundled_files(tracker, depot, manifest):
    if tracker.has_state('bundled_ingested'):
        return

    # requirements
    if not tracker.has_state('loose_ingested'):
        return

    LOG.info(f'Ingesting bundled files from depot {depot} manifest {manifest}')
    ingest_bundled(paths, depot, manifest)
    # tracker.set_state('bundled_ingested')


def archive_zip(tracker, depot, manifest):
    if tracker.has_state('archived'):
        return

    # requirements
    if not tracker.has_state('zipped'):
        return

    LOG.info(f'Archiving depot {depot} manifest {manifest}')

    local['rsync']


def main():
    upload_appinfos()
    depots = fetch_depot_manifests()

    for depot, manifests in depots.items():
        for manifest in manifests:
            tracker = StateTracker(paths, depot, manifest)
            download_zip(tracker, depot, manifest)
            ingest_loose_files(tracker, depot, manifest)
            # archive_zip(tracker, depot, manifest)
            ingest_bundled_files(tracker, depot, manifest)


if __name__ == '__main__':
    main()
