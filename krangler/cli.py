import json
import logging
import os
import requests
import zipfile

from atomicwrites import atomic_write
from pathlib import Path, PurePosixPath
from plumbum import local
from pydantic import BaseSettings, AnyHttpUrl

from .ingest_loose import ingest_source
from .ingest_bundled import ingest_bundled
from .state_tracking import StateTracker
from .paths import Paths

logging.basicConfig(format="%(asctime)s | %(message)s", level=logging.INFO)
LOG = logging.getLogger()


class Settings(BaseSettings):
    api_url: AnyHttpUrl
    appinfo_dir: Path
    root_dir: Path
    remote_bundle_dir: Path = None
    remote_zip_dir: Path = None
    steam_user: str
    steam_password: str
    molly_guard: str


settings = Settings()
paths = Paths(settings.root_dir, remote_zips=settings.remote_zip_dir, remote_bundles=settings.remote_bundle_dir)

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
                manifests[str(depot_id)] = ai_depot['manifests'][branch_name]

            cl['branches'][branch_name] = branch
        return cl
    except Exception as e:
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
                try:
                    requests.put(url, json=changelist, params={'molly_guard': settings.molly_guard})
                    LOG.info(f'Uploaded changelist {cid}')
                except requests.RequestExceptio:
                    LOG.exception(f'Could not upload appinfo {file}')
                    pass


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


def download_depot(tracker, depot, manifest):
    if current_scratch_manifests[depot] != manifest:
        dl_dir = paths.scratch_path(depot)
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
        current_scratch_manifests[depot] = manifest

def stage_and_ingest_loose(tracker, depot, manifest):
    if tracker.has_state('loose_ingested'):
        return

    LOG.info(f"Staging depot {depot} manifest {manifest}")
    existing_zip = paths.remote_zip_path(depot, manifest)
    existing_path = paths.remote_bundle_path(depot, manifest)
    if current_scratch_manifests[depot] == manifest:
        path = paths.scratch_path(depot)
    elif existing_zip and existing_zip.exists():
        path = existing_zip
    elif existing_path and existing_path.exists():
        path = existing_path
    else:
        download_depot(tracker, depot, manifest)
        path = paths.scratch_path(depot)

    LOG.info(f'Ingesting loose files from depot {depot} manifest {manifest} via {path}')
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
        zip_path.chmod(0o644)

    tracker.set_state('zipped')


def ingest_loose_files(path, tracker, depot, manifest):
    if tracker.has_state('loose_ingested'):
        return

    LOG.info(f'Ingesting loose files from depot {depot} manifest {manifest}')
    ingest_source(path, paths, depot, manifest)
    tracker.set_state('loose_ingested')


def ingest_bundled_files(tracker, depot, manifest):
    if tracker.has_state('bundled_ingested'):
        return

    # requirements
    if not tracker.has_state('loose_ingested'):
        return

    LOG.info(f'Ingesting bundled files from depot {depot} manifest {manifest}')
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


def main():
    upload_appinfos()
    depots = fetch_depot_manifests()

    for depot, manifests in depots.items():
        for manifest in manifests:
            tracker = StateTracker(paths, depot, manifest)
            stage_and_ingest_loose(tracker, depot, manifest)
            # download_zip(tracker, depot, manifest)
            # ingest_loose_files(tracker, depot, manifest)
            # archive_zip(tracker, depot, manifest)
            ingest_bundled_files(tracker, depot, manifest)


if __name__ == '__main__':
    main()
