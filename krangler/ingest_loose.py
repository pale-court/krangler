import codecs
from hashlib import sha256
import logging
from atomicwrites import atomic_write
import os
import ndjson
from pathlib import Path, PurePosixPath
from rich.progress import track
import zipfile
from zstandard import ZstdCompressor

from krangler import poe_util

from .paths import Paths
from .state_tracking import StateTracker

LOG = logging.getLogger()


def walk_zip(zip_path: Path):
    with zipfile.ZipFile(zip_path, 'r') as zip:
        for e in zip.infolist():
            if not e.is_dir():
                with zip.open(e.filename) as fh:
                    yield PurePosixPath(e.filename), e.file_size, fh


def walk_dir(dir_path: Path):
    for root, dirs, files in os.walk(dir_path, topdown=True):
        if '.DepotDownloader' in dirs:
            dirs.remove('.DepotDownloader')
        root_path = Path(root)
        for file in files:
            path = root_path / file
            rel_path = PurePosixPath(path.relative_to(dir_path))
            with path.open('rb') as fh:
                yield rel_path, path.stat().st_size, fh


def walk_source(path: Path):
    if path.suffix == '.zip':
        yield from walk_zip(path)
    else:
        yield from walk_dir(path)


def ingest_zip(zip_path: Path, paths: Paths, depot: int, manifest: int):
    index_path = paths.loose_index_path(depot, manifest)
    with zipfile.ZipFile(zip_path, 'r') as zip:
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(index_path, mode='wb') as release_manifest:
            cctx = ZstdCompressor()
            with cctx.stream_writer(release_manifest, closefd=False) as compression:
                with codecs.getwriter('utf-8')(compression) as codec:
                    index_writer = ndjson.writer(codec)
                    for e in zip.infolist():
                        if not e.is_dir():
                            with zip.open(e.filename) as fh:
                                payload = fh.read()
                                sha256hash = sha256(payload).hexdigest()
                                row = {'path': e.filename, 'sha256': sha256hash,
                                       'phash': str(poe_util.hash_file_path(e.filename)),
                                       'size': e.file_size, 'comp': False}
                                index_writer.writerow(row)
                                try:
                                    sub_path = paths.loose_data_path(
                                        sha256hash)
                                    if not sub_path.exists():
                                        sub_path.parent.mkdir(
                                            parents=True, exist_ok=True)
                                        with atomic_write(sub_path, mode='wb') as out_fh:
                                            out_fh.write(payload)
                                        sub_path.chmod(0o644)
                                except FileExistsError:
                                    pass
        index_path.chmod(0o644)


def ingest_source(path: Path, paths: Paths, depot: int, manifest: int):
    index_path = paths.loose_index_path(depot, manifest)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with poe_util.atomic_compressed_ndjson_writer(index_path) as index_writer:
        for file_path, file_size, fh in track(walk_source(path), description='Loose files'):
            payload = fh.read()
            sha256hash = sha256(payload).hexdigest()
            row = {'path': str(file_path), 'sha256': sha256hash,
                   'phash': str(poe_util.hash_file_path(file_path)),
                   'size': file_size, 'comp': False}
            index_writer.writerow(row)
            try:
                sub_path = paths.loose_data_path(sha256hash)
                if not sub_path.exists():
                    sub_path.parent.mkdir(parents=True, exist_ok=True)
                    with atomic_write(sub_path, mode='wb') as out_fh:
                        out_fh.write(payload)
                    sub_path.chmod(0o644)
            except FileExistsError:
                pass
