import codecs
from hashlib import sha256
import logging
from atomicwrites import atomic_write
import os
import ndjson
from pathlib import Path
import zipfile
from zstandard import ZstdCompressor

from krangler import poe_util

from .paths import Paths
from .state_tracking import StateTracker

LOG = logging.getLogger()


def ingest_zip(paths: Paths, depot: int, manifest: int):
    zip_path = paths.zip_path(depot, manifest)
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
                                except FileExistsError:
                                    pass
