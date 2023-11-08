import codecs
from dataclasses import dataclass
from hashlib import sha256
import logging
from atomicwrites import atomic_write
import ndjson
from pathlib import Path, PurePath, PurePosixPath
from rich.progress import track
from typing import Callable, Dict
import zipfile
import zlib
from zstandard import ZstdCompressor

from krangler import ggpk, poe_util, source

from .bundled_extent_map import BundledExtentMap
from .paths import Paths
from .protos import depot_downloader_pb2

LOG = logging.getLogger()


def ingest_zip(zip_path: Path, paths: Paths, depot: int, manifest: int):
    phasher = poe_util.PoE_3_21_2_Hash()
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
                                       'phash': str(phasher.hash_file_path(e.filename)),
                                       'size': e.file_size}
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

@dataclass
class ManifestEntry:
    sha1: bytes
    size: int


def try_open_depot_manifest(source: source.Source, depot, manifest):
    steam_manifest_cands = [
        f".DepotDownloader/{depot}_{manifest}.bin",
        f".DepotDownloader/{manifest}.bin",
    ]
    for cand in steam_manifest_cands:
        try:
            with source.open(cand) as fh:
                pmf = depot_downloader_pb2.ProtoManifest()
                pmf.ParseFromString(zlib.decompress(fh.read(), -15))
                if pmf.IsInitialized():
                    ret: Dict[PurePosixPath, ManifestEntry] = {}
                    sha1_from_path = {}
                    for file in pmf.Files:
                        if not file.Flags or not file.Flags & 64:
                            # The serialized path type in the protobufs is OS dependent, normalize it.
                            filename = PurePosixPath(PurePath(file.FileName))
                            ret[filename] = ManifestEntry(
                                sha1=file.FileHash, size=file.TotalSize)
                    return ret
        except FileNotFoundError as e:
            pass


def ingest_source(source: source.Source, paths: Paths, depot: int, manifest: int):
    depot_manifest = try_open_depot_manifest(source, depot, manifest)

    phasher = poe_util.PoE_3_21_2_Hash()
    index_path = paths.loose_index_path(depot, manifest)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        poe_util.atomic_compressed_ndjson_writer(index_path) as index_writer,
        BundledExtentMap(paths.bundled_extent_db()) as bem,
    ):
        def store_one(file_path: Path, file_size: int, file_hash: bytes, file_data: bytes | Callable[[], bytes]):
            try:
                sub_path = paths.loose_data_path(file_hash)
                if not sub_path.exists():
                    sub_path.parent.mkdir(parents=True, exist_ok=True)
                    with atomic_write(sub_path, mode='wb') as out_fh:
                        if callable(file_data):
                            file_data = file_data()
                        out_fh.write(file_data)
                    sub_path.chmod(0o644)
            except FileExistsError:
                pass

        def record_one(file_path: Path, file_size: int, file_hash: bytes):
            row = {'path': str(file_path), 'sha256': file_hash.hex(),
                   'phash': str(phasher.hash_file_path(file_path)),
                   'size': file_size}
            index_writer.writerow(row)

        # If we encounter a pack as we traverse, remember this to process it once
        # we've finished ingesting the loose files.
        pack_file = None

        if depot_manifest:
            # If we have a depot manifest, use it to drive traversal.
            for path, entry in track(depot_manifest.items(), description = 'Depot files'):
                if path.name == 'Content.ggpk':
                    pack_file = source.open(path)
                    continue

                # See if we've seen the SHA1 hash before.
                sha256hash = bem.get_sha256_from_sha1(entry.sha1)

                file_data = None
                if sha256hash:
                    # If we got a hash, lazily load the file data so we can
                    # get away cheaply if the data already exists.
                    file_data = lambda: source.open(path).read()
                else:
                    # If not, open the file by name and hash it, remembering the SHA256
                    # and data for ingestion.
                    with source.open(path) as fh:
                        file_data = fh.read()
                    sha256hash = sha256(file_data).digest()
                    bem.set_sha256_from_sha1(entry.sha1, sha256hash)
                
                # As we don't necessarily have the datafile on disk even if we have the
                # hash correspondence, process it regardless.
                store_one(path, entry.size, sha256hash, file_data)
                record_one(path, entry.size, sha256hash)
        else:
            # If we don't have a manifest, we have to fall back to walking the source.
            for file_path, file_size, fh in track(source.walk(), description='Loose files'):
                # Exclude metadata from ingestion.
                if Path('.DepotDownloader') in file_path.parents:
                    continue

                # Remember the pack file if encountered and avoid ingesting it.
                if file_path.name == 'Content.ggpk':
                    pack = source.open('Content.ggpk')
                    continue

                sha1hash = None
                sha256hash = None
                file_data = None
                computed_hash = False
                if depot_manifest and file_path in depot_manifest:
                    sha1hash = depot_manifest[file_path]
                    sha256hash = bem.get_sha256_from_sha1(sha1hash)
                    file_data = lambda: fh.read()

                if not sha256hash:
                    file_data = fh.read()
                    sha256hash = sha256(file_data).digest()
                    computed_hash = True

                store_one(file_path, file_size, sha256hash, file_data)
                record_one(file_path, file_size, sha256hash)
                if sha1hash is not None and computed_hash:
                    bem.set_sha256_from_sha1(sha1hash, sha256hash)

        if pack_file is not None:
            orphans = 0
            zerohash = 0
            both = 0
            with ggpk.open_pack_source(pack_file) as pack:
                for offset in track(pack.files, description = 'Packed files'):
                    file = pack.files[offset]
                    file_path = pack.file_path(offset)

                    fit = True
                    has_zerohash = not any(file.sha256)
                    is_orphan = file_path is None
                    
                    if has_zerohash:
                        zerohash += 1
                        fit = False
                    if is_orphan:
                        orphans += 1
                        fit = False
                    if has_zerohash and is_orphan:
                        both += 1
                    
                    if not fit:
                        continue

                    lazy_file_data = lambda: pack.file_data(offset)
                    store_one(file_path, file.data_size, file.sha256, lazy_file_data)
                    record_one(file_path, file.data_size, file.sha256)
            if orphans > 0:
                LOG.info(f"found {orphans} orphans in pack")
            if zerohash > 0:
                LOG.info(f"found {zerohash} entries with zeroed hash")
            if zerohash > 0:
                LOG.info(f"{both} zerohash orphans")
