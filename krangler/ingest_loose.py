import codecs
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
import logging
from atomicwrites import atomic_write
import ndjson
from pathlib import Path, PurePath, PurePosixPath
from rich.progress import track
from typing import Callable, Dict, List, Optional, Set
import zipfile
import zlib
from zstandard import ZstdCompressor

from krangler import ggpk, poe_util, source
from krangler.store import ArtifactStore, BatchQueue, Object

from .bundled_extent_map import BundledExtentMap
from .logging import LOG
from .paths import Paths
from .protos import depot_downloader_pb2


def ingest_zip(zip_path: Path, paths: Paths, depot: int, manifest: int):
    phasher = poe_util.PoE_3_21_2_Hash()
    index_path = paths.loose_index_path(depot, manifest)
    with zipfile.ZipFile(zip_path, "r") as zip:
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(index_path, mode="wb") as release_manifest:
            cctx = ZstdCompressor()
            with cctx.stream_writer(release_manifest, closefd=False) as compression:
                with codecs.getwriter("utf-8")(compression) as codec:
                    index_writer = ndjson.writer(codec)
                    for e in zip.infolist():
                        if not e.is_dir():
                            with zip.open(e.filename) as fh:
                                payload = fh.read()
                                sha256hash = sha256(payload).hexdigest()
                                row = {
                                    "path": e.filename,
                                    "sha256": sha256hash,
                                    "phash": str(phasher.hash_file_path(e.filename)),
                                    "size": e.file_size,
                                }
                                index_writer.writerow(row)
                                try:
                                    sub_path = paths.loose_data_path(sha256hash)
                                    if not sub_path.exists():
                                        sub_path.parent.mkdir(
                                            parents=True, exist_ok=True
                                        )
                                        with atomic_write(
                                                sub_path, mode="wb"
                                        ) as out_fh:
                                            out_fh.write(payload)
                                        sub_path.chmod(0o644)
                                except FileExistsError:
                                    pass
        index_path.chmod(0o644)


@dataclass
class ManifestEntry:
    path: PurePosixPath
    size: int
    sha1: Optional[bytes] = None


@dataclass
class ManifestLookups:
    by_index: List[ManifestEntry]
    by_path: Dict[PurePosixPath, ManifestEntry]


def try_open_depot_manifest(source: source.Source, paths: Paths, depot, manifest):
    origins = {
        "disk": lambda filename: open(paths.separate_manifest_path() / filename, "rb"),
        "source": lambda filename: source.open(f".DepotDownloader/{filename}"),
    }
    steam_manifest_cands = [
        f"{depot}_{manifest}.bin",
        f"{manifest}.bin",
    ]
    for origin_name, origin in origins.items():
        for cand_leaf in steam_manifest_cands:
            try:
                with origin(cand_leaf) as fh:
                    LOG.info(f"found manifest in {origin_name}: {cand_leaf}")
                    pmf = depot_downloader_pb2.ProtoManifest()
                    pmf.ParseFromString(zlib.decompress(fh.read(), -15))
                    if pmf.IsInitialized():
                        by_index = []
                        by_path = {}
                        sha1_from_path = {}
                        for file in pmf.Files:
                            if not file.Flags or not file.Flags & 64:
                                # The serialized path type in the protobufs is OS dependent, normalize it.
                                filename = PurePosixPath(PurePath(file.FileName))
                                entry = ManifestEntry(
                                    path=filename,
                                    sha1=file.FileHash,
                                    size=file.TotalSize,
                                )
                                by_path[filename] = entry
                                by_index.append(entry)
                        return ManifestLookups(by_index=by_index, by_path=by_path)
            except FileNotFoundError as e:
                pass


def generate_manifest_from_source(source: source.Source):
    by_index = []
    by_path = {}
    for file_path, file_size, fh in track(
            source.walk(), description="Loose tree traversal"
    ):
        # Exclude metadata from ingestion.
        if Path(".DepotDownloader") in file_path.parents:
            continue

        entry = ManifestEntry(path=file_path, size=file_size)

        by_path[file_path] = entry
        by_index.append(entry)

    return ManifestLookups(by_index=by_index, by_path=by_path)


def ingest_source(
        source: source.Source,
        paths: Paths,
        store: ArtifactStore,
        bem: BundledExtentMap,
        depot: int,
        manifest: int,
):
    depot_manifest = try_open_depot_manifest(source, paths, depot, manifest)
    # If we don't have a manifest, we have to fall back to walking the source.
    if depot_manifest is None:
        depot_manifest = generate_manifest_from_source(source)

    phasher = poe_util.PoE_3_21_2_Hash()
    with store.index_writer(depot, manifest, "loose") as index_writer:

        def record_one(file_path: Path, file_size: int, file_hash: bytes):
            row = {
                "path": str(file_path),
                "sha256": file_hash.hex(),
                "phash": str(phasher.hash_file_path(file_path)),
                "size": file_size,
            }
            index_writer.writerow(row)

        queue = BatchQueue(store, size_budget=200 * 2 ** 20, count_budget=50 * 1000)

        # Reshape things so that the depot manifest serves to populate the first
        # mass exist query thanks to SHA1->SHA256 mapping.
        # This is followed by all the files with unknown hashes, which in the
        # case of a missing manifest is all files.
        # As we process those unknown files, persist a SHA1->SHA256 mapping if
        # we have a manifest.

        entry_addresses: List[bytes] = []  # addresses for each entry
        has_addr: Dict[bytes, Object] = {}  # maps from address to data callback
        missing_addr: Dict[int, Object] = {}  # maps from entry ordinal to data callback

        # If we have a depot manifest, use it to accelerate traversal and for
        # early rejection by asking the DB about addresses.

        for i, entry in enumerate(depot_manifest.by_index):
            path = entry.path
            if path.name == "Content.ggpk":
                store.set_depot_fact(depot, manifest, "has_pack")
                pack_file = source.open(path)
                entry_addresses.append(None)  # dummy element to keep list in sync
                continue
            elif path.name == "_.index.bin":
                store.set_depot_fact(depot, manifest, "has_bundles")

            # See if we've seen the SHA1 hash before.
            sha256hash = bem.get_sha256_from_sha1(entry.sha1) if entry.sha1 else None

            def make_lazy_load_function(path):
                return lambda: source.open(path).read()

            file_data = make_lazy_load_function(path)

            entry_addresses.append(sha256hash)  # might be None
            obj = Object(data=file_data, size=entry.size, sha1=entry.sha1)
            if sha256hash:
                has_addr[sha256hash] = obj
                record_one(entry.path, entry.size, sha256hash)
            else:
                missing_addr[i] = obj

        # Query for which addresses already exist.
        # Insert in bulk all the objects that do not exist.
        with store.write_data_bulk() as bulk:
            for addr in store.list_missing_objects(has_addr.keys()):
                data = has_addr[addr].data
                if callable(data):
                    data = data()
                bulk.store(addr, data)

        # Process loose files
        for i, entry in track(missing_addr.items(), description="Loose unknown files"):
            data = entry.data
            if callable(data):
                data = data()
            addr = sha256(data).digest()

            mf_entry = depot_manifest.by_index[i]
            record_one(mf_entry.path, mf_entry.size, addr)

            entry_addresses[i] = addr
            if entry.sha1:
                bem.set_sha256_from_sha1(entry.sha1, addr)

            obj = Object(data=data, size=len(data), sha1=entry.sha1)
            queue.store_one(addr, obj)

        # Process eventual GGPK pack file
        if source.contains("Content.ggpk"):
            all_objects: Dict[bytes, Object] = {}
            with ggpk.open_pack_source(pack_file) as pack:
                last_offset = None
                for offset in track(pack.files, description="Discovering packed files"):
                    file = pack.files[offset]
                    file_path = pack.file_path(offset)

                    if file_path:  # orphans don't have any path from the root

                        def make_lazy_load_function(offset):
                            return lambda: pack.file_data(offset)

                        lazy_file_data = make_lazy_load_function(offset)
                        all_objects[file.sha256] = Object(
                            data=lazy_file_data, size=file.data_size
                        )
                        record_one(file_path, file.data_size, file.sha256)

                for addr in track(
                        store.list_missing_objects(all_objects.keys()),
                        description="Storing new packed files",
                ):
                    queue.store_one(addr, all_objects[addr])

                queue.flush()

        # Submit any final uncommitted objects
        queue.flush()
