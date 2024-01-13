from dataclasses import dataclass, field

from hashlib import sha256
from io import BytesIO
import logging
import os
from pathlib import PurePosixPath
import struct
from typing import Dict, List, Optional
from rich.progress import track, Progress, MofNCompleteColumn

from krangler import poe_util

import ooz

from .bundled_extent_map import BundledExtentMap
from .paths import Paths
from .store import ArtifactStore, BatchQueue, Object


def readf(fh: BytesIO, fmt: str):
    return struct.unpack(fmt, fh.read(struct.calcsize(fmt)))


def readfi(fh: BytesIO, fmt: str, count: int):
    return struct.iter_unpack(fmt, fh.read(struct.calcsize(fmt) * count))


@dataclass
class BundleRecord:
    name: str
    uncompressed_size: int

    def bin_path(self):
        return PurePosixPath(f"Bundles2/{self.name}.bundle.bin")


@dataclass
class FileRecord:
    path_hash: int
    bundle_index: int
    file_offset: int
    file_size: int


@dataclass
class PathRep:
    hash: int
    offset: int
    size: int
    recursive_size: int


class CompressedBundle:
    def __init__(self, fh):
        self.fh = fh
        self.uncompressed_size, self.total_payload_size, head_payload_size = readf(
            fh, "<III"
        )
        (
            first_file_encode,
            unk10,
            uncompressed_size2,
            total_payload_size2,
            block_count,
            self.uncompressed_block_granularity,
        ) = readf(fh, "<IIQQII")
        fh.seek(4 * 4, 1)
        self.block_sizes = list(map(lambda x: x[0], readfi(fh, "<I", block_count)))
        self.data_start = fh.tell()

    def decompress_all(self):
        ret = bytearray()
        self.fh.seek(self.data_start)
        for i, bsize in enumerate(self.block_sizes):
            if i + 1 != len(self.block_sizes):
                usize = self.uncompressed_block_granularity
            else:
                usize = self.uncompressed_size - i * self.uncompressed_block_granularity
            ret.extend(ooz.decompress(self.fh.read(bsize), usize))
        return ret


class BundleIndex:
    def __init__(self, index_data):
        with BytesIO(index_data) as zfh:
            index_bundle = CompressedBundle(zfh)
            fh = BytesIO(index_bundle.decompress_all())

        self.bundles = []
        (bundle_count,) = readf(fh, "<I")
        for _ in range(bundle_count):
            (bnamelen,) = readf(fh, "<I")
            bnameraw = fh.read(bnamelen)
            bname = bnameraw.decode("UTF-8")
            bunclen, = readf(fh, "<I")
            self.bundles.append(BundleRecord(name=bname, uncompressed_size=bunclen))

        self.files = []
        (file_count,) = readf(fh, "<I")
        for _ in range(file_count):
            path_hash, bundle_index, file_offset, file_size = readf(fh, "<QIII")
            self.files.append(
                FileRecord(
                    path_hash=path_hash,
                    bundle_index=bundle_index,
                    file_offset=file_offset,
                    file_size=file_size,
                )
            )

        self.path_reps = []
        (path_rep_count,) = readf(fh, "<I")
        for _ in range(path_rep_count):
            hash, offset, size, recursive_size = readf(fh, "<QIII")
            self.path_reps.append(
                PathRep(
                    hash=hash, offset=offset, size=size, recursive_size=recursive_size
                )
            )
        self.path_comp = fh.read()


def _cut_ntmbs(slice):
    for i, b in enumerate(slice):
        if b == 0:
            return slice[:i].tobytes(), slice[i + 1:]
    raise ValueError


@dataclass
class PathHashTable:
    path_by_ihash: Dict[int, str] = field(default_factory=dict)
    ohash_by_ihash: Dict[int, int] = field(default_factory=dict)

    ihasher: poe_util.PathHasher = None
    ohasher: Optional[poe_util.PathHasher] = None


def _generate_paths(rep, path_view, recurse=False):
    slice_end = rep.offset + (rep.recursive_size if recurse else rep.size)
    slice = path_view[rep.offset: slice_end]
    base_phase = False
    bases = []
    while len(slice):
        (cmd,) = struct.unpack("<I", slice[:4])
        slice = slice[4:]

        # toggle phase on zero command word
        if cmd == 0:
            base_phase = not base_phase
            if base_phase:
                bases.clear()
            continue

        # otherwise build a base or emit a string
        s, slice = _cut_ntmbs(slice)
        if cmd <= len(bases):
            s = bases[cmd - 1] + s.decode("UTF-8")
        else:
            s = s.decode("UTF-8")

        if base_phase:
            bases.append(s)
        else:
            yield s


def _generate_path_hash_table(index: BundleIndex):
    LOG = logging.getLogger()
    ret = PathHashTable()

    path_data = CompressedBundle(BytesIO(index.path_comp)).decompress_all()
    path_view = memoryview(path_data)

    rep_hashes = list(map(lambda x: x.hash, index.path_reps))

    legacy_hasher = poe_util.PoE_3_11_2_Hash()
    modern_hasher = poe_util.PoE_3_21_2_Hash()

    if legacy_hasher.hash_dir_path("Art") in rep_hashes:
        ret.ihasher = legacy_hasher
        ret.ohasher = modern_hasher
    elif modern_hasher.hash_dir_path("Art") in rep_hashes:
        ret.ihasher = modern_hasher
    else:
        raise RuntimeError("Unknown root hash algorithm/seed")

    for rep in index.path_reps:
        for s in _generate_paths(rep, path_view):
            ihash = ret.ihasher.hash_file_path(s)
            ohash = ret.ohasher.hash_file_path(s) if ret.ohasher else ihash
            ret.path_by_ihash[ihash] = s
            ret.ohash_by_ihash[ihash] = ohash

    return ret


def ingest_bundled(paths: Paths, store: ArtifactStore, depot: int, manifest: int):
    # grab the NDJSON with the loose files to find the index and bundle files
    index_file_addr = None
    bundle_by_path = {}
    bundle_by_ohash = {}
    with store.index_reader(depot, manifest, "loose") as reader:
        for row in reader:
            if row["path"] == "Bundles2/_.index.bin":
                index_file_addr = row["sha256"]
            elif row["path"].endswith(".bundle.bin"):
                bundle_by_path[row["path"]] = row
                bundle_by_ohash[int(row["phash"])] = row

    if not index_file_addr:
        return

    index_file_data = store.read_data(index_file_addr)

    index = BundleIndex(index_file_data)

    # Sort file list to traverse in bundle and offset order.
    l2: List[FileRecord] = sorted(
        index.files, key=lambda f: (f.bundle_index, f.file_offset, f.file_size)
    )

    l2b: List[List[FileRecord]] = [[] for _ in index.bundles]
    for frec in l2:
        l2b[frec.bundle_index].append(frec)

    with (
        store.index_writer(depot, manifest, "bundled") as index_writer,
        BundledExtentMap(paths.bundled_extent_db()) as bem,
        Progress(
            *Progress.get_default_columns(), MofNCompleteColumn(), refresh_per_second=1
        ) as progress,
    ):
        queue = BatchQueue(store, size_budget=200 * 2 ** 20, count_budget=50 * 1000)
        bid = None
        brec = None
        bhash_bin = None
        bhash_hex = None
        bdata = None

        ttask = progress.add_task("Total entries", total=len(l2))
        btask = progress.add_task("Bundles", total=len(index.bundles))

        ctask = progress.add_task("Caching paths", total=1)
        # Generate and hash paths from inner structure.
        path_hashes = _generate_path_hash_table(index)
        progress.remove_task(ctask)

        # Idea:
        # For each outer bundle, check all frecs against BEM to find hashes of all bundled files.
        # Bulk check bundled hashes against the store to see which ones are missing.
        # If any are missing, decompress and slice them, bulk uploading them.
        #
        # There's three states for a bundled file:
        # - in BEM and in data store;
        # - in BEM and not in data store;
        # - not in BEM and thus unknown.
        #
        # We can only leverage the BEM to save hash computation, it cannot be the source truth of existence.
        # For ease of coding, do two passes over frecs.
        # First pass queries each frec against the BEM and if not known, lazily loads the whole bundle and compute
        # new BEM entries and hashes. After the first pass we have a full set of frec hashes collected.
        # This information is needed for populating the index as well.
        # We then make a bulk query against the data store for missing entries.
        # The second pass slices out and uploads missing file data.

        # More idea:
        # Do two passes over the whole input to collect more hashes as runtime cost seems dominated by querying the
        # database for missing hashes.
        # Maybe partition bundles into groups by uncompressed size and file count.
        # That way we don't need two data passes.
        # A question is, do we partition before checking the BEM or after? Might make more sense to do it after as we
        # can skip counting the satisfied objects for the initial BEM population, but on the other hand it means that if
        # the database doesn't have the object, we've got objects we need to populate and cut again then.
        # It doesn't matter too much for uploading, but the double-download would still suck.
        # The proper play is probably to go with the up-front partition into as large chunks as we can keep resident
        # individually and if they end up being sparsely needed, there's still comparatively few of them.

        # So... partition up-front on compressed size and count into chunks; much like we previously did on a per-bundle
        # basis. For each chunk:
        # - consult BEM and decompress+hash slices that aren't in there;
        # - store new tuples to BEM;
        # - check missing addrs by hashes alone to avoid fetching anything in vain;
        # - do another pass to fill in bundles still needed;
        # - upload whole chunk.

        def group_bundles():
            ret = []
            acc_group = []
            max_size = 1 * 2 ** 30
            acc_size = 0
            max_count = 100 * 10 ** 3
            acc_count = 0
            for bid, frecs in enumerate(l2b):
                if acc_size >= max_size or acc_count >= max_count:
                    ret.append((acc_group, acc_size, acc_count))
                    acc_group = []
                    acc_size = 0
                    acc_count = 0
                acc_group.append(bid)
                acc_count += len(frecs)
                acc_size += index.bundles[bid].uncompressed_size

            if len(acc_group):
                ret.append((acc_group, acc_size, acc_count))

            return ret

        bid_groups = group_bundles()
        for membs, _, _ in bid_groups:
            new_bem_keys = []
            new_bem_values = []
            datafiles = {}
            found_fhashes_bin: dict[bytes, (int, int, int)] = {}
            for bid in membs:
                brec = index.bundles[bid]
                bhash_hex = bundle_by_path[str(brec.bin_path())]["sha256"]
                bhash_bin = bytes.fromhex(bhash_hex)
                bdata = lambda: CompressedBundle(BytesIO(store.read_data(bhash_bin))).decompress_all()
                for frec in l2b[bid]:
                    if fhash_bin := bem.get_extent_hash(
                            bhash_bin, frec.file_offset, frec.file_size
                    ):
                        fhash_hex = fhash_bin.hex()
                    else:
                        # extract and record the bundled hash in the output
                        if callable(bdata):
                            # Manifest data if not already there as we need to read from it.
                            bdata = bdata()
                        start = frec.file_offset
                        end = start + frec.file_size
                        slice = memoryview(bdata)[start:end]
                        fhash_bin = sha256(slice).digest()
                        fhash_hex = fhash_bin.hex()
                        new_bem_keys.append((bhash_bin, frec.file_offset, frec.file_size))
                        new_bem_values.append(fhash_bin)

                    found_fhashes_bin[fhash_bin] = (bid, frec.file_offset, frec.file_size)
                    path = path_hashes.path_by_ihash[frec.path_hash]
                    # write an index entry
                    nd = {
                        "sha256": fhash_hex,
                        "path": path,
                        "phash": path_hashes.ohash_by_ihash[frec.path_hash],
                        "size": frec.file_size,
                    }
                    index_writer.writerow(nd)
                    progress.update(ttask, advance=1)

                datafiles[bid] = bdata

            if len(new_bem_keys):
                bem.set_extent_hashes(new_bem_keys, new_bem_values)

            # Phase 2: find missing objects and upload them
            missing_objects = store.list_missing_objects(found_fhashes_bin.keys())
            if len(missing_objects):
                ordered_objs: list[((int, int, int), bytes)] = sorted(
                    [(found_fhashes_bin[addr], addr) for addr in missing_objects])
                for (bid, start, size), addr in ordered_objs:
                    bdata = datafiles[bid]
                    if callable(bdata):
                        # Manifest data if not already there as we need to read from it.
                        bdata = bdata()
                        datafiles[bid] = bdata
                    end = start + size
                    slice = bdata[start:end]
                    queue.store_one(addr, Object(data=slice, size=size))

            progress.update(btask, advance=len(membs))
            del datafiles

        queue.flush()
