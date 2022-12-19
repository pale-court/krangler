from argparse import ArgumentError
import cProfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from io import BytesIO
import logging
import os
from pathlib import PurePosixPath
from stat import FILE_ATTRIBUTE_OFFLINE
import struct
from time import sleep
from typing import Dict, List
from atomicwrites import atomic_write
from rich.progress import track, Progress, MofNCompleteColumn
import timeit

import zstandard

from krangler import poe_util
from .paths import Paths

import ndjson

import codecs
import ooz
from zstandard import ZstdCompressor, ZstdDecompressor

from .bundled_extent_map import BundledExtentMap


def readf(fh: BytesIO, fmt: str):
    return struct.unpack(fmt, fh.read(struct.calcsize(fmt)))


def readfi(fh: BytesIO, fmt: str, count: int):
    return struct.iter_unpack(fmt, fh.read(struct.calcsize(fmt) * count))


@dataclass
class BundleRecord:
    name: str
    uncompressed_size: int

    def bin_path(self):
        return PurePosixPath(f'Bundles2/{self.name}.bundle.bin')


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
            fh, '<III')
        first_file_encode, unk10, uncompressed_size2, total_payload_size2, block_count, self.uncompressed_block_granularity = readf(fh,
                                                                                                                                    '<IIQQII')
        fh.seek(4*4, 1)
        self.block_sizes = list(
            map(lambda x: x[0], readfi(fh, '<I', block_count)))
        self.data_start = fh.tell()

    def decompress_all(self):
        ret = bytearray()
        self.fh.seek(self.data_start)
        for i, bsize in enumerate(self.block_sizes):
            if i+1 != len(self.block_sizes):
                usize = self.uncompressed_block_granularity
            else:
                usize = self.uncompressed_size - i * self.uncompressed_block_granularity
            ret.extend(ooz.decompress(self.fh.read(bsize), usize))
        return ret


class BundleIndex:
    def __init__(self, index_path):
        with index_path.open('rb') as zfh:
            index_bundle = CompressedBundle(zfh)
            fh = BytesIO(index_bundle.decompress_all())

        self.bundles = []
        bundle_count, = readf(fh, '<I')
        for _ in range(bundle_count):
            bnamelen, = readf(fh, '<I')
            bnameraw = fh.read(bnamelen)
            bname = bnameraw.decode('UTF-8')
            bunclen = readf(fh, '<I')
            self.bundles.append(BundleRecord(
                name=bname, uncompressed_size=bunclen))

        self.files = []
        file_count, = readf(fh, '<I')
        for _ in range(file_count):
            path_hash, bundle_index, file_offset, file_size = readf(
                fh, '<QIII')
            self.files.append(FileRecord(
                path_hash=path_hash, bundle_index=bundle_index, file_offset=file_offset, file_size=file_size))

        self.path_reps = []
        path_rep_count, = readf(fh, '<I')
        for _ in range(path_rep_count):
            hash, offset, size, recursive_size = readf(fh, '<QIII')
            self.path_reps.append(
                PathRep(hash=hash, offset=offset, size=size, recursive_size=recursive_size))
        self.path_comp = fh.read()


def _cut_ntmbs(slice):
    for i, b in enumerate(slice):
        if b == 0:
            return slice[:i].tobytes(), slice[i+1:]
    raise ValueError


def _generate_path_hash_table(index: BundleIndex):
    LOG = logging.getLogger()
    ret: Dict[int, str] = {}

    path_data = CompressedBundle(BytesIO(index.path_comp)).decompress_all()
    path_view = memoryview(path_data)
    running_string_size = 0
    for rep in index.path_reps:
        slice = path_view[rep.offset:rep.offset+rep.size]
        base_phase = False
        bases = []
        while len(slice):
            cmd, = struct.unpack('<I', slice[:4])
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
                s = bases[cmd-1] + s.decode('UTF-8')
            else:
                s = s.decode('UTF-8')

            if base_phase:
                bases.append(s)
            else:
                running_string_size += len(s)
                hash = poe_util.hash_file_path(s)
                ret[hash] = s

    return ret

def _compressable_path(path) -> bool:
    p = PurePosixPath(path)
    if p.suffix.lower() in {'.bank', '.bin', '.bk2', '.dll', '.exe', '.ogg', '.png'} or path.startswith('ShaderCache'):
        return False
    return True

def ingest_bundled(paths: Paths, depot: int, manifest: int):
    LOG = logging.getLogger()

    # grab the NDJSON with the loose files to find the index and bundle files
    index_file_path = None
    bundle_by_path = {}
    bundle_by_phash = {}
    with paths.loose_index_path(depot, manifest).open('rb') as zfh:
        zctx = ZstdDecompressor()
        with zctx.stream_reader(zfh) as fh:
            with codecs.getreader('utf-8')(fh) as codec:
                reader = ndjson.reader(codec)
                for row in reader:
                    if row['path'] == 'Bundles2/_.index.bin':
                        index_file_path = paths.loose_data_path(row['sha256'])
                    elif row['path'].endswith('.bundle.bin'):
                        bundle_by_path[row['path']] = row
                        bundle_by_phash[int(row['phash'])] = row

    if not index_file_path:
        return

    index = BundleIndex(index_file_path)

    # Sort file list to traverse in bundle and offset order.
    l2: List[FileRecord] = sorted(index.files, key=lambda f: (
        f.bundle_index, f.file_offset, f.file_size))

    l2b: List[List[FileRecord]] = [[] for _ in index.bundles]
    for frec in l2:
        l2b[frec.bundle_index].append(frec)

    hashes_on_disk = set()
    for _, _, files in os.walk(paths.bundled_data_tree()):
        for file in files:
            try:
                hashes_on_disk.add(bytes.fromhex(file[:64]))
            except:
                pass

    with poe_util.atomic_compressed_ndjson_writer(paths.bundled_index_path(depot, manifest)) as index_writer:
        with BundledExtentMap(paths.bundled_extent_db()) as bem:
            populated_path_cache = False
            bid = None
            brec = None
            bhash_bin = None
            bhash_hex = None
            bdata = None
            with Progress(*Progress.get_default_columns(), MofNCompleteColumn(), refresh_per_second=1) as progress:
                ttask = progress.add_task('Total entries', total=len(l2))
                btask = progress.add_task('Bundles', total=len(index.bundles))

                new_bem_keys = []
                new_bem_values = []
                for bid, frecs in enumerate(l2b):
                    brec = index.bundles[bid]
                    bhash_hex = bundle_by_path[str(brec.bin_path())]["sha256"]
                    bhash_bin = bytes.fromhex(bhash_hex)
                    bdata = None

                    for frec in frecs:
                        path = bem.get_path(frec.path_hash)
                        if not path and not populated_path_cache:
                            ctask = progress.add_task('Caching paths', total=1)
                            # Generate and hash paths from inner structure.
                            path_by_phash = _generate_path_hash_table(index)
                            populated_path_cache = True
                            bem.set_paths(path_by_phash.items())
                            del path_by_phash
                            progress.remove_task(ctask)
                            path = bem.get_path(frec.path_hash)
                        should_compress = _compressable_path(path)
                        if fhash_bin := bem.get_extent_hash(bhash_bin, frec.file_offset, frec.file_size):
                            fhash_hex = fhash_bin.hex()
                        else:
                            # extract and record the bundled hash in the output
                            if not bdata:
                                bdata = CompressedBundle(paths.loose_data_path(
                                    bhash_hex).open('rb')).decompress_all()
                            start = frec.file_offset
                            end = start + frec.file_size
                            slice = memoryview(bdata)[start:end]
                            fhash_bin = sha256(slice).digest()
                            fhash_hex = fhash_bin.hex()
                            if fhash_bin not in hashes_on_disk:
                                fpath = paths.bundled_data_path(fhash_hex, compressed=should_compress)
                                if not fpath.exists():
                                    with atomic_write(fpath, mode='wb', overwrite=True) as fh:
                                        bcctx = ZstdCompressor()
                                        for chunk in bcctx.read_to_iter(slice):
                                            fh.write(chunk)
                                    fpath.chmod(0o644)
                            new_bem_keys.append(
                                (bhash_bin, frec.file_offset, frec.file_size))
                            new_bem_values.append(fhash_bin)
                        progress.update(ttask, advance=1)
                        # write an index entry
                        nd = {
                            'sha256': fhash_hex,
                            'path': path,
                            'phash': str(frec.path_hash),
                            'size': frec.file_size,
                            'comp': should_compress,
                        }
                        index_writer.writerow(nd)

                    progress.update(btask, advance=1)

                if len(new_bem_keys):
                    bem.set_extent_hashes(new_bem_keys, new_bem_values)
