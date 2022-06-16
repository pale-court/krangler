from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
import logging
import os
from pathlib import PurePosixPath
from stat import FILE_ATTRIBUTE_OFFLINE
import struct
from time import sleep
from typing import List
from atomicwrites import atomic_write
from rich.progress import track, Progress, MofNCompleteColumn

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


@contextmanager
def atomic_compressed_ndjson_writer(path):
    with atomic_write(path, mode='wb', overwrite=True) as ifh:
        cctx = ZstdCompressor()
        with cctx.stream_writer(ifh, closefd=False, write_return_read=True) as idx_compressor:
            with codecs.getwriter('utf-8')(idx_compressor) as codec:
                yield ndjson.writer(codec)


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

    # Generate and hash paths from inner structure.
    path_by_phash = {}
    pass

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

    with atomic_compressed_ndjson_writer(paths.bundled_index_path(depot, manifest)) as index_writer:
        with BundledExtentMap(paths.bundled_extent_db()) as bem:
            bid = None
            brec = None
            bhash_bin = None
            bhash_hex = None
            bdata = None
            with Progress(*Progress.get_default_columns(), MofNCompleteColumn(), refresh_per_second=1) as progress:
                ttask = progress.add_task('Total entries...', total=len(l2))
                btask = progress.add_task('Bundles...', total=len(index.bundles))

                new_bem_keys = []
                new_bem_values = []
                for bid, frecs in enumerate(l2b):
                    brec = index.bundles[bid]
                    bhash_hex = bundle_by_path[str(brec.bin_path())]["sha256"]
                    bhash_bin = bytes.fromhex(bhash_hex)
                    bdata = None

                    for frec in frecs:
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
                                fpath = paths.bundled_data_path(fhash_hex)
                                if not fpath.exists():
                                    with atomic_write(fpath, mode='wb', overwrite=True) as fh:
                                        bcctx = ZstdCompressor()
                                        for chunk in bcctx.read_to_iter(slice):
                                            fh.write(chunk)
                            new_bem_keys.append(
                                (bhash_bin, frec.file_offset, frec.file_size))
                            new_bem_values.append(fhash_bin)
                        progress.update(ttask, advance=1)
                        # write an index entry
                        nd = {
                            'sha256': fhash_hex,
                            # 'path': path_by_phash[frec.path_hash],
                            'size': frec.file_size,
                            'comp': True,
                        }
                        index_writer.writerow(nd)

                    progress.update(btask, advance=1)

                if len(new_bem_keys):
                    bem.set_extent_hashes(new_bem_keys, new_bem_values)


'''
    bundled_extent_map.build_db(paths.bundled_extent_db())

    EXTENT_MAP_COMMIT_THRESHOLD = 1000
    ingested_files = 0
    engine = create_engine(paths.bundled_extent_db(), echo=False, future=True)
    with Session(engine) as session:
        bhash_by_bid = []
        our_bundle_hashes = {}
        for brec in index.bundles:
            bsha256 = bundle_by_path[str(brec.bin_path())]["sha256"]
            bhash_by_bid.append(bsh256)
            our_bundle_hashes[bsha256] = None

        bundle_hashes = {}
        for bh in session.query(BundleHash).all():
            if bh.hash in our_bundle_hashes:
                bundle_hashes[bh.hash] = bh.id
                del our_bundle_hashes[bh.hash]

        for hash in our_bundle_hashes:
            bobj = BundleHash(hash=hash)
            session.add(bobj)
            bundle_hashes[hash] = bobj.id

        session.flush()

        with atomic_write(paths.bundled_index_path(depot, manifest), mode='wb', overwrite=True) as ifh:
            cctx = ZstdCompressor()
            with cctx.stream_writer(ifh, closefd=False, write_return_read=True) as idx_compressor:
                with codecs.getwriter('utf-8')(idx_compressor) as codec:
                    index_writer = ndjson.writer(codec)

                    for brec in index.bundles:

                    brec: BundleRecord = None
                    bdata = None
                    bhash_row = None
                    # for each bundled file entry (sorted for extraction synergy):
                    for bid, frecs in track(bundle_extents.items(), description="Unbundling files"):
                        brec = index.bundles[bid]
                        bdata = None

                    """
                    for frec_idx, frec in enumerate(track(l2, description="Unbundling files")):
                        if brec is None or bid != frec.bundle_index:
                            bid = frec.bundle_index
                            brec = index.bundles[bid]
                            bsha256 = bundle_by_path[str(
                                brec.bin_path())]["sha256"]
                            bdata = None
                            if row := session.query(BundleHash).filter_by(hash=bsha256).one_or_none():
                                bhash_id = row.id
                            else:
                                bhash_id = BundleHash(hash=bsha256).id
                        # look up the extent and the bundle hash to determine if the file doesn't need to be extracted;
                        if shortcut := session.execute(select(ExtentMap).
                                                       filter_by(bundle_hash_id=bhash_id, file_offset=frec.file_offset, file_size=frec.file_size)
                                                       # .filter(ExtentMap.bundle_hash.has(hash=bsha256))
                                                       ).scalar_one_or_none():
                            fsha256 = shortcut.file_hash.hash
                        else:
                            # extract and record the bundled hash in the output
                            if not bdata:
                                bdata = CompressedBundle(paths.loose_data_path(
                                    bsha256).open('rb')).decompress_all()
                            start = frec.file_offset
                            end = start + frec.file_size
                            slice = memoryview(bdata)[start:end]
                            fsha256 = sha256(slice).hexdigest()
                            bpath = paths.bundled_data_path(fsha256)
                            if not bpath.exists():
                                with atomic_write(bpath, mode='wb', overwrite=True) as fh:
                                    bcctx = ZstdCompressor()
                                    for chunk in bcctx.read_to_iter(slice):
                                        fh.write(chunk)
                            row = ExtentMap(
                                file_offset=frec.file_offset, file_size=frec.file_size)
                            row.bundle_hash = bhash_row
                            if fhash_row := session.query(FileHash).filter_by(hash=fsha256).one_or_none():
                                row.file_hash = fhash_row
                            else:
                                row.file_hash = FileHash(hash=fsha256)
                            session.add(row)
                            session.flush()

                        ingested_files += 1
                        if ingested_files == EXTENT_MAP_COMMIT_THRESHOLD:
                            # session.commit()
                            ingested_files = 0

                        # write an index for bundled files
                        nd = {
                            'sha256': fsha256,
                            # 'path': path_by_phash[frec.path_hash],
                            'size': frec.file_size,
                            'comp': True,
                        }
                        try:
                            index_writer.writerow(nd)
                        except Exception as e:
                            print(cctx.frame_progression())
                            raise e
                    """
        session.commit()
'''
