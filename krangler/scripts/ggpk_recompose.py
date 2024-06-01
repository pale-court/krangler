import argparse
import os
import struct
from contextlib import nullcontext
from pathlib import Path
from typing import Optional, IO

import more_itertools
from atomicwrites import atomic_write
from more_itertools import peekable, take
from zstandard import ZstdDecompressor

from krangler.data_pile import DataPile
from krangler.ggpk import PackFile


class SkeletonFile:
    def __init__(self, fh):
        self.fh = fh

        # Parse GGSK chunk
        buf = fh.read(12)
        rec_len, tag, version = struct.unpack('<I4sI', buf)
        if tag != b'GGSK':
            raise RuntimeError(f"Skeleton file tag not GGSK: {tag}")
        if version > 2:
            raise RuntimeError(f"Unsupported GGSK version {version} encountered")
        self.original_header = fh.read(rec_len - 12)

        # Parse OFFS chunk
        buf = fh.read(16)
        rec_len, tag, dir_count, file_count = struct.unpack('<I4sII', buf)

        # Read directory offset mapping
        self.dir_ds_to_pk = {}
        self.dir_pk_to_ds = {}
        buf = fh.read(dir_count * 16)
        for ds_offset, pk_offset in struct.iter_unpack('<QQ', buf):
            self.dir_ds_to_pk[ds_offset] = pk_offset
            self.dir_pk_to_ds[pk_offset] = ds_offset

        # Read directory offset mapping
        self.file_ds_to_pk = {}
        self.file_pk_to_ds = {}
        buf = fh.read(file_count * 16)
        for i, (ds_offset, pk_offset) in enumerate(struct.iter_unpack('<QQ', buf)):
            if version == 1:
                # The first offset is at the correct offset and subsequent offsets are shifted by the data size.
                if i == 0:
                    fh.seek(ds_offset)
                ds_offset = fh.tell()

                # Advance past this chunk header so that the file pointer is at the next chunk for the next iteration.
                rec_len, tag, name_len, addr = struct.unpack('<I4sI32s', fh.read(44))
                fh.seek(name_len * 2, os.SEEK_CUR)

            self.file_ds_to_pk[ds_offset] = pk_offset
            self.file_pk_to_ds[pk_offset] = ds_offset

    def read_chunk(self, offset: int) -> bytes:
        self.fh.seek(offset)
        rec_len, tag, name_len = struct.unpack('<I4sI', self.fh.read(12))
        if tag == b'FILE':
            # For files `rec_len` needs to truncate to the chunk header size.
            rec_len = 44 + name_len * 2
        self.fh.seek(offset)
        return self.fh.read(rec_len)


class FreeListFile:
    def __init__(self, fh):
        self.fh = fh
        buf = fh.read(12)
        rec_len, tag, version = struct.unpack('<I4sI', buf)
        if tag != b'GGFR':
            raise RuntimeError(f"Free file tag not GGFR: {tag}")
        if version > 2:
            raise RuntimeError(f"Unsupported GGFR version {version} encountered")
        offset_bias = 4 if version == 1 else 0
        free_count, = struct.unpack('<I', fh.read(4))
        self.fr_to_pk = {}
        self.pk_to_fr = {}
        buf = fh.read(free_count * 16)
        for fr_offset, pk_offset in struct.iter_unpack('<QQ', buf):
            self.fr_to_pk[fr_offset + offset_bias] = pk_offset
            self.pk_to_fr[pk_offset] = fr_offset + offset_bias
        self.offset = 16 + len(buf)

    def read_chunk(self, offset: int) -> bytes:
        # This function can only be called in storage order as the compressed stream can't seek backwards.
        assert offset == self.offset
        if offset < self.offset:
            raise RuntimeError("Cannot read backwards from compressed skeleton file")
        self.fh.read(offset - self.offset)
        buf = self.fh.read(4)
        rec_len, = struct.unpack('<I', buf)
        self.offset += rec_len
        return buf + self.fh.read(rec_len - 4)


def recompose_ggpk(target_file: IO[bytes], data_pile: DataPile, skel_file: IO[bytes], free_file: Optional[IO[bytes]], *,
                   compact=False):
    skeleton = SkeletonFile(skel_file)
    freelist = FreeListFile(free_file) if free_file else None

    # Plan:
    # In addition to the first GGPK chunk, sweep across the three types of chunk and record where they are located in
    # the original file. In the case of compaction, build a second record of the extents of non-FREE chunks and use that
    # to build a new offset into the target file. This would have to be consulted when rewriting PDIR entries as they
    # contain literal offsets to entries.
    # In the case of compaction, emit a small final FREE chunk to have somewhere to point the GGPK key as some tools may
    # expect it to be a valid chunk.

    all_entries = {0: ('GGPK', (0, 0))}
    pdir_i, pdir_n = 0, len(pdirs := skeleton.dir_pk_to_ds)
    file_i, file_n = 0, len(files := skeleton.file_pk_to_ds)
    free_i, free_n = 0, len(frees := freelist.pk_to_fr) if freelist else 0

    pdirs = peekable(skeleton.dir_pk_to_ds.items())
    files = peekable(skeleton.file_pk_to_ds.items())
    frees = peekable(freelist.pk_to_fr.items() if freelist else [])

    while True:
        elem: Optional[(str, (int, int))] = None
        if pdir := pdirs.peek(None):
            if not elem or pdir[0] < elem[1][0]:
                elem = ('PDIR', pdir)
        if file := files.peek(None):
            if not elem or file[0] < elem[1][0]:
                elem = ('FILE', file)
        if free := frees.peek(None):
            if not elem or free[0] < elem[1][0]:
                elem = ('FREE', free)
        if not elem:
            break

        all_entries[elem[1][0]] = elem
        if elem[0] == 'PDIR':
            next(pdirs)
        elif elem[0] == 'FILE':
            next(files)
        elif elem[0] == 'FREE':
            next(frees)

    write_offset = 0
    for target_offset, (kind, (pack_offset, read_offset)) in all_entries.items():
        new_data = []
        assert write_offset == target_offset
        if kind == 'GGPK':
            # TODO(LV): remap root/free entries if compacting
            new_data.append(skeleton.original_header)
        elif kind == 'PDIR':
            # TODO(LV): remap directory entries if compacting
            new_data.append(skeleton.read_chunk(read_offset))
        elif kind == 'FILE':
            chunk = skeleton.read_chunk(read_offset)
            new_data.append(chunk)
            rec_len, tag, name_len, addr = struct.unpack_from('<I4sI32s', chunk, 0)
            payload = data_pile.read_one(addr)
            new_data.append(payload)
        elif kind == 'FREE':
            new_data.append(freelist.read_chunk(read_offset))

        for lump in new_data:
            target_file.write(lump)
            write_offset += len(lump)

    # raise RuntimeError("Not done yet")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', help='Target file', required=True)
    parser.add_argument('--pile', help='Data pile directory for file data', required=True)
    parser.add_argument('--skeleton-file', help='Skeleton file to recompose', required=True)
    parser.add_argument('--free-file', help='Free list file to populate holes with')
    parser.add_argument('--compact', help='Compact (defragment) the resulting file', action='store_true')
    args = parser.parse_args()

    pile = DataPile(Path(args.pile))
    dctx = ZstdDecompressor()
    with (
        Path(args.skeleton_file).open('rb') as skel_file,
        Path(args.free_file).open('rb') if args.free_file else nullcontext(None) as free_zstd,
        dctx.stream_reader(free_zstd, read_across_frames=True, closefd=False) if free_zstd else nullcontext(
            None) as free_file,
        atomic_write(args.output, mode='wb', overwrite=True) as target_file,
    ):
        recompose_ggpk(target_file=target_file, data_pile=pile, skel_file=skel_file, free_file=free_file,
                       compact=args.compact)


if __name__ == "__main__":
    main()
