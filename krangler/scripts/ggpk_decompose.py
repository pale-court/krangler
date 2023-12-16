import struct
import sys
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import List

from krangler.ggpk import PackSource
from atomicwrites import atomic_write
from zstandard import ZstdCompressor


@dataclass
class DecomposedPack:
    skeleton: bytes
    free_list: bytes


class DataPile:
    def __init__(self, root: Path):
        self.root = root
        if not root.exists():
            root.mkdir(parents=True, exist_ok=True)
        for part in "0123456789abcdef":
            (root / part).mkdir(exist_ok=True)

    def _item_path(self, sha256: bytes) -> Path:
        hash_hex = sha256.hex()
        return self.root / hash_hex[0] / f'{hash_hex}.bin'

    def has_one(self, sha256: bytes) -> bool:
        return self._item_path(sha256).exists()

    def has_many(self, sha256s: List[bytes]) -> List[bool]:
        ret = []
        for sha256 in sha256s:
            ret.append(self.has_one(sha256))
        return ret

    def write_one(self, sha256: bytes, payload: bytes):
        p = self._item_path(sha256)
        try:
            with atomic_write(p, mode='wb', overwrite=False) as writer:
                writer.write(payload)
        except (FileExistsError, PermissionError) as e:
            pass


def decompose_ggpk(pack: PackSource, pile: DataPile) -> DecomposedPack:
    # Store data payload on pile
    all_offsets = list(pack.files.keys())
    all_hashes = []
    for file_offset in all_offsets:
        file_chunk = pack.files[file_offset]
        all_hashes.append(file_chunk.sha256)

    all_exists = pile.has_many(all_hashes)

    for i in [i for i, exists in enumerate(all_exists) if exists is False]:
        file_offset = all_offsets[i]
        file_chunk = pack.files[file_offset]
        file_path = pack.file_path(file_offset)
        if file_path is not None:
            file_data = pack.file_data(file_offset)
            pile.write_one(file_chunk.sha256, file_data)

    # Generate dense skeleton file
    skeleton = BytesIO()

    # Write skeleton header
    rec_len = 4 + 4 + 4 + len(pack.ggpk)
    skeleton.write(struct.pack(f'<I4sI{len(pack.ggpk)}s', rec_len, b'GGSK', 1, pack.ggpk))
    write_offset = rec_len

    # Write offset list
    dir_count = len(pack.dirs)
    file_count = len(pack.files)
    rec_len = 4 + 4 + 4 + 4 + dir_count * (8 + 8) + file_count * (8 + 8)
    write_offset += rec_len
    data_start = write_offset
    skeleton.write(struct.pack('<I4sII', rec_len, b'OFFS', dir_count, file_count))
    for dir_offset, chunk in pack.dirs.items():
        skeleton.write(struct.pack('<QQ', write_offset, dir_offset))
        write_offset += chunk.rec_len
    for file_offset, chunk in pack.files.items():
        skeleton.write(struct.pack('<QQ', write_offset, file_offset))
        write_offset += chunk.rec_len

    # Write entries
    assert data_start == skeleton.tell()
    for offset, chunk in pack.dirs.items():
        skeleton.write(pack.read_at(offset, chunk.rec_len))
    for offset, chunk in pack.files.items():
        skeleton.write(pack.read_at(offset, chunk.rec_len - chunk.data_size))

    # Generate free list file
    zctx = ZstdCompressor()
    free_list = BytesIO()
    with zctx.stream_writer(free_list, closefd=False) as writer:
        free_index: dict[int, int] = {}

        header_size = 4 + 4 + 4 + len(pack.frees) * (8 + 8)
        print(f'{len(pack.frees)=}, {header_size=}')
        write_offset = header_size
        data_start = write_offset
        for free_offset, chunk in pack.frees.items():
            free_index[write_offset] = free_offset
            write_offset += chunk.rec_len
        writer.write(struct.pack('<I4sII', header_size, b'GGFR', 1, len(free_index)))
        for here, there in free_index.items():
            writer.write(struct.pack('<QQ', here, there))

        for free_offset in pack.frees:
            free_chunk = pack.frees[free_offset]
            writer.write(pack.read_at(free_offset, free_chunk.rec_len))

    return DecomposedPack(skeleton=skeleton.getvalue(), free_list=free_list.getvalue())


def main() -> int:
    source_path = Path(sys.argv[1])
    pile_path = Path(sys.argv[2])
    out_path = Path(sys.argv[3])
    gid = sys.argv[4]
    skel_path = out_path / f'Content-{gid}.ggpk-skeleton'
    free_path = out_path / f'Content-{gid}.ggpk-free'

    if skel_path.exists() and free_path.exists():
        return 0

    with source_path.open(mode='rb') as pack_fh:
        pack = PackSource(pack_fh)
        pile = DataPile(pile_path)
        dp = decompose_ggpk(pack, pile)
        print(f"{len(dp.skeleton)=}, {len(dp.free_list)=}")
        for path, data in [(skel_path, dp.skeleton), (free_path, dp.free_list)]:
            if not path.exists():
                try:
                    with atomic_write(path, mode='wb') as sfh:
                        sfh.write(data)
                except (FileExistsError, PermissionError) as e:
                    pass


if __name__ == "__main__":
    main()
