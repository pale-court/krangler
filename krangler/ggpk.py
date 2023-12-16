from krangler import source
import io
from io import BytesIO
from dataclasses import dataclass
import struct
from typing import List

_GGPK_TAG = b'GGPK'
_FILE_TAG = b'FILE'
_PDIR_TAG = b'PDIR'
_FREE_TAG = b'FREE'


@dataclass
class PackRecord:
    rec_len: int
    tag: str


@dataclass
class PackDir(PackRecord):
    name: str
    sha256: bytes
    children: List[int]


@dataclass
class PackFile(PackRecord):
    name: str
    sha256: bytes
    data_offset: int
    data_size: int


@dataclass
class PackFree(PackRecord):
    next_free: int
    data_offset: int
    data_size: int


class PackSource(source.Source):
    def __init__(self, fh):
        self.fh = fh
        self.dirs: dict[int, PackDir] = {}
        self.files: dict[int, PackFile] = {}
        self.frees: dict[int, PackFree] = {}
        self.parents: dict[int, int] = {}

        pack_offset = 0
        fh.seek(0, io.SEEK_END)
        pack_size = fh.tell()
        fh.seek(0, io.SEEK_SET)

        self.ggpk = fh.read(28)
        rec_len, tag, version, child0, child1 = struct.unpack('<I4sIQQ', self.ggpk)
        if tag != _GGPK_TAG or version not in (2, 3):
            raise RuntimeError("Invalid GGPK chunk")
        pack_offset = fh.tell()
        while pack_offset < pack_size:
            rec_len, tag = struct.unpack('<I4s', fh.read(8))
            next_offset = pack_offset + rec_len
            if tag == _PDIR_TAG:
                if pack_offset in (child0, child1):
                    self.root_offset = pack_offset
                buf = fh.read(8 + 32)
                name_len, child_count = struct.unpack('<II', buf[:8])
                sha256 = buf[8:]
                name = fh.read(name_len * 2)[:-2].decode('UTF-16LE')
                children = []
                buf = fh.read(12 * child_count)
                for i in range(child_count):
                    name_hash, child_offset = struct.unpack('<IQ', buf[12*i:12*(i+1)])
                    children.append(child_offset)
                    self.parents[child_offset] = pack_offset
                self.dirs[pack_offset] = PackDir(rec_len=rec_len, tag=tag, name=name, sha256=sha256, children=children)
            elif tag == _FILE_TAG:
                buf = fh.read(4 + 32)
                name_len, = struct.unpack('<I', buf[:4])
                sha256 = buf[4:]
                name = fh.read(name_len * 2)[:-2].decode('UTF-16LE')
                data_offset = fh.tell()
                data_size = rec_len - (data_offset - pack_offset)
                self.files[pack_offset] = PackFile(rec_len=rec_len, tag=tag, name=name, sha256=sha256,
                                                   data_offset=data_offset,
                                                   data_size=data_size)
            elif tag == _FREE_TAG:
                next_free, = struct.unpack('<Q', fh.read(8))
                data_offset = fh.tell()
                data_size = rec_len - (data_offset - pack_offset)
                self.frees[pack_offset] = PackFree(rec_len=rec_len, tag=tag, next_free=next_free,
                                                   data_offset=data_offset,
                                                   data_size=data_size)
            else:
                print(struct.pack('<4s', tag))
                raise RuntimeError(f"Invalid chunk tag at offset {pack_offset}")
            fh.seek(next_offset, io.SEEK_SET)
            pack_offset = next_offset

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.fh.close()

    def file_path(self, offset):
        file = self.files[offset]
        segments = [file.name]
        dir_offset = None
        while dir_offset != self.root_offset:
            dir_offset = self.parents.get(offset)
            if dir_offset is None:
                return None
            dir = self.dirs[dir_offset]
            segments.append(dir.name)
            offset = dir_offset
        return "/".join(reversed(segments))

    def file_data(self, offset) -> bytes:
        file = self.files[offset]
        self.fh.seek(file.data_offset)
        return self.fh.read(file.data_size)

    def read_at(self, offset: int, size: int) -> bytes:
        self.fh.seek(offset)
        return self.fh.read(size)


def open_pack_source(fh: BytesIO) -> source.Source:
    return PackSource(fh)
