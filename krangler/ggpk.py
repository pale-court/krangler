from krangler import source
import io
from io import BytesIO
from dataclasses import dataclass
import struct
from typing import List

def _chunk_tag(s: str):
    return struct.unpack('<I', struct.pack('<BBBB', ord(s[0]), ord(s[1]), ord(s[2]), ord(s[3])))[0]

_GGPK_TAG = _chunk_tag('GGPK')
_FILE_TAG = _chunk_tag('FILE')
_PDIR_TAG = _chunk_tag('PDIR')
_FREE_TAG = _chunk_tag('FREE')

@dataclass
class PackDir:
    name: str
    sha256: bytes
    children: List[int]

@dataclass
class PackFile:
    name: str
    sha256: bytes
    data_offset: int
    data_size: int

class PackSource(source.Source):
    def __init__(self, fh):
        self.fh = fh
        self.dirs = {}
        self.files = {}
        self.parents = {}

        pack_offset = 0
        fh.seek(0, io.SEEK_END)
        pack_size = fh.tell()
        fh.seek(0, io.SEEK_SET)

        rec_len, tag, version, child0, child1 = struct.unpack('<IIIQQ', fh.read(28))
        if tag != _GGPK_TAG or version not in (2, 3):
            raise RuntimeError("Invalid GGPK chunk")
        pack_offset = fh.tell()
        while pack_offset < pack_size:
            rec_len, tag = struct.unpack('<II', fh.read(8))
            next_offset = pack_offset + rec_len
            if tag == _PDIR_TAG:
                if pack_offset in (child0, child1):
                    self.root_offset = pack_offset
                name_len, child_count = struct.unpack('<II', fh.read(8))
                sha256 = fh.read(32)
                name = fh.read(name_len*2)[:-2].decode('UTF-16LE')
                children = []
                for _ in range(child_count):
                    name_hash, child_offset = struct.unpack('<IQ', fh.read(12))
                    children.append(child_offset)
                    self.parents[child_offset] = pack_offset
                self.dirs[pack_offset] = PackDir(name=name, sha256=sha256, children=children)
            elif tag == _FILE_TAG:
                name_len, = struct.unpack('<I', fh.read(4))
                sha256 = fh.read(32)
                name = fh.read(name_len*2)[:-2].decode('UTF-16LE')
                data_offset = fh.tell()
                data_size = rec_len - (data_offset - pack_offset)
                self.files[pack_offset] = PackFile(name=name, sha256=sha256, data_offset=data_offset, data_size=data_size)
            elif tag == _FREE_TAG:
                pass
            else:
                print(struct.pack('<I', tag))
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

def open_pack_source(fh: BytesIO) -> source.Source:
    return PackSource(fh)