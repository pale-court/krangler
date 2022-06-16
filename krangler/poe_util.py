from pathlib import PurePosixPath

from . import _fnv as fnv

def hash_file_path(p: PurePosixPath | str):
    p = PurePosixPath(p)
    if p.is_absolute():
        p = p.relative_to('/')
    s = (str(p).lower() + '++').encode('UTF-8')
    return fnv.hash(s, algorithm=fnv.fnv_1a, bits=64)

def hash_dir_path(p: PurePosixPath | str):
    p = PurePosixPath(p)
    if p.is_absolute():
        p = p.relative_to('/')
    s = (str(p) + '++').encode('UTF-8')
    return fnv.hash(s, algorithm=fnv.fnv_1a, bits=64)