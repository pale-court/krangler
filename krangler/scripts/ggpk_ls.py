from pathlib import Path, PurePosixPath
import sys

from krangler.ggpk import PackSource

def main():
    path = Path(sys.argv[1])
    source = PackSource(path.open('rb'))
    for offset in source.files:
        if (path := source.file_path(offset)):
            path = PurePosixPath(path)
            info = source.files[offset]
            print(path, info.sha256.hex(), info.data_size)