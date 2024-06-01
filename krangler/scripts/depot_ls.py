import sys
import zlib
from pathlib import Path, PurePosixPath, PurePath

from krangler.protos import depot_downloader_pb2


def main():
    for root in sys.argv[1:]:
        p = Path(root)
        depot = 238961
        gid = p.name
        with (p / ".DepotDownloader" / f"{depot}_{gid}.bin").open("rb") as fh:
            pmf = depot_downloader_pb2.ProtoManifest()
            pmf.ParseFromString(zlib.decompress(fh.read(), -15))
            if pmf.IsInitialized():
                for file in pmf.Files:
                    if not file.Flags or not file.Flags & 64:
                        # The serialized path type in the protobufs is OS dependent, normalize it.
                        filename = PurePosixPath(PurePath(file.FileName))
                        print(filename, file.FileHash.hex(), file.TotalSize)
