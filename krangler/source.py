from io import BytesIO
import os
from pathlib import Path, PurePosixPath
import typing
import zipfile


class Source(typing.Protocol):
    def contains(self, path) -> bool:
        pass

    def open(self, path, mode) -> typing.Optional[BytesIO]:
        pass

    def walk(self):
        pass


class ZipSource(Source):
    def __init__(self, zip_path):
        self.zip = zipfile.ZipFile(zip_path, "r")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.zip.close()

    def contains(self, path) -> bool:
        return str(path) in self.zip.namelist()

    def open(self, path, mode="r") -> BytesIO:
        if "b" in mode:
            mode = mode.replace("b", "")
        try:
            return self.zip.open(str(path), mode=mode)
        except KeyError:
            raise FileNotFoundError()

    def walk(self):
        for e in self.zip.infolist():
            if not e.is_dir() and ".DepotDownloader" not in e.filename:
                with self.zip.open(e.filename) as fh:
                    yield PurePosixPath(e.filename), e.file_size, fh


class DiskSource(Source):
    def __init__(self, dir_path):
        self.dir_path = dir_path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def contains(self, path) -> bool:
        return (self.dir_path / path).exists()

    def open(self, path: Path | str, mode="rb") -> BytesIO:
        return (self.dir_path / path).open(mode=mode)

    def walk(self):
        for root, dirs, files in os.walk(self.dir_path, topdown=True):
            if ".DepotDownloader" in dirs:
                dirs.remove(".DepotDownloader")
            root_path = Path(root)
            for file in files:
                path = root_path / file
                rel_path = PurePosixPath(path.relative_to(self.dir_path))
                with path.open("rb") as fh:
                    yield rel_path, path.stat().st_size, fh


def open_source(path: Path):
    if not path.exists():
        raise RuntimeError(f"Cannot open missing source {path}")
    if path.is_dir():
        return DiskSource(path)
    if path.is_file() and path.suffix == ".zip":
        return ZipSource(path)
