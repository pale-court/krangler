import codecs
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
import logging
from typing import Iterable, List, Protocol, Set, Optional, Callable

import ndjson
from atomicwrites import atomic_write
from contextlib import contextmanager

import psycopg
from zstandard import ZstdCompressor, ZstdDecompressor
from krangler import poe_util
from krangler.logging import LOG
from krangler.paths import Paths
from krangler.state_tracking import StateTracker


class ArtifactStore(Protocol):
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    @contextmanager
    def index_writer(self, depot, manifest, kind):
        pass

    @contextmanager
    def index_reader(self, depot, manifest, kind):
        pass

    def list_missing_objects(self, addrs: Iterable[bytes]) -> Set[bytes]:
        pass

    def write_data(self, file_hash, file_data):
        pass

    def read_data(self, file_hash) -> Optional[bytes]:
        pass

    def has_depot_fact(self, depot, gid, fact) -> bool:
        pass

    def set_depot_fact(self, depot, gid, fact):
        pass

    def unset_depot_fact(self, depot, gid, fact):
        pass


class FilesystemStore(ArtifactStore):
    def __init__(self, paths: Paths):
        self.paths = paths

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @contextmanager
    def index_writer(self, depot, manifest, kind):
        if kind == "loose":
            index_path = self.paths.loose_index_path(depot, manifest)
        elif kind == "bundled":
            index_path = self.paths.bundled_index_path(depot, manifest)
        else:
            raise RuntimeError(f"Unknown index kind {kind}")
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with poe_util.atomic_compressed_ndjson_writer(index_path) as index_writer:
            yield index_writer

    @contextmanager
    def index_reader(self, depot, manifest, kind):
        if kind == "loose":
            index_path = self.paths.loose_index_path(depot, manifest)
        elif kind == "bundled":
            index_path = self.paths.bundled_index_path(depot, manifest)
        else:
            raise RuntimeError(f"Unknown index kind {kind}")
        dctx = ZstdDecompressor()
        with (
            index_path.open('rb') as ifh,
            dctx.stream_reader(ifh, read_across_frames=True, closefd=False) as reader,
            codecs.getreader('UTF-8')(reader) as reader,
            ndjson.reader(reader) as reader,
        ):
            yield reader

    def write_data(self, file_hash, file_data):
        try:
            sub_path = self.paths.loose_data_path(file_hash)
            if not sub_path.exists():
                sub_path.parent.mkdir(parents=True, exist_ok=True)
                with atomic_write(sub_path, mode="wb") as out_fh:
                    if callable(file_data):
                        file_data = file_data()
                    out_fh.write(file_data)
                sub_path.chmod(0o644)
        except FileExistsError:
            pass

    def read_data(self, file_hash) -> Optional[bytes]:
        sub_path = self.paths.loose_data_path(file_hash)
        if sub_path.exists():
            with sub_path.open('rb') as ifh:
                return ifh.read()

    def has_depot_fact(self, depot, gid, fact) -> bool:
        return StateTracker(self.paths, depot, gid).has_state(fact)

    def set_depot_fact(self, depot, gid, fact):
        StateTracker(self.paths, depot, gid).set_state(fact)

    def unset_depot_fact(self, depot, gid, fact):
        StateTracker(self.paths, depot, gid).clear_state(fact)


class DatabaseStore(ArtifactStore):
    def __init__(self, db_uri: str):
        self.conn = psycopg.connect(db_uri)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.conn.close()

    @contextmanager
    def write_data_bulk(self):
        with self.conn.cursor() as cur:
            class BulkWrite:
                def store(self, addr, data):
                    cctx = ZstdCompressor()
                    cdata = cctx.compress(data)
                    compression = None
                    if len(data) > len(cdata):
                        data = cdata
                        compression = "zstd"
                    cur.execute(
                        """
                            INSERT INTO data (content_hash, data, compression) VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """,
                        (addr, data, compression),
                    )

            yield BulkWrite()
            cur.connection.commit()

    # Note that elements in addrs need to be unique.
    def list_missing_objects(self, addrs: Iterable[bytes]) -> Set[bytes]:
        ret = set()
        with (
            self.conn.cursor() as cur,
            self.conn.transaction() as txn,
        ):
            cur.execute(
                """
                    CREATE TEMPORARY TABLE candidates
                    (addr BYTEA PRIMARY KEY NOT NULL)
                    ON COMMIT DROP
                """
            )
            with cur.copy(
                    "COPY candidates (addr) FROM STDIN WITH (FORMAT BINARY)"
            ) as copy:
                copy.set_types(["bytea"])
                for addr in addrs:
                    copy.write_row((addr,))
            cur.execute(
                """
                    SELECT addr FROM candidates
                    WHERE NOT EXISTS (SELECT FROM data WHERE content_hash = addr)
                """
            )
            for (addr,) in cur:
                ret.add(addr)
            # cur.execute("DROP TABLE candidates")
        cur.connection.commit()
        return ret

    @contextmanager
    def index_writer(self, depot, manifest, kind):
        bio = BytesIO()
        with poe_util.compressed_ndjson_writer(bio) as writer:
            yield writer
        self.conn.execute(
            """
                INSERT INTO index (gid, kind, data, compression)
                VALUES (%s, %s, %s, 'zstd')
                ON CONFLICT ON CONSTRAINT index_pkey DO
                    UPDATE SET data = excluded.data, compression = excluded.compression
            """,
            (manifest, kind, bio.getbuffer()),
        )
        self.conn.commit()

    @contextmanager
    def index_reader(self, depot, manifest, kind):
        data, = self.conn.execute(
            """
                SELECT data FROM index
                WHERE gid = %s AND kind = %s AND compression = 'zstd'
            """,
            (manifest, kind),
        ).fetchone()
        dctx = ZstdDecompressor()
        bio = BytesIO(data)
        with (
            dctx.stream_reader(bio, read_across_frames=True, closefd=False) as zreader,
            codecs.getreader('UTF-8')(zreader) as treader,
        ):
            reader = ndjson.reader(treader)
            yield reader

    def write_data(self, file_hash: bytes | str, file_data):
        if isinstance(file_hash, str):
            file_hash = codecs.decode(file_hash, 'hex')
        with self.conn.cursor() as cur:
            existing_row = cur.execute(
                "SELECT FROM data WHERE content_hash = %s",
                (file_hash,),
            ).fetchone()
            if existing_row is None:
                # Manifest the data as we need the contents
                if callable(file_data):
                    file_data = file_data()

                # Attempt to compress it
                cctx = ZstdCompressor()
                cdata = cctx.compress(file_data)
                comp = "zstd"
                if len(file_data) <= len(cdata):
                    # If it's not smaller, use the uncompressed file data
                    cdata = file_data
                    comp = None
                cur.execute(
                    """
                        INSERT INTO data (content_hash, data, compression) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (file_hash, cdata, comp),
                )
                cur.connection.commit()

    def read_data(self, file_hash: bytes | str) -> Optional[bytes]:
        if isinstance(file_hash, str):
            file_hash = codecs.decode(file_hash, 'hex')
        with self.conn.cursor() as cur:
            existing_row = cur.execute(
                "SELECT data, compression FROM data WHERE content_hash = %s",
                (file_hash,),
            ).fetchone()
            if existing_row:
                data, compression = existing_row
                if compression is None:
                    return data
                elif compression == "zstd":
                    dctx = ZstdDecompressor()
                    with dctx.stream_reader(BytesIO(data), read_across_frames=True) as reader:
                        return reader.read()
                else:
                    raise RuntimeError(f"Unknown compression {compression} for content with address {file_hash}")

    def has_depot_fact(self, depot, gid, fact) -> bool:
        with self.conn.cursor() as cur:
            return cur.execute(
                "SELECT fact FROM depot_fact WHERE depot = %s AND gid = %s AND fact = %s",
                (depot, gid, fact),
            ).fetchone() is not None

    def set_depot_fact(self, depot, gid, fact):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO depot_fact (depot, gid, fact) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (depot, gid, fact),
            )
            cur.connection.commit()

    def unset_depot_fact(self, depot, gid, fact):
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM depot_fact WHERE depot = %s AND gid = %s AND fact = %s",
                (depot, gid, fact),
            )
            cur.connection.commit()


@dataclass
class Object:
    data: bytes | Callable[[], bytes]  # data can be lazily obtained
    size: Optional[int]
    sha1: Optional[bytes] = None  # we may not have any manifest to source SHA1 from


class BatchQueue:
    def __init__(self, store: ArtifactStore, *, size_budget=None, count_budget=None):
        self.store = store
        self.size_budget = size_budget
        self.count_budget = count_budget
        self.size_acc = 0
        self.count_acc = 0
        self.objects = {}

    def store_one(self, addr: bytes, obj: Object):
        if obj.size is not None:
            self.size_acc += obj.size
        else:
            if callable(obj.data):
                obj.data = obj.data()
            obj.size = len(obj.data)
            self.size_acc += obj.size
        self.count_acc += 1
        self.objects[addr] = obj

        # Upload a bulk batch if it's time
        self.flush(force=False)

    def flush(self, *, force=True):
        above_size = self.size_acc >= self.size_budget if self.size_budget else False
        above_count = (
            self.count_acc >= self.count_budget if self.count_budget else False
        )
        if force or above_size or above_count:
            missing_addrs = self.store.list_missing_objects(self.objects.keys())
            LOG.info(
                f"flushing {self.size_acc} bytes over {self.count_acc} items, {len(missing_addrs)} new"
            )
            with self.store.write_data_bulk() as bulk:
                for addr in missing_addrs:
                    data = self.objects[addr].data
                    if callable(data):
                        data = data()
                    bulk.store(addr, data)
            self.objects.clear()
            self.size_acc = 0
            self.count_acc = 0
