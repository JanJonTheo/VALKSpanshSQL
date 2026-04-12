from __future__ import annotations

import gzip
import io
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import duckdb
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from .db import connect_db, create_indexes, drop_indexes, init_db, set_meta

ProgressCallback = Callable[[dict[str, Any]], None]

SYSTEM_COLUMNS = [
    "name", "x", "y", "z", "population", "controlling_faction", "faction_count",
    "total_bodies", "total_planets", "total_stars", "ringed_body_count",
    "water_world_count", "earthlike_world_count",
    "body_type_counts_json", "body_subtype_counts_json", "ring_type_counts_json",
    "imported_at", "source_tag",
]

PARQUET_COLUMNS = ["seq"] + SYSTEM_COLUMNS
PARQUET_SCHEMA = pa.schema([
    ("seq", pa.int64()),
    ("name", pa.string()),
    ("x", pa.float64()),
    ("y", pa.float64()),
    ("z", pa.float64()),
    ("population", pa.int64()),
    ("controlling_faction", pa.string()),
    ("faction_count", pa.int32()),
    ("total_bodies", pa.int32()),
    ("total_planets", pa.int32()),
    ("total_stars", pa.int32()),
    ("ringed_body_count", pa.int32()),
    ("water_world_count", pa.int32()),
    ("earthlike_world_count", pa.int32()),
    ("body_type_counts_json", pa.string()),
    ("body_subtype_counts_json", pa.string()),
    ("ring_type_counts_json", pa.string()),
    ("imported_at", pa.timestamp("us")),
    ("source_tag", pa.string()),
])


@dataclass(slots=True)
class ImportStats:
    systems_seen: int = 0
    systems_written: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    dump_size_bytes: int = 0
    bytes_read: int = 0
    paused_seconds: float = 0.0
    temp_files_deleted: int = 0
    temp_bytes_deleted: int = 0

    @property
    def elapsed_s(self) -> float:
        end = self.finished_at or time.time()
        return max(0.0, end - self.started_at)

    @property
    def active_elapsed_s(self) -> float:
        return max(0.0, self.elapsed_s - self.paused_seconds)

    @property
    def systems_per_sec(self) -> float:
        elapsed = self.active_elapsed_s
        return self.systems_seen / elapsed if elapsed else 0.0

    @property
    def bytes_per_sec(self) -> float:
        elapsed = self.active_elapsed_s
        return self.bytes_read / elapsed if elapsed else 0.0

    @property
    def progress_ratio(self) -> float:
        if self.dump_size_bytes <= 0:
            return 0.0
        return min(1.0, self.bytes_read / self.dump_size_bytes)

    @property
    def eta_seconds(self) -> float | None:
        if self.dump_size_bytes <= 0 or self.bytes_per_sec <= 0:
            return None
        remaining = max(0, self.dump_size_bytes - self.bytes_read)
        return remaining / self.bytes_per_sec


class CountingReader:
    def __init__(self, raw):
        self.raw = raw
        self.bytes_read = 0

    def read(self, size=-1):
        data = self.raw.read(size)
        if data:
            self.bytes_read += len(data)
        return data

    def readinto(self, b):
        n = self.raw.readinto(b)
        if n and n > 0:
            self.bytes_read += n
        return n

    def readline(self, size=-1):
        data = self.raw.readline(size)
        if data:
            self.bytes_read += len(data)
        return data

    def readable(self):
        return True

    def seekable(self):
        return False

    def writable(self):
        return False

    def close(self):
        return self.raw.close()

    @property
    def closed(self):
        return self.raw.closed

    def __getattr__(self, item):
        return getattr(self.raw, item)


class TrackedStream:
    def __init__(self, fh, counter: CountingReader):
        self.fh = fh
        self.counter = counter

    @property
    def bytes_read(self) -> int:
        return self.counter.bytes_read

    def __enter__(self):
        enter = getattr(self.fh, "__enter__", None)
        if callable(enter):
            enter()
        return self

    def __exit__(self, exc_type, exc, tb):
        exit_ = getattr(self.fh, "__exit__", None)
        if callable(exit_):
            return exit_(exc_type, exc, tb)
        self.fh.close()
        return False


@dataclass(slots=True)
class TempCleanupStats:
    files_deleted: int = 0
    bytes_deleted: int = 0


def cleanup_temp_files(
    temp_dir: str | Path,
    *,
    older_than_hours: float = 24.0,
    patterns: tuple[str, ...] = ("*.tmp", "*.temp", "*.duckdb.tmp", "duckdb_temp*", "*.wal", "*.spill", "*.parquet"),
) -> TempCleanupStats:
    tmp = Path(temp_dir)
    stats = TempCleanupStats()
    if not tmp.exists():
        return stats

    cutoff = time.time() - max(0.0, older_than_hours) * 3600.0
    seen: set[Path] = set()
    for pattern in patterns:
        for path in tmp.rglob(pattern):
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            try:
                st = path.stat()
                if st.st_mtime > cutoff:
                    continue
                path.unlink(missing_ok=True)
                stats.files_deleted += 1
                stats.bytes_deleted += st.st_size
            except Exception:
                continue
    return stats


def get_free_disk_bytes(path: str | Path) -> int:
    target = Path(path)
    probe = target if target.exists() else target.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    return int(usage.free)


def _pause_for_low_disk(*, db_path: str | Path, temp_dir: str | Path, min_free_gb: float, resume_free_gb: float,
                        poll_seconds: float, stats: ImportStats, progress_cb: ProgressCallback | None) -> None:
    min_free_bytes = int(max(0.0, min_free_gb) * (1024 ** 3))
    resume_free_bytes = int(max(min_free_gb, resume_free_gb) * (1024 ** 3))
    if min_free_bytes <= 0:
        return
    free_db = get_free_disk_bytes(db_path)
    free_tmp = get_free_disk_bytes(temp_dir)
    if min(free_db, free_tmp) >= min_free_bytes:
        return
    paused_at = time.time()
    if progress_cb:
        progress_cb({"paused": True, "pause_reason": "low_disk", "free_db_bytes": free_db, "free_temp_bytes": free_tmp,
                     "min_free_bytes": min_free_bytes, "resume_free_bytes": resume_free_bytes})
    while True:
        time.sleep(max(1.0, poll_seconds))
        free_db = get_free_disk_bytes(db_path)
        free_tmp = get_free_disk_bytes(temp_dir)
        if min(free_db, free_tmp) >= resume_free_bytes:
            stats.paused_seconds += time.time() - paused_at
            if progress_cb:
                progress_cb({"paused": False, "pause_reason": "low_disk", "free_db_bytes": free_db, "free_temp_bytes": free_tmp,
                             "min_free_bytes": min_free_bytes, "resume_free_bytes": resume_free_bytes})
            return


def _open_maybe_compressed(path: str | Path) -> TrackedStream:
    path = Path(path)
    lower = path.name.lower()
    raw = open(path, "rb")
    counter = CountingReader(raw)
    if lower.endswith(".gz"):
        return TrackedStream(gzip.GzipFile(fileobj=counter, mode="rb"), counter)
    if lower.endswith(".zst"):
        dctx = zstd.ZstdDecompressor()
        return TrackedStream(io.BufferedReader(dctx.stream_reader(counter)), counter)
    return TrackedStream(counter, counter)


def _system_name(system: dict[str, Any]) -> str | None:
    return system.get("name") or system.get("systemName")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _coords(system: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    coords = system.get("coords") or {}
    x = _to_float(coords.get("x", system.get("x")))
    y = _to_float(coords.get("y", system.get("y")))
    z = _to_float(coords.get("z", system.get("z")))
    return x, y, z


def _population(system: dict[str, Any]) -> int | None:
    value = system.get("population")
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _controlling_faction(system: dict[str, Any]) -> str | None:
    cf = system.get("controllingFaction")
    if isinstance(cf, dict):
        return cf.get("name") or cf.get("faction")
    if isinstance(cf, str):
        return cf
    return system.get("controllingFactionName")


def _faction_count(system: dict[str, Any], controlling_faction: str | None) -> int:
    factions = system.get("factions") or []
    if isinstance(factions, list):
        names = set()
        for faction in factions:
            if isinstance(faction, dict):
                name = faction.get("name") or faction.get("faction")
            else:
                name = str(faction)
            if name:
                names.add(name)
        if controlling_faction:
            names.add(controlling_faction)
        return len(names)
    return 1 if controlling_faction else 0


def _summarize_bodies(system: dict[str, Any]) -> dict[str, Any]:
    bodies = system.get("bodies") or []
    body_type_counts: dict[str, int] = {}
    body_subtype_counts: dict[str, int] = {}
    ring_type_counts: dict[str, int] = {}
    total_bodies = 0
    total_planets = 0
    total_stars = 0
    ringed_body_count = 0
    water_world_count = 0
    earthlike_world_count = 0

    for body in bodies:
        if not isinstance(body, dict):
            continue
        total_bodies += 1
        body_type = str(body.get("type") or "Unknown")
        body_type_counts[body_type] = body_type_counts.get(body_type, 0) + 1
        if body_type == "Planet":
            total_planets += 1
        elif body_type == "Star":
            total_stars += 1

        subtype = body.get("subType") or body.get("subtype")
        if subtype:
            subtype = str(subtype)
            body_subtype_counts[subtype] = body_subtype_counts.get(subtype, 0) + 1
            if subtype == "Water world":
                water_world_count += 1
            elif subtype == "Earth-like world":
                earthlike_world_count += 1

        rings = body.get("rings") or []
        if rings:
            ringed_body_count += 1
            for ring in rings:
                if not isinstance(ring, dict):
                    continue
                ring_type = str(ring.get("type") or ring.get("ringType") or "Unknown")
                ring_type_counts[ring_type] = ring_type_counts.get(ring_type, 0) + 1

    return {
        "total_bodies": total_bodies,
        "total_planets": total_planets,
        "total_stars": total_stars,
        "ringed_body_count": ringed_body_count,
        "water_world_count": water_world_count,
        "earthlike_world_count": earthlike_world_count,
        "body_type_counts_json": __import__('json').dumps(body_type_counts, ensure_ascii=False, sort_keys=True),
        "body_subtype_counts_json": __import__('json').dumps(body_subtype_counts, ensure_ascii=False, sort_keys=True),
        "ring_type_counts_json": __import__('json').dumps(ring_type_counts, ensure_ascii=False, sort_keys=True),
    }


def _row_dict_from_system(system: dict[str, Any], source_tag: str, seq: int) -> dict[str, Any] | None:
    name = _system_name(system)
    if not name:
        return None
    x, y, z = _coords(system)
    controlling_faction = _controlling_faction(system)
    summary = _summarize_bodies(system)
    return {
        "seq": seq,
        "name": name,
        "x": x,
        "y": y,
        "z": z,
        "population": _population(system),
        "controlling_faction": controlling_faction,
        "faction_count": _faction_count(system, controlling_faction),
        "total_bodies": summary["total_bodies"],
        "total_planets": summary["total_planets"],
        "total_stars": summary["total_stars"],
        "ringed_body_count": summary["ringed_body_count"],
        "water_world_count": summary["water_world_count"],
        "earthlike_world_count": summary["earthlike_world_count"],
        "body_type_counts_json": summary["body_type_counts_json"],
        "body_subtype_counts_json": summary["body_subtype_counts_json"],
        "ring_type_counts_json": summary["ring_type_counts_json"],
        "imported_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "source_tag": source_tag,
    }


def _emit_progress(*, stats: ImportStats, db_path: str, temp_dir: str, progress_cb: ProgressCallback | None,
                   phase: str = "reading", message: str | None = None) -> None:
    if not progress_cb:
        return
    progress_cb({
        "phase": phase,
        "message": message or "",
        "systems_seen": stats.systems_seen,
        "systems_written": stats.systems_written,
        "elapsed_s": stats.elapsed_s,
        "active_elapsed_s": stats.active_elapsed_s,
        "systems_per_sec": stats.systems_per_sec,
        "bytes_read": stats.bytes_read,
        "dump_size_bytes": stats.dump_size_bytes,
        "progress_ratio": stats.progress_ratio,
        "eta_seconds": stats.eta_seconds,
        "paused_seconds": stats.paused_seconds,
        "free_db_bytes": get_free_disk_bytes(db_path),
        "free_temp_bytes": get_free_disk_bytes(temp_dir),
        "temp_files_deleted": stats.temp_files_deleted,
        "temp_bytes_deleted": stats.temp_bytes_deleted,
    })


def _write_parquet_batch(writer: pq.ParquetWriter | None, batch: list[dict[str, Any]], parquet_path: str,
                         compression: str) -> pq.ParquetWriter:
    arrays = {col: [row.get(col) for row in batch] for col in PARQUET_COLUMNS}
    table = pa.Table.from_pydict(arrays, schema=PARQUET_SCHEMA)
    if writer is None:
        writer = pq.ParquetWriter(parquet_path, PARQUET_SCHEMA, compression=compression)
    writer.write_table(table)
    return writer


def convert_dump_to_parquet(dump_path: str, parquet_path: str, *, row_group_size: int = 50000,
                            compression: str = "zstd", progress_cb: ProgressCallback | None = None,
                            db_path_for_progress: str | None = None, temp_dir_for_progress: str | None = None,
                            low_disk_mode: bool = False, min_free_gb: float = 20.0, resume_free_gb: float = 25.0,
                            low_disk_poll_seconds: float = 10.0) -> ImportStats:
    parquet_path = str(parquet_path)
    Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
    stats = ImportStats(started_at=time.time(), dump_size_bytes=os.path.getsize(dump_path))
    source_tag = Path(dump_path).name
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    db_prog = db_path_for_progress or parquet_path
    tmp_prog = temp_dir_for_progress or str(Path(parquet_path).parent)

    try:
        with _open_maybe_compressed(dump_path) as tracked:
            for system in ijson.items(tracked.fh, "item"):
                if low_disk_mode:
                    _pause_for_low_disk(db_path=db_prog, temp_dir=tmp_prog, min_free_gb=min_free_gb,
                                        resume_free_gb=resume_free_gb, poll_seconds=low_disk_poll_seconds,
                                        stats=stats, progress_cb=progress_cb)
                stats.bytes_read = tracked.bytes_read
                stats.systems_seen += 1
                row = _row_dict_from_system(system, source_tag, stats.systems_seen)
                if row is not None:
                    batch.append(row)
                if len(batch) >= row_group_size:
                    writer = _write_parquet_batch(writer, batch, parquet_path, compression)
                    stats.systems_written += len(batch)
                    batch.clear()
                    _emit_progress(stats=stats, db_path=db_prog, temp_dir=tmp_prog, progress_cb=progress_cb,
                                   phase="parquet", message="Parquet wird geschrieben")
        if batch:
            writer = _write_parquet_batch(writer, batch, parquet_path, compression)
            stats.systems_written += len(batch)
            batch.clear()
        if writer is not None:
            writer.close()
        stats.finished_at = time.time()
        _emit_progress(stats=stats, db_path=db_prog, temp_dir=tmp_prog, progress_cb=progress_cb,
                       phase="parquet_done", message="Parquet erstellt")
        return stats
    finally:
        if writer is not None:
            writer.close()


def import_parquet(db_path: str, parquet_path: str, *, temp_dir: str | None = None, mode: str = "full",
                   progress_cb: ProgressCallback | None = None) -> ImportStats:
    mode = mode.lower().strip()
    if mode not in {"full", "patch"}:
        raise ValueError("mode muss 'full' oder 'patch' sein")

    init_db(db_path, temp_dir=temp_dir)
    con = connect_db(db_path, temp_dir=temp_dir)
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    stats = ImportStats(started_at=time.time(), dump_size_bytes=os.path.getsize(parquet_path), bytes_read=os.path.getsize(parquet_path))
    source_tag = Path(parquet_path).name
    try:
        drop_indexes(con)
        counts = con.execute(
            "SELECT COUNT(*) AS rows, COUNT(DISTINCT name) AS distinct_names FROM read_parquet(?)",
            [parquet_path],
        ).fetchone()
        stats.systems_seen = int(counts[0])
        distinct_names = int(counts[1])
        _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb,
                       phase="db_import", message="Parquet wird in DuckDB importiert")

        select_sql = f"""
            SELECT {', '.join(SYSTEM_COLUMNS)}
            FROM (
                SELECT {', '.join(PARQUET_COLUMNS)},
                       ROW_NUMBER() OVER (PARTITION BY name ORDER BY seq DESC) AS rn
                FROM read_parquet(?)
            ) t
            WHERE rn = 1
        """

        if mode == "full":
            con.execute("DELETE FROM systems")
            con.execute(
                f"INSERT INTO systems ({', '.join(SYSTEM_COLUMNS)}) {select_sql}",
                [parquet_path],
            )
            stats.systems_written = distinct_names
        else:
            update_assignments = ",\n    ".join([f"{c} = s.{c}" for c in SYSTEM_COLUMNS if c != "name"])
            insert_cols = ", ".join(SYSTEM_COLUMNS)
            insert_vals = ", ".join([f"s.{c}" for c in SYSTEM_COLUMNS])
            merge_sql = f"""
                MERGE INTO systems AS t
                USING ({select_sql}) AS s
                ON t.name = s.name
                WHEN MATCHED THEN UPDATE SET
                    {update_assignments}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            con.execute(merge_sql, [parquet_path])
            stats.systems_written = distinct_names

        set_meta(con, "last_import_mode", mode)
        set_meta(con, "last_import_source", source_tag)
        set_meta(con, "last_import_finished_utc", datetime.utcnow().isoformat(timespec="seconds"))
        create_indexes(con)
        con.execute("ANALYZE systems")
        con.execute("CHECKPOINT")
        stats.finished_at = time.time()
        _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb,
                       phase="done", message="DuckDB-Import abgeschlossen")
        return stats
    finally:
        con.close()


def import_dump(db_path: str, dump_path: str, *, temp_dir: str | None = None, batch_size: int = 5000,
                mode: str = "full", cleanup_temp_before: bool = False, cleanup_temp_after: bool = False,
                cleanup_older_than_hours: float = 24.0, low_disk_mode: bool = False, min_free_gb: float = 20.0,
                resume_free_gb: float = 25.0, low_disk_poll_seconds: float = 10.0,
                progress_cb: ProgressCallback | None = None, keep_parquet: bool = False) -> ImportStats:
    lower = dump_path.lower()
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    if cleanup_temp_before:
        cleanup_stats = cleanup_temp_files(temp_dir, older_than_hours=cleanup_older_than_hours)
    else:
        cleanup_stats = TempCleanupStats()

    if lower.endswith(".parquet"):
        stats = import_parquet(db_path, dump_path, temp_dir=temp_dir, mode=mode, progress_cb=progress_cb)
        stats.temp_files_deleted += cleanup_stats.files_deleted
        stats.temp_bytes_deleted += cleanup_stats.bytes_deleted
        return stats

    temp_parquet = str(Path(temp_dir) / f"{Path(dump_path).stem}.{int(time.time())}.parquet")
    convert_stats = convert_dump_to_parquet(
        dump_path,
        temp_parquet,
        row_group_size=max(1000, batch_size),
        compression="zstd",
        progress_cb=progress_cb,
        db_path_for_progress=db_path,
        temp_dir_for_progress=temp_dir,
        low_disk_mode=low_disk_mode,
        min_free_gb=min_free_gb,
        resume_free_gb=resume_free_gb,
        low_disk_poll_seconds=low_disk_poll_seconds,
    )
    db_stats = import_parquet(db_path, temp_parquet, temp_dir=temp_dir, mode=mode, progress_cb=progress_cb)

    if not keep_parquet:
        try:
            size = os.path.getsize(temp_parquet)
            os.remove(temp_parquet)
            db_stats.temp_files_deleted += 1
            db_stats.temp_bytes_deleted += size
        except OSError:
            pass

    if cleanup_temp_after:
        cleanup_stats_after = cleanup_temp_files(temp_dir, older_than_hours=cleanup_older_than_hours)
        db_stats.temp_files_deleted += cleanup_stats_after.files_deleted
        db_stats.temp_bytes_deleted += cleanup_stats_after.bytes_deleted

    db_stats.systems_seen = convert_stats.systems_seen
    db_stats.bytes_read = convert_stats.bytes_read
    db_stats.dump_size_bytes = convert_stats.dump_size_bytes
    db_stats.paused_seconds += convert_stats.paused_seconds
    db_stats.temp_files_deleted += cleanup_stats.files_deleted
    db_stats.temp_bytes_deleted += cleanup_stats.bytes_deleted
    return db_stats
