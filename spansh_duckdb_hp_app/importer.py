from __future__ import annotations

import gzip
import io
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import ijson
import zstandard as zstd

from .db import connect_db, init_db, set_meta, drop_indexes, create_indexes

ProgressCallback = Callable[[dict[str, Any]], None]


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
    patterns: tuple[str, ...] = ("*.tmp", "*.temp", "*.duckdb.tmp", "duckdb_temp*", "*.wal", "*.spill"),
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


def _pause_for_low_disk(
    *,
    db_path: str | Path,
    temp_dir: str | Path,
    min_free_gb: float,
    resume_free_gb: float,
    poll_seconds: float,
    stats: ImportStats,
    progress_cb: ProgressCallback | None,
) -> None:
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
        progress_cb({
            "paused": True,
            "pause_reason": "low_disk",
            "free_db_bytes": free_db,
            "free_temp_bytes": free_tmp,
            "min_free_bytes": min_free_bytes,
            "resume_free_bytes": resume_free_bytes,
        })

    while True:
        time.sleep(max(1.0, poll_seconds))
        free_db = get_free_disk_bytes(db_path)
        free_tmp = get_free_disk_bytes(temp_dir)
        if min(free_db, free_tmp) >= resume_free_bytes:
            stats.paused_seconds += time.time() - paused_at
            if progress_cb:
                progress_cb({
                    "paused": False,
                    "pause_reason": "low_disk",
                    "free_db_bytes": free_db,
                    "free_temp_bytes": free_tmp,
                    "min_free_bytes": min_free_bytes,
                    "resume_free_bytes": resume_free_bytes,
                })
            return
        if progress_cb:
            progress_cb({
                "paused": True,
                "pause_reason": "low_disk",
                "free_db_bytes": free_db,
                "free_temp_bytes": free_tmp,
                "min_free_bytes": min_free_bytes,
                "resume_free_bytes": resume_free_bytes,
            })


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


def iter_systems_from_dump(path: str | Path) -> Iterator[dict[str, Any]]:
    with _open_maybe_compressed(path) as tracked:
        yield from ijson.items(tracked.fh, "item")


def _system_name(system: dict[str, Any]) -> str | None:
    return system.get("name") or system.get("systemName")


def _coords(system: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    coords = system.get("coords") or {}
    x = coords.get("x", system.get("x"))
    y = coords.get("y", system.get("y"))
    z = coords.get("z", system.get("z"))
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
        "body_type_counts_json": json.dumps(body_type_counts, ensure_ascii=False, sort_keys=True),
        "body_subtype_counts_json": json.dumps(body_subtype_counts, ensure_ascii=False, sort_keys=True),
        "ring_type_counts_json": json.dumps(ring_type_counts, ensure_ascii=False, sort_keys=True),
    }


def _row_from_system(system: dict[str, Any], source_tag: str) -> tuple[Any, ...] | None:
    name = _system_name(system)
    if not name:
        return None
    x, y, z = _coords(system)
    controlling_faction = _controlling_faction(system)
    summary = _summarize_bodies(system)
    return (
        name,
        x,
        y,
        z,
        _population(system),
        controlling_faction,
        _faction_count(system, controlling_faction),
        summary["total_bodies"],
        summary["total_planets"],
        summary["total_stars"],
        summary["ringed_body_count"],
        summary["water_world_count"],
        summary["earthlike_world_count"],
        summary["body_type_counts_json"],
        summary["body_subtype_counts_json"],
        summary["ring_type_counts_json"],
        datetime.now(timezone.utc).replace(tzinfo=None),
        source_tag,
    )


INSERT_SQL = """
INSERT INTO systems (
    name, x, y, z, population, controlling_faction, faction_count,
    total_bodies, total_planets, total_stars, ringed_body_count,
    water_world_count, earthlike_world_count,
    body_type_counts_json, body_subtype_counts_json, ring_type_counts_json,
    imported_at, source_tag
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_SQL = INSERT_SQL + """
ON CONFLICT(name) DO UPDATE SET
    x = excluded.x,
    y = excluded.y,
    z = excluded.z,
    population = excluded.population,
    controlling_faction = excluded.controlling_faction,
    faction_count = excluded.faction_count,
    total_bodies = excluded.total_bodies,
    total_planets = excluded.total_planets,
    total_stars = excluded.total_stars,
    ringed_body_count = excluded.ringed_body_count,
    water_world_count = excluded.water_world_count,
    earthlike_world_count = excluded.earthlike_world_count,
    body_type_counts_json = excluded.body_type_counts_json,
    body_subtype_counts_json = excluded.body_subtype_counts_json,
    ring_type_counts_json = excluded.ring_type_counts_json,
    imported_at = excluded.imported_at,
    source_tag = excluded.source_tag
"""


def _emit_progress(
    *,
    stats: ImportStats,
    db_path: str,
    temp_dir: str,
    progress_cb: ProgressCallback | None,
) -> None:
    if not progress_cb:
        return
    progress_cb({
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


def import_dump(
    db_path: str,
    dump_path: str,
    *,
    temp_dir: str | None = None,
    batch_size: int = 5000,
    mode: str = "full",
    cleanup_temp_before: bool = False,
    cleanup_temp_after: bool = False,
    cleanup_older_than_hours: float = 24.0,
    low_disk_mode: bool = False,
    min_free_gb: float = 20.0,
    resume_free_gb: float = 25.0,
    low_disk_poll_seconds: float = 10.0,
    progress_cb: ProgressCallback | None = None,
) -> ImportStats:
    mode = mode.lower().strip()
    if mode not in {"full", "patch"}:
        raise ValueError("mode muss 'full' oder 'patch' sein")

    init_db(db_path, temp_dir=temp_dir)
    con = connect_db(db_path, temp_dir=temp_dir)
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    stats = ImportStats(started_at=time.time(), dump_size_bytes=os.path.getsize(dump_path))
    source_tag = Path(dump_path).name

    if cleanup_temp_before:
        cleanup_stats = cleanup_temp_files(temp_dir, older_than_hours=cleanup_older_than_hours)
        stats.temp_files_deleted += cleanup_stats.files_deleted
        stats.temp_bytes_deleted += cleanup_stats.bytes_deleted

    try:
        drop_indexes(con)
        if mode == "full":
            con.execute("DELETE FROM systems")
            # Full dumps can still contain duplicate system names. We still use UPSERT below
            # so later occurrences safely replace earlier ones during the same import.

        batch: list[tuple[Any, ...]] = []
        with _open_maybe_compressed(dump_path) as tracked:
            for system in ijson.items(tracked.fh, "item"):
                if low_disk_mode:
                    _pause_for_low_disk(
                        db_path=db_path,
                        temp_dir=temp_dir,
                        min_free_gb=min_free_gb,
                        resume_free_gb=resume_free_gb,
                        poll_seconds=low_disk_poll_seconds,
                        stats=stats,
                        progress_cb=progress_cb,
                    )

                stats.bytes_read = tracked.bytes_read
                stats.systems_seen += 1
                row = _row_from_system(system, source_tag)
                if row is not None:
                    batch.append(row)

                if len(batch) >= batch_size:
                    con.executemany(UPSERT_SQL, batch)
                    stats.systems_written += len(batch)
                    batch.clear()
                    _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb)

            if batch:
                con.executemany(UPSERT_SQL, batch)
                stats.systems_written += len(batch)
                batch.clear()

        set_meta(con, "last_import_mode", mode)
        set_meta(con, "last_import_source", source_tag)
        set_meta(con, "last_import_finished_utc", datetime.now(timezone.utc).isoformat())
        create_indexes(con)
        con.execute("ANALYZE systems")
        con.checkpoint()

        if cleanup_temp_after:
            cleanup_stats = cleanup_temp_files(temp_dir, older_than_hours=0.0)
            stats.temp_files_deleted += cleanup_stats.files_deleted
            stats.temp_bytes_deleted += cleanup_stats.bytes_deleted

        stats.bytes_read = stats.dump_size_bytes
        stats.finished_at = time.time()
        _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb)
        return stats
    finally:
        con.close()
