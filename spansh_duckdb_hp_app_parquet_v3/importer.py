from __future__ import annotations

import gzip
import io
import json
import multiprocessing as mp
import mmap
import os
import queue
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import ijson
try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover
    orjson = None
try:
    import simdjson  # type: ignore
except Exception:  # pragma: no cover
    simdjson = None
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from .db import connect_db, create_indexes, drop_indexes, init_db, set_meta

ProgressCallback = Callable[[dict[str, Any]], None]

SYSTEM_COLUMNS = [
    "system_key", "system_address", "name", "x", "y", "z", "population", "controlling_faction", "faction_count",
    "total_bodies", "total_planets", "total_stars", "ringed_body_count",
    "water_world_count", "earthlike_world_count",
    "body_type_counts_json", "body_subtype_counts_json", "ring_type_counts_json",
    "imported_at", "source_tag",
]

PARQUET_COLUMNS = ["seq"] + SYSTEM_COLUMNS
PARQUET_SCHEMA = pa.schema([
    ("seq", pa.int64()),
    ("system_key", pa.string()),
    ("system_address", pa.int64()),
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

_JSON_DUMPS = json.dumps
_PROGRESS_EVERY_SECONDS = 0.75
_DEFAULT_ROW_GROUP_SIZE = 250_000
_DEFAULT_TARGET_ROWS_PER_FILE = 1_000_000


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
    read_seconds: float = 0.0
    transform_seconds: float = 0.0
    parquet_write_seconds: float = 0.0
    duckdb_seconds: float = 0.0
    parquet_file_count: int = 0

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
    def __init__(self, raw: Any):
        self.raw = raw
        self.bytes_read = 0

    def read(self, size: int = -1):
        data = self.raw.read(size)
        if data:
            self.bytes_read += len(data)
        return data

    def readinto(self, b):
        n = self.raw.readinto(b)
        if n and n > 0:
            self.bytes_read += n
        return n

    def readline(self, size: int = -1):
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
    def __init__(self, fh: Any, counter: CountingReader):
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


@dataclass(slots=True)
class ColumnBuffer:
    seq: list[int] = field(default_factory=list)
    system_key: list[str | None] = field(default_factory=list)
    system_address: list[int | None] = field(default_factory=list)
    name: list[str | None] = field(default_factory=list)
    x: list[float | None] = field(default_factory=list)
    y: list[float | None] = field(default_factory=list)
    z: list[float | None] = field(default_factory=list)
    population: list[int | None] = field(default_factory=list)
    controlling_faction: list[str | None] = field(default_factory=list)
    faction_count: list[int] = field(default_factory=list)
    total_bodies: list[int] = field(default_factory=list)
    total_planets: list[int] = field(default_factory=list)
    total_stars: list[int] = field(default_factory=list)
    ringed_body_count: list[int] = field(default_factory=list)
    water_world_count: list[int] = field(default_factory=list)
    earthlike_world_count: list[int] = field(default_factory=list)
    body_type_counts_json: list[str] = field(default_factory=list)
    body_subtype_counts_json: list[str] = field(default_factory=list)
    ring_type_counts_json: list[str] = field(default_factory=list)
    imported_at: list[datetime] = field(default_factory=list)
    source_tag: list[str] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.seq)

    def append(
        self,
        *,
        seq: int,
        system_key: str,
        system_address: int | None,
        name: str,
        x: float | None,
        y: float | None,
        z: float | None,
        population: int | None,
        controlling_faction: str | None,
        faction_count: int,
        total_bodies: int,
        total_planets: int,
        total_stars: int,
        ringed_body_count: int,
        water_world_count: int,
        earthlike_world_count: int,
        body_type_counts_json: str,
        body_subtype_counts_json: str,
        ring_type_counts_json: str,
        imported_at: datetime,
        source_tag: str,
    ) -> None:
        self.seq.append(seq)
        self.system_key.append(system_key)
        self.system_address.append(system_address)
        self.name.append(name)
        self.x.append(x)
        self.y.append(y)
        self.z.append(z)
        self.population.append(population)
        self.controlling_faction.append(controlling_faction)
        self.faction_count.append(faction_count)
        self.total_bodies.append(total_bodies)
        self.total_planets.append(total_planets)
        self.total_stars.append(total_stars)
        self.ringed_body_count.append(ringed_body_count)
        self.water_world_count.append(water_world_count)
        self.earthlike_world_count.append(earthlike_world_count)
        self.body_type_counts_json.append(body_type_counts_json)
        self.body_subtype_counts_json.append(body_subtype_counts_json)
        self.ring_type_counts_json.append(ring_type_counts_json)
        self.imported_at.append(imported_at)
        self.source_tag.append(source_tag)

    def to_table(self) -> pa.Table:
        return pa.Table.from_pydict({col: getattr(self, col) for col in PARQUET_COLUMNS}, schema=PARQUET_SCHEMA)


def get_free_disk_bytes(path: str | os.PathLike[str]) -> int:
    base = Path(path)
    if base.is_file():
        base = base.parent
    ensure = base if base.exists() else base.parent
    usage = shutil.disk_usage(str(ensure))
    return int(usage.free)


def cleanup_temp_files(temp_dir: str | os.PathLike[str], *, older_than_hours: float = 24.0) -> TempCleanupStats:
    temp_path = Path(temp_dir)
    if not temp_path.exists():
        return TempCleanupStats()
    cutoff = time.time() - max(0.0, older_than_hours) * 3600.0
    deleted = 0
    bytes_deleted = 0
    for entry in temp_path.iterdir():
        if not entry.is_file():
            continue
        if entry.suffix.lower() not in {".parquet", ".tmp", ".temp", ".duckdb", ".wal"}:
            continue
        try:
            stat = entry.stat()
            if stat.st_mtime > cutoff:
                continue
            size = stat.st_size
            entry.unlink(missing_ok=True)
            deleted += 1
            bytes_deleted += size
        except OSError:
            pass
    return TempCleanupStats(files_deleted=deleted, bytes_deleted=bytes_deleted)


def _pause_for_low_disk(*, db_path: str, temp_dir: str, min_free_gb: float, resume_free_gb: float,
                        poll_seconds: float, stats: ImportStats, progress_cb: ProgressCallback | None) -> None:
    threshold = int(min_free_gb * (1024 ** 3))
    resume_threshold = int(max(resume_free_gb, min_free_gb) * (1024 ** 3))
    free_db = get_free_disk_bytes(db_path)
    free_tmp = get_free_disk_bytes(temp_dir)
    if free_db >= threshold and free_tmp >= threshold:
        return

    start_pause = time.time()
    if progress_cb:
        progress_cb({
            "pause_reason": "low_disk",
            "paused": True,
            "free_db_bytes": free_db,
            "free_temp_bytes": free_tmp,
            "systems_seen": stats.systems_seen,
            "systems_written": stats.systems_written,
        })

    while True:
        time.sleep(max(1.0, poll_seconds))
        free_db = get_free_disk_bytes(db_path)
        free_tmp = get_free_disk_bytes(temp_dir)
        if free_db >= resume_threshold and free_tmp >= resume_threshold:
            break
    stats.paused_seconds += time.time() - start_pause
    if progress_cb:
        progress_cb({
            "pause_reason": "low_disk",
            "paused": False,
            "free_db_bytes": free_db,
            "free_temp_bytes": free_tmp,
            "systems_seen": stats.systems_seen,
            "systems_written": stats.systems_written,
        })


def _open_maybe_compressed(path: str) -> TrackedStream:
    lower = path.lower()
    if lower.endswith(".gz"):
        raw = open(path, "rb")
        gz = gzip.GzipFile(fileobj=raw, mode="rb")
        counter = CountingReader(gz)
        return TrackedStream(io.BufferedReader(counter), counter)
    if lower.endswith(".zst") or lower.endswith(".zstd"):
        raw = open(path, "rb")
        dctx = zstd.ZstdDecompressor()
        stream = dctx.stream_reader(raw)
        counter = CountingReader(stream)
        return TrackedStream(io.BufferedReader(counter), counter)
    raw = open(path, "rb")
    counter = CountingReader(raw)
    return TrackedStream(io.BufferedReader(counter), counter)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _system_name(system: dict[str, Any]) -> str | None:
    name = system.get("name") or system.get("system_name")
    if not name:
        return None
    return str(name)


def _system_address(system: dict[str, Any]) -> int | None:
    for key in ("id64", "systemAddress", "system_address"):
        value = system.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def _coords(system: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    coords = system.get("coords") or {}
    x = _to_float(coords.get("x", system.get("x")))
    y = _to_float(coords.get("y", system.get("y")))
    z = _to_float(coords.get("z", system.get("z")))
    return x, y, z


def _system_key(system: dict[str, Any], name: str, x: float | None, y: float | None, z: float | None) -> str:
    address = _system_address(system)
    if address is not None:
        return f"addr:{address}"
    if x is not None and y is not None and z is not None:
        return f"coords:{name.lower()}|{x:.8f}|{y:.8f}|{z:.8f}"
    return f"name:{name.lower()}"


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
        "body_type_counts_json": _JSON_DUMPS(body_type_counts, ensure_ascii=False, sort_keys=True),
        "body_subtype_counts_json": _JSON_DUMPS(body_subtype_counts, ensure_ascii=False, sort_keys=True),
        "ring_type_counts_json": _JSON_DUMPS(ring_type_counts, ensure_ascii=False, sort_keys=True),
    }


def _json_loads_fast(data: bytes, parser_mode: str = "auto") -> tuple[dict[str, Any], str]:
    errors: list[str] = []

    if parser_mode in {"auto", "simdjson"}:
        if simdjson is not None:
            try:
                parser = simdjson.Parser()
                parsed = parser.parse(data)
                if hasattr(parsed, "as_dict"):
                    return parsed.as_dict(), "simdjson"
                return dict(parsed), "simdjson"
            except Exception as exc:
                errors.append(f"simdjson: {exc}")
                if parser_mode == "simdjson":
                    raise
        elif parser_mode == "simdjson":
            raise RuntimeError("simdjson ist nicht verfügbar")

    if parser_mode in {"auto", "orjson"}:
        if orjson is not None:
            try:
                return orjson.loads(data), "orjson"
            except Exception as exc:
                errors.append(f"orjson: {exc}")
                if parser_mode == "orjson":
                    raise
        elif parser_mode == "orjson":
            raise RuntimeError("orjson ist nicht verfügbar")

    if parser_mode in {"auto", "json"}:
        try:
            return json.loads(data), "json"
        except Exception as exc:
            errors.append(f"json: {exc}")

    raise RuntimeError("Kein JSON-Parser erfolgreich: " + " | ".join(errors))


def _find_array_bounds(mm: mmap.mmap) -> tuple[int, int]:
    size = len(mm)
    start = 0
    while start < size and chr(mm[start]).isspace():
        start += 1
    if start >= size or mm[start] != ord("["):
        raise ValueError("Ultra mmap Mode erwartet ein JSON-Array auf Top-Level.")
    end = size - 1
    while end >= 0 and chr(mm[end]).isspace():
        end -= 1
    if end < 0 or mm[end] != ord("]"):
        raise ValueError("Ultra mmap Mode erwartet ein JSON-Array mit abschließender ']'.")
    return start + 1, end


def _skip_ws_and_commas(mm: mmap.mmap, pos: int, end: int) -> int:
    while pos < end and mm[pos] in b" \t\r\n,":
        pos += 1
    return pos

def _find_next_object_start(mm: mmap.mmap, pos: int, end: int) -> int:
    pos = _skip_ws_and_commas(mm, pos, end)
    while pos < end and mm[pos] != ord("{"):
        pos += 1
    return pos


def _find_object_end(mm: mmap.mmap, start: int, end: int) -> int:
    depth = 0
    in_string = False
    escape = False
    pos = start
    while pos < end:
        ch = mm[pos]
        if in_string:
            if escape:
                escape = False
            elif ch == 0x5C:
                escape = True
            elif ch == 0x22:
                in_string = False
        else:
            if ch == 0x22:
                in_string = True
            elif ch == 0x7B:
                depth += 1
            elif ch == 0x7D:
                depth -= 1
                if depth == 0:
                    return pos + 1
        pos += 1
    raise ValueError("Ungültiges JSON: Objekt nicht abgeschlossen.")


def _build_ultra_ranges(path: str, workers: int, progress_cb: ProgressCallback | None = None) -> tuple[list[tuple[int, int]], dict[str, Any]]:
    """Baue Worker-Bereiche nur auf echten Objektgrenzen auf und liefere detaillierten Prep-Fortschritt."""
    file_size = os.path.getsize(path)
    file_ext = Path(path).suffix.lower()
    prep_started = time.time()

    def emit(phase: str, message: str, **extra: Any) -> None:
        if not progress_cb:
            return
        done = int(extra.get("prep_bytes_done", 0))
        total = int(extra.get("prep_bytes_total", file_size or 1))
        elapsed = max(0.001, time.time() - prep_started)
        speed_bps = done / elapsed if done > 0 else 0.0
        eta = ((total - done) / speed_bps) if speed_bps > 0 and done < total else 0.0
        payload = {
            "phase": "preparing",
            "message": message,
            "prep_phase": phase,
            "prep_bytes_done": done,
            "prep_bytes_total": total,
            "prep_percent": (done / total * 100.0) if total else 0.0,
            "prep_speed_mb_s": speed_bps / (1024 * 1024),
            "prep_eta_seconds": eta,
            "prep_elapsed_s": elapsed,
            "file_size_bytes": file_size,
            "file_type": file_ext or ".json",
            "mmap_possible": file_ext == ".json",
            "mmap_active": True,
            **extra,
        }
        progress_cb(payload)

    emit("file_check", "Datei wird geprüft", prep_bytes_done=0, prep_bytes_total=file_size, json_array_detected=False, prep_found_object_boundaries=0, prep_worker_segments_built=0, worker_ranges=[])

    object_starts: list[int] = []
    object_ends: list[int] = []
    ranges: list[tuple[int, int]] = []
    meta: dict[str, Any] = {
        "file_size_bytes": file_size,
        "file_type": file_ext or ".json",
        "mmap_possible": file_ext == ".json",
        "mmap_active": True,
        "json_array_detected": False,
        "array_start_offset": None,
        "array_end_offset": None,
        "prep_found_object_boundaries": 0,
        "prep_worker_segments_built": 0,
        "worker_ranges": [],
    }

    with open(path, "rb") as fh:
        with mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            emit("json_container_check", "JSON-Container wird geprüft", prep_bytes_done=0, prep_bytes_total=file_size, json_array_detected=False, prep_found_object_boundaries=0, prep_worker_segments_built=0, worker_ranges=[])
            arr_start, arr_end = _find_array_bounds(mm)
            meta["json_array_detected"] = True
            meta["array_start_offset"] = arr_start
            meta["array_end_offset"] = arr_end
            emit("json_container_check", "JSON-Array erkannt", prep_bytes_done=arr_start, prep_bytes_total=file_size, json_array_detected=True, array_start_offset=arr_start, array_end_offset=arr_end, prep_found_object_boundaries=0, prep_worker_segments_built=0, worker_ranges=[])

            last_emit = 0.0
            pos = _find_next_object_start(mm, arr_start, arr_end)
            while pos < arr_end:
                obj_end = _find_object_end(mm, pos, arr_end)
                object_starts.append(pos)
                object_ends.append(obj_end)
                now = time.time()
                if now - last_emit >= 0.25:
                    emit(
                        "scan_object_boundaries",
                        f"Objektgrenzen werden gescannt ({len(object_starts):,} erkannt)",
                        prep_bytes_done=min(file_size, obj_end),
                        prep_bytes_total=file_size,
                        json_array_detected=True,
                        array_start_offset=arr_start,
                        array_end_offset=arr_end,
                        prep_found_object_boundaries=len(object_starts),
                        prep_worker_segments_built=0,
                        current_scan_offset=obj_end,
                        worker_ranges=[],
                    )
                    last_emit = now
                pos = _find_next_object_start(mm, obj_end, arr_end)

    if not object_starts:
        return [], meta

    worker_count = max(1, min(workers, len(object_starts)))
    chunk_size = (len(object_starts) + worker_count - 1) // worker_count

    for i in range(worker_count):
        start_idx = i * chunk_size
        if start_idx >= len(object_starts):
            break
        end_idx = min(len(object_starts), start_idx + chunk_size) - 1
        start_pos = object_starts[start_idx]
        if end_idx + 1 < len(object_starts):
            end_pos = object_starts[end_idx + 1]
        else:
            end_pos = object_ends[end_idx]
        ranges.append((start_pos, end_pos))
        emit(
            "build_worker_segments",
            f"Worker-Segmente werden berechnet ({i + 1}/{worker_count})",
            prep_bytes_done=file_size,
            prep_bytes_total=file_size,
            json_array_detected=True,
            array_start_offset=meta["array_start_offset"],
            array_end_offset=meta["array_end_offset"],
            prep_found_object_boundaries=len(object_starts),
            prep_worker_segments_built=len(ranges),
            current_scan_offset=meta["array_end_offset"],
            worker_ranges=[
                {"worker_id": idx, "start_offset": s, "end_offset": e}
                for idx, (s, e) in enumerate(ranges)
            ],
        )

    meta.update({
        "prep_found_object_boundaries": len(object_starts),
        "prep_worker_segments_built": len(ranges),
        "worker_ranges": [
            {"worker_id": idx, "start_offset": s, "end_offset": e}
            for idx, (s, e) in enumerate(ranges)
        ],
    })
    emit("import_start", "Vorbereitung abgeschlossen, Import startet", prep_bytes_done=file_size, prep_bytes_total=file_size, **meta)
    return ranges, meta


def _ultra_worker_mmap(
    path: str,
    start: int,
    end: int,
    parquet_path: str,
    source_tag: str,
    compression: str,
    row_group_size: int,
    target_rows_per_file: int,
    worker_id: int,
    parser_mode: str,
    result_queue: Any,
) -> None:
    stats = {"systems_seen": 0, "systems_written": 0, "bytes_read": 0, "transform_seconds": 0.0, "parquet_write_seconds": 0.0, "parser_requested": parser_mode, "parser_active": None}
    last_report = time.time()
    imported_at = datetime.now(timezone.utc)
    writer = ParquetPartWriter(parquet_path, compression=compression, row_group_size=row_group_size, target_rows_per_file=target_rows_per_file, worker_label=f"w{worker_id:02d}")
    try:
        with open(path, "rb") as fh:
            with mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                pos = _find_next_object_start(mm, start, end)
                buffer = ColumnBuffer()
                while pos < end:
                    pos = _skip_ws_and_commas(mm, pos, end)
                    if pos >= end or mm[pos] != ord("{"):
                        break
                    obj_end = _find_object_end(mm, pos, end)
                    payload = bytes(mm[pos:obj_end]).strip()
                    if not payload:
                        break
                    t0 = time.time()
                    system, active_parser = _json_loads_fast(payload, parser_mode=parser_mode)
                    stats["parser_active"] = active_parser
                    stats["transform_seconds"] += max(0.0, time.time() - t0)
                    stats["systems_seen"] += 1
                    stats["bytes_read"] = max(stats["bytes_read"], obj_end - start)
                    if _append_system_to_buffer(buffer, system, source_tag, stats["systems_seen"], imported_at):
                        if len(buffer) >= row_group_size:
                            t1 = time.time()
                            writer.write_table(buffer.to_table())
                            stats["parquet_write_seconds"] += max(0.0, time.time() - t1)
                            stats["systems_written"] += len(buffer)
                            buffer = ColumnBuffer()
                    pos = _skip_ws_and_commas(mm, obj_end, end)
                    if time.time() - last_report >= _PROGRESS_EVERY_SECONDS:
                        result_queue.put({"type": "ultra_progress", "worker_id": worker_id, **stats})
                        last_report = time.time()
                if len(buffer):
                    t1 = time.time()
                    writer.write_table(buffer.to_table())
                    stats["parquet_write_seconds"] += max(0.0, time.time() - t1)
                    stats["systems_written"] += len(buffer)
        result_queue.put({
            "type": "worker_done",
            "worker_id": worker_id,
            "parquet_files": writer.files,
            "parquet_bytes": writer.bytes_written,
            **stats,
        })
    except Exception as exc:
        result_queue.put({"type": "error", "worker_id": worker_id, "message": str(exc)})
    finally:
        writer.close()


def run_ultra(
    db_path: str,
    dump_path: str,
    *,
    temp_dir: str | None = None,
    mode: str = "full",
    progress_cb: ProgressCallback | None = None,
    threads: int | None = None,
    memory_limit: str = "32GB",
    ultra_workers: int = 4,
    row_group_size: int = _DEFAULT_ROW_GROUP_SIZE,
    target_rows_per_file: int = _DEFAULT_TARGET_ROWS_PER_FILE,
    keep_parquet: bool = False,
    parser_mode: str = "auto",
) -> ImportStats:
    path = Path(dump_path)
    if path.suffix.lower() != ".json":
        raise ValueError("Ultra mmap Mode unterstützt nur unkomprimierte .json-Dumps. Bitte .gz vorher entpacken.")
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    ranges, prep_meta = _build_ultra_ranges(str(path), max(1, ultra_workers), progress_cb=progress_cb)
    if not ranges:
        raise ValueError("Kein JSON-Objekt im Dump gefunden.")
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    temp_base = str(Path(temp_dir) / f"{path.stem}.ultra.{int(time.time())}.parquet")
    source_tag = path.name
    stats = ImportStats(started_at=time.time(), dump_size_bytes=path.stat().st_size)
    procs = []
    for worker_id, (start, end) in enumerate(ranges):
        p = ctx.Process(
            target=_ultra_worker_mmap,
            args=(str(path), start, end, temp_base, source_tag, "zstd", row_group_size, target_rows_per_file, worker_id, parser_mode, result_queue),
            daemon=True,
        )
        p.start()
        procs.append(p)
    done = 0
    parquet_files: list[str] = []
    parquet_bytes = 0
    progress_by_worker = {i: {"worker_id": i, "systems_seen": 0, "systems_written": 0, "bytes_read": 0, "transform_seconds": 0.0, "parquet_write_seconds": 0.0, "parser_requested": parser_mode, "parser_active": None} for i in range(len(procs))}
    try:
        while done < len(procs):
            msg = result_queue.get()
            if not isinstance(msg, dict):
                continue
            mtype = msg.get("type")
            if mtype == "error":
                raise RuntimeError(str(msg.get("message") or "Ultra mmap worker failed"))
            worker_id = int(msg.get("worker_id", 0))
            if mtype in {"ultra_progress", "worker_done"}:
                progress_by_worker[worker_id].update({
                    "systems_seen": int(msg.get("systems_seen", 0)),
                    "systems_written": int(msg.get("systems_written", 0)),
                    "bytes_read": int(msg.get("bytes_read", 0)),
                    "transform_seconds": float(msg.get("transform_seconds", 0.0)),
                    "parquet_write_seconds": float(msg.get("parquet_write_seconds", 0.0)),
                    "parser_requested": str(msg.get("parser_requested", parser_mode)),
                    "parser_active": msg.get("parser_active"),
                })
                stats.systems_seen = sum(v["systems_seen"] for v in progress_by_worker.values())
                stats.systems_written = sum(v["systems_written"] for v in progress_by_worker.values())
                stats.bytes_read = min(stats.dump_size_bytes, sum(v["bytes_read"] for v in progress_by_worker.values()))
                stats.transform_seconds = sum(v["transform_seconds"] for v in progress_by_worker.values())
                stats.parquet_write_seconds = sum(v["parquet_write_seconds"] for v in progress_by_worker.values())
                if progress_cb:
                    _emit_progress(
                        stats=stats,
                        db_path=db_path,
                        temp_dir=temp_dir,
                        progress_cb=progress_cb,
                        phase="ultra_parquet",
                        message=f"Ultra mmap: {len(procs)} Worker aktiv",
                        extra={
                            "parallel_writers": len(procs),
                            "ultra_mmap_mode": True,
                            "ultra_workers": len(procs),
                            "worker_progress": [progress_by_worker[i] for i in sorted(progress_by_worker)],
                            "parser_requested": parser_mode,
                            "parser_active": next((w.get("parser_active") for w in progress_by_worker.values() if w.get("parser_active")), None),
                            **prep_meta,
                        },
                    )
            if mtype == "worker_done":
                done += 1
                parquet_files.extend(str(p) for p in msg.get("parquet_files", []))
                parquet_bytes += int(msg.get("parquet_bytes", 0))
        stats.finished_at = time.time()
        stats.parquet_file_count = len(sorted(set(parquet_files)))
        if progress_cb:
            _emit_progress(
                stats=stats,
                db_path=db_path,
                temp_dir=temp_dir,
                progress_cb=progress_cb,
                phase="ultra_parquet_done",
                message="Ultra mmap: Parquet erstellt",
                extra={
                    "parquet_files": sorted(set(parquet_files)),
                    "parquet_bytes": parquet_bytes,
                    "ultra_mmap_mode": True,
                    "ultra_workers": len(procs),
                    "worker_progress": [progress_by_worker[i] for i in sorted(progress_by_worker)],
                    "parser_requested": parser_mode,
                    "parser_active": next((w.get("parser_active") for w in progress_by_worker.values() if w.get("parser_active")), None),
                    **prep_meta,
                },
            )
        db_stats = import_parquet(db_path, str(Path(temp_base).with_name(f"{Path(temp_base).stem}*.parquet")), temp_dir=temp_dir, mode=mode, progress_cb=progress_cb, threads=threads, memory_limit=memory_limit)
        db_stats.systems_seen = stats.systems_seen
        db_stats.bytes_read = stats.dump_size_bytes
        db_stats.dump_size_bytes = stats.dump_size_bytes
        db_stats.transform_seconds = stats.transform_seconds
        db_stats.parquet_write_seconds = stats.parquet_write_seconds
        db_stats.parquet_file_count = stats.parquet_file_count
        if not keep_parquet:
            for parquet_file in sorted(Path(temp_dir).glob(f"{Path(temp_base).stem}*.parquet")):
                try:
                    size = parquet_file.stat().st_size
                    parquet_file.unlink(missing_ok=True)
                    db_stats.temp_files_deleted += 1
                    db_stats.temp_bytes_deleted += size
                except OSError:
                    pass
        return db_stats
    finally:
        for proc in procs:
            proc.join(timeout=1.0)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2.0)


def _append_system_to_buffer(buffer: ColumnBuffer, system: dict[str, Any], source_tag: str, seq: int, imported_at: datetime) -> bool:
    name = _system_name(system)
    if not name:
        return False
    x, y, z = _coords(system)
    controlling_faction = _controlling_faction(system)
    summary = _summarize_bodies(system)
    buffer.append(
        seq=seq,
        system_key=_system_key(system, name, x, y, z),
        system_address=_system_address(system),
        name=name,
        x=x,
        y=y,
        z=z,
        population=_population(system),
        controlling_faction=controlling_faction,
        faction_count=_faction_count(system, controlling_faction),
        total_bodies=summary["total_bodies"],
        total_planets=summary["total_planets"],
        total_stars=summary["total_stars"],
        ringed_body_count=summary["ringed_body_count"],
        water_world_count=summary["water_world_count"],
        earthlike_world_count=summary["earthlike_world_count"],
        body_type_counts_json=summary["body_type_counts_json"],
        body_subtype_counts_json=summary["body_subtype_counts_json"],
        ring_type_counts_json=summary["ring_type_counts_json"],
        imported_at=imported_at,
        source_tag=source_tag,
    )
    return True


def _emit_progress(*, stats: ImportStats, db_path: str, temp_dir: str, progress_cb: ProgressCallback | None,
                   phase: str = "reading", message: str | None = None, extra: dict[str, Any] | None = None) -> None:
    if not progress_cb:
        return
    payload = {
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
        "read_seconds": stats.read_seconds,
        "transform_seconds": stats.transform_seconds,
        "parquet_write_seconds": stats.parquet_write_seconds,
        "duckdb_seconds": stats.duckdb_seconds,
        "parquet_file_count": stats.parquet_file_count,
    }
    if extra:
        payload.update(extra)
    progress_cb(payload)


class ParquetPartWriter:
    def __init__(
        self,
        base_path: str | os.PathLike[str],
        *,
        compression: str = "zstd",
        row_group_size: int = _DEFAULT_ROW_GROUP_SIZE,
        target_rows_per_file: int = _DEFAULT_TARGET_ROWS_PER_FILE,
        worker_label: str | None = None,
    ) -> None:
        self.base_path = Path(base_path)
        self.base_path.parent.mkdir(parents=True, exist_ok=True)
        self.compression = compression
        self.row_group_size = max(1, int(row_group_size))
        self.target_rows_per_file = max(self.row_group_size, int(target_rows_per_file))
        self.worker_label = worker_label
        self.part_index = 0
        self.writer: pq.ParquetWriter | None = None
        self.current_rows = 0
        self.files: list[str] = []
        self.bytes_written = 0

    def _part_path(self) -> Path:
        stem = self.base_path.stem
        if self.worker_label:
            name = f"{stem}.{self.worker_label}.part-{self.part_index:06d}.parquet"
        else:
            name = f"{stem}.part-{self.part_index:06d}.parquet"
        return self.base_path.with_name(name)

    def _open_next(self) -> None:
        self.close()
        self.part_index += 1
        path = self._part_path()
        self.writer = pq.ParquetWriter(str(path), PARQUET_SCHEMA, compression=self.compression)
        self.current_rows = 0
        self.files.append(str(path))

    def write_table(self, table: pa.Table) -> None:
        offset = 0
        total_rows = table.num_rows
        while offset < total_rows:
            if self.writer is None or self.current_rows >= self.target_rows_per_file:
                self._open_next()
            remaining_in_file = self.target_rows_per_file - self.current_rows
            chunk_rows = min(total_rows - offset, remaining_in_file)
            chunk = table.slice(offset, chunk_rows)
            self.writer.write_table(chunk, row_group_size=self.row_group_size)
            self.current_rows += chunk_rows
            offset += chunk_rows

    def close(self) -> None:
        if self.writer is not None:
            path = self.files[-1]
            self.writer.close()
            self.writer = None
            try:
                self.bytes_written += Path(path).stat().st_size
            except OSError:
                pass


# --- multiprocessing helpers ---

def _worker_transform_write(task_queue: Any, result_queue: Any, base_parquet_path: str, source_tag: str,
                            compression: str, row_group_size: int, target_rows_per_file: int, worker_idx: int) -> None:
    writer = ParquetPartWriter(
        base_parquet_path,
        compression=compression,
        row_group_size=row_group_size,
        target_rows_per_file=target_rows_per_file,
        worker_label=f"w{worker_idx:02d}",
    )
    imported_at = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        while True:
            payload = task_queue.get()
            if payload is None:
                break
            batch: list[tuple[int, dict[str, Any]]] = payload
            t0 = time.time()
            buffer = ColumnBuffer()
            seen = len(batch)
            for seq, system in batch:
                _append_system_to_buffer(buffer, system, source_tag, seq, imported_at)
            t1 = time.time()
            written = len(buffer)
            if written:
                table = buffer.to_table()
                writer.write_table(table)
            t2 = time.time()
            result_queue.put({
                "type": "batch",
                "systems_seen": seen,
                "systems_written": written,
                "transform_seconds": t1 - t0,
                "parquet_write_seconds": t2 - t1,
            })
    except Exception as exc:
        result_queue.put({"type": "error", "message": str(exc)})
        raise
    finally:
        writer.close()
        result_queue.put({
            "type": "worker_done",
            "parquet_files": writer.files,
            "parquet_bytes": writer.bytes_written,
        })


def _drain_worker_results(result_queue: Any, stats: ImportStats) -> tuple[list[str], int, str | None]:
    files: list[str] = []
    bytes_written = 0
    error: str | None = None
    while True:
        try:
            msg = result_queue.get_nowait()
        except queue.Empty:
            break
        if not isinstance(msg, dict):
            continue
        if msg.get("type") == "batch":
            stats.systems_written += int(msg.get("systems_written", 0))
            stats.transform_seconds += float(msg.get("transform_seconds", 0.0))
            stats.parquet_write_seconds += float(msg.get("parquet_write_seconds", 0.0))
        elif msg.get("type") == "worker_done":
            files.extend([str(p) for p in msg.get("parquet_files", [])])
            bytes_written += int(msg.get("parquet_bytes", 0))
        elif msg.get("type") == "error" and error is None:
            error = str(msg.get("message") or "Worker process failed")
    return files, bytes_written, error


def convert_dump_to_parquet(
    dump_path: str,
    parquet_path: str,
    *,
    row_group_size: int = 50_000,
    compression: str = "zstd",
    progress_cb: ProgressCallback | None = None,
    db_path_for_progress: str | None = None,
    temp_dir_for_progress: str | None = None,
    low_disk_mode: bool = False,
    min_free_gb: float = 20.0,
    resume_free_gb: float = 25.0,
    low_disk_poll_seconds: float = 10.0,
    max_systems: int | None = None,
    target_rows_per_file: int = _DEFAULT_TARGET_ROWS_PER_FILE,
    parallel_writers: int = 1,
) -> ImportStats:
    parquet_path = str(parquet_path)
    Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
    stats = ImportStats(started_at=time.time(), dump_size_bytes=os.path.getsize(dump_path))
    source_tag = Path(dump_path).name
    db_prog = db_path_for_progress or parquet_path
    tmp_prog = temp_dir_for_progress or str(Path(parquet_path).parent)
    row_group_size = max(1_000, int(row_group_size))
    target_rows_per_file = max(row_group_size, int(target_rows_per_file))
    parallel_writers = max(1, int(parallel_writers))

    output_files: list[str] = []
    output_bytes = 0
    last_emit = 0.0

    if parallel_writers <= 1:
        imported_at = datetime.now(timezone.utc).replace(tzinfo=None)
        writer = ParquetPartWriter(
            parquet_path,
            compression=compression,
            row_group_size=row_group_size,
            target_rows_per_file=target_rows_per_file,
            worker_label=None,
        )
        buffer = ColumnBuffer()
        try:
            with _open_maybe_compressed(dump_path) as tracked:
                t_read = time.time()
                for system in ijson.items(tracked.fh, "item"):
                    if low_disk_mode:
                        _pause_for_low_disk(
                            db_path=db_prog,
                            temp_dir=tmp_prog,
                            min_free_gb=min_free_gb,
                            resume_free_gb=resume_free_gb,
                            poll_seconds=low_disk_poll_seconds,
                            stats=stats,
                            progress_cb=progress_cb,
                        )
                    stats.read_seconds += max(0.0, time.time() - t_read)
                    stats.bytes_read = tracked.bytes_read
                    stats.systems_seen += 1
                    t0 = time.time()
                    _append_system_to_buffer(buffer, system, source_tag, stats.systems_seen, imported_at)
                    stats.transform_seconds += time.time() - t0
                    if len(buffer) >= row_group_size:
                        t1 = time.time()
                        writer.write_table(buffer.to_table())
                        stats.parquet_write_seconds += time.time() - t1
                        stats.systems_written += len(buffer)
                        buffer = ColumnBuffer()
                    now = time.time()
                    if now - last_emit >= _PROGRESS_EVERY_SECONDS:
                        last_emit = now
                        _emit_progress(
                            stats=stats,
                            db_path=db_prog,
                            temp_dir=tmp_prog,
                            progress_cb=progress_cb,
                            phase="parquet",
                            message="Parquet wird geschrieben",
                        )
                    t_read = time.time()
                    if max_systems is not None and stats.systems_seen >= max_systems:
                        break
                if len(buffer):
                    t1 = time.time()
                    writer.write_table(buffer.to_table())
                    stats.parquet_write_seconds += time.time() - t1
                    stats.systems_written += len(buffer)
        finally:
            writer.close()
            output_files = writer.files
            output_bytes = writer.bytes_written
    else:
        ctx = mp.get_context("spawn")
        task_queue = ctx.Queue(maxsize=max(2, parallel_writers * 2))
        result_queue = ctx.Queue()
        workers = [
            ctx.Process(
                target=_worker_transform_write,
                args=(task_queue, result_queue, parquet_path, source_tag, compression, row_group_size, target_rows_per_file, i),
                daemon=True,
            )
            for i in range(parallel_writers)
        ]
        for proc in workers:
            proc.start()
        worker_done_count = 0
        try:
            with _open_maybe_compressed(dump_path) as tracked:
                batch: list[tuple[int, dict[str, Any]]] = []
                t_read = time.time()
                for system in ijson.items(tracked.fh, "item"):
                    if low_disk_mode:
                        _pause_for_low_disk(
                            db_path=db_prog,
                            temp_dir=tmp_prog,
                            min_free_gb=min_free_gb,
                            resume_free_gb=resume_free_gb,
                            poll_seconds=low_disk_poll_seconds,
                            stats=stats,
                            progress_cb=progress_cb,
                        )
                    stats.read_seconds += max(0.0, time.time() - t_read)
                    stats.bytes_read = tracked.bytes_read
                    stats.systems_seen += 1
                    batch.append((stats.systems_seen, system))
                    if len(batch) >= row_group_size:
                        task_queue.put(batch)
                        batch = []
                    files, bytes_written, error = _drain_worker_results(result_queue, stats)
                    if files:
                        output_files.extend(files)
                    output_bytes += bytes_written
                    if error:
                        raise RuntimeError(error)
                    now = time.time()
                    if now - last_emit >= _PROGRESS_EVERY_SECONDS:
                        last_emit = now
                        _emit_progress(
                            stats=stats,
                            db_path=db_prog,
                            temp_dir=tmp_prog,
                            progress_cb=progress_cb,
                            phase="parquet",
                            message=f"Parquet wird parallel geschrieben ({parallel_writers} Worker)",
                            extra={"parallel_writers": parallel_writers},
                        )
                    t_read = time.time()
                    if max_systems is not None and stats.systems_seen >= max_systems:
                        break
                if batch:
                    task_queue.put(batch)
            for _ in workers:
                task_queue.put(None)
            while worker_done_count < len(workers):
                try:
                    msg = result_queue.get(timeout=0.2)
                except queue.Empty:
                    files, bytes_written, error = _drain_worker_results(result_queue, stats)
                    output_files.extend(files)
                    output_bytes += bytes_written
                    if error:
                        raise RuntimeError(error)
                    continue
                if isinstance(msg, dict) and msg.get("type") == "worker_done":
                    worker_done_count += 1
                    output_files.extend([str(p) for p in msg.get("parquet_files", [])])
                    output_bytes += int(msg.get("parquet_bytes", 0))
                elif isinstance(msg, dict) and msg.get("type") == "batch":
                    stats.systems_written += int(msg.get("systems_written", 0))
                    stats.transform_seconds += float(msg.get("transform_seconds", 0.0))
                    stats.parquet_write_seconds += float(msg.get("parquet_write_seconds", 0.0))
                elif isinstance(msg, dict) and msg.get("type") == "error":
                    raise RuntimeError(str(msg.get("message") or "Worker process failed"))
                if time.time() - last_emit >= _PROGRESS_EVERY_SECONDS:
                    last_emit = time.time()
                    _emit_progress(
                        stats=stats,
                        db_path=db_prog,
                        temp_dir=tmp_prog,
                        progress_cb=progress_cb,
                        phase="parquet",
                        message=f"Parquet wird parallel geschrieben ({parallel_writers} Worker)",
                        extra={"parallel_writers": parallel_writers},
                    )
        finally:
            for proc in workers:
                proc.join(timeout=1.0)
                if proc.is_alive():
                    proc.terminate()
                    proc.join(timeout=2.0)
            files, bytes_written, error = _drain_worker_results(result_queue, stats)
            output_files.extend(files)
            output_bytes += bytes_written
            if error:
                raise RuntimeError(error)

    stats.finished_at = time.time()
    stats.parquet_file_count = len(sorted(set(output_files)))
    _emit_progress(
        stats=stats,
        db_path=db_prog,
        temp_dir=tmp_prog,
        progress_cb=progress_cb,
        phase="parquet_done",
        message="Parquet erstellt",
        extra={
            "parquet_files": sorted(set(output_files)),
            "parquet_glob": str(Path(parquet_path).with_name(f"{Path(parquet_path).stem}*.parquet")),
            "parallel_writers": parallel_writers,
            "parquet_bytes": output_bytes,
        },
    )
    return stats


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _parquet_source_expr(parquet_path: str | list[str]) -> str:
    if isinstance(parquet_path, str):
        return f"read_parquet('{_escape_sql_string(parquet_path)}')"
    paths = ", ".join(f"'{_escape_sql_string(str(p))}'" for p in parquet_path)
    return f"read_parquet([{paths}])"


def _resolve_parquet_inputs(parquet_path: str) -> str | list[str]:
    p = Path(parquet_path)
    if any(ch in parquet_path for ch in "*?["):
        return parquet_path
    if p.is_dir():
        files = sorted(str(x) for x in p.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"Keine Parquet-Dateien in {parquet_path}")
        return files
    if p.exists():
        return parquet_path
    sibling_files = sorted(str(x) for x in p.parent.glob(f"{p.stem}*.parquet"))
    if sibling_files:
        return sibling_files
    raise FileNotFoundError(parquet_path)


def import_parquet(db_path: str, parquet_path: str, *, temp_dir: str | None = None, mode: str = "full",
                   progress_cb: ProgressCallback | None = None, threads: int | None = None,
                   memory_limit: str = "8GB") -> ImportStats:
    mode = mode.lower().strip()
    if mode not in {"full", "patch"}:
        raise ValueError("mode muss 'full' oder 'patch' sein")

    parquet_input = _resolve_parquet_inputs(parquet_path)
    if isinstance(parquet_input, str):
        dump_size = os.path.getsize(parquet_input) if Path(parquet_input).exists() else 0
        source_tag = Path(parquet_input).name
    else:
        dump_size = sum(Path(p).stat().st_size for p in parquet_input if Path(p).exists())
        source_tag = f"{len(parquet_input)} parquet files"

    init_db(db_path, temp_dir=temp_dir, threads=threads, memory_limit=memory_limit)
    con = connect_db(db_path, temp_dir=temp_dir, threads=threads, memory_limit=memory_limit)
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    stats = ImportStats(started_at=time.time(), dump_size_bytes=dump_size, bytes_read=dump_size)
    stats.parquet_file_count = len(parquet_input) if isinstance(parquet_input, list) else (0 if any(ch in parquet_input for ch in "*?[") else 1)
    source_expr = _parquet_source_expr(parquet_input)
    insert_cols = ", ".join(SYSTEM_COLUMNS)
    select_cols = ", ".join(SYSTEM_COLUMNS)
    dedup_source = f"""
        SELECT {select_cols}
        FROM (
            SELECT *, row_number() OVER (PARTITION BY system_key ORDER BY seq DESC) AS rn
            FROM {source_expr}
        ) q
        WHERE rn = 1
    """
    try:
        t0 = time.time()
        drop_indexes(con)
        counts = con.execute(
            f"SELECT COUNT(*) AS rows, COUNT(DISTINCT system_key) AS distinct_keys FROM {source_expr}"
        ).fetchone()
        stats.duckdb_seconds += time.time() - t0
        stats.systems_seen = int(counts[0])
        distinct_keys = int(counts[1])
        _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb,
                       phase="duckdb", message="DuckDB importiert Parquet",
                       extra={"parquet_file_count": stats.parquet_file_count, "parquet_source": source_tag})

        t1 = time.time()
        if mode == "full":
            con.execute(f"CREATE OR REPLACE TABLE systems AS {dedup_source}")
            stats.systems_written = distinct_keys
        else:
            update_assignments = ", ".join([f"{col} = s.{col}" for col in SYSTEM_COLUMNS if col != "system_key"])
            insert_vals = ", ".join([f"s.{col}" for col in SYSTEM_COLUMNS])
            merge_sql = f"""
                MERGE INTO systems AS t
                USING ({dedup_source}) AS s
                ON t.system_key = s.system_key
                WHEN MATCHED THEN UPDATE SET
                    {update_assignments}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            con.execute(merge_sql)
            stats.systems_written = distinct_keys
        stats.duckdb_seconds += time.time() - t1

        t2 = time.time()
        set_meta(con, "last_import_mode", mode)
        set_meta(con, "last_import_source", source_tag)
        set_meta(con, "last_import_finished_utc", datetime.utcnow().isoformat(timespec="seconds"))
        create_indexes(con)
        con.execute("ANALYZE systems")
        con.execute("CHECKPOINT")
        stats.duckdb_seconds += time.time() - t2
        stats.finished_at = time.time()
        _emit_progress(stats=stats, db_path=db_path, temp_dir=temp_dir, progress_cb=progress_cb,
                       phase="done", message="DuckDB-Import abgeschlossen",
                       extra={"parquet_file_count": stats.parquet_file_count, "parquet_source": source_tag})
        return stats
    finally:
        con.close()


def import_dump(db_path: str, dump_path: str, *, temp_dir: str | None = None, batch_size: int = 50_000,
                mode: str = "full", cleanup_temp_before: bool = False, cleanup_temp_after: bool = False,
                cleanup_older_than_hours: float = 24.0, low_disk_mode: bool = False, min_free_gb: float = 20.0,
                resume_free_gb: float = 25.0, low_disk_poll_seconds: float = 10.0,
                progress_cb: ProgressCallback | None = None, keep_parquet: bool = False,
                threads: int | None = None, memory_limit: str = "8GB",
                target_rows_per_file: int = _DEFAULT_TARGET_ROWS_PER_FILE,
                parallel_writers: int = 1, ultra_mmap_mode: bool = False, ultra_workers: int | None = None, parser_mode: str = "auto") -> ImportStats:
    lower = dump_path.lower()
    temp_dir = str(temp_dir or Path.cwd() / "tmp")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    if cleanup_temp_before:
        cleanup_stats = cleanup_temp_files(temp_dir, older_than_hours=cleanup_older_than_hours)
    else:
        cleanup_stats = TempCleanupStats()

    if ultra_mmap_mode:
        return run_ultra(db_path=db_path, dump_path=dump_path, temp_dir=temp_dir, mode=mode, progress_cb=progress_cb, threads=threads, memory_limit=memory_limit, ultra_workers=ultra_workers or parallel_writers or 4, row_group_size=max(1_000, batch_size), target_rows_per_file=target_rows_per_file, keep_parquet=keep_parquet, parser_mode=parser_mode)

    if lower.endswith(".parquet") or (Path(dump_path).exists() and Path(dump_path).is_dir()):
        stats = import_parquet(db_path, dump_path, temp_dir=temp_dir, mode=mode, progress_cb=progress_cb,
                               threads=threads, memory_limit=memory_limit)
        stats.temp_files_deleted += cleanup_stats.files_deleted
        stats.temp_bytes_deleted += cleanup_stats.bytes_deleted
        return stats

    temp_base = str(Path(temp_dir) / f"{Path(dump_path).stem}.{int(time.time())}.parquet")
    convert_stats = convert_dump_to_parquet(
        dump_path,
        temp_base,
        row_group_size=max(1_000, batch_size),
        compression="zstd",
        progress_cb=progress_cb,
        db_path_for_progress=db_path,
        temp_dir_for_progress=temp_dir,
        low_disk_mode=low_disk_mode,
        min_free_gb=min_free_gb,
        resume_free_gb=resume_free_gb,
        low_disk_poll_seconds=low_disk_poll_seconds,
        target_rows_per_file=target_rows_per_file,
        parallel_writers=parallel_writers,
    )
    temp_glob = str(Path(temp_base).with_name(f"{Path(temp_base).stem}*.parquet"))
    db_stats = import_parquet(db_path, temp_glob, temp_dir=temp_dir, mode=mode, progress_cb=progress_cb,
                              threads=threads, memory_limit=memory_limit)

    if not keep_parquet:
        for parquet_file in sorted(Path(temp_dir).glob(f"{Path(temp_base).stem}*.parquet")):
            try:
                size = parquet_file.stat().st_size
                parquet_file.unlink(missing_ok=True)
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
    db_stats.read_seconds = convert_stats.read_seconds
    db_stats.transform_seconds = convert_stats.transform_seconds
    db_stats.parquet_write_seconds = convert_stats.parquet_write_seconds
    db_stats.parquet_file_count = convert_stats.parquet_file_count
    db_stats.temp_files_deleted += cleanup_stats.files_deleted
    db_stats.temp_bytes_deleted += cleanup_stats.bytes_deleted
    return db_stats
