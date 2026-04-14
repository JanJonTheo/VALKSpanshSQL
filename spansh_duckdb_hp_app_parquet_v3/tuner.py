from __future__ import annotations

import itertools
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .importer import convert_dump_to_parquet, import_parquet

TuneProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(slots=True)
class TuneResult:
    threads: int
    memory_limit: str
    sample_systems: int
    elapsed_s: float
    systems_per_sec: float
    parquet_size_bytes: int
    db_size_bytes: int
    score: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "threads": self.threads,
            "memory_limit": self.memory_limit,
            "sample_systems": self.sample_systems,
            "elapsed_s": self.elapsed_s,
            "systems_per_sec": self.systems_per_sec,
            "parquet_size_bytes": self.parquet_size_bytes,
            "db_size_bytes": self.db_size_bytes,
            "score": self.score,
        }


def _normalize_threads(values: list[int] | tuple[int, ...] | None) -> list[int]:
    if values:
        return sorted({max(1, int(v)) for v in values})
    cpu = os.cpu_count() or 8
    return sorted({max(1, min(cpu, v)) for v in (4, 6, 8, 10, 12, cpu)})


def _normalize_memory(values: list[str] | tuple[str, ...] | None) -> list[str]:
    if values:
        return [str(v).upper() for v in values]
    return ["16GB", "24GB", "32GB", "40GB"]


def autotune_import(
    dump_path: str,
    *,
    temp_dir: str,
    sample_systems: int = 100000,
    thread_candidates: list[int] | None = None,
    memory_candidates: list[str] | None = None,
    compression: str = "zstd",
    progress_cb: TuneProgressCallback | None = None,
) -> dict[str, Any]:
    tmp = Path(temp_dir)
    tmp.mkdir(parents=True, exist_ok=True)

    thread_candidates = _normalize_threads(thread_candidates)
    memory_candidates = _normalize_memory(memory_candidates)

    sample_parquet = str(tmp / f"autotune_sample_{int(time.time())}.parquet")
    if progress_cb:
        progress_cb({
            "phase": "prepare",
            "message": f"Erzeuge Sample-Parquet mit {sample_systems:,} Systemen",
            "sample_systems": sample_systems,
        })

    convert_stats = convert_dump_to_parquet(
        dump_path=dump_path,
        parquet_path=sample_parquet,
        row_group_size=min(max(10_000, sample_systems // 4), 100_000),
        compression=compression,
        progress_cb=progress_cb,
        db_path_for_progress=sample_parquet,
        temp_dir_for_progress=temp_dir,
        max_systems=sample_systems,
        parallel_writers=1,
        target_rows_per_file=max(sample_systems, 250_000),
    )

    sample_glob = str(Path(sample_parquet).with_name(f"{Path(sample_parquet).stem}*.parquet"))
    sample_files = sorted(tmp.glob(f"{Path(sample_parquet).stem}*.parquet"))
    if not sample_files:
        raise RuntimeError("Auto-Tuning konnte kein Sample-Parquet erzeugen.")

    parquet_size = sum(p.stat().st_size for p in sample_files)
    results: list[TuneResult] = []
    combos = list(itertools.product(thread_candidates, memory_candidates))

    for idx, (threads, memory_limit) in enumerate(combos, start=1):
        trial_db = str(tmp / f"autotune_{threads}_{memory_limit.lower()}_{int(time.time()*1000)}.duckdb")
        if progress_cb:
            progress_cb({
                "phase": "benchmark",
                "message": f"Benchmark {idx}/{len(combos)}: {threads} Threads / {memory_limit}",
                "threads": threads,
                "memory_limit": memory_limit,
                "sample_systems": convert_stats.systems_seen,
            })
        started = time.time()
        stats = import_parquet(
            db_path=trial_db,
            parquet_path=sample_glob,
            temp_dir=temp_dir,
            mode="full",
            threads=threads,
            memory_limit=memory_limit,
            progress_cb=None,
        )
        elapsed = max(0.001, time.time() - started)
        db_size = Path(trial_db).stat().st_size if Path(trial_db).exists() else 0
        systems_per_sec = stats.systems_written / elapsed if elapsed else 0.0
        mem_num = float(memory_limit.upper().replace("GB", "").strip() or 0)
        score = systems_per_sec / max(1.0, math.sqrt(mem_num))
        results.append(TuneResult(
            threads=threads,
            memory_limit=memory_limit,
            sample_systems=stats.systems_written,
            elapsed_s=elapsed,
            systems_per_sec=systems_per_sec,
            parquet_size_bytes=parquet_size,
            db_size_bytes=db_size,
            score=score,
        ))
        try:
            Path(trial_db).unlink(missing_ok=True)
        except Exception:
            pass

    results.sort(key=lambda r: (r.systems_per_sec, r.score), reverse=True)
    best = results[0]

    for p in sample_files:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    summary = {
        "best_threads": best.threads,
        "best_memory_limit": best.memory_limit,
        "sample_systems": best.sample_systems,
        "tested_combinations": len(results),
        "results": [r.as_dict() for r in results],
    }
    if progress_cb:
        progress_cb({
            "phase": "done",
            "message": f"Empfehlung: {best.threads} Threads / {best.memory_limit}",
            **summary,
        })
    return summary
