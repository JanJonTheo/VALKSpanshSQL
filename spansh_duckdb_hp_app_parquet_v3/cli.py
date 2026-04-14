from __future__ import annotations

import argparse
import json

from .db import init_db
from .importer import convert_dump_to_parquet, import_dump, import_parquet
from .query import query_systems, write_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spansh-project-integrated")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db", help="DuckDB initialisieren")
    p_init.add_argument("--db", required=True)
    p_init.add_argument("--temp-dir", default=None)
    p_init.add_argument("--threads", type=int, default=None)
    p_init.add_argument("--memory-limit", default="32GB")

    def add_import_args(p):
        p.add_argument("--db", required=True)
        p.add_argument("--dump", required=True)
        p.add_argument("--temp-dir", default=None)
        p.add_argument("--threads", type=int, default=None)
        p.add_argument("--memory-limit", default="32GB")
        p.add_argument("--batch-size", type=int, default=50000)
        p.add_argument("--cleanup-temp-before", action="store_true")
        p.add_argument("--cleanup-temp-after", action="store_true")
        p.add_argument("--cleanup-older-than-hours", type=float, default=24.0)
        p.add_argument("--low-disk-mode", action="store_true")
        p.add_argument("--min-free-gb", type=float, default=20.0)
        p.add_argument("--resume-free-gb", type=float, default=25.0)
        p.add_argument("--low-disk-poll-seconds", type=float, default=10.0)
        p.add_argument("--keep-parquet", action="store_true")
        p.add_argument("--target-rows-per-file", type=int, default=1000000)
        p.add_argument("--parallel-writers", type=int, default=1)
        p.add_argument("--ultra-mmap", action="store_true")
        p.add_argument("--ultra-workers", type=int, default=None)
        p.add_argument("--parser-mode", default="auto", choices=["auto", "simdjson", "orjson", "json"])

    p_convert = sub.add_parser("convert-parquet", help="Dump streamend in Parquet umwandeln")
    p_convert.add_argument("--dump", required=True)
    p_convert.add_argument("--parquet", required=True)
    p_convert.add_argument("--row-group-size", type=int, default=50000)
    p_convert.add_argument("--target-rows-per-file", type=int, default=1000000)
    p_convert.add_argument("--parallel-writers", type=int, default=1)
    p_convert.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "brotli", "none"])

    p_full = sub.add_parser("import-full", help="Full Dump importieren (JSON/GZ/ZST oder Parquet)")
    add_import_args(p_full)

    p_patch = sub.add_parser("apply-patch", help="Patch Dump importieren (JSON/GZ/ZST oder Parquet)")
    add_import_args(p_patch)

    p_parquet_full = sub.add_parser("import-parquet-full", help="Parquet Full Import")
    p_parquet_full.add_argument("--db", required=True)
    p_parquet_full.add_argument("--parquet", required=True)
    p_parquet_full.add_argument("--temp-dir", default=None)
    p_parquet_full.add_argument("--threads", type=int, default=None)
    p_parquet_full.add_argument("--memory-limit", default="32GB")

    p_parquet_patch = sub.add_parser("import-parquet-patch", help="Parquet Patch Import")
    p_parquet_patch.add_argument("--db", required=True)
    p_parquet_patch.add_argument("--parquet", required=True)
    p_parquet_patch.add_argument("--temp-dir", default=None)
    p_parquet_patch.add_argument("--threads", type=int, default=None)
    p_parquet_patch.add_argument("--memory-limit", default="32GB")

    p_query = sub.add_parser("query-systems", help="Systeme für Planner abfragen")
    p_query.add_argument("--db", required=True)
    p_query.add_argument("--reference", required=True)
    p_query.add_argument("--radius", type=float, required=True)
    p_query.add_argument("--filter", default="ww_or_elw", choices=["any", "ww", "elw", "ww_or_elw", "ww_and_elw"])
    p_query.add_argument("--claimable-only", action="store_true")
    p_query.add_argument("--limit", type=int, default=200)
    p_query.add_argument("--temp-dir", default=None)
    p_query.add_argument("--output", default=None)
    p_query.add_argument("--no-links", action="store_true")
    p_query.add_argument("--link-template-file", default=None)

    return parser


def _print_stats(stats):
    print(json.dumps({
        "systems_seen": stats.systems_seen,
        "systems_written": stats.systems_written,
        "elapsed_s": round(stats.elapsed_s, 2),
        "active_elapsed_s": round(stats.active_elapsed_s, 2),
        "systems_per_sec": round(stats.systems_per_sec, 2),
        "progress_ratio": round(stats.progress_ratio, 4),
        "paused_seconds": round(stats.paused_seconds, 2),
        "temp_files_deleted": stats.temp_files_deleted,
        "temp_bytes_deleted": stats.temp_bytes_deleted,
    }, ensure_ascii=False, indent=2))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "init-db":
        path = init_db(args.db, temp_dir=args.temp_dir, threads=args.threads, memory_limit=args.memory_limit)
        print(f"DB initialisiert: {path}")
        return 0

    if args.cmd == "convert-parquet":
        compression = None if args.compression == "none" else args.compression
        stats = convert_dump_to_parquet(
            args.dump,
            args.parquet,
            row_group_size=args.row_group_size,
            compression=compression or "zstd",
            target_rows_per_file=args.target_rows_per_file,
            parallel_writers=args.parallel_writers,
            progress_cb=lambda data: print(json.dumps(data, ensure_ascii=False)),
        )
        _print_stats(stats)
        return 0

    if args.cmd in {"import-full", "apply-patch"}:
        stats = import_dump(
            db_path=args.db,
            dump_path=args.dump,
            temp_dir=args.temp_dir,
            batch_size=args.batch_size,
            mode="full" if args.cmd == "import-full" else "patch",
            cleanup_temp_before=args.cleanup_temp_before,
            cleanup_temp_after=args.cleanup_temp_after,
            cleanup_older_than_hours=args.cleanup_older_than_hours,
            low_disk_mode=args.low_disk_mode,
            min_free_gb=args.min_free_gb,
            resume_free_gb=args.resume_free_gb,
            low_disk_poll_seconds=args.low_disk_poll_seconds,
            progress_cb=lambda data: print(json.dumps(data, ensure_ascii=False)),
            keep_parquet=args.keep_parquet,
            threads=args.threads,
            memory_limit=args.memory_limit,
            target_rows_per_file=args.target_rows_per_file,
            parallel_writers=args.parallel_writers,
            ultra_mmap_mode=args.ultra_mmap,
            ultra_workers=args.ultra_workers,
            parser_mode=args.parser_mode,
        )
        _print_stats(stats)
        return 0

    if args.cmd in {"import-parquet-full", "import-parquet-patch"}:
        stats = import_parquet(
            args.db,
            args.parquet,
            temp_dir=args.temp_dir,
            mode="full" if args.cmd == "import-parquet-full" else "patch",
            threads=args.threads,
            memory_limit=args.memory_limit,
            progress_cb=lambda data: print(json.dumps(data, ensure_ascii=False)),
        )
        _print_stats(stats)
        return 0

    rows = query_systems(
        args.db,
        args.reference,
        args.radius,
        body_filter=args.filter,
        claimable_only=args.claimable_only,
        limit=args.limit,
        temp_dir=args.temp_dir,
        include_links=not args.no_links,
        link_template_path=args.link_template_file,
    )
    if args.output:
        write_csv(rows, args.output)
        print(f"CSV geschrieben: {args.output}")
    else:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
