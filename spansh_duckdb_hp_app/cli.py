from __future__ import annotations

import argparse
import json

from .db import init_db
from .importer import import_dump
from .query import query_systems, write_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spansh-duckdb-hp")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db", help="DuckDB initialisieren")
    p_init.add_argument("--db", required=True)
    p_init.add_argument("--temp-dir", default=None)

    def add_import_args(p):
        p.add_argument("--db", required=True)
        p.add_argument("--dump", required=True)
        p.add_argument("--temp-dir", default=None)
        p.add_argument("--batch-size", type=int, default=5000)
        p.add_argument("--cleanup-temp-before", action="store_true")
        p.add_argument("--cleanup-temp-after", action="store_true")
        p.add_argument("--cleanup-older-than-hours", type=float, default=24.0)
        p.add_argument("--low-disk-mode", action="store_true")
        p.add_argument("--min-free-gb", type=float, default=20.0)
        p.add_argument("--resume-free-gb", type=float, default=25.0)
        p.add_argument("--low-disk-poll-seconds", type=float, default=10.0)

    p_full = sub.add_parser("import-full", help="Full Dump importieren")
    add_import_args(p_full)

    p_patch = sub.add_parser("apply-patch", help="Patch Dump anwenden")
    add_import_args(p_patch)

    p_query = sub.add_parser("query-systems", help="WW/ELW Systeme abfragen")
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


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "init-db":
        path = init_db(args.db, temp_dir=args.temp_dir)
        print(f"DB initialisiert: {path}")
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
        )
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
