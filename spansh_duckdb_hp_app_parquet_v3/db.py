from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from .config import default_db_path, default_temp_dir, ensure_dir, ensure_parent

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS systems (
    name VARCHAR PRIMARY KEY,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE,
    population BIGINT,
    controlling_faction VARCHAR,
    faction_count INTEGER DEFAULT 0,
    total_bodies INTEGER DEFAULT 0,
    total_planets INTEGER DEFAULT 0,
    total_stars INTEGER DEFAULT 0,
    ringed_body_count INTEGER DEFAULT 0,
    water_world_count INTEGER DEFAULT 0,
    earthlike_world_count INTEGER DEFAULT 0,
    body_type_counts_json JSON,
    body_subtype_counts_json JSON,
    ring_type_counts_json JSON,
    imported_at TIMESTAMP,
    source_tag VARCHAR
);

CREATE TABLE IF NOT EXISTS app_meta (
    key VARCHAR PRIMARY KEY,
    value VARCHAR
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_systems_name ON systems(name);
CREATE INDEX IF NOT EXISTS idx_systems_xyz ON systems(x, y, z);
CREATE INDEX IF NOT EXISTS idx_systems_claimable ON systems(controlling_faction, faction_count);
CREATE INDEX IF NOT EXISTS idx_systems_ww ON systems(water_world_count);
CREATE INDEX IF NOT EXISTS idx_systems_elw ON systems(earthlike_world_count);
"""

DROP_INDEX_SQL = """
DROP INDEX IF EXISTS idx_systems_name;
DROP INDEX IF EXISTS idx_systems_xyz;
DROP INDEX IF EXISTS idx_systems_claimable;
DROP INDEX IF EXISTS idx_systems_ww;
DROP INDEX IF EXISTS idx_systems_elw;
"""


def get_db_path(custom_path: str | os.PathLike[str] | None = None) -> Path:
    return Path(custom_path) if custom_path else default_db_path()


def get_temp_dir(custom_path: str | os.PathLike[str] | None = None) -> Path:
    return Path(custom_path) if custom_path else default_temp_dir()


def connect_db(
    db_path: str | os.PathLike[str] | None = None,
    temp_dir: str | os.PathLike[str] | None = None,
    *,
    read_only: bool = False,
    threads: int | None = None,
    memory_limit: str = "8GB",
) -> duckdb.DuckDBPyConnection:
    path = get_db_path(db_path)
    tmp = get_temp_dir(temp_dir)
    ensure_parent(path)
    ensure_dir(tmp)

    con = duckdb.connect(str(path), read_only=read_only)
    temp_sql = str(tmp).replace("\\", "/")
    con.execute(f"PRAGMA temp_directory='{temp_sql}';")
    con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    con.execute(f"PRAGMA threads={threads or max(1, os.cpu_count() or 4)};")
    con.execute("PRAGMA enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order = false;")
    return con


def init_db(
    db_path: str | os.PathLike[str] | None = None,
    temp_dir: str | os.PathLike[str] | None = None,
) -> Path:
    path = get_db_path(db_path)
    con = connect_db(path, temp_dir=temp_dir)
    try:
        con.execute(SCHEMA_SQL)
        set_meta(con, "schema_version", "3")
        con.checkpoint()
    finally:
        con.close()
    return path


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(INDEX_SQL)


def drop_indexes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DROP_INDEX_SQL)


def set_meta(con: duckdb.DuckDBPyConnection, key: str, value: Any) -> None:
    con.execute(
        """
        INSERT INTO app_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        [key, str(value)],
    )


def get_meta(con: duckdb.DuckDBPyConnection, key: str, default: str = "") -> str:
    row = con.execute("SELECT value FROM app_meta WHERE key = ?", [key]).fetchone()
    return row[0] if row else default
