from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .config import build_links
from .db import connect_db

CLAIMABLE_RULE_TEXT = (
    "claimable = controlling_faction leer UND faction_count = 0; "
    "Permit-/Architekt-/externe Anspruchsstatus werden dabei nicht berücksichtigt"
)


def _reference_coords(con, reference_system: str) -> tuple[float, float, float]:
    row = con.execute(
        "SELECT x, y, z FROM systems WHERE lower(name) = lower(?)",
        [reference_system],
    ).fetchone()
    if not row or row[0] is None or row[1] is None or row[2] is None:
        raise ValueError(f"Referenzsystem nicht gefunden oder ohne Koordinaten: {reference_system}")
    return float(row[0]), float(row[1]), float(row[2])


def query_systems(
    db_path: str,
    reference_system: str,
    radius_ly: float,
    *,
    body_filter: str = "ww_or_elw",
    claimable_only: bool = False,
    limit: int = 200,
    temp_dir: str | None = None,
    include_links: bool = True,
    link_template_path: str | None = None,
) -> list[dict[str, Any]]:
    con = connect_db(db_path, temp_dir=temp_dir, read_only=True)
    try:
        rx, ry, rz = _reference_coords(con, reference_system)
        where = [
            "x IS NOT NULL", "y IS NOT NULL", "z IS NOT NULL",
            "sqrt(pow(x - ?, 2) + pow(y - ?, 2) + pow(z - ?, 2)) <= ?",
            "lower(name) <> lower(?)",
        ]
        params: list[Any] = [rx, ry, rz, radius_ly, reference_system]

        body_filter = body_filter.lower()
        if body_filter == "ww":
            where.append("water_world_count > 0")
        elif body_filter == "elw":
            where.append("earthlike_world_count > 0")
        elif body_filter == "ww_or_elw":
            where.append("(water_world_count > 0 OR earthlike_world_count > 0)")
        elif body_filter == "ww_and_elw":
            where.append("(water_world_count > 0 AND earthlike_world_count > 0)")
        elif body_filter != "any":
            raise ValueError("body_filter muss one of: any, ww, elw, ww_or_elw, ww_and_elw sein")

        if claimable_only:
            where.append("coalesce(controlling_faction, '') = ''")
            where.append("coalesce(faction_count, 0) = 0")

        sql = f"""
            SELECT
                name,
                round(sqrt(pow(x - ?, 2) + pow(y - ?, 2) + pow(z - ?, 2)), 2) AS distance_ly,
                total_bodies,
                total_planets,
                total_stars,
                ringed_body_count,
                water_world_count,
                earthlike_world_count,
                population,
                faction_count,
                controlling_faction,
                body_type_counts_json,
                body_subtype_counts_json,
                ring_type_counts_json,
                (coalesce(controlling_faction, '') = '' AND coalesce(faction_count, 0) = 0) AS claimable
            FROM systems
            WHERE {' AND '.join(where)}
            ORDER BY distance_ly ASC, total_bodies DESC, name ASC
            LIMIT ?
        """
        final_params = [rx, ry, rz] + params + [limit]
        rows = con.execute(sql, final_params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            body_type_counts = json.loads(row[11] or "{}")
            body_subtype_counts = json.loads(row[12] or "{}")
            ring_type_counts = json.loads(row[13] or "{}")
            system_name = row[0]
            links = build_links(system_name, link_template_path) if include_links else {}
            out.append({
                "name": system_name,
                "distance_ly": row[1],
                "total_bodies": row[2],
                "total_planets": row[3],
                "total_stars": row[4],
                "ringed_body_count": row[5],
                "water_world_count": row[6],
                "earthlike_world_count": row[7],
                "population": row[8],
                "faction_count": row[9],
                "controlling_faction": row[10],
                "body_type_counts": body_type_counts,
                "body_subtype_counts": body_subtype_counts,
                "ring_type_counts": ring_type_counts,
                "claimable": bool(row[14]),
                "claimable_rule": CLAIMABLE_RULE_TEXT,
                "edsm_url": links.get("edsm", ""),
                "inara_url": links.get("inara", ""),
                "edgis_url": links.get("edgis", ""),
            })
        return out
    finally:
        con.close()


def write_csv(rows: list[dict[str, Any]], output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow([
            "System", "Distanz (ly)", "Bodies gesamt", "Planeten", "Sterne", "Ringed Bodies",
            "Water Worlds", "Earth-like Worlds", "Population", "Faction Count", "Controlling Faction",
            "Claimable", "Claimable-Regel",
            "Body Types", "Body Subtypes", "Ring Types",
            "EDSM", "Inara", "EDGIS",
        ])
        for row in rows:
            writer.writerow([
                row.get("name", ""),
                row.get("distance_ly", ""),
                row.get("total_bodies", ""),
                row.get("total_planets", ""),
                row.get("total_stars", ""),
                row.get("ringed_body_count", ""),
                row.get("water_world_count", ""),
                row.get("earthlike_world_count", ""),
                row.get("population", ""),
                row.get("faction_count", ""),
                row.get("controlling_faction", ""),
                "yes" if row.get("claimable") else "no",
                row.get("claimable_rule", CLAIMABLE_RULE_TEXT),
                json.dumps(row.get("body_type_counts", {}), ensure_ascii=False, sort_keys=True),
                json.dumps(row.get("body_subtype_counts", {}), ensure_ascii=False, sort_keys=True),
                json.dumps(row.get("ring_type_counts", {}), ensure_ascii=False, sort_keys=True),
                row.get("edsm_url", ""),
                row.get("inara_url", ""),
                row.get("edgis_url", ""),
            ])
