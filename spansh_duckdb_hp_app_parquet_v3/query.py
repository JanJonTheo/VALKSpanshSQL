from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
import ssl

try:
    import certifi
except Exception:
    certifi = None

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


def _json_loads_safe(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sum_matching_counts(mapping: dict[str, Any], aliases: Iterable[str]) -> int:
    alias_set = {a.casefold() for a in aliases}
    total = 0
    for key, value in mapping.items():
        if str(key).casefold() in alias_set:
            try:
                total += int(value or 0)
            except Exception:
                pass
    return total


def _augment_counts(row: dict[str, Any]) -> dict[str, Any]:
    subtype_counts = row.get("body_subtype_counts", {}) or {}
    type_counts = row.get("body_type_counts", {}) or {}

    row["hmcw_count"] = _sum_matching_counts(
        subtype_counts,
        ["High metal content world", "High metal content body"],
    )
    row["icy_world_count"] = _sum_matching_counts(
        subtype_counts,
        ["Icy body", "Icy world"],
    )
    row["rocky_icy_world_count"] = _sum_matching_counts(
        subtype_counts,
        ["Rocky ice body", "Rocky ice world"],
    )
    row["gas_giant_count"] = _sum_matching_counts(
        subtype_counts,
        [
            "Gas giant with water based life",
            "Gas giant with ammonia based life",
            "Sudarsky class i gas giant",
            "Sudarsky class ii gas giant",
            "Sudarsky class iii gas giant",
            "Sudarsky class iv gas giant",
            "Sudarsky class v gas giant",
            "Helium rich gas giant",
            "Water giant",
            "Gas giant",
        ],
    )
    if row["gas_giant_count"] <= 0:
        row["gas_giant_count"] = _sum_matching_counts(type_counts, ["Planet"]) - (
            int(row.get("water_world_count") or 0)
            + int(row.get("earthlike_world_count") or 0)
            + int(row.get("hmcw_count") or 0)
            + int(row.get("icy_world_count") or 0)
            + int(row.get("rocky_icy_world_count") or 0)
        )
        row["gas_giant_count"] = max(0, int(row["gas_giant_count"]))
    return row


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
            body_type_counts = _json_loads_safe(row[11])
            body_subtype_counts = _json_loads_safe(row[12])
            ring_type_counts = _json_loads_safe(row[13])
            system_name = row[0]
            links = build_links(system_name, link_template_path) if include_links else {}
            result_row: dict[str, Any] = {
                "reference_system": reference_system,
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
                "inara_url": links.get("inara", f"https://inara.cz/elite/starsystem/?search={quote_plus(system_name)}"),
                "edgis_url": links.get("edgis", f"https://edgis.elitedangereuse.fr/static/sysmap.html?system={quote_plus(system_name)}"),
            }
            out.append(_augment_counts(result_row))
        return out
    finally:
        con.close()


def query_multiple_systems(
    db_path: str,
    reference_systems: list[str],
    radius_ly: float,
    *,
    body_filter: str = "ww_or_elw",
    claimable_only: bool = False,
    limit: int = 200,
    temp_dir: str | None = None,
    include_links: bool = True,
    link_template_path: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    results: dict[str, list[dict[str, Any]]] = {}
    for reference in reference_systems:
        ref = reference.strip()
        if not ref:
            continue
        results[ref] = query_systems(
            db_path,
            ref,
            radius_ly,
            body_filter=body_filter,
            claimable_only=claimable_only,
            limit=limit,
            temp_dir=temp_dir,
            include_links=include_links,
            link_template_path=link_template_path,
        )
    return results


def write_csv(rows: list[dict[str, Any]], output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow([
            "Referenzsystem", "System", "Distanz (ly)", "WW", "ELW", "HMCW", "Icy", "Rocky-Icy",
            "Gas Giants", "Rings", "Bodies gesamt", "Planeten", "Sterne", "Population",
            "Faction Count", "Controlling Faction", "Claimable", "Claimable-Regel",
            "Body Types", "Body Subtypes", "Ring Types", "EDSM", "Inara", "EDGIS",
        ])
        for row in rows:
            writer.writerow([
                row.get("reference_system", ""),
                row.get("name", ""),
                row.get("distance_ly", ""),
                row.get("water_world_count", ""),
                row.get("earthlike_world_count", ""),
                row.get("hmcw_count", ""),
                row.get("icy_world_count", ""),
                row.get("rocky_icy_world_count", ""),
                row.get("gas_giant_count", ""),
                row.get("ringed_body_count", ""),
                row.get("total_bodies", ""),
                row.get("total_planets", ""),
                row.get("total_stars", ""),
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


DISCORD_LIMIT = 1900


def _discord_table(rows: list[dict[str, Any]]) -> str:
    header = (
        f"{'System':<22} {'Dist':>6} {'WW':>3} {'ELW':>4} {'HMCW':>5} {'Icy':>4} "
        f"{'R-Icy':>5} {'GG':>3} {'Rng':>4}"
    )
    sep = "-" * len(header)
    lines = [header, sep]
    for row in rows:
        name = str(row.get("name", ""))[:22]
        lines.append(
            f"{name:<22} "
            f"{float(row.get('distance_ly', 0) or 0):>6.1f} "
            f"{int(row.get('water_world_count', 0) or 0):>3} "
            f"{int(row.get('earthlike_world_count', 0) or 0):>4} "
            f"{int(row.get('hmcw_count', 0) or 0):>5} "
            f"{int(row.get('icy_world_count', 0) or 0):>4} "
            f"{int(row.get('rocky_icy_world_count', 0) or 0):>5} "
            f"{int(row.get('gas_giant_count', 0) or 0):>3} "
            f"{int(row.get('ringed_body_count', 0) or 0):>4}"
        )
    return "\n".join(lines)


def format_discord_messages(
    reference_system: str,
    rows: list[dict[str, Any]],
    *,
    radius_ly: float,
    body_filter: str,
    claimable_only: bool,
    limit: int,
) -> list[str]:
    title = f"## {reference_system}"
    info = (
        f"Radius: **{radius_ly:.1f} ly** | Filter: **{body_filter}** | "
        f"Claimable only: **{'yes' if claimable_only else 'no'}** | Limit: **{limit}** | Treffer: **{len(rows)}**"
    )
    if not rows:
        return [f"{title}\n{info}\n\nKeine Treffer."]

    chunks: list[str] = []
    current_rows: list[dict[str, Any]] = []
    current_links: list[str] = []

    def build_message(chunk_rows: list[dict[str, Any]], chunk_links: list[str], part_no: int, part_count: int) -> str:
        part = f"\nTeil {part_no}/{part_count}" if part_count > 1 else ""
        table = _discord_table(chunk_rows)
        links_block = "\n".join(chunk_links)
        return f"{title}{part}\n{info}\n```text\n{table}\n```\n{links_block}".strip()

    provisional_messages: list[tuple[list[dict[str, Any]], list[str]]] = []
    for row in rows:
        link_line = (
            f"- **{row.get('name','')}** | [Inara]({row.get('inara_url','')}) | "
            f"[EDGIS]({row.get('edgis_url','')})"
        )
        trial_rows = current_rows + [row]
        trial_links = current_links + [link_line]
        msg = build_message(trial_rows, trial_links, 1, 1)
        if len(msg) > DISCORD_LIMIT and current_rows:
            provisional_messages.append((current_rows, current_links))
            current_rows = [row]
            current_links = [link_line]
        else:
            current_rows = trial_rows
            current_links = trial_links
    if current_rows:
        provisional_messages.append((current_rows, current_links))

    part_count = len(provisional_messages)
    for idx, (chunk_rows, chunk_links) in enumerate(provisional_messages, start=1):
        chunks.append(build_message(chunk_rows, chunk_links, idx, part_count))
    return chunks


def send_discord_messages(webhook_url: str, messages: list[str], timeout: float = 20.0) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    ssl_context = ssl.create_default_context(cafile=certifi.where()) if certifi else ssl.create_default_context()
    for index, content in enumerate(messages, start=1):
        payload = json.dumps({"content": content}).encode("utf-8")
        request = Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "SpanshPlanner/1.0"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout, context=ssl_context) as response:
                results.append({"index": index, "status": getattr(response, "status", 200), "ok": True})
        except Exception as exc:
            results.append({"index": index, "status": None, "ok": False, "error": str(exc)})
    return results
