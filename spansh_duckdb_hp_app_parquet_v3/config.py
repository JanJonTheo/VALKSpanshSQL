from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import quote_plus


def default_db_path() -> Path:
    env = os.getenv("SPANSH_DB_PATH")
    if env:
        return Path(env)
    return Path.cwd() / "db" / "spansh.duckdb"


def default_temp_dir() -> Path:
    env = os.getenv("SPANSH_TEMP_DIR")
    if env:
        return Path(env)
    return Path.cwd() / "tmp"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def default_link_template_path() -> Path:
    env = os.getenv("SPANSH_LINK_TEMPLATE_PATH")
    if env:
        return Path(env)
    return Path.cwd() / "link_templates.json"


DEFAULT_LINK_TEMPLATES = {
    "edsm": "https://www.edsm.net/en/search/systems/index/name/{name_query}",
    "inara": "https://inara.cz/elite/search/?search={name_query}",
    "edgis": "https://edgis.elitedangereuse.fr/",
}


def load_link_templates(path: str | os.PathLike[str] | None = None) -> dict[str, str]:
    templates = dict(DEFAULT_LINK_TEMPLATES)
    tpl_path = Path(path) if path else default_link_template_path()
    if tpl_path.exists():
        with tpl_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(key, str) and isinstance(value, str):
                    templates[key.lower()] = value
    return templates


def build_links(system_name: str, path: str | os.PathLike[str] | None = None) -> dict[str, str]:
    templates = load_link_templates(path)
    encoded = quote_plus(system_name)
    raw_encoded = quote_plus(system_name, safe="")
    links: dict[str, str] = {}
    for key, template in templates.items():
        try:
            links[key] = template.format(
                name=system_name,
                name_query=encoded,
                name_path=raw_encoded,
            )
        except Exception:
            links[key] = template
    return links
