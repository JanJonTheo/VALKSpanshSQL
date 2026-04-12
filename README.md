# Spansh DuckDB HP App V3

Diese Version ergänzt einen **Parquet-Workflow** für deutlich schnellere Full- und Patch-Imports in DuckDB.

## Was neu ist

- **Streaming-Konvertierung** von `json`, `json.gz`, `json.zst` nach **Parquet**
- **Parquet Full Import** in DuckDB
- **Parquet Patch Import** in DuckDB
- Normale `import-full` und `apply-patch` erkennen JSON/GZ/ZST **oder Parquet** automatisch
- Bei JSON/GZ/ZST wird intern zuerst ein **temporäres Parquet** erzeugt und danach in DuckDB importiert

## Warum das schneller ist

Der teure Teil bei großen Full Imports ist oft nicht DuckDB, sondern:
- Python + JSON Parsing
- viele `UPSERT`s in kleinen Batches

Mit V3 wird der Dump zunächst in eine spaltenorientierte **Parquet-Datei** geschrieben. Danach übernimmt DuckDB den eigentlichen Full-/Patch-Import viel effizienter per SQL.

## Empfehlung

Für sehr große Dumps:
1. **einmal in Parquet konvertieren**
2. anschließend aus Parquet importieren
3. Parquet für spätere Neuimporte wiederverwenden

## Beispiele

### DB initialisieren

```bash
python -m spansh_duckdb_hp_app_parquet_v3.cli init-db --db C:\DB\spansh.duckdb --temp-dir C:\Temp\duckdb
```

### Dump nach Parquet konvertieren

```bash
python -m spansh_duckdb_hp_app_parquet_v3.cli convert-parquet --dump X:\Dumps\galaxy.json.gz --parquet C:\Temp\galaxy.parquet --row-group-size 50000
```

### Full Import direkt aus Parquet

```bash
python -m spansh_duckdb_hp_app_parquet_v3.cli import-parquet-full --db C:\DB\spansh.duckdb --parquet C:\Temp\galaxy.parquet --temp-dir C:\Temp\duckdb
```

### Patch Import direkt aus Parquet

```bash
python -m spansh_duckdb_hp_app_parquet_v3.cli import-parquet-patch --db C:\DB\spansh.duckdb --parquet C:\Temp\galaxy_patch.parquet --temp-dir C:\Temp\duckdb
```

### Full Import direkt aus JSON/GZ/ZST

```bash
python -m spansh_duckdb_hp_app_parquet_v3.cli import-full --db C:\DB\spansh.duckdb --temp-dir C:\Temp\duckdb --dump X:\Dumps\galaxy.json.gz --batch-size 50000 --keep-parquet
```

Wenn `--keep-parquet` gesetzt ist, bleibt das intern erzeugte Parquet im Temp-Verzeichnis erhalten.

## GUI

GUI starten:

```bash
python -m spansh_duckdb_hp_app_parquet_v3.gui
```

Die GUI kann jetzt auch `.parquet` als Quelle auswählen.

## Speicherhinweis

- JSON/GZ/ZST → Parquet ist meist schneller als direkter DB-Import
- Die temporäre Parquet-Datei benötigt zusätzlichen Platz
- DB und Temp-Verzeichnis sollten **lokal auf SSD/NVMe** liegen
