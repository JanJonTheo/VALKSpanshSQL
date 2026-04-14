from __future__ import annotations

import json
import sys
from pathlib import Path

from PySide6.QtCore import QObject, QThread, Qt, Signal
from PySide6.QtGui import QFont, QTextOption
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QDoubleSpinBox,
)

from .config import default_db_path, default_temp_dir
from .db import get_db_settings, init_db
from .importer import import_dump
from .query import (
    format_discord_messages,
    query_systems,
    send_discord_messages,
    write_csv,
)

LOW_DISK_POLL_SECONDS = 10.0
MONO_FONT = QFont("Menlo", 10)


class ImportWorker(QObject):
    progress = Signal(dict)
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        db_path: str,
        dump_path: str,
        temp_dir: str,
        mode: str,
        batch_size: int,
        cleanup_temp_before: bool,
        cleanup_temp_after: bool,
        cleanup_older_than_hours: float,
        low_disk_mode: bool,
        min_free_gb: float,
        resume_free_gb: float,
        threads: int,
        memory_limit: str,
        target_rows_per_file: int,
        parallel_writers: int,
        ultra_mmap_mode: bool,
        ultra_workers: int,
        parser_mode: str,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.dump_path = dump_path
        self.temp_dir = temp_dir
        self.mode = mode
        self.batch_size = batch_size
        self.cleanup_temp_before = cleanup_temp_before
        self.cleanup_temp_after = cleanup_temp_after
        self.cleanup_older_than_hours = cleanup_older_than_hours
        self.low_disk_mode = low_disk_mode
        self.min_free_gb = min_free_gb
        self.resume_free_gb = resume_free_gb
        self.threads = threads
        self.memory_limit = memory_limit
        self.target_rows_per_file = target_rows_per_file
        self.parallel_writers = parallel_writers
        self.ultra_mmap_mode = ultra_mmap_mode
        self.ultra_workers = ultra_workers
        self.parser_mode = parser_mode

    def run(self) -> None:
        try:
            stats = import_dump(
                db_path=self.db_path,
                dump_path=self.dump_path,
                temp_dir=self.temp_dir,
                batch_size=self.batch_size,
                mode=self.mode,
                cleanup_temp_before=self.cleanup_temp_before,
                cleanup_temp_after=self.cleanup_temp_after,
                cleanup_older_than_hours=self.cleanup_older_than_hours,
                low_disk_mode=self.low_disk_mode,
                min_free_gb=self.min_free_gb,
                resume_free_gb=self.resume_free_gb,
                low_disk_poll_seconds=LOW_DISK_POLL_SECONDS,
                progress_cb=lambda data: self.progress.emit(data),
                threads=self.threads,
                memory_limit=self.memory_limit,
                target_rows_per_file=self.target_rows_per_file,
                parallel_writers=self.parallel_writers,
                ultra_mmap_mode=self.ultra_mmap_mode,
                ultra_workers=self.ultra_workers,
                parser_mode=self.parser_mode,
            )
            self.finished.emit(
                {
                    "systems_seen": stats.systems_seen,
                    "systems_written": stats.systems_written,
                    "elapsed_s": round(stats.elapsed_s, 2),
                    "active_elapsed_s": round(stats.active_elapsed_s, 2),
                    "systems_per_sec": round(stats.systems_per_sec, 2),
                    "progress_ratio": round(stats.progress_ratio, 4),
                    "paused_seconds": round(stats.paused_seconds, 2),
                    "temp_files_deleted": stats.temp_files_deleted,
                    "temp_bytes_deleted": stats.temp_bytes_deleted,
                }
            )
        except Exception as exc:
            self.error.emit(str(exc))


class PlannerWorker(QObject):
    progress = Signal(dict)
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        *,
        mode: str,
        db_path: str,
        temp_dir: str,
        references: list[str],
        radius_ly: float,
        body_filter: str,
        claimable_only: bool,
        limit: int,
        webhook_url: str = "",
        preview_messages: dict[str, list[str]] | None = None,
    ) -> None:
        super().__init__()
        self.mode = mode
        self.db_path = db_path
        self.temp_dir = temp_dir
        self.references = references
        self.radius_ly = radius_ly
        self.body_filter = body_filter
        self.claimable_only = claimable_only
        self.limit = limit
        self.webhook_url = webhook_url
        self.preview_messages = preview_messages or {}

    def run(self) -> None:
        try:
            if self.mode == "query":
                self._run_query()
            elif self.mode == "send":
                self._run_send()
            else:
                raise ValueError(f"Unbekannter Planner-Worker-Modus: {self.mode}")
        except Exception as exc:
            self.error.emit(str(exc))

    def _emit(self, **data: object) -> None:
        self.progress.emit(dict(data))

    def _run_query(self) -> None:
        total_refs = max(1, len(self.references))
        results: dict[str, list[dict]] = {}
        flattened: list[dict] = []
        total_hits = 0

        self._emit(step="db", message="DB-Zugriff wird vorbereitet", current=0, total=total_refs, ratio=0.02)
        for idx, reference in enumerate(self.references, start=1):
            base_ratio = (idx - 1) / total_refs
            self._emit(
                step="select",
                message=f"SELECT für Referenzsystem {reference}",
                current=idx,
                total=total_refs,
                reference=reference,
                ratio=min(0.9, base_ratio + 0.08),
                references_done=idx - 1,
                total_hits=total_hits,
            )
            rows = query_systems(
                self.db_path,
                reference,
                self.radius_ly,
                body_filter=self.body_filter,
                claimable_only=self.claimable_only,
                limit=self.limit,
                temp_dir=self.temp_dir,
                include_links=True,
            )
            results[reference] = rows
            flattened.extend(rows)
            total_hits += len(rows)
            self._emit(
                step="result_list",
                message=f"Ergebnisliste für {reference} wird erstellt",
                current=idx,
                total=total_refs,
                reference=reference,
                ratio=min(0.95, idx / total_refs),
                references_done=idx,
                total_hits=total_hits,
            )

        self._emit(
            step="done",
            message="Abfrage abgeschlossen",
            current=total_refs,
            total=total_refs,
            ratio=1.0,
            references_done=total_refs,
            total_hits=total_hits,
        )
        self.finished.emit({"mode": "query", "results": results, "rows": flattened})

    def _run_send(self) -> None:
        all_messages = [(ref, msg) for ref, msgs in self.preview_messages.items() for msg in msgs]
        total = max(1, len(all_messages))
        sent_ok = 0
        errors: list[str] = []
        by_reference: dict[str, list[dict]] = {}

        self._emit(step="discord_prepare", message="Discord-Versand wird vorbereitet", current=0, total=total, ratio=0.02)

        for idx, (reference, message) in enumerate(all_messages, start=1):
            self._emit(
                step="discord_send",
                message=f"Discord-Versand {idx}/{total} für {reference}",
                current=idx,
                total=total,
                ratio=(idx - 1) / total,
                reference=reference,
                discord_messages=idx - 1,
            )
            result = send_discord_messages(self.webhook_url, [message])
            entry = result[0] if result else {"ok": False, "error": "Unbekannter Fehler"}
            by_reference.setdefault(reference, []).append(entry)
            if entry.get("ok"):
                sent_ok += 1
            else:
                errors.append(f"{reference}: {entry.get('error', 'unbekannt')}")
            self._emit(
                step="discord_send",
                message=f"Discord-Versand {idx}/{total} abgeschlossen",
                current=idx,
                total=total,
                ratio=idx / total,
                reference=reference,
                discord_messages=idx,
                sent_ok=sent_ok,
                first_error=errors[0] if errors else "",
            )

        self.finished.emit(
            {
                "mode": "send",
                "results": by_reference,
                "sent_ok": sent_ok,
                "sent_total": len(all_messages),
                "errors": errors,
            }
        )


class ResultTableWindow(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Ergebnisliste der Abfrage")
        self.resize(1500, 900)
        layout = QVBoxLayout(self)
        self.summary_label = QLabel("Keine Daten")
        layout.addWidget(self.summary_label)

        self.table = QTableWidget(0, 14)
        self.table.setHorizontalHeaderLabels(
            [
                "Referenz", "System", "Distanz", "WW", "ELW", "HMCW", "Icy", "Rocky-Icy",
                "Gas Giants", "Rings", "Bodies", "Planeten", "Inara", "EDGIS",
            ]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        for col in range(2, 12):
            header.setSectionResizeMode(col, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(12, QHeaderView.Stretch)
        header.setSectionResizeMode(13, QHeaderView.Stretch)
        layout.addWidget(self.table, 1)

    def set_rows(self, rows: list[dict]) -> None:
        self.summary_label.setText(f"Treffer gesamt: {len(rows):,}")
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            values = [
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
                row.get("inara_url", ""),
                row.get("edgis_url", ""),
            ]
            for c, value in enumerate(values):
                self.table.setItem(r, c, QTableWidgetItem(str(value)))
        self.table.resizeRowsToContents()
        self.table.setSortingEnabled(True)


class MarkdownPreviewWindow(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Markdown-Vorschau der Discord-Nachrichten")
        self.resize(1400, 900)
        layout = QVBoxLayout(self)
        self.summary_label = QLabel("Keine Vorschau vorhanden")
        layout.addWidget(self.summary_label)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

    def set_messages(self, messages_by_reference: dict[str, list[str]]) -> None:
        self.tabs.clear()
        total_messages = sum(len(v) for v in messages_by_reference.values())
        self.summary_label.setText(f"Referenzsysteme: {len(messages_by_reference)} | Discord-Nachrichten: {total_messages}")

        for reference, messages in messages_by_reference.items():
            page = QWidget()
            page_layout = QVBoxLayout(page)
            text = QPlainTextEdit()
            text.setReadOnly(True)
            text.setLineWrapMode(QPlainTextEdit.NoWrap)
            text.setWordWrapMode(QTextOption.NoWrap)
            text.setFont(MONO_FONT)
            blocks = [f"===== {reference} | Nachricht {idx}/{len(messages)} =====\n{msg}" for idx, msg in enumerate(messages, start=1)]
            text.setPlainText("\n\n".join(blocks).strip())
            page_layout.addWidget(text)
            self.tabs.addTab(page, f"{reference} ({len(messages)})")


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Spansh Import + Planner")
        self.resize(1600, 980)
        self._import_thread: QThread | None = None
        self._import_worker: ImportWorker | None = None
        self._planner_thread: QThread | None = None
        self._planner_worker: PlannerWorker | None = None
        self._planner_mode: str | None = None
        self._rows: list[dict] = []
        self._planner_results: dict[str, list[dict]] = {}
        self._preview_messages: dict[str, list[str]] = {}
        self.results_window = ResultTableWindow(self)
        self.preview_window = MarkdownPreviewWindow(self)
        self._build_ui()

    def _wrap_scroll(self, widget: QWidget) -> QScrollArea:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        scroll.setWidget(widget)
        return scroll

    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        path_group = QGroupBox("Pfade")
        path_layout = QGridLayout(path_group)
        self.db_edit = QLineEdit(str(default_db_path()))
        self.temp_edit = QLineEdit(str(default_temp_dir()))
        self.dump_edit = QLineEdit()
        db_btn = QPushButton("DB wählen")
        temp_btn = QPushButton("Temp wählen")
        dump_btn = QPushButton("Dump wählen")
        db_btn.clicked.connect(self._pick_db)
        temp_btn.clicked.connect(self._pick_temp)
        dump_btn.clicked.connect(self._pick_dump)
        path_layout.addWidget(QLabel("DuckDB"), 0, 0)
        path_layout.addWidget(self.db_edit, 0, 1)
        path_layout.addWidget(db_btn, 0, 2)
        path_layout.addWidget(QLabel("Temp"), 1, 0)
        path_layout.addWidget(self.temp_edit, 1, 1)
        path_layout.addWidget(temp_btn, 1, 2)
        path_layout.addWidget(QLabel("Dump"), 2, 0)
        path_layout.addWidget(self.dump_edit, 2, 1)
        path_layout.addWidget(dump_btn, 2, 2)
        path_layout.setColumnStretch(1, 1)
        root.addWidget(path_group)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._wrap_scroll(self._build_import_tab()), "Import")
        self.tabs.addTab(self._wrap_scroll(self._build_planner_tab()), "Abfrage / Planner")
        root.addWidget(self.tabs, 1)

    def _build_import_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)

        imp_group = QGroupBox("Spansh-Import")
        imp = QGridLayout(imp_group)
        self.batch_spin = QSpinBox(); self.batch_spin.setRange(100, 500000); self.batch_spin.setValue(100000)
        self.threads_spin = QSpinBox(); self.threads_spin.setRange(1, 256); self.threads_spin.setValue(12)
        self.memory_edit = QLineEdit("32GB")
        self.target_rows_spin = QSpinBox(); self.target_rows_spin.setRange(10000, 10000000); self.target_rows_spin.setSingleStep(100000); self.target_rows_spin.setValue(1000000)
        self.parallel_writers_spin = QSpinBox(); self.parallel_writers_spin.setRange(1, 32); self.parallel_writers_spin.setValue(4)
        self.ultra_mmap_check = QCheckBox("Ultra mmap Mode"); self.ultra_mmap_check.setChecked(True)
        self.ultra_workers_spin = QSpinBox(); self.ultra_workers_spin.setRange(1, 32); self.ultra_workers_spin.setValue(4)
        self.cleanup_before_check = QCheckBox("Temp vor Import bereinigen")
        self.cleanup_after_check = QCheckBox("Temp nach Import bereinigen")
        self.cleanup_hours_spin = QDoubleSpinBox(); self.cleanup_hours_spin.setRange(0.0, 10000.0); self.cleanup_hours_spin.setValue(24.0); self.cleanup_hours_spin.setSuffix(" h")
        self.low_disk_check = QCheckBox("Low-Disk-Safe-Modus"); self.low_disk_check.setChecked(True)
        self.min_free_spin = QDoubleSpinBox(); self.min_free_spin.setRange(0.0, 4096.0); self.min_free_spin.setValue(20.0); self.min_free_spin.setSuffix(" GB")
        self.resume_free_spin = QDoubleSpinBox(); self.resume_free_spin.setRange(0.0, 4096.0); self.resume_free_spin.setValue(25.0); self.resume_free_spin.setSuffix(" GB")
        self.parser_combo = QComboBox(); self.parser_combo.addItems(["auto", "simdjson", "orjson", "json"]); self.parser_combo.setCurrentText("auto")

        init_btn = QPushButton("DB initialisieren")
        full_btn = QPushButton("Full Import")
        patch_btn = QPushButton("Patch Import")
        init_btn.clicked.connect(self._init_db)
        full_btn.clicked.connect(lambda: self._start_import("full"))
        patch_btn.clicked.connect(lambda: self._start_import("patch"))

        imp.addWidget(QLabel("Batch-Größe"), 0, 0); imp.addWidget(self.batch_spin, 0, 1)
        imp.addWidget(QLabel("Threads"), 0, 2); imp.addWidget(self.threads_spin, 0, 3)
        imp.addWidget(QLabel("Memory"), 1, 0); imp.addWidget(self.memory_edit, 1, 1)
        imp.addWidget(QLabel("Datei-Zielgröße (Rows)"), 1, 2); imp.addWidget(self.target_rows_spin, 1, 3)
        imp.addWidget(QLabel("Parallel Writer"), 2, 0); imp.addWidget(self.parallel_writers_spin, 2, 1)
        imp.addWidget(self.ultra_mmap_check, 2, 2); imp.addWidget(self.ultra_workers_spin, 2, 3)
        imp.addWidget(QLabel("JSON-Parser"), 3, 0); imp.addWidget(self.parser_combo, 3, 1)
        imp.addWidget(QLabel("Cleanup älter als"), 3, 2); imp.addWidget(self.cleanup_hours_spin, 3, 3)
        imp.addWidget(self.cleanup_before_check, 4, 0, 1, 2)
        imp.addWidget(self.cleanup_after_check, 4, 2, 1, 2)
        imp.addWidget(self.low_disk_check, 5, 0, 1, 2)
        imp.addWidget(QLabel("Min frei"), 5, 2); imp.addWidget(self.min_free_spin, 5, 3)
        imp.addWidget(QLabel("Resume frei"), 6, 2); imp.addWidget(self.resume_free_spin, 6, 3)
        poll_info = QLabel("Poll-Intervall ist nicht mehr separat konfigurierbar. Für den Low-Disk-Safe-Modus wird intern fest 10 s verwendet.")
        poll_info.setWordWrap(True)
        imp.addWidget(poll_info, 6, 0, 1, 2)
        imp.addWidget(init_btn, 7, 0)
        imp.addWidget(full_btn, 7, 2)
        imp.addWidget(patch_btn, 7, 3)
        imp.setColumnStretch(1, 1)
        layout.addWidget(imp_group)

        self.import_status_label = QLabel("Bereit")
        self.import_progress = QProgressBar(); self.import_progress.setRange(0, 1000)
        layout.addWidget(self.import_status_label)
        layout.addWidget(self.import_progress)

        progress_group = QGroupBox("Fortschritt")
        progress_layout = QVBoxLayout(progress_group)
        self.progress_table = QTableWidget(8, 4)
        self.progress_table.setHorizontalHeaderLabels(["Bezeichnung", "Wert", "Bezeichnung", "Wert"])
        self.progress_table.verticalHeader().setVisible(False)
        self.progress_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.progress_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.progress_table.setFocusPolicy(Qt.NoFocus)
        self.progress_table.setAlternatingRowColors(True)
        self.progress_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.progress_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.progress_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.progress_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.progress_table.setMinimumHeight(200)
        progress_layout.addWidget(self.progress_table)

        self.worker_table = QTableWidget(0, 6)
        self.worker_table.setHorizontalHeaderLabels(["Worker", "gesehen", "geschrieben", "Bytes", "Parser", "Range"])
        self.worker_table.verticalHeader().setVisible(False)
        self.worker_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.worker_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.worker_table.setAlternatingRowColors(True)
        for col in range(5):
            self.worker_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.worker_table.setMinimumHeight(160)
        progress_layout.addWidget(self.worker_table)
        layout.addWidget(progress_group)

        debug_group = QGroupBox("Debug / JSON")
        debug_layout = QVBoxLayout(debug_group)
        self.stats_text = QPlainTextEdit()
        self.stats_text.setReadOnly(True)
        self.stats_text.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.stats_text.setWordWrapMode(QTextOption.NoWrap)
        self.stats_text.setFont(MONO_FONT)
        self.stats_text.setMinimumHeight(180)
        debug_layout.addWidget(self.stats_text)
        layout.addWidget(debug_group)

        self._fill_progress_pairs([])
        return page

    def _build_planner_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)

        planner_group = QGroupBox("Planner")
        planner = QGridLayout(planner_group)
        self.single_ref_edit = QLineEdit("Sol")
        self.multi_ref_edit = QPlainTextEdit(); self.multi_ref_edit.setPlaceholderText("Weitere Referenz-Systeme, eines pro Zeile"); self.multi_ref_edit.setMaximumHeight(110)
        self.radius_spin = QDoubleSpinBox(); self.radius_spin.setRange(0.1, 100000.0); self.radius_spin.setValue(50.0); self.radius_spin.setSuffix(" ly")
        self.filter_combo = QComboBox(); self.filter_combo.addItems(["any", "ww", "elw", "ww_or_elw", "ww_and_elw"]); self.filter_combo.setCurrentText("ww_or_elw")
        self.claimable_check = QCheckBox("Nur claimable")
        self.limit_spin = QSpinBox(); self.limit_spin.setRange(1, 100000); self.limit_spin.setValue(200)
        self.webhook_edit = QLineEdit(); self.webhook_edit.setPlaceholderText("Discord Webhook URL")

        run_btn = QPushButton("Abfrage ausführen")
        export_btn = QPushButton("CSV exportieren")
        preview_btn = QPushButton("Preview aktualisieren")
        send_btn = QPushButton("An Discord senden")
        result_window_btn = QPushButton("Ergebnisliste öffnen")
        preview_window_btn = QPushButton("Markdown-Vorschau öffnen")
        run_btn.clicked.connect(self._run_planner_query)
        export_btn.clicked.connect(self._export_planner_csv)
        preview_btn.clicked.connect(self._refresh_preview_only)
        send_btn.clicked.connect(self._send_preview_to_discord)
        result_window_btn.clicked.connect(self._show_results_window)
        preview_window_btn.clicked.connect(self._show_preview_window)

        planner.addWidget(QLabel("Referenzsystem"), 0, 0); planner.addWidget(self.single_ref_edit, 0, 1)
        planner.addWidget(QLabel("Radius"), 0, 2); planner.addWidget(self.radius_spin, 0, 3)
        planner.addWidget(QLabel("Weitere Referenzsysteme"), 1, 0); planner.addWidget(self.multi_ref_edit, 1, 1, 2, 3)
        planner.addWidget(QLabel("Filter"), 3, 0); planner.addWidget(self.filter_combo, 3, 1)
        planner.addWidget(QLabel("Limit je Referenzsystem"), 3, 2); planner.addWidget(self.limit_spin, 3, 3)
        planner.addWidget(self.claimable_check, 4, 0, 1, 2)
        planner.addWidget(QLabel("Discord Webhook"), 5, 0); planner.addWidget(self.webhook_edit, 5, 1, 1, 3)
        planner.addWidget(run_btn, 6, 0)
        planner.addWidget(export_btn, 6, 1)
        planner.addWidget(preview_btn, 6, 2)
        planner.addWidget(send_btn, 6, 3)
        planner.addWidget(result_window_btn, 7, 0, 1, 2)
        planner.addWidget(preview_window_btn, 7, 2, 1, 2)
        planner.setColumnStretch(1, 1)
        planner.setColumnStretch(3, 1)
        layout.addWidget(planner_group)

        status_group = QGroupBox("Status der Abfrage / des Discord-Versands")
        status_layout = QVBoxLayout(status_group)
        self.planner_status_label = QLabel("Bereit")
        self.planner_progress = QProgressBar(); self.planner_progress.setRange(0, 1000)
        status_layout.addWidget(self.planner_status_label)
        status_layout.addWidget(self.planner_progress)

        self.planner_status_table = QTableWidget(6, 4)
        self.planner_status_table.setHorizontalHeaderLabels(["Bezeichnung", "Wert", "Bezeichnung", "Wert"])
        self.planner_status_table.verticalHeader().setVisible(False)
        self.planner_status_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.planner_status_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.planner_status_table.setFocusPolicy(Qt.NoFocus)
        self.planner_status_table.setAlternatingRowColors(True)
        self.planner_status_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.planner_status_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.planner_status_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.planner_status_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        status_layout.addWidget(self.planner_status_table)

        self.planner_log_text = QPlainTextEdit()
        self.planner_log_text.setReadOnly(True)
        self.planner_log_text.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.planner_log_text.setWordWrapMode(QTextOption.NoWrap)
        self.planner_log_text.setFont(MONO_FONT)
        self.planner_log_text.setMinimumHeight(220)
        status_layout.addWidget(self.planner_log_text)
        layout.addWidget(status_group)

        hint = QLabel("Die Ergebnisliste und die Markdown-Vorschau werden in eigenen Fenstern geöffnet.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self._fill_planner_status_pairs([])
        return page

    def _pick_db(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "DuckDB wählen", self.db_edit.text(), "DuckDB (*.duckdb)")
        if path:
            self.db_edit.setText(path)

    def _pick_temp(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Temp-Verzeichnis wählen", self.temp_edit.text())
        if path:
            self.temp_edit.setText(path)

    def _pick_dump(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Dump wählen", "", "Dump/Parquet (*.json *.json.gz *.json.zst *.parquet *.gz *.zst)")
        if path:
            self.dump_edit.setText(path)

    def _init_db(self) -> None:
        try:
            path = init_db(
                self.db_edit.text().strip(),
                temp_dir=self.temp_edit.text().strip(),
                threads=self.threads_spin.value(),
                memory_limit=self.memory_edit.text().strip() or "32GB",
            )
            settings = get_db_settings(
                self.db_edit.text().strip(),
                temp_dir=self.temp_edit.text().strip(),
                threads=self.threads_spin.value(),
                memory_limit=self.memory_edit.text().strip() or "32GB",
            )
            QMessageBox.information(self, "DB", f"DB initialisiert: {path}\nThreads: {settings['threads']}\nMemory: {settings['memory_limit']}")
        except Exception as exc:
            QMessageBox.critical(self, "Fehler", str(exc))

    def _start_import(self, mode: str) -> None:
        if self._import_thread is not None:
            QMessageBox.information(self, "Import läuft", "Es läuft bereits ein Import.")
            return
        db_path = self.db_edit.text().strip(); dump_path = self.dump_edit.text().strip(); temp_dir = self.temp_edit.text().strip()
        if not db_path or not dump_path or not temp_dir:
            QMessageBox.warning(self, "Fehlende Eingabe", "Bitte DB-, Temp- und Dump-Pfad angeben.")
            return

        self.import_progress.setValue(0)
        self.import_status_label.setText("Import läuft")
        self.stats_text.clear()
        self._fill_progress_pairs([])
        self.worker_table.setRowCount(0)

        self._import_thread = QThread(self)
        self._import_worker = ImportWorker(
            db_path,
            dump_path,
            temp_dir,
            mode,
            self.batch_spin.value(),
            self.cleanup_before_check.isChecked(),
            self.cleanup_after_check.isChecked(),
            self.cleanup_hours_spin.value(),
            self.low_disk_check.isChecked(),
            self.min_free_spin.value(),
            self.resume_free_spin.value(),
            self.threads_spin.value(),
            self.memory_edit.text().strip() or "32GB",
            self.target_rows_spin.value(),
            self.parallel_writers_spin.value(),
            self.ultra_mmap_check.isChecked(),
            self.ultra_workers_spin.value(),
            self.parser_combo.currentText(),
        )
        self._import_worker.moveToThread(self._import_thread)
        self._import_thread.started.connect(self._import_worker.run)
        self._import_worker.progress.connect(self._on_import_progress)
        self._import_worker.finished.connect(self._on_import_finished)
        self._import_worker.finished.connect(self._cleanup_import_thread)
        self._import_worker.error.connect(self._on_import_error)
        self._import_worker.error.connect(self._cleanup_import_thread)
        self._import_thread.start()

    def _on_import_progress(self, data: dict) -> None:
        ratio = float(data.get("progress_ratio", 0) or 0)
        self.import_progress.setValue(max(0, min(1000, int(ratio * 1000))))
        bytes_read = int(data.get("bytes_read", 0) or 0)
        dump_size = int(data.get("dump_size_bytes", 0) or 0)
        eta = data.get("eta_seconds")
        phase = str(data.get("phase", "-") or "-")
        pairs = [
            ("Phase", phase), ("Meldung", str(data.get("message", "-"))),
            ("Parser aktiv", str(data.get("parser_active", "-"))), ("Parser angefordert", str(data.get("parser_requested", "-"))),
            ("Fortschritt", f"{ratio * 100:.2f} %"), ("ETA", self._fmt_eta(eta)),
            ("Systeme gesehen", f"{int(data.get('systems_seen', 0) or 0):,}"), ("Systeme geschrieben", f"{int(data.get('systems_written', 0) or 0):,}"),
            ("Systeme / s", f"{float(data.get('systems_per_sec', 0) or 0):.1f}"), ("Verarbeitete Bytes", f"{self._fmt_bytes(bytes_read)} / {self._fmt_bytes(dump_size)}"),
            ("Dateigröße", self._fmt_bytes(data.get("file_size_bytes") or dump_size)), ("Ultra mmap / Worker", f"{data.get('ultra_mmap_mode', False)} / {data.get('ultra_workers', '-') }"),
            ("JSON-Array erkannt", str(data.get("json_array_detected", "-"))), ("Objektgrenzen / Segmente", f"{int(data.get('prep_found_object_boundaries', 0) or 0):,} / {int(data.get('prep_worker_segments_built', 0) or 0)}"),
            ("Freier Speicher DB / Temp", f"{float(data.get('free_db_gb', 0) or 0):.1f} GB / {float(data.get('free_temp_gb', 0) or 0):.1f} GB"), ("Pause gesamt", f"{float(data.get('paused_seconds', 0) or 0):.1f} s"),
        ]
        self._fill_progress_pairs(pairs)
        self._fill_worker_table(data)
        self.stats_text.setPlainText(json.dumps(data, indent=2, ensure_ascii=False))
        mode_tag = "Ultra mmap" if data.get("ultra_mmap_mode") else "Standard"
        self.import_status_label.setText(f"{mode_tag} | {phase} | {ratio * 100:.1f}% | {data.get('message', '')}")

    def _on_import_finished(self, data: dict) -> None:
        self.import_progress.setValue(1000)
        self.stats_text.setPlainText(json.dumps(data, indent=2, ensure_ascii=False))
        self.import_status_label.setText("Import abgeschlossen")
        self._fill_progress_pairs([
            ("Status", "Abgeschlossen"), ("Systeme gesehen", f"{int(data.get('systems_seen', 0) or 0):,}"),
            ("Systeme geschrieben", f"{int(data.get('systems_written', 0) or 0):,}"), ("Dauer", f"{float(data.get('elapsed_s', 0) or 0):.2f} s"),
            ("Aktive Dauer", f"{float(data.get('active_elapsed_s', 0) or 0):.2f} s"), ("Systeme / s", f"{float(data.get('systems_per_sec', 0) or 0):.2f}"),
            ("Temp-Dateien gelöscht", str(data.get("temp_files_deleted", 0))), ("Temp-Bytes gelöscht", self._fmt_bytes(data.get("temp_bytes_deleted", 0))),
        ])
        QMessageBox.information(self, "Fertig", "Import abgeschlossen")

    def _on_import_error(self, message: str) -> None:
        self.import_status_label.setText("Import fehlgeschlagen")
        self.stats_text.setPlainText(message)
        QMessageBox.critical(self, "Importfehler", message)

    def _cleanup_import_thread(self) -> None:
        if self._import_thread is not None:
            self._import_thread.quit()
            self._import_thread.wait(2000)
            self._import_thread.deleteLater()
        self._import_thread = None
        self._import_worker = None

    def _reference_systems(self) -> list[str]:
        refs: list[str] = []
        primary = self.single_ref_edit.text().strip()
        if primary:
            refs.append(primary)
        for line in self.multi_ref_edit.toPlainText().splitlines():
            line = line.strip()
            if line and line not in refs:
                refs.append(line)
        return refs

    def _run_planner_query(self) -> None:
        refs = self._reference_systems()
        if not refs:
            QMessageBox.warning(self, "Fehlende Eingabe", "Bitte mindestens ein Referenzsystem angeben.")
            return
        if self._planner_thread is not None:
            QMessageBox.information(self, "Planner", "Es läuft bereits eine Abfrage oder ein Discord-Versand.")
            return
        self._planner_mode = "query"
        self.planner_log_text.clear()
        self.planner_progress.setValue(0)
        self.planner_status_label.setText("Abfrage läuft")
        self._fill_planner_status_pairs([])
        self._start_planner_worker(
            PlannerWorker(
                mode="query",
                db_path=self.db_edit.text().strip(),
                temp_dir=self.temp_edit.text().strip(),
                references=refs,
                radius_ly=self.radius_spin.value(),
                body_filter=self.filter_combo.currentText(),
                claimable_only=self.claimable_check.isChecked(),
                limit=self.limit_spin.value(),
            )
        )

    def _refresh_preview_only(self) -> None:
        if not self._planner_results:
            QMessageBox.information(self, "Preview", "Noch keine Abfrageergebnisse vorhanden.")
            return
        self._build_preview_messages()
        self._show_preview_window()

    def _build_preview_messages(self) -> None:
        self._preview_messages = {}
        for reference_system, rows in self._planner_results.items():
            self._preview_messages[reference_system] = format_discord_messages(
                reference_system,
                rows,
                radius_ly=self.radius_spin.value(),
                body_filter=self.filter_combo.currentText(),
                claimable_only=self.claimable_check.isChecked(),
                limit=self.limit_spin.value(),
            )
        self.preview_window.set_messages(self._preview_messages)

    def _send_preview_to_discord(self) -> None:
        webhook = self.webhook_edit.text().strip()
        if not webhook:
            QMessageBox.warning(self, "Discord", "Bitte eine Discord Webhook URL angeben.")
            return
        if not self._preview_messages:
            if not self._planner_results:
                QMessageBox.information(self, "Discord", "Noch keine Abfrageergebnisse vorhanden.")
                return
            self._build_preview_messages()
        if self._planner_thread is not None:
            QMessageBox.information(self, "Discord", "Es läuft bereits eine Abfrage oder ein Discord-Versand.")
            return

        self._planner_mode = "send"
        self.planner_progress.setValue(0)
        self.planner_status_label.setText("Discord-Versand läuft")
        self.planner_log_text.appendPlainText("Discord-Versand gestartet")
        self._start_planner_worker(
            PlannerWorker(
                mode="send",
                db_path=self.db_edit.text().strip(),
                temp_dir=self.temp_edit.text().strip(),
                references=[],
                radius_ly=self.radius_spin.value(),
                body_filter=self.filter_combo.currentText(),
                claimable_only=self.claimable_check.isChecked(),
                limit=self.limit_spin.value(),
                webhook_url=webhook,
                preview_messages=self._preview_messages,
            )
        )

    def _start_planner_worker(self, worker: PlannerWorker) -> None:
        self._planner_thread = QThread(self)
        self._planner_worker = worker
        self._planner_worker.moveToThread(self._planner_thread)
        self._planner_thread.started.connect(self._planner_worker.run)
        self._planner_worker.progress.connect(self._on_planner_progress)
        self._planner_worker.finished.connect(self._on_planner_finished)
        self._planner_worker.finished.connect(self._cleanup_planner_thread)
        self._planner_worker.error.connect(self._on_planner_error)
        self._planner_worker.error.connect(self._cleanup_planner_thread)
        self._planner_thread.start()

    def _on_planner_progress(self, data: dict) -> None:
        ratio = float(data.get("ratio", 0) or 0)
        self.planner_progress.setValue(max(0, min(1000, int(ratio * 1000))))
        step = str(data.get("step", "-") or "-")
        message = str(data.get("message", "-") or "-")
        ref = str(data.get("reference", "") or "")
        current = int(data.get("current", 0) or 0)
        total = int(data.get("total", 0) or 0)
        first_error = str(data.get("first_error", "") or "")
        pairs = [
            ("Schritt", step), ("Meldung", message),
            ("Fortschritt", f"{ratio * 100:.1f} %"), ("Referenzsystem", ref or "-"),
            ("Bearbeitet", f"{current} / {total}" if total else str(current)), ("Treffer gesamt", f"{int(data.get('total_hits', 0) or 0):,}"),
            ("Discord-Nachrichten", str(int(data.get("discord_messages", 0) or 0))), ("Erfolgreich gesendet", str(int(data.get("sent_ok", 0) or 0))),
            ("Erster Fehler", first_error or "-"), ("DB-Datei", Path(self.db_edit.text().strip()).name or "-"),
            ("Filter", self.filter_combo.currentText()), ("Claimable", "ja" if self.claimable_check.isChecked() else "nein"),
        ]
        self._fill_planner_status_pairs(pairs)
        self.planner_status_label.setText(message)
        log_line = f"[{step}] {message}"
        if ref:
            log_line += f" | Referenz: {ref}"
        self.planner_log_text.appendPlainText(log_line)

    def _on_planner_finished(self, data: dict) -> None:
        self.planner_progress.setValue(1000)
        mode = data.get("mode")
        if mode == "query":
            self._planner_results = data.get("results", {}) or {}
            self._rows = data.get("rows", []) or []
            self.results_window.set_rows(self._rows)
            self._build_preview_messages()
            self.planner_status_label.setText(f"Abfrage abgeschlossen. Treffer: {len(self._rows):,}")
            self._fill_planner_status_pairs([
                ("Status", "Abgeschlossen"), ("Treffer gesamt", f"{len(self._rows):,}"),
                ("Referenzsysteme", str(len(self._planner_results))), ("Discord-Nachrichten", str(sum(len(v) for v in self._preview_messages.values()))),
            ])
            self._show_results_window()
            self._show_preview_window()
        elif mode == "send":
            sent_ok = int(data.get("sent_ok", 0) or 0)
            sent_total = int(data.get("sent_total", 0) or 0)
            errors = data.get("errors", []) or []
            self.planner_status_label.setText(f"Discord-Versand abgeschlossen: {sent_ok}/{sent_total}")
            self._fill_planner_status_pairs([
                ("Status", "Discord-Versand abgeschlossen"), ("Gesendet", f"{sent_ok}/{sent_total}"),
                ("Fehler", str(len(errors))), ("Erster Fehler", errors[0] if errors else "-"),
            ])
            if errors:
                QMessageBox.warning(self, "Discord", "\n".join(errors[:10]))
            else:
                QMessageBox.information(self, "Discord", f"Alle {sent_total} Nachrichten wurden erfolgreich gesendet.")

    def _on_planner_error(self, message: str) -> None:
        self.planner_status_label.setText("Fehler")
        self.planner_log_text.appendPlainText(f"[error] {message}")
        QMessageBox.critical(self, "Planner-Fehler", message)

    def _cleanup_planner_thread(self) -> None:
        if self._planner_thread is not None:
            self._planner_thread.quit()
            self._planner_thread.wait(2000)
            self._planner_thread.deleteLater()
        self._planner_thread = None
        self._planner_worker = None
        self._planner_mode = None

    def _show_results_window(self) -> None:
        if not self._rows:
            QMessageBox.information(self, "Ergebnisliste", "Noch keine Abfrageergebnisse vorhanden.")
            return
        self.results_window.set_rows(self._rows)
        self.results_window.show()
        self.results_window.raise_()
        self.results_window.activateWindow()

    def _show_preview_window(self) -> None:
        if not self._preview_messages:
            QMessageBox.information(self, "Markdown-Vorschau", "Noch keine Vorschau vorhanden.")
            return
        self.preview_window.set_messages(self._preview_messages)
        self.preview_window.show()
        self.preview_window.raise_()
        self.preview_window.activateWindow()

    def _export_planner_csv(self) -> None:
        if not self._rows:
            QMessageBox.information(self, "Export", "Keine Daten vorhanden.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "CSV exportieren", str(Path.cwd() / "planner_results.csv"), "CSV (*.csv)")
        if not path:
            return
        try:
            write_csv(self._rows, path)
            QMessageBox.information(self, "Export", f"CSV geschrieben: {path}")
        except Exception as exc:
            QMessageBox.critical(self, "Fehler", str(exc))

    def _fill_progress_pairs(self, pairs: list[tuple[str, str]]) -> None:
        cells = 8 * 2
        items = pairs[:cells]
        while len(items) < cells:
            items.append(("", ""))
        idx = 0
        for row in range(8):
            left_label, left_value = items[idx]
            right_label, right_value = items[idx + 1]
            self.progress_table.setItem(row, 0, QTableWidgetItem(left_label))
            self.progress_table.setItem(row, 1, QTableWidgetItem(left_value))
            self.progress_table.setItem(row, 2, QTableWidgetItem(right_label))
            self.progress_table.setItem(row, 3, QTableWidgetItem(right_value))
            idx += 2
        self.progress_table.resizeRowsToContents()

    def _fill_planner_status_pairs(self, pairs: list[tuple[str, str]]) -> None:
        cells = 6 * 2
        items = pairs[:cells]
        while len(items) < cells:
            items.append(("", ""))
        idx = 0
        for row in range(6):
            left_label, left_value = items[idx]
            right_label, right_value = items[idx + 1]
            self.planner_status_table.setItem(row, 0, QTableWidgetItem(left_label))
            self.planner_status_table.setItem(row, 1, QTableWidgetItem(left_value))
            self.planner_status_table.setItem(row, 2, QTableWidgetItem(right_label))
            self.planner_status_table.setItem(row, 3, QTableWidgetItem(right_value))
            idx += 2
        self.planner_status_table.resizeRowsToContents()

    def _fill_worker_table(self, data: dict) -> None:
        workers = data.get("workers") or []
        ranges = data.get("worker_ranges") or []
        range_map = {f"W{idx}": value for idx, value in enumerate(ranges)}
        self.worker_table.setRowCount(len(workers))
        for row_idx, worker in enumerate(workers):
            name = str(worker.get("name", f"W{row_idx}"))
            values = [
                name,
                f"{int(worker.get('seen', 0) or 0):,}",
                f"{int(worker.get('written', 0) or 0):,}",
                self._fmt_bytes(worker.get("bytes", 0)),
                str(worker.get("parser", "-")),
                str(range_map.get(name, "")),
            ]
            for col_idx, value in enumerate(values):
                self.worker_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.worker_table.resizeRowsToContents()

    @staticmethod
    def _fmt_bytes(value: int | float | None) -> str:
        try:
            num = float(value or 0)
        except Exception:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        while num >= 1024 and idx < len(units) - 1:
            num /= 1024.0
            idx += 1
        return f"{num:.2f} {units[idx]}"

    @staticmethod
    def _fmt_eta(value: object) -> str:
        if value in (None, "", False):
            return "-"
        try:
            seconds = int(float(value))
        except Exception:
            return "-"
        if seconds < 60:
            return f"{seconds}s"
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours}:{minutes:02d}:{sec:02d}"


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
