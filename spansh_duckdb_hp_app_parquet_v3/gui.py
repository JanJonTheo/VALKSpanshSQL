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
    QSpinBox,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QDoubleSpinBox,
)

from .config import default_db_path, default_temp_dir
from .db import get_db_settings, init_db
from .importer import import_dump
from .query import (
    format_discord_messages,
    query_multiple_systems,
    send_discord_messages,
    write_csv,
)

LOW_DISK_POLL_SECONDS = 10.0


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
        db_path: str,
        temp_dir: str,
        refs: list[str],
        radius_ly: float,
        body_filter: str,
        claimable_only: bool,
        limit: int,
        webhook_url: str = "",
        send_to_discord: bool = False,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.temp_dir = temp_dir
        self.refs = refs
        self.radius_ly = radius_ly
        self.body_filter = body_filter
        self.claimable_only = claimable_only
        self.limit = limit
        self.webhook_url = webhook_url
        self.send_to_discord = send_to_discord

    def _emit(self, step: int, total_steps: int, phase: str, message: str, **extra: object) -> None:
        total_steps = max(1, total_steps)
        ratio = max(0.0, min(1.0, step / total_steps))
        payload = {
            "step": step,
            "total_steps": total_steps,
            "progress_ratio": ratio,
            "phase": phase,
            "message": message,
        }
        payload.update(extra)
        self.progress.emit(payload)

    def run(self) -> None:
        try:
            total_steps = max(4, len(self.refs) + 4)
            step = 0
            self._emit(step, total_steps, "start", "Abfrage/Planer wird vorbereitet", refs=len(self.refs))

            step += 1
            self._emit(step, total_steps, "db", "DB-Zugriff wird initialisiert")

            results: dict[str, list[dict]] = {}
            all_rows: list[dict] = []
            for idx, ref in enumerate(self.refs, start=1):
                step += 1
                self._emit(
                    step,
                    total_steps,
                    "select",
                    f"SELECT für Referenzsystem {ref} wird ausgeführt",
                    current_reference=ref,
                    reference_index=idx,
                    reference_total=len(self.refs),
                )
                rows = query_multiple_systems(
                    self.db_path,
                    [ref],
                    self.radius_ly,
                    body_filter=self.body_filter,
                    claimable_only=self.claimable_only,
                    limit=self.limit,
                    temp_dir=self.temp_dir,
                    include_links=True,
                ).get(ref, [])
                results[ref] = rows
                all_rows.extend(rows)
                self._emit(
                    step,
                    total_steps,
                    "select",
                    f"Ergebnisliste für {ref} erstellt",
                    current_reference=ref,
                    rows_for_reference=len(rows),
                    total_rows=len(all_rows),
                    reference_index=idx,
                    reference_total=len(self.refs),
                )

            step += 1
            self._emit(step, total_steps, "preview", "Discord-Vorschau wird aufgebaut", total_rows=len(all_rows))
            preview_messages: dict[str, list[str]] = {}
            for ref, rows in results.items():
                preview_messages[ref] = format_discord_messages(
                    ref,
                    rows,
                    radius_ly=self.radius_ly,
                    body_filter=self.body_filter,
                    claimable_only=self.claimable_only,
                    limit=self.limit,
                )

            send_results: dict[str, list[dict]] = {}
            if self.send_to_discord:
                total_messages = sum(len(v) for v in preview_messages.values())
                if not self.webhook_url:
                    raise ValueError("Bitte eine Discord Webhook URL angeben.")
                step += 1
                sent_messages = 0
                for ref, messages in preview_messages.items():
                    self._emit(
                        step,
                        total_steps,
                        "discord",
                        f"Discord-Versand für {ref} wird gestartet",
                        current_reference=ref,
                        total_messages=total_messages,
                        sent_messages=sent_messages,
                    )
                    result = send_discord_messages(self.webhook_url, messages)
                    send_results[ref] = result
                    sent_messages += len(messages)
                    first_error = next((item.get("error") for item in result if not item.get("ok")), "")
                    self._emit(
                        step,
                        total_steps,
                        "discord",
                        f"Discord-Versand für {ref} abgeschlossen",
                        current_reference=ref,
                        total_messages=total_messages,
                        sent_messages=sent_messages,
                        first_error=first_error,
                    )

            step = total_steps
            self._emit(step, total_steps, "done", "Abfrage/Planer abgeschlossen", total_rows=len(all_rows))
            self.finished.emit({
                "results": results,
                "rows": all_rows,
                "preview_messages": preview_messages,
                "send_results": send_results,
                "sent": self.send_to_discord,
            })
        except Exception as exc:
            self.error.emit(str(exc))


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Spansh Import + Planner")
        self.resize(1920, 1180)
        self._thread: QThread | None = None
        self._worker: ImportWorker | None = None
        self._planner_thread: QThread | None = None
        self._planner_worker: PlannerWorker | None = None
        self._rows: list[dict] = []
        self._planner_results: dict[str, list[dict]] = {}
        self._preview_messages: dict[str, list[str]] = {}
        self._build_ui()

    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

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
        root.addWidget(path_group)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_import_tab(), "Import")
        self.tabs.addTab(self._build_planner_tab(), "Abfrage / Planner")
        root.addWidget(self.tabs, 1)

    def _build_import_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)

        imp_group = QGroupBox("Spansh-Import")
        imp = QGridLayout(imp_group)
        self.batch_spin = QSpinBox()
        self.batch_spin.setRange(100, 500000)
        self.batch_spin.setValue(100000)
        self.threads_spin = QSpinBox()
        self.threads_spin.setRange(1, 256)
        self.threads_spin.setValue(12)
        self.memory_edit = QLineEdit("32GB")
        self.target_rows_spin = QSpinBox()
        self.target_rows_spin.setRange(10000, 10000000)
        self.target_rows_spin.setSingleStep(100000)
        self.target_rows_spin.setValue(1000000)
        self.parallel_writers_spin = QSpinBox()
        self.parallel_writers_spin.setRange(1, 32)
        self.parallel_writers_spin.setValue(4)
        self.ultra_mmap_check = QCheckBox("Ultra mmap Mode")
        self.ultra_mmap_check.setChecked(True)
        self.ultra_workers_spin = QSpinBox()
        self.ultra_workers_spin.setRange(1, 32)
        self.ultra_workers_spin.setValue(4)
        self.cleanup_before_check = QCheckBox("Temp vor Import bereinigen")
        self.cleanup_after_check = QCheckBox("Temp nach Import bereinigen")
        self.cleanup_hours_spin = QDoubleSpinBox()
        self.cleanup_hours_spin.setRange(0.0, 10000.0)
        self.cleanup_hours_spin.setValue(24.0)
        self.cleanup_hours_spin.setSuffix(" h")
        self.low_disk_check = QCheckBox("Low-Disk-Safe-Modus")
        self.low_disk_check.setChecked(True)
        self.min_free_spin = QDoubleSpinBox()
        self.min_free_spin.setRange(0.0, 4096.0)
        self.min_free_spin.setValue(20.0)
        self.min_free_spin.setSuffix(" GB")
        self.resume_free_spin = QDoubleSpinBox()
        self.resume_free_spin.setRange(0.0, 4096.0)
        self.resume_free_spin.setValue(25.0)
        self.resume_free_spin.setSuffix(" GB")
        self.parser_combo = QComboBox()
        self.parser_combo.addItems(["auto", "simdjson", "orjson", "json"])
        self.parser_combo.setCurrentText("auto")

        init_btn = QPushButton("DB initialisieren")
        full_btn = QPushButton("Full Import")
        patch_btn = QPushButton("Patch Import")
        init_btn.clicked.connect(self._init_db)
        full_btn.clicked.connect(lambda: self._start_import("full"))
        patch_btn.clicked.connect(lambda: self._start_import("patch"))

        imp.addWidget(QLabel("Batch-Größe"), 0, 0)
        imp.addWidget(self.batch_spin, 0, 1)
        imp.addWidget(QLabel("Threads"), 0, 2)
        imp.addWidget(self.threads_spin, 0, 3)
        imp.addWidget(QLabel("Memory"), 1, 0)
        imp.addWidget(self.memory_edit, 1, 1)
        imp.addWidget(QLabel("Datei-Zielgröße (Rows)"), 1, 2)
        imp.addWidget(self.target_rows_spin, 1, 3)
        imp.addWidget(QLabel("Parallel Writer"), 2, 0)
        imp.addWidget(self.parallel_writers_spin, 2, 1)
        imp.addWidget(self.ultra_mmap_check, 2, 2)
        imp.addWidget(self.ultra_workers_spin, 2, 3)
        imp.addWidget(QLabel("JSON-Parser"), 3, 0)
        imp.addWidget(self.parser_combo, 3, 1)
        imp.addWidget(QLabel("Cleanup älter als"), 3, 2)
        imp.addWidget(self.cleanup_hours_spin, 3, 3)
        imp.addWidget(self.cleanup_before_check, 4, 0, 1, 2)
        imp.addWidget(self.cleanup_after_check, 4, 2, 1, 2)
        imp.addWidget(self.low_disk_check, 5, 0, 1, 2)
        imp.addWidget(QLabel("Min frei"), 5, 2)
        imp.addWidget(self.min_free_spin, 5, 3)
        imp.addWidget(QLabel("Resume frei"), 6, 2)
        imp.addWidget(self.resume_free_spin, 6, 3)

        poll_info = QLabel(
            f"Poll-Intervall ist nicht mehr separat konfigurierbar. "
            f"Für den Low-Disk-Safe-Modus wird intern fest {LOW_DISK_POLL_SECONDS:.0f} s verwendet."
        )
        poll_info.setWordWrap(True)
        imp.addWidget(poll_info, 6, 0, 1, 2)
        imp.addWidget(init_btn, 7, 0)
        imp.addWidget(full_btn, 7, 2)
        imp.addWidget(patch_btn, 7, 3)
        layout.addWidget(imp_group)

        self.import_status_label = QLabel("Bereit")
        self.import_progress = QProgressBar()
        self.import_progress.setRange(0, 1000)
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
        self.progress_table.setMinimumHeight(280)
        progress_layout.addWidget(self.progress_table)

        self.worker_table = QTableWidget(0, 6)
        self.worker_table.setHorizontalHeaderLabels(["Worker", "gesehen", "geschrieben", "Bytes", "Parser", "Range"])
        self.worker_table.verticalHeader().setVisible(False)
        self.worker_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.worker_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.worker_table.setAlternatingRowColors(True)
        self.worker_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.worker_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.worker_table.setMinimumHeight(220)
        progress_layout.addWidget(self.worker_table)
        layout.addWidget(progress_group)

        debug_group = QGroupBox("Debug / JSON")
        debug_layout = QVBoxLayout(debug_group)
        self.stats_text = QPlainTextEdit()
        self.stats_text.setReadOnly(True)
        self.stats_text.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.stats_text.setWordWrapMode(QTextOption.NoWrap)
        self.stats_text.setFont(QFont("Menlo", 10))
        debug_layout.addWidget(self.stats_text)
        layout.addWidget(debug_group, 1)

        self._fill_progress_pairs([])
        return page

    def _build_planner_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)

        planner_group = QGroupBox("Planner")
        planner = QGridLayout(planner_group)
        self.single_ref_edit = QLineEdit("Sol")
        self.multi_ref_edit = QPlainTextEdit()
        self.multi_ref_edit.setPlaceholderText("Weitere Referenz-Systeme, eines pro Zeile")
        self.multi_ref_edit.setMaximumHeight(110)
        self.radius_spin = QDoubleSpinBox()
        self.radius_spin.setRange(0.1, 100000.0)
        self.radius_spin.setValue(50.0)
        self.radius_spin.setSuffix(" ly")
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["any", "ww", "elw", "ww_or_elw", "ww_and_elw"])
        self.filter_combo.setCurrentText("ww_or_elw")
        self.claimable_check = QCheckBox("Nur claimable")
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(1, 100000)
        self.limit_spin.setValue(200)
        self.webhook_edit = QLineEdit()
        self.webhook_edit.setPlaceholderText("Discord Webhook URL")
        self.run_btn = QPushButton("Abfrage ausführen")
        self.export_btn = QPushButton("CSV exportieren")
        self.preview_btn = QPushButton("Preview aktualisieren")
        self.send_btn = QPushButton("An Discord senden")
        self.run_btn.clicked.connect(self._run_planner_query)
        self.export_btn.clicked.connect(self._export_planner_csv)
        self.preview_btn.clicked.connect(self._refresh_preview_only)
        self.send_btn.clicked.connect(self._send_preview_to_discord)

        planner.addWidget(QLabel("Referenzsystem"), 0, 0)
        planner.addWidget(self.single_ref_edit, 0, 1)
        planner.addWidget(QLabel("Radius"), 0, 2)
        planner.addWidget(self.radius_spin, 0, 3)
        planner.addWidget(QLabel("Weitere Referenzsysteme"), 1, 0)
        planner.addWidget(self.multi_ref_edit, 1, 1, 2, 3)
        planner.addWidget(QLabel("Filter"), 3, 0)
        planner.addWidget(self.filter_combo, 3, 1)
        planner.addWidget(QLabel("Limit je Referenzsystem"), 3, 2)
        planner.addWidget(self.limit_spin, 3, 3)
        planner.addWidget(self.claimable_check, 4, 0, 1, 2)
        planner.addWidget(QLabel("Discord Webhook"), 5, 0)
        planner.addWidget(self.webhook_edit, 5, 1, 1, 3)
        planner.addWidget(self.run_btn, 6, 0)
        planner.addWidget(self.export_btn, 6, 1)
        planner.addWidget(self.preview_btn, 6, 2)
        planner.addWidget(self.send_btn, 6, 3)
        layout.addWidget(planner_group)

        self.planner_status_label = QLabel("Bereit")
        self.planner_progress = QProgressBar()
        self.planner_progress.setRange(0, 1000)
        layout.addWidget(self.planner_status_label)
        layout.addWidget(self.planner_progress)

        planner_progress_group = QGroupBox("Fortschritt Abfrage / Discord")
        planner_progress_layout = QVBoxLayout(planner_progress_group)
        self.planner_progress_table = QTableWidget(5, 4)
        self.planner_progress_table.setHorizontalHeaderLabels(["Bezeichnung", "Wert", "Bezeichnung", "Wert"])
        self.planner_progress_table.verticalHeader().setVisible(False)
        self.planner_progress_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.planner_progress_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.planner_progress_table.setFocusPolicy(Qt.NoFocus)
        self.planner_progress_table.setAlternatingRowColors(True)
        self.planner_progress_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.planner_progress_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.planner_progress_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.planner_progress_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        planner_progress_layout.addWidget(self.planner_progress_table)
        self.planner_log = QPlainTextEdit()
        self.planner_log.setReadOnly(True)
        self.planner_log.setMaximumHeight(120)
        self.planner_log.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.planner_log.setWordWrapMode(QTextOption.NoWrap)
        self.planner_log.setFont(QFont("Menlo", 10))
        planner_progress_layout.addWidget(self.planner_log)
        layout.addWidget(planner_progress_group)
        self._fill_table_pairs(self.planner_progress_table, 5, [])

        splitter = QSplitter(Qt.Vertical)

        self.result_table = QTableWidget(0, 12)
        self.result_table.setHorizontalHeaderLabels(
            [
                "Referenz", "System", "Distanz", "WW", "ELW", "HMCW", "Icy", "Rocky-Icy",
                "Gas Giants", "Rings", "Inara", "EDGIS",
            ]
        )
        self.result_table.verticalHeader().setVisible(False)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.result_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        for col in range(2, 10):
            self.result_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeToContents)
        self.result_table.horizontalHeader().setSectionResizeMode(10, QHeaderView.Stretch)
        self.result_table.horizontalHeader().setSectionResizeMode(11, QHeaderView.Stretch)
        splitter.addWidget(self.result_table)

        preview_group = QGroupBox("Discord-Vorschau")
        preview_layout = QVBoxLayout(preview_group)
        self.preview_text = QPlainTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.preview_text.setWordWrapMode(QTextOption.NoWrap)
        self.preview_text.setFont(QFont("Menlo", 10))
        preview_layout.addWidget(self.preview_text)
        splitter.addWidget(preview_group)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        layout.addWidget(splitter, 1)
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
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Dump wählen",
            "",
            "Dump/Parquet (*.json *.json.gz *.json.zst *.parquet *.gz *.zst)",
        )
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
            QMessageBox.information(
                self,
                "DB",
                f"DB initialisiert: {path}\nThreads: {settings['threads']}\nMemory: {settings['memory_limit']}",
            )
        except Exception as exc:
            QMessageBox.critical(self, "Fehler", str(exc))

    def _start_import(self, mode: str) -> None:
        if self._thread is not None:
            QMessageBox.information(self, "Import läuft", "Es läuft bereits ein Import.")
            return
        db_path = self.db_edit.text().strip()
        dump_path = self.dump_edit.text().strip()
        temp_dir = self.temp_edit.text().strip()
        if not db_path or not dump_path or not temp_dir:
            QMessageBox.warning(self, "Fehlende Eingabe", "Bitte DB-, Temp- und Dump-Pfad angeben.")
            return

        self.import_progress.setValue(0)
        self.import_status_label.setText("Import läuft")
        self.stats_text.clear()
        self._fill_progress_pairs([])
        self.worker_table.setRowCount(0)

        self._thread = QThread(self)
        self._worker = ImportWorker(
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
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_import_progress)
        self._worker.finished.connect(self._on_import_finished)
        self._worker.error.connect(self._on_import_error)
        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._cleanup_thread)
        self._thread.start()

    def _on_import_progress(self, data: dict) -> None:
        ratio = float(data.get("progress_ratio", 0) or 0)
        self.import_progress.setValue(max(0, min(1000, int(ratio * 1000))))
        bytes_read = int(data.get("bytes_read", 0) or 0)
        dump_size = int(data.get("dump_size_bytes", 0) or 0)
        eta = data.get("eta_seconds")
        phase = str(data.get("phase", "-") or "-")

        pairs = [
            ("Phase", phase),
            ("Meldung", str(data.get("message", "-"))),
            ("Parser aktiv", str(data.get("parser_active", "-"))),
            ("Parser angefordert", str(data.get("parser_requested", "-"))),
            ("Fortschritt", f"{ratio * 100:.2f} %"),
            ("ETA", self._fmt_eta(eta)),
            ("Systeme gesehen", f"{int(data.get('systems_seen', 0) or 0):,}"),
            ("Systeme geschrieben", f"{int(data.get('systems_written', 0) or 0):,}"),
            ("Systeme / s", f"{float(data.get('systems_per_sec', 0) or 0):.1f}"),
            ("Verarbeitete Bytes", f"{self._fmt_bytes(bytes_read)} / {self._fmt_bytes(dump_size)}"),
            ("Dateigröße", self._fmt_bytes(data.get("file_size_bytes") or dump_size)),
            ("Ultra mmap / Worker", f"{data.get('ultra_mmap_mode', False)} / {data.get('ultra_workers', '-') }"),
            ("JSON-Array erkannt", str(data.get("json_array_detected", "-"))),
            (
                "Objektgrenzen / Segmente",
                f"{int(data.get('prep_found_object_boundaries', 0) or 0):,} / {int(data.get('prep_worker_segments_built', 0) or 0)}",
            ),
            (
                "Freier Speicher DB / Temp",
                f"{float(data.get('free_db_gb', 0) or 0):.1f} GB / {float(data.get('free_temp_gb', 0) or 0):.1f} GB",
            ),
            ("Pause gesamt", f"{float(data.get('paused_seconds', 0) or 0):.1f} s"),
        ]
        self._fill_progress_pairs(pairs)
        self._fill_worker_table(data)
        self.stats_text.setPlainText(json.dumps(data, indent=2, ensure_ascii=False))
        mode_tag = "Ultra mmap" if data.get("ultra_mmap_mode") else "Standard"
        self.import_status_label.setText(
            f"{mode_tag} | {phase} | {ratio * 100:.1f}% | {data.get('systems_per_sec', 0):.0f} Sys/s | "
            f"ETA {self._fmt_eta(eta)}"
        )

    def _on_import_finished(self, data: dict) -> None:
        self.import_progress.setValue(1000)
        self.stats_text.setPlainText(json.dumps(data, indent=2, ensure_ascii=False))
        self.import_status_label.setText("Import abgeschlossen")
        self._fill_progress_pairs(
            [
                ("Status", "Abgeschlossen"),
                ("Systeme gesehen", f"{int(data.get('systems_seen', 0) or 0):,}"),
                ("Systeme geschrieben", f"{int(data.get('systems_written', 0) or 0):,}"),
                ("Dauer", f"{float(data.get('elapsed_s', 0) or 0):.2f} s"),
                ("Aktive Dauer", f"{float(data.get('active_elapsed_s', 0) or 0):.2f} s"),
                ("Systeme / s", f"{float(data.get('systems_per_sec', 0) or 0):.2f}"),
                ("Temp-Dateien gelöscht", str(data.get("temp_files_deleted", 0))),
                ("Temp-Bytes gelöscht", self._fmt_bytes(data.get("temp_bytes_deleted", 0))),
            ]
        )
        QMessageBox.information(self, "Fertig", "Import abgeschlossen")

    def _on_import_error(self, message: str) -> None:
        self.import_status_label.setText("Import fehlgeschlagen")
        self.stats_text.setPlainText(message)
        QMessageBox.critical(self, "Importfehler", message)

    def _cleanup_thread(self) -> None:
        if self._thread is not None:
            self._thread.deleteLater()
        self._thread = None
        self._worker = None

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
        self._start_planner_worker(send_to_discord=False)

    def _refresh_preview_only(self) -> None:
        if not self._planner_results:
            QMessageBox.information(self, "Preview", "Noch keine Abfrageergebnisse vorhanden.")
            return
        self._build_preview_messages()

    def _build_preview_messages(self) -> None:
        self._preview_messages = {}
        preview_blocks: list[str] = []
        for reference_system, rows in self._planner_results.items():
            messages = format_discord_messages(
                reference_system,
                rows,
                radius_ly=self.radius_spin.value(),
                body_filter=self.filter_combo.currentText(),
                claimable_only=self.claimable_check.isChecked(),
                limit=self.limit_spin.value(),
            )
            self._preview_messages[reference_system] = messages
            for idx, message in enumerate(messages, start=1):
                preview_blocks.append(f"===== {reference_system} | Nachricht {idx}/{len(messages)} =====\n{message}")
        self.preview_text.setPlainText("\n\n".join(preview_blocks).strip())

    def _send_preview_to_discord(self) -> None:
        webhook = self.webhook_edit.text().strip()
        if not webhook:
            QMessageBox.warning(self, "Discord", "Bitte eine Discord Webhook URL angeben.")
            return
        if not self._preview_messages:
            QMessageBox.information(self, "Discord", "Noch keine Vorschau vorhanden.")
            return
        results: list[str] = []
        failed = False
        for reference_system, messages in self._preview_messages.items():
            send_result = send_discord_messages(webhook, messages)
            ok = sum(1 for item in send_result if item.get("ok"))
            errors = [item for item in send_result if not item.get("ok")]
            if errors:
                failed = True
                results.append(f"{reference_system}: {ok}/{len(messages)} gesendet, Fehler: {errors[0].get('error', 'unbekannt')}")
            else:
                results.append(f"{reference_system}: {ok}/{len(messages)} gesendet")
        if failed:
            QMessageBox.warning(self, "Discord", "\n".join(results))
        else:
            QMessageBox.information(self, "Discord", "\n".join(results))

    def _start_planner_worker(self, *, send_to_discord: bool) -> None:
        if self._planner_thread is not None:
            QMessageBox.information(self, "Planner", "Es läuft bereits eine Abfrage oder ein Discord-Versand.")
            return

        refs = self._reference_systems()
        db_path = self.db_edit.text().strip()
        temp_dir = self.temp_edit.text().strip()
        if not db_path or not temp_dir:
            QMessageBox.warning(self, "Fehlende Eingabe", "Bitte DB- und Temp-Pfad angeben.")
            return

        self.planner_progress.setValue(0)
        self.planner_status_label.setText("Läuft")
        self.planner_log.clear()
        self._fill_table_pairs(self.planner_progress_table, 5, [])
        self._set_planner_controls_enabled(False)

        self._planner_thread = QThread(self)
        self._planner_worker = PlannerWorker(
            db_path=db_path,
            temp_dir=temp_dir,
            refs=refs,
            radius_ly=self.radius_spin.value(),
            body_filter=self.filter_combo.currentText(),
            claimable_only=self.claimable_check.isChecked(),
            limit=self.limit_spin.value(),
            webhook_url=self.webhook_edit.text().strip(),
            send_to_discord=send_to_discord,
        )
        self._planner_worker.moveToThread(self._planner_thread)
        self._planner_thread.started.connect(self._planner_worker.run)
        self._planner_worker.progress.connect(self._on_planner_progress)
        self._planner_worker.finished.connect(self._on_planner_finished)
        self._planner_worker.error.connect(self._on_planner_error)
        self._planner_worker.finished.connect(self._planner_thread.quit)
        self._planner_worker.error.connect(self._planner_thread.quit)
        self._planner_thread.finished.connect(self._cleanup_planner_thread)
        self._planner_thread.start()

    def _on_planner_progress(self, data: dict) -> None:
        ratio = float(data.get("progress_ratio", 0) or 0)
        self.planner_progress.setValue(max(0, min(1000, int(ratio * 1000))))
        phase = str(data.get("phase", "-") or "-")
        message = str(data.get("message", "-") or "-")
        current_reference = str(data.get("current_reference", "") or "")
        refs_done = int(data.get("reference_index", 0) or 0)
        refs_total = int(data.get("reference_total", 0) or 0)
        total_rows = int(data.get("total_rows", 0) or 0)
        sent_messages = int(data.get("sent_messages", 0) or 0)
        total_messages = int(data.get("total_messages", 0) or 0)
        first_error = str(data.get("first_error", "") or "")

        self.planner_status_label.setText(f"{phase} | {message}")
        pairs = [
            ("Phase", phase),
            ("Meldung", message),
            ("Referenzsystem", current_reference or "-"),
            ("Referenzen", f"{refs_done}/{refs_total}" if refs_total else "-"),
            ("Fortschritt", f"{ratio * 100:.1f} %"),
            ("Treffer gesamt", f"{total_rows:,}"),
            ("Discord-Nachrichten", f"{sent_messages}/{total_messages}" if total_messages else "-"),
            ("Fehler", first_error or "-"),
        ]
        self._fill_table_pairs(self.planner_progress_table, 5, pairs)
        self.planner_log.appendPlainText(f"[{phase}] {message}")

    def _on_planner_finished(self, data: dict) -> None:
        self.planner_progress.setValue(1000)
        self._planner_results = data.get("results") or {}
        self._rows = data.get("rows") or []
        self._preview_messages = data.get("preview_messages") or {}
        self._fill_result_table(self._rows)
        self._render_preview_messages()

        send_results = data.get("send_results") or {}
        sent = bool(data.get("sent"))
        self.planner_status_label.setText("Abgeschlossen")
        summary_pairs = [
            ("Status", "Abgeschlossen"),
            ("Referenzsysteme", str(len(self._planner_results))),
            ("Treffer gesamt", f"{len(self._rows):,}"),
            ("Discord-Versand", "ja" if sent else "nein"),
        ]
        if sent:
            total_messages = sum(len(v) for v in self._preview_messages.values())
            ok_messages = sum(1 for entries in send_results.values() for item in entries if item.get("ok"))
            first_error = next((item.get("error") for entries in send_results.values() for item in entries if not item.get("ok")), "")
            summary_pairs.extend([
                ("Gesendete Nachrichten", f"{ok_messages}/{total_messages}"),
                ("Erster Fehler", first_error or "-"),
            ])
        self._fill_table_pairs(self.planner_progress_table, 5, summary_pairs)
        self._set_planner_controls_enabled(True)

        if sent:
            failed = any(not item.get("ok") for entries in send_results.values() for item in entries)
            lines: list[str] = []
            for reference_system, entries in send_results.items():
                ok = sum(1 for item in entries if item.get("ok"))
                errors = [item for item in entries if not item.get("ok")]
                if errors:
                    lines.append(f"{reference_system}: {ok}/{len(entries)} gesendet, Fehler: {errors[0].get('error', 'unbekannt')}")
                else:
                    lines.append(f"{reference_system}: {ok}/{len(entries)} gesendet")
            if failed:
                QMessageBox.warning(self, "Discord", "\n".join(lines))
            else:
                QMessageBox.information(self, "Discord", "\n".join(lines) or "Discord-Versand abgeschlossen")
        else:
            QMessageBox.information(self, "Planner", f"Abfrage abgeschlossen. Treffer: {len(self._rows)}")

    def _on_planner_error(self, message: str) -> None:
        self.planner_status_label.setText("Fehlgeschlagen")
        self.planner_log.appendPlainText(f"[error] {message}")
        self._fill_table_pairs(self.planner_progress_table, 5, [("Status", "Fehlgeschlagen"), ("Fehler", message)])
        self._set_planner_controls_enabled(True)
        QMessageBox.critical(self, "Planner-Fehler", message)

    def _cleanup_planner_thread(self) -> None:
        if self._planner_thread is not None:
            self._planner_thread.deleteLater()
        self._planner_thread = None
        self._planner_worker = None

    def _set_planner_controls_enabled(self, enabled: bool) -> None:
        self.run_btn.setEnabled(enabled)
        self.export_btn.setEnabled(enabled)
        self.preview_btn.setEnabled(enabled)
        self.send_btn.setEnabled(enabled)

    def _render_preview_messages(self) -> None:
        preview_blocks: list[str] = []
        for reference_system, messages in self._preview_messages.items():
            for idx, message in enumerate(messages, start=1):
                preview_blocks.append(f"===== {reference_system} | Nachricht {idx}/{len(messages)} =====\n{message}")
        self.preview_text.setPlainText("\n\n".join(preview_blocks).strip())

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

    def _fill_result_table(self, rows: list[dict]) -> None:
        self.result_table.setRowCount(len(rows))
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
                row.get("inara_url", ""),
                row.get("edgis_url", ""),
            ]
            for c, value in enumerate(values):
                self.result_table.setItem(r, c, QTableWidgetItem(str(value)))
        self.result_table.resizeRowsToContents()

    def _fill_progress_pairs(self, pairs: list[tuple[str, str]]) -> None:
        self._fill_table_pairs(self.progress_table, 8, pairs)

    def _fill_table_pairs(self, table: QTableWidget, row_count: int, pairs: list[tuple[str, str]]) -> None:
        cells = row_count * 2
        items = pairs[:cells]
        while len(items) < cells:
            items.append(("", ""))
        idx = 0
        for row in range(row_count):
            left_label, left_value = items[idx]
            right_label, right_value = items[idx + 1]
            table.setItem(row, 0, QTableWidgetItem(left_label))
            table.setItem(row, 1, QTableWidgetItem(left_value))
            table.setItem(row, 2, QTableWidgetItem(right_label))
            table.setItem(row, 3, QTableWidgetItem(right_value))
            idx += 2
        table.resizeRowsToContents()

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
