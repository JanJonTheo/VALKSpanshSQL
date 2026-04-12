from __future__ import annotations

import json
import sys
from pathlib import Path

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .config import default_db_path, default_temp_dir
from .db import init_db
from .importer import import_dump
from .query import query_systems, write_csv


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
        low_disk_poll_seconds: float,
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
        self.low_disk_poll_seconds = low_disk_poll_seconds

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
                low_disk_poll_seconds=self.low_disk_poll_seconds,
                progress_cb=lambda data: self.progress.emit(data),
            )
            self.finished.emit({
                "systems_seen": stats.systems_seen,
                "systems_written": stats.systems_written,
                "elapsed_s": round(stats.elapsed_s, 2),
                "active_elapsed_s": round(stats.active_elapsed_s, 2),
                "systems_per_sec": round(stats.systems_per_sec, 2),
                "progress_ratio": round(stats.progress_ratio, 4),
                "paused_seconds": round(stats.paused_seconds, 2),
                "temp_files_deleted": stats.temp_files_deleted,
                "temp_bytes_deleted": stats.temp_bytes_deleted,
            })
        except Exception as exc:
            self.error.emit(str(exc))


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Spansh DuckDB High Performance Importer")
        self.resize(1500, 900)
        self._thread: QThread | None = None
        self._worker: ImportWorker | None = None
        self._rows: list[dict] = []
        self._build_ui()

    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        cfg_group = QGroupBox("Pfade")
        cfg = QGridLayout(cfg_group)
        self.db_edit = QLineEdit(str(default_db_path()))
        self.temp_edit = QLineEdit(str(default_temp_dir()))
        self.dump_edit = QLineEdit()
        db_btn = QPushButton("DB wählen")
        temp_btn = QPushButton("Temp wählen")
        dump_btn = QPushButton("Dump wählen")
        db_btn.clicked.connect(self._pick_db)
        temp_btn.clicked.connect(self._pick_temp)
        dump_btn.clicked.connect(self._pick_dump)
        cfg.addWidget(QLabel("DuckDB"), 0, 0)
        cfg.addWidget(self.db_edit, 0, 1)
        cfg.addWidget(db_btn, 0, 2)
        cfg.addWidget(QLabel("Temp"), 1, 0)
        cfg.addWidget(self.temp_edit, 1, 1)
        cfg.addWidget(temp_btn, 1, 2)
        cfg.addWidget(QLabel("Dump"), 2, 0)
        cfg.addWidget(self.dump_edit, 2, 1)
        cfg.addWidget(dump_btn, 2, 2)
        root.addWidget(cfg_group)

        imp_group = QGroupBox("Import")
        imp = QGridLayout(imp_group)
        self.batch_spin = QSpinBox()
        self.batch_spin.setRange(100, 500000)
        self.batch_spin.setValue(5000)
        self.cleanup_before_check = QCheckBox("Temp vor Import bereinigen")
        self.cleanup_after_check = QCheckBox("Temp nach Import bereinigen")
        self.cleanup_hours_spin = QDoubleSpinBox()
        self.cleanup_hours_spin.setRange(0.0, 10000.0)
        self.cleanup_hours_spin.setValue(24.0)
        self.cleanup_hours_spin.setSuffix(" h")
        self.low_disk_check = QCheckBox("Low-Disk-Safe-Modus")
        self.low_disk_check.setChecked(True)
        self.min_free_spin = QDoubleSpinBox()
        self.min_free_spin.setRange(0.0, 1024.0)
        self.min_free_spin.setValue(20.0)
        self.min_free_spin.setSuffix(" GB")
        self.resume_free_spin = QDoubleSpinBox()
        self.resume_free_spin.setRange(0.0, 1024.0)
        self.resume_free_spin.setValue(25.0)
        self.resume_free_spin.setSuffix(" GB")
        self.poll_spin = QDoubleSpinBox()
        self.poll_spin.setRange(1.0, 3600.0)
        self.poll_spin.setValue(10.0)
        self.poll_spin.setSuffix(" s")
        init_btn = QPushButton("DB initialisieren")
        full_btn = QPushButton("Full Import")
        patch_btn = QPushButton("Patch Import")
        init_btn.clicked.connect(self._init_db)
        full_btn.clicked.connect(lambda: self._start_import("full"))
        patch_btn.clicked.connect(lambda: self._start_import("patch"))
        imp.addWidget(QLabel("Batch-Größe"), 0, 0)
        imp.addWidget(self.batch_spin, 0, 1)
        imp.addWidget(QLabel("Cleanup älter als"), 0, 2)
        imp.addWidget(self.cleanup_hours_spin, 0, 3)
        imp.addWidget(self.cleanup_before_check, 1, 0, 1, 2)
        imp.addWidget(self.cleanup_after_check, 1, 2, 1, 2)
        imp.addWidget(self.low_disk_check, 2, 0, 1, 2)
        imp.addWidget(QLabel("Min frei"), 2, 2)
        imp.addWidget(self.min_free_spin, 2, 3)
        imp.addWidget(QLabel("Resume frei"), 3, 2)
        imp.addWidget(self.resume_free_spin, 3, 3)
        imp.addWidget(QLabel("Poll-Intervall"), 3, 0)
        imp.addWidget(self.poll_spin, 3, 1)
        imp.addWidget(init_btn, 4, 0)
        imp.addWidget(full_btn, 4, 2)
        imp.addWidget(patch_btn, 4, 3)
        root.addWidget(imp_group)

        self.status_label = QLabel("Bereit")
        self.progress = QProgressBar()
        self.progress.setRange(0, 1000)
        root.addWidget(self.status_label)
        root.addWidget(self.progress)

        self.stats_text = QPlainTextEdit()
        self.stats_text.setReadOnly(True)
        root.addWidget(self.stats_text, 1)

        query_group = QGroupBox("Abfrage")
        query_form = QGridLayout(query_group)
        self.ref_edit = QLineEdit("Sol")
        self.radius_spin = QDoubleSpinBox()
        self.radius_spin.setRange(0.1, 100000.0)
        self.radius_spin.setValue(50.0)
        self.radius_spin.setSuffix(" ly")
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["any", "ww", "elw", "ww_or_elw", "ww_and_elw"])
        self.filter_combo.setCurrentText("ww_or_elw")
        self.claimable_check = QCheckBox("Nur claimable")
        self.links_check = QCheckBox("Links exportieren")
        self.links_check.setChecked(True)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(1, 100000)
        self.limit_spin.setValue(200)
        self.query_btn = QPushButton("Abfrage ausführen")
        self.export_btn = QPushButton("CSV exportieren")
        self.query_btn.clicked.connect(self._run_query)
        self.export_btn.clicked.connect(self._export_csv)
        query_form.addWidget(QLabel("Referenzsystem"), 0, 0)
        query_form.addWidget(self.ref_edit, 0, 1)
        query_form.addWidget(QLabel("Radius"), 0, 2)
        query_form.addWidget(self.radius_spin, 0, 3)
        query_form.addWidget(QLabel("Filter"), 1, 0)
        query_form.addWidget(self.filter_combo, 1, 1)
        query_form.addWidget(QLabel("Limit"), 1, 2)
        query_form.addWidget(self.limit_spin, 1, 3)
        query_form.addWidget(self.claimable_check, 2, 0, 1, 2)
        query_form.addWidget(self.links_check, 2, 2, 1, 2)
        query_form.addWidget(self.query_btn, 3, 2)
        query_form.addWidget(self.export_btn, 3, 3)
        root.addWidget(query_group)

        self.table = QTableWidget(0, 17)
        self.table.setHorizontalHeaderLabels([
            "System", "Distanz", "Bodies", "Planeten", "Sterne", "Ringed", "WW", "ELW", "Population",
            "Faction Count", "Controlling Faction", "Claimable", "Body Types", "Ring Types", "EDSM", "Inara", "EDGIS"
        ])
        root.addWidget(self.table, 3)

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
            path = init_db(self.db_edit.text().strip(), temp_dir=self.temp_edit.text().strip())
            QMessageBox.information(self, "DB", f"DB initialisiert: {path}")
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

        self.progress.setValue(0)
        self.status_label.setText("Import läuft")
        self.stats_text.clear()
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
            self.poll_spin.value(),
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
        if data.get("pause_reason") == "low_disk":
            free_db_gb = data.get("free_db_bytes", 0) / (1024 ** 3)
            free_tmp_gb = data.get("free_temp_bytes", 0) / (1024 ** 3)
            if data.get("paused"):
                self.status_label.setText(f"Pausiert wegen wenig Speicher | DB {free_db_gb:.1f} GB | Temp {free_tmp_gb:.1f} GB")
            else:
                self.status_label.setText("Import fortgesetzt")
            return

        ratio = float(data.get("progress_ratio", 0.0))
        self.progress.setValue(max(0, min(1000, int(ratio * 1000))))
        eta = data.get("eta_seconds")
        eta_text = "n/a" if eta is None else f"{eta:.0f}s"
        free_db_gb = data.get("free_db_bytes", 0) / (1024 ** 3)
        free_tmp_gb = data.get("free_temp_bytes", 0) / (1024 ** 3)
        bytes_read_gb = data.get("bytes_read", 0) / (1024 ** 3)
        dump_size_gb = data.get("dump_size_bytes", 0) / (1024 ** 3)
        self.stats_text.setPlainText(json.dumps({
            "systems_seen": data.get("systems_seen", 0),
            "systems_written": data.get("systems_written", 0),
            "elapsed_s": data.get("elapsed_s", 0),
            "active_elapsed_s": data.get("active_elapsed_s", 0),
            "systems_per_sec": data.get("systems_per_sec", 0),
            "bytes_read_gb": round(bytes_read_gb, 2),
            "dump_size_gb": round(dump_size_gb, 2),
            "eta_seconds": eta,
            "paused_seconds": data.get("paused_seconds", 0),
            "free_db_gb": round(free_db_gb, 2),
            "free_temp_gb": round(free_tmp_gb, 2),
            "temp_files_deleted": data.get("temp_files_deleted", 0),
            "temp_bytes_deleted_gb": round(data.get("temp_bytes_deleted", 0) / (1024 ** 3), 3),
        }, indent=2, ensure_ascii=False))
        self.status_label.setText(
            f"{ratio * 100:.1f}% | ETA {eta_text} | Gelesen {bytes_read_gb:.1f}/{dump_size_gb:.1f} GB | Frei DB {free_db_gb:.1f} GB | Frei Temp {free_tmp_gb:.1f} GB"
        )

    def _on_import_finished(self, data: dict) -> None:
        self.progress.setValue(1000)
        self.stats_text.setPlainText(json.dumps(data, indent=2, ensure_ascii=False))
        self.status_label.setText("Import abgeschlossen")
        QMessageBox.information(self, "Fertig", "Import abgeschlossen")

    def _on_import_error(self, message: str) -> None:
        self.status_label.setText("Import fehlgeschlagen")
        QMessageBox.critical(self, "Importfehler", message)

    def _cleanup_thread(self) -> None:
        self._thread = None
        self._worker = None

    def _run_query(self) -> None:
        try:
            rows = query_systems(
                self.db_edit.text().strip(),
                self.ref_edit.text().strip(),
                self.radius_spin.value(),
                body_filter=self.filter_combo.currentText(),
                claimable_only=self.claimable_check.isChecked(),
                limit=self.limit_spin.value(),
                temp_dir=self.temp_edit.text().strip(),
                include_links=self.links_check.isChecked(),
            )
            self._rows = rows
            self._fill_table(rows)
            self.status_label.setText(f"{len(rows)} Systeme gefunden")
        except Exception as exc:
            QMessageBox.critical(self, "Fehler", str(exc))

    def _export_csv(self) -> None:
        if not self._rows:
            QMessageBox.information(self, "Export", "Keine Daten vorhanden.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "CSV exportieren", str(Path.cwd() / "results.csv"), "CSV (*.csv)")
        if not path:
            return
        try:
            write_csv(self._rows, path)
            QMessageBox.information(self, "Export", f"CSV geschrieben: {path}")
        except Exception as exc:
            QMessageBox.critical(self, "Fehler", str(exc))

    def _fill_table(self, rows: list[dict]) -> None:
        self.table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            values = [
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
                json.dumps(row.get("body_type_counts", {}), ensure_ascii=False, sort_keys=True),
                json.dumps(row.get("ring_type_counts", {}), ensure_ascii=False, sort_keys=True),
                row.get("edsm_url", ""),
                row.get("inara_url", ""),
                row.get("edgis_url", ""),
            ]
            for c, value in enumerate(values):
                self.table.setItem(r, c, QTableWidgetItem(str(value)))
        self.table.resizeColumnsToContents()


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
