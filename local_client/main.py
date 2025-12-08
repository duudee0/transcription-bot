#!/usr/bin/env python3
"""
Improved PySide6 GUI client for Task API Wrapper
- Cleaner, structured logging in GUI (events list shows labeled entries: Response: Text: URL:)
- Unified handling of callback events vs direct responses
- Generic "Open last URL" and "Download last URL" actions for any URL received in response/callback
- Background downloads report results to GUI safely via callback_queue
- Minor fixes (missing uuid import, tolerant JSON parsing)

Run:
    pip install PySide6 fastapi uvicorn httpx pydantic
    python pyside6_task_wrapper_client_improved.py

"""

import sys
import os
import json
import threading
import socket
import queue
import time
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
from typing import Optional, List

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QTextEdit, QPushButton, QComboBox, QFileDialog,
    QListWidget, QCheckBox, QMessageBox, QSpinBox
)
from PySide6.QtCore import QTimer, QUrl
from PySide6.QtGui import QDesktopServices

import tempfile
import shutil
import urllib.parse
import uuid

import httpx
from fastapi import FastAPI, Request
import uvicorn

# --------------------------- Defaults / Config ---------------------------
DEFAULT_WRAPPER_URL = os.environ.get("WRAPPER_URL", "http://localhost:8003")
CALLBACK_PORT = int(os.environ.get("CALLBACK_PORT", "9199"))
FILE_SERVER_PORT = int(os.environ.get("FILE_SERVER_PORT", "8999"))
FILE_SERVER_ENABLED = False

PREDEFINED_CHAINS = {
    "comprehensive_analysis": ["gigachat-service", "llm-service"],
    "voice_question": ["whisper", "local-llm"],
    "text_to_speech": ["local-llm", "voiceover"],
    "voice_to_voice": ["whisper", "local-llm", "voiceover"],
}

# --------------------------- Utility functions ---------------------------

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def ensure_list_from_text(text: str) -> List[str]:
    text = text.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [p.strip() for p in text.split(",") if p.strip()]

# --------------------------- Callback server ---------------------------

callback_queue = queue.Queue()


def create_callback_app():
    app = FastAPI()

    @app.post("/callback")
    async def receive_callback_noid(request: Request):
        try:
            payload = await request.json()
        except Exception:
            body = await request.body()
            payload = {"raw": body.decode(errors="ignore")}

        task_id = None
        if isinstance(payload, dict):
            task_id = payload.get("task_id") or payload.get("original_message_id") or payload.get("id")

        callback_queue.put({"task_id": task_id, "payload": payload})
        return {"status": "received"}

    @app.post("/callback/{task_id}")
    async def receive_callback(task_id: str, request: Request):
        try:
            payload = await request.json()
        except Exception:
            body = await request.body()
            payload = {"raw": body.decode(errors="ignore")}

        callback_queue.put({"task_id": task_id, "payload": payload})
        return {"status": "received"}

    @app.get("/callback/health")
    async def health():
        return {"status": "ok"}

    return app


def run_callback_server(host: str = "0.0.0.0", port: int = CALLBACK_PORT):
    app = create_callback_app()
    uvicorn.run(app, host=host, port=port, log_level="warning")

# --------------------------- Simple file server ---------------------------

class ThreadedHTTPServer(threading.Thread):
    def __init__(self, directory: str, host: str = "0.0.0.0", port: int = FILE_SERVER_PORT):
        super().__init__(daemon=True)
        self.directory = os.path.abspath(directory)
        self.host = host
        self.port = port
        self.httpd: Optional[TCPServer] = None
        self._running = False

    def run(self):
        os.chdir(self.directory)
        handler = SimpleHTTPRequestHandler
        class _TCPServer(TCPServer):
            allow_reuse_address = True

        with _TCPServer((self.host, self.port), handler) as httpd:
            self.httpd = httpd
            self._running = True
            try:
                httpd.serve_forever()
            except Exception:
                pass
            finally:
                self._running = False

    def stop(self):
        if self.httpd:
            try:
                self.httpd.shutdown()
            except Exception:
                pass

# --------------------------- Main GUI ---------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Task Wrapper GUI Client — PySide6 (improved)")
        self.resize(950, 720)

        self.wrapper_url_input = QLineEdit(DEFAULT_WRAPPER_URL)
        self.callback_host_input = QLineEdit(get_local_ip())
        self.callback_port_input = QSpinBox()
        self.callback_port_input.setRange(1024, 65535)
        self.callback_port_input.setValue(CALLBACK_PORT)

        self.file_server_dir = None
        self.file_server_thread: Optional[ThreadedHTTPServer] = None

        self._last_url = None
        self._last_text_snippet = None

        self.setup_ui()

        # timer to poll callback queue
        self.timer = QTimer()
        self.timer.setInterval(300)
        self.timer.timeout.connect(self.check_callbacks)
        self.timer.start()

        # start callback server thread (non-blocking)
        self.callback_thread = threading.Thread(
            target=run_callback_server, args=("0.0.0.0", self.callback_port_input.value()), daemon=True
        )
        self.callback_thread.start()

    def setup_ui(self):
        central = QWidget()
        layout = QVBoxLayout(central)

        # top row: wrapper url / callback host/port
        row = QHBoxLayout()
        row.addWidget(QLabel("Wrapper URL:"))
        row.addWidget(self.wrapper_url_input)
        row.addWidget(QLabel("Callback host:"))
        row.addWidget(self.callback_host_input)
        row.addWidget(QLabel("Callback port:"))
        row.addWidget(self.callback_port_input)
        layout.addLayout(row)

        # task type and service chain
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Task type:"))
        self.task_type_input = QLineEdit("analyze_text")
        row2.addWidget(self.task_type_input)

        row2.addWidget(QLabel("Predefined chains:"))
        self.predef_combo = QComboBox()
        self.predef_combo.addItem("(none)")
        for k in PREDEFINED_CHAINS.keys():
            self.predef_combo.addItem(k)
        row2.addWidget(self.predef_combo)

        row2.addWidget(QLabel("Or custom chain (JSON or comma list):"))
        self.chain_input = QLineEdit()
        row2.addWidget(self.chain_input)

        layout.addLayout(row2)

        # payload type
        payload_row = QHBoxLayout()
        payload_row.addWidget(QLabel("Payload type:"))
        self.payload_type = QComboBox()
        self.payload_type.addItems(["text", "url", "file_url", "audio_url", "custom_json"])
        payload_row.addWidget(self.payload_type)

        payload_row.addWidget(QLabel("timeout (s):"))
        self.timeout_input = QSpinBox()
        self.timeout_input.setRange(1, 600)
        self.timeout_input.setValue(30)
        payload_row.addWidget(self.timeout_input)

        layout.addLayout(payload_row)

        # input areas
        self.text_input = QTextEdit()
        self.text_input.setPlaceholderText("Enter text payload here (for text type)")
        layout.addWidget(self.text_input)

        file_row = QHBoxLayout()
        self.select_file_btn = QPushButton("Select file to serve (for file_url)")
        self.select_file_btn.clicked.connect(self.select_file)
        file_row.addWidget(self.select_file_btn)
        self.start_file_server_btn = QPushButton("Start/Stop file server")
        self.start_file_server_btn.clicked.connect(self.toggle_file_server)
        file_row.addWidget(self.start_file_server_btn)
        self.file_server_status = QLabel("File server: stopped")
        file_row.addWidget(self.file_server_status)
        layout.addLayout(file_row)

        # parameters JSON
        layout.addWidget(QLabel("Parameters (JSON):"))
        self.parameters_input = QTextEdit()
        self.parameters_input.setPlaceholderText('{"language": "ru", "detailed_analysis": true}')
        layout.addWidget(self.parameters_input)

        # callback checkbox for async webhook
        cb_row = QHBoxLayout()
        self.use_callback_checkbox = QCheckBox("Request async client callback (wrapper will call this client's callback)")
        self.use_callback_checkbox.setChecked(True)
        cb_row.addWidget(self.use_callback_checkbox)
        layout.addLayout(cb_row)

        # buttons send
        btn_row = QHBoxLayout()
        self.send_btn = QPushButton("Create Task")
        self.send_btn.clicked.connect(self.create_task)
        btn_row.addWidget(self.send_btn)

        self.list_tasks_btn = QPushButton("List tasks (wrapper /tasks)")
        self.list_tasks_btn.clicked.connect(self.list_tasks)
        btn_row.addWidget(self.list_tasks_btn)

        layout.addLayout(btn_row)

        # output
        layout.addWidget(QLabel("Response / Events:"))

        # events list (лог событий)
        self.events_list = QListWidget()
        layout.addWidget(self.events_list)

        # detailed callback viewer
        layout.addWidget(QLabel("Callback detail:"))
        self.callback_text = QTextEdit()
        self.callback_text.setReadOnly(True)
        self.callback_text.setMinimumHeight(160)
        layout.addWidget(self.callback_text)

        # action buttons for last URL
        actions_row = QHBoxLayout()
        self.open_url_btn = QPushButton("Open last URL in browser")
        self.open_url_btn.setEnabled(False)
        self.open_url_btn.clicked.connect(self.open_last_url)
        actions_row.addWidget(self.open_url_btn)

        self.download_url_btn = QPushButton("Download last URL")
        self.download_url_btn.setEnabled(False)
        self.download_url_btn.clicked.connect(self.download_last_url)
        actions_row.addWidget(self.download_url_btn)

        layout.addLayout(actions_row)

        self.setCentralWidget(central)

    # ---------------- file server / selection ----------------

    def select_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select file to serve")
        if not path:
            return
        directory, filename = os.path.split(path)
        self.file_server_dir = directory
        self.selected_filename = filename
        self.file_server_status.setText(f"Selected: {filename} (dir: {directory})")

        if not (self.file_server_thread and self.file_server_thread.is_alive()):
            host = "0.0.0.0"
            port = FILE_SERVER_PORT
            self.file_server_thread = ThreadedHTTPServer(self.file_server_dir, host=host, port=port)
            self.file_server_thread.start()
            time.sleep(0.2)
            self.file_server_status.setText(f"Serving {self.file_server_dir} on port {port}")

        host_for_url = self.callback_host_input.text().strip() or get_local_ip()
        file_url = f"http://{host_for_url}:{FILE_SERVER_PORT}/{self.selected_filename}"

        idx = self.payload_type.findText("file_url")
        if idx != -1:
            self.payload_type.setCurrentIndex(idx)
        self.text_input.setPlainText(file_url)

        try:
            clipboard = QApplication.clipboard()
            clipboard.setText(file_url)
        except Exception:
            pass

        self.events_list.addItem(f"🔗 File served -> {file_url} (copied to clipboard)")

    def toggle_file_server(self):
        if self.file_server_thread and self.file_server_thread.is_alive():
            self.file_server_thread.stop()
            time.sleep(0.2)
            self.file_server_thread = None
            self.file_server_status.setText("File server: stopped")
            self.events_list.addItem("🛑 File server stopped")
            return

        if not self.file_server_dir:
            QMessageBox.warning(self, "No directory", "Please select a file first to serve its directory.")
            return

        host = "0.0.0.0"
        port = FILE_SERVER_PORT
        self.file_server_thread = ThreadedHTTPServer(self.file_server_dir, host=host, port=port)
        self.file_server_thread.start()
        time.sleep(0.2)
        self.file_server_status.setText(f"Serving {self.file_server_dir} on port {port}")
        self.events_list.addItem(f"📂 File server started on port {port} (dir: {self.file_server_dir})")

    # ---------------- payload / task creation ----------------

    def determine_payload(self):
        ptype = self.payload_type.currentText()
        if ptype == "text":
            return {"text": self.text_input.toPlainText()}
        elif ptype == "url":
            return {"url": self.text_input.toPlainText().strip()}
        elif ptype == "file_url":
            if getattr(self, "selected_filename", None) and self.file_server_thread:
                host = self.callback_host_input.text().strip() or get_local_ip()
                url = f"http://{host}:{FILE_SERVER_PORT}/{self.selected_filename}"
                return {"file_url": url}
            else:
                return {"file_url": self.text_input.toPlainText().strip()}
        elif ptype == "audio_url":
            return {"audio_url": self.text_input.toPlainText().strip()}
        else:
            txt = self.text_input.toPlainText().strip()
            if not txt:
                return {}
            try:
                return json.loads(txt)
            except Exception:
                return {"text": txt}

    def create_task(self):
        wrapper_url = self.wrapper_url_input.text().strip().rstrip("/")
        if not wrapper_url:
            QMessageBox.warning(self, "Wrapper URL missing", "Please set the wrapper URL.")
            return

        task_type = self.task_type_input.text().strip()
        if not task_type:
            QMessageBox.warning(self, "Task type missing", "Please set task_type")
            return

        chain = []
        predef = self.predef_combo.currentText()
        if predef and predef != "(none)":
            chain = PREDEFINED_CHAINS.get(predef, [])
        custom_chain_txt = self.chain_input.text().strip()
        if custom_chain_txt:
            custom_list = ensure_list_from_text(custom_chain_txt)
            if custom_list:
                chain = custom_list

        params_txt = self.parameters_input.toPlainText().strip()
        params = {}
        if params_txt:
            try:
                params = json.loads(params_txt)
            except Exception:
                QMessageBox.warning(self, "Invalid JSON", "Parameters must be valid JSON")
                return

        input_data = self.determine_payload()

        callback_enabled = self.use_callback_checkbox.isChecked()
        callback_host = self.callback_host_input.text().strip() or get_local_ip()
        callback_port = self.callback_port_input.value()

        task_body = {
            "task_type": task_type,
            "input_data": input_data,
            "parameters": params,
            "timeout": self.timeout_input.value(),
            "service_chain": chain if chain else None,
        }

        if callback_enabled:
            task_body["callback_url"] = f"http://{callback_host}:{callback_port}/callback"

        # Send request and try to display structured response in GUI
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(f"{wrapper_url}/api/v1/tasks", json=task_body)
                status = resp.status_code
                try:
                    data = resp.json()
                except Exception:
                    data = None
        except Exception as e:
            callback_queue.put({"gui_message": f"❌ Failed to send task: {e}"})
            return

        # Build a structured message for the events list
        if data and isinstance(data, dict):
            tid = data.get("task_id") or data.get("id") or data.get("task") or None
            # extract possible text/url from response
            result = data.get("result") or data.get("data") or {}
            text_snippet = None
            url_snippet = None
            if isinstance(result, dict):
                for key in ("text", "transcription", "original_text"):
                    if key in result and isinstance(result[key], str):
                        text_snippet = result[key]
                        break
                for key in ("audio_url", "file_url", "url"):
                    if key in result and isinstance(result[key], str):
                        url_snippet = result[key]
                        break
            # if nothing in result, check top-level
            if not text_snippet:
                for key in ("text",):
                    if key in data and isinstance(data[key], str):
                        text_snippet = data[key]

            lines = [f"➡ Sent task -> HTTP {status}"]
            if tid:
                lines.append(f"  task_id: {tid}")
            if text_snippet:
                lines.append(f"  text: {text_snippet[:200]}")
                self._last_text_snippet = text_snippet
            if url_snippet:
                lines.append(f"  url: {url_snippet}")
                self._last_url = url_snippet
                # enable URL buttons
                callback_queue.put({"gui_message": "_enable_url_buttons"})

            callback_queue.put({"gui_message": "\n".join(lines)})
            # Also show full JSON in detail viewer
            callback_queue.put({"gui_message": "_show_full_json", "json": data, "task_id": tid})
        else:
            # fallback: show raw text/status
            text = resp.text if 'resp' in locals() else ''
            callback_queue.put({"gui_message": f"➡ Sent task -> HTTP {status} response: {text}"})

    def list_tasks(self):
        wrapper_url = self.wrapper_url_input.text().strip().rstrip("/")
        if not wrapper_url:
            QMessageBox.warning(self, "Wrapper URL missing", "Please set the wrapper URL.")
            return
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.get(f"{wrapper_url}/tasks")
                data = resp.json()
        except Exception as e:
            callback_queue.put({"gui_message": f"❌ Failed to list tasks: {e}"})
            return
        callback_queue.put({"gui_message": f"📋 Tasks: {json.dumps(data, ensure_ascii=False)[:1000]}"})

    # ---------------- callbacks / GUI event processing ----------------

    def check_callbacks(self):
        # process incoming callback queue items (both API callbacks and GUI messages)
        while not callback_queue.empty():
            try:
                item = callback_queue.get_nowait()
            except Exception:
                break

            # special GUI messages posted by background threads
            if item.get("gui_message"):
                msg = item.get("gui_message")
                if msg == "_enable_url_buttons":
                    self.open_url_btn.setEnabled(True)
                    self.download_url_btn.setEnabled(True)
                    continue
                if msg == "_show_full_json":
                    data = item.get("json")
                    tid = item.get("task_id")
                    try:
                        pretty = json.dumps(data, ensure_ascii=False, indent=2)
                    except Exception:
                        pretty = str(data)
                    display_text = f"Task ID: {tid}\n\n{pretty}"
                    self.callback_text.setPlainText(display_text)
                    self.events_list.addItem(f"⬅ Callback (full JSON shown) task_id={tid}")
                    continue
                # generic message
                self.events_list.addItem(str(msg))
                continue

            tid = item.get("task_id")
            payload = item.get("payload")

            try:
                pretty = json.dumps(payload, ensure_ascii=False, indent=2)
            except Exception:
                pretty = str(payload)

            display_text = f"Task ID: {tid}\n\n{pretty}"
            self.callback_text.setPlainText(display_text)

            # Update events list with structured info
            # Try to find text/url in payload
            text_snippet = None
            url_snippet = None
            if isinstance(payload, dict):
                result = payload.get("result") or payload.get("data") or {}
                if isinstance(result, dict):
                    for key in ("text", "original_text", "transcription"):
                        if key in result and isinstance(result[key], str):
                            text_snippet = result[key]
                            break
                    for key in ("audio_url", "file_url", "url"):
                        if key in result and isinstance(result[key], str):
                            url_snippet = result[key]
                            break
                if not text_snippet:
                    for key in ("text",):
                        if key in payload and isinstance(payload[key], str):
                            text_snippet = payload[key]

            # append structured lines to events list
            header = f"⬅ Callback task_id={tid}"
            self.events_list.addItem(header)
            if text_snippet:
                self.events_list.addItem(f"  📝 Text: {text_snippet[:200]}")
                self._last_text_snippet = text_snippet
            if url_snippet:
                self.events_list.addItem(f"  🔗 URL: {url_snippet}")
                self._last_url = url_snippet
                self.open_url_btn.setEnabled(True)
                self.download_url_btn.setEnabled(True)

    # ---------------- open / download ----------------

    def open_last_url(self):
        url = self._last_url
        if not url:
            self.events_list.addItem("⚠️ No URL available to open")
            return
        QDesktopServices.openUrl(QUrl(url))
        self.events_list.addItem(f"🔗 Opened: {url}")

    def download_last_url(self):
        url = self._last_url
        if not url:
            self.events_list.addItem("⚠️ No URL available to download")
            return

        def _download_and_report(url):
            try:
                parsed = urllib.parse.urlparse(url)
                filename = os.path.basename(parsed.path) or f"file_{uuid.uuid4().hex}"
                tmpdir = tempfile.gettempdir()
                out_path = os.path.join(tmpdir, filename)
                with httpx.Client(timeout=60.0) as client:
                    r = client.get(url)
                    r.raise_for_status()
                    with open(out_path, "wb") as f:
                        f.write(r.content)
                # Post message back to main thread via queue
                callback_queue.put({"gui_message": f"⬇️ Downloaded to {out_path}"})
                # try to open containing folder (best-effort)
                try:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(out_path))
                except Exception:
                    pass
            except Exception as e:
                callback_queue.put({"gui_message": f"❌ Failed to download: {e}"})

        th = threading.Thread(target=_download_and_report, args=(url,), daemon=True)
        th.start()
        callback_queue.put({"gui_message": f"⏳ Download started: {url}"})


# --------------------------- Main entry ---------------------------

def main():
    app = QApplication([])
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
