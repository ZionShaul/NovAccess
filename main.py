"""
NovAccess – main.py
tkinter GUI for invoice processing automation.
"""

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from datetime import datetime
from pathlib import Path

# Load .env (GOOGLE_API_KEY)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import processor


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NovAccess – פענוח חשבוניות")
        self.resizable(True, True)
        self.minsize(660, 520)
        self._output_folder: str | None = None
        self._stop_event = threading.Event()
        self._log_file = None          # open file handle during a run
        self._log_lock = threading.Lock()
        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)

        # --- Folder selection ---
        top = ttk.LabelFrame(self, text="הגדרות", padding=10)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="תיקיית חשבוניות:").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self.folder_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.folder_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(top, text="בחר…", command=self._browse_folder).grid(row=0, column=2, padx=(6, 0))

        # --- Action buttons ---
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=4)

        self.start_btn = ttk.Button(btn_frame, text="▶  התחל עיבוד", command=self._start)
        self.start_btn.pack(side="left")

        self.stop_btn = ttk.Button(btn_frame, text="■  עצור", command=self._stop, state="disabled")
        self.stop_btn.pack(side="left", padx=(6, 0))

        self.open_btn = ttk.Button(btn_frame, text="📂  פתח תיקייה", command=self._open_folder, state="disabled")
        self.open_btn.pack(side="left", padx=(6, 0))

        self.copy_btn = ttk.Button(btn_frame, text="📋  העתק לוג", command=self._copy_log, state="disabled")
        self.copy_btn.pack(side="left", padx=(6, 0))

        # --- Progress bar ---
        self.progress = ttk.Progressbar(self, mode="determinate")
        self.progress.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 4))

        # --- Log area ---
        log_frame = ttk.LabelFrame(self, text="יומן פעילות", padding=4)
        log_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=(0, 10))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, state="disabled", wrap="word",
            font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4",
            insertbackground="white",
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="בחר תיקיית חשבוניות")
        if folder:
            self.folder_var.set(folder)

    def _open_folder(self):
        if self._output_folder:
            os.startfile(self._output_folder)

    def _copy_log(self):
        text = self.log_text.get("1.0", "end")
        self.clipboard_clear()
        self.clipboard_append(text)

    def _stop(self):
        self._stop_event.set()
        self.stop_btn.config(state="disabled")
        self.log("עצירה מתבצעת – ממתין לסיום הקובץ הנוכחי...")

    def _start(self):
        folder = self.folder_var.get().strip()
        api_key = os.environ.get("GOOGLE_API_KEY", "").strip()

        if not folder:
            messagebox.showerror("שגיאה", "יש לבחור תיקיית חשבוניות.")
            return
        if not Path(folder).is_dir():
            messagebox.showerror("שגיאה", "התיקייה שנבחרה אינה קיימת.")
            return
        if not api_key:
            messagebox.showerror(
                "שגיאה",
                "לא נמצא מפתח API.\nיש להגדיר GOOGLE_API_KEY בקובץ .env ליד main.py",
            )
            return

        # Reset state
        self._stop_event.clear()
        self._clear_log()
        self.progress["value"] = 0
        self.progress["maximum"] = 100
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.open_btn.config(state="disabled")
        self.copy_btn.config(state="disabled")
        self._output_folder = folder

        # Open log file
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_path = Path(folder) / f"log_{timestamp}.txt"
        try:
            self._log_file = open(log_path, "w", encoding="utf-8")
        except Exception:
            self._log_file = None

        thread = threading.Thread(
            target=self._run_in_thread,
            args=(folder, api_key),
            daemon=True,
        )
        thread.start()

    def _run_in_thread(self, folder: str, api_key: str):
        try:
            output_path = processor.process_folder(
                folder_path=folder,
                api_key=api_key,
                log_fn=self.log,
                progress_fn=self.set_progress,
                stop_event=self._stop_event,
            )
        except Exception as exc:
            self.log(f"\n[שגיאה קריטית] {exc}")
            output_path = None

        self.after(0, self._on_complete, output_path)

    # ------------------------------------------------------------------
    # Thread-safe UI callbacks
    # ------------------------------------------------------------------

    def log(self, message: str):
        self.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.config(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        # Write to log file (thread-safe via lock)
        if self._log_file:
            with self._log_lock:
                try:
                    self._log_file.write(message + "\n")
                    self._log_file.flush()
                except Exception:
                    pass

    def _clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")

    def set_progress(self, current: int, total: int):
        self.after(0, self._update_progress, current, total)

    def _update_progress(self, current: int, total: int):
        if total > 0:
            self.progress["maximum"] = total
            self.progress["value"] = current

    def _on_complete(self, output_path: str | None):
        # Close log file
        if self._log_file:
            try:
                self._log_file.close()
            except Exception:
                pass
            self._log_file = None

        self.start_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.open_btn.config(state="normal")
        self.copy_btn.config(state="normal")

        if output_path:
            messagebox.showinfo(
                "הושלם",
                f"העיבוד הסתיים בהצלחה.\n\nקובץ האקסל נשמר ב:\n{output_path}",
            )
        else:
            messagebox.showwarning(
                "הושלם",
                "העיבוד הסתיים.\nלא נוצר קובץ אקסל (לא חולצו נתונים או שלא נמצאו קבצי PDF).",
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = App()
    app.mainloop()
