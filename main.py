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

_LOG_BG  = "#1e1e1e"
_LOG_FG  = "#d4d4d4"
_LOG_FONT = ("Consolas", 9)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NovAccess – פענוח חשבוניות")
        self.resizable(True, True)
        self.minsize(660, 540)

        # --- Processing tab state ---
        self._output_folder: str | None = None
        self._stop_event = threading.Event()
        self._log_file = None
        self._log_lock = threading.Lock()

        # --- Merge tab state ---
        self._merge_files: list = []
        self._merge_out_dir_var = tk.StringVar()
        self._merge_out_name_var = tk.StringVar(
            value=f"merged_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        )

        self._merge_output_path: str | None = None

        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        nb = ttk.Notebook(self)
        nb.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)

        tab1 = ttk.Frame(nb)
        nb.add(tab1, text="  עיבוד חשבוניות  ")

        tab2 = ttk.Frame(nb)
        nb.add(tab2, text="  איחוד קבצים  ")

        self._build_process_tab(tab1)
        self._build_merge_tab(tab2)

    # ── Processing tab ─────────────────────────────────────────────────

    def _build_process_tab(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(3, weight=1)

        # Folder selection
        top = ttk.LabelFrame(parent, text="הגדרות", padding=10)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="תיקיית חשבוניות:").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self.folder_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.folder_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(top, text="בחר…", command=self._browse_folder).grid(row=0, column=2, padx=(6, 0))

        ttk.Label(top, text="קובץ לקוחות (אופציונלי):").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=(6, 0))
        self.customer_file_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.customer_file_var).grid(row=1, column=1, sticky="ew", pady=(6, 0))
        ttk.Button(top, text="בחר…", command=self._browse_customer_file).grid(row=1, column=2, padx=(6, 0), pady=(6, 0))

        # Action buttons
        btn_frame = ttk.Frame(parent)
        btn_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=4)

        self.start_btn = ttk.Button(btn_frame, text="▶  התחל עיבוד", command=self._start)
        self.start_btn.pack(side="left")

        self.stop_btn = ttk.Button(btn_frame, text="■  עצור", command=self._stop, state="disabled")
        self.stop_btn.pack(side="left", padx=(6, 0))

        self.open_btn = ttk.Button(btn_frame, text="📂  פתח תיקייה", command=self._open_folder, state="disabled")
        self.open_btn.pack(side="left", padx=(6, 0))

        self.copy_btn = ttk.Button(btn_frame, text="📋  העתק לוג", command=self._copy_log, state="disabled")
        self.copy_btn.pack(side="left", padx=(6, 0))

        # Progress bar
        self.progress = ttk.Progressbar(parent, mode="determinate")
        self.progress.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 4))

        # Log area
        log_frame = ttk.LabelFrame(parent, text="יומן פעילות", padding=4)
        log_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=(0, 10))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, state="disabled", wrap="word",
            font=_LOG_FONT, bg=_LOG_BG, fg=_LOG_FG,
            insertbackground="white",
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")

    # ── Merge tab ──────────────────────────────────────────────────────

    def _build_merge_tab(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=3)   # file list gets most space
        parent.rowconfigure(4, weight=1)   # merge log gets some space

        # Row 0 — add/remove buttons
        sel_frame = ttk.LabelFrame(parent, text="בחירת קבצים", padding=8)
        sel_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))

        ttk.Button(sel_frame, text="הוסף קבצים…", command=self._merge_add_files).pack(side="right", padx=(4, 0))
        ttk.Button(sel_frame, text="הסר נבחרים",  command=self._merge_remove_selected).pack(side="right", padx=(4, 0))
        ttk.Button(sel_frame, text="נקה הכל",     command=self._merge_clear_all).pack(side="right")

        # Row 1 — file listbox
        list_frame = ttk.LabelFrame(parent, text="קבצים שנבחרו", padding=4)
        list_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=4)
        list_frame.rowconfigure(0, weight=1)
        list_frame.columnconfigure(0, weight=1)

        sb = ttk.Scrollbar(list_frame, orient="vertical")
        self._merge_listbox = tk.Listbox(
            list_frame, selectmode="extended",
            yscrollcommand=sb.set,
            font=("Segoe UI", 9),
            activestyle="dotbox",
        )
        sb.config(command=self._merge_listbox.yview)
        self._merge_listbox.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")

        # Row 2 — output settings
        out_frame = ttk.LabelFrame(parent, text="הגדרות פלט", padding=8)
        out_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=4)
        out_frame.columnconfigure(1, weight=1)

        ttk.Label(out_frame, text="תיקיית פלט:").grid(row=0, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(out_frame, textvariable=self._merge_out_dir_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(out_frame, text="בחר…", command=self._merge_browse_output_dir).grid(row=0, column=2, padx=(4, 0))

        ttk.Label(out_frame, text="שם קובץ:").grid(row=1, column=0, sticky="e", padx=(0, 4), pady=(6, 0))
        ttk.Entry(out_frame, textvariable=self._merge_out_name_var).grid(row=1, column=1, columnspan=2, sticky="ew", pady=(6, 0))


        # Row 3 — action buttons
        act_frame = ttk.Frame(parent)
        act_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=4)

        ttk.Button(act_frame, text="  ⚡  איחד קבצים  ", command=self._merge_start).pack(side="left")
        ttk.Button(act_frame, text="📂  פתח תיקיית פלט", command=self._merge_open_output).pack(side="left", padx=(6, 0))

        # Row 4 — merge log
        mlog_frame = ttk.LabelFrame(parent, text="יומן איחוד", padding=4)
        mlog_frame.grid(row=4, column=0, sticky="nsew", padx=10, pady=(0, 10))
        mlog_frame.rowconfigure(0, weight=1)
        mlog_frame.columnconfigure(0, weight=1)

        self._merge_log_text = scrolledtext.ScrolledText(
            mlog_frame, state="disabled", wrap="word", height=6,
            font=_LOG_FONT, bg=_LOG_BG, fg=_LOG_FG,
            insertbackground="white",
        )
        self._merge_log_text.grid(row=0, column=0, sticky="nsew")

    # ------------------------------------------------------------------
    # Processing tab — event handlers
    # ------------------------------------------------------------------

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="בחר תיקיית חשבוניות")
        if folder:
            self.folder_var.set(folder)

    def _browse_customer_file(self):
        path = filedialog.askopenfilename(
            title="בחר קובץ לקוחות (Excel)",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.customer_file_var.set(path)

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

        self._stop_event.clear()
        self._clear_log()
        self.progress["value"] = 0
        self.progress["maximum"] = 100
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.open_btn.config(state="disabled")
        self.copy_btn.config(state="disabled")
        self._output_folder = folder

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_path = Path(folder) / f"log_{timestamp}.txt"
        try:
            self._log_file = open(log_path, "w", encoding="utf-8")
        except Exception:
            self._log_file = None

        threading.Thread(
            target=self._run_in_thread,
            args=(folder, api_key),
            daemon=True,
        ).start()

    def _run_in_thread(self, folder: str, api_key: str):
        customers_path = self.customer_file_var.get().strip() or None
        try:
            output_path = processor.process_folder(
                folder_path=folder,
                api_key=api_key,
                log_fn=self.log,
                progress_fn=self.set_progress,
                stop_event=self._stop_event,
                customers_path=customers_path,
            )
        except Exception as exc:
            self.log(f"\n[שגיאה קריטית] {exc}")
            output_path = None

        self.after(0, self._on_complete, output_path)

    # ------------------------------------------------------------------
    # Processing tab — thread-safe UI callbacks
    # ------------------------------------------------------------------

    def log(self, message: str):
        self.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.config(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
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

    # ------------------------------------------------------------------
    # Merge tab — event handlers
    # ------------------------------------------------------------------

    def _merge_add_files(self):
        paths = filedialog.askopenfilenames(
            title="בחר קבצי Excel לאיחוד",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialdir=self._merge_out_dir_var.get() or self.folder_var.get() or str(Path.home()),
        )
        added = 0
        for p in paths:
            if p not in self._merge_files:
                self._merge_files.append(p)
                added += 1
        if added:
            self._merge_refresh_listbox()
            # Auto-suggest output dir from first added file if not yet set
            if not self._merge_out_dir_var.get() and paths:
                self._merge_out_dir_var.set(str(Path(paths[0]).parent))

    def _merge_remove_selected(self):
        indices = list(self._merge_listbox.curselection())
        for i in reversed(indices):
            del self._merge_files[i]
        self._merge_refresh_listbox()

    def _merge_clear_all(self):
        self._merge_files.clear()
        self._merge_refresh_listbox()

    def _merge_refresh_listbox(self):
        self._merge_listbox.delete(0, "end")
        for p in self._merge_files:
            self._merge_listbox.insert("end", f"  {Path(p).name}")

    def _merge_browse_output_dir(self):
        folder = filedialog.askdirectory(title="בחר תיקיית פלט לקובץ המאוחד")
        if folder:
            self._merge_out_dir_var.set(folder)

    def _merge_open_output(self):
        target = self._merge_out_dir_var.get() or (
            str(Path(self._merge_output_path).parent) if self._merge_output_path else None
        )
        if target and Path(target).exists():
            os.startfile(target)

    def _merge_start(self):
        if not self._merge_files:
            messagebox.showerror("שגיאה", "יש לבחור לפחות קובץ אחד לאיחוד.")
            return

        out_dir  = self._merge_out_dir_var.get().strip()
        out_name = self._merge_out_name_var.get().strip()

        if not out_dir:
            messagebox.showerror("שגיאה", "יש לבחור תיקיית פלט.")
            return
        if not Path(out_dir).is_dir():
            messagebox.showerror("שגיאה", "תיקיית הפלט שנבחרה אינה קיימת.")
            return
        if not out_name:
            messagebox.showerror("שגיאה", "יש להזין שם קובץ פלט.")
            return
        if not out_name.lower().endswith(".xlsx"):
            out_name += ".xlsx"

        output_path = str(Path(out_dir) / out_name)

        if Path(output_path).exists():
            if not messagebox.askyesno("אישור", f"הקובץ '{out_name}' כבר קיים. האם לדרוס אותו?"):
                return

        self._merge_log_clear()
        self._merge_log(f"מתחיל איחוד {len(self._merge_files)} קבצים…")

        threading.Thread(
            target=self._merge_run_in_thread,
            args=(list(self._merge_files), output_path),
            daemon=True,
        ).start()

    def _merge_run_in_thread(self, file_paths, output_path):
        try:
            detail_out = processor.merge_excel_files(
                file_paths=file_paths,
                output_path=output_path,
                log_fn=self._merge_log,
            )
            self.after(0, self._merge_on_complete, detail_out, None, None)
        except Exception as exc:
            self.after(0, self._merge_on_complete, None, None, str(exc))

    def _merge_on_complete(self, detail_out, summary_out, error):
        if error:
            self._merge_log(f"[שגיאה] {error}")
            messagebox.showerror("שגיאת איחוד", error)
        else:
            self._merge_output_path = detail_out
            self._merge_log("האיחוד הושלם בהצלחה.")
            messagebox.showinfo("הושלם", f"האיחוד הושלם בהצלחה!\n\nקובץ: {detail_out}")

    # ------------------------------------------------------------------
    # Merge tab — thread-safe log helpers
    # ------------------------------------------------------------------

    def _merge_log(self, message: str):
        self.after(0, self._merge_append_log, message)

    def _merge_append_log(self, message: str):
        self._merge_log_text.config(state="normal")
        self._merge_log_text.insert("end", message + "\n")
        self._merge_log_text.see("end")
        self._merge_log_text.config(state="disabled")

    def _merge_log_clear(self):
        self._merge_log_text.config(state="normal")
        self._merge_log_text.delete("1.0", "end")
        self._merge_log_text.config(state="disabled")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = App()
    app.mainloop()
