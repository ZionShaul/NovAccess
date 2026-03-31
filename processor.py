"""
NovAccess – processor.py
Gemini API logic, invoice processing, and Excel export.
Uses google-generativeai SDK (same pattern as amber-automation).
"""

import json
import re
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path

import google.generativeai as genai
import google.api_core.exceptions
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPLIER_PROMPT_MAP = {
    "ALEX":     "Prompt_ALEX.txt",
    "MASHBIR":  "Prompt_MASHBIR.txt",
    "KOL_BO":   "Prompt_KOL_BO.txt",
    "AMIR":     "Prompt_AMIR.txt",
    "TIV_CHIM": "Prompt_TIV.txt",
}

EXPECTED_COLUMNS = [
    "ספק", "לקוח", "מספר_חשבונית", "תאריך_חשבונית",
    "מספר_תעודת_משלוח", "תאריך_תעודה", "מקט",
    "תיאור_מוצר", "כמות", "מחיר_ליחידה", "סהכ_מחיר",
]

# Model fallback list — primary first
MODELS = ["gemini-flash-latest", "gemini-2.0-flash-lite"]

MAX_RETRIES = 3

# Timeout exception types for the old SDK
_GEMINI_TIMEOUT = (TimeoutError, google.api_core.exceptions.DeadlineExceeded)

_SCRIPT_DIR = Path(__file__).parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_prompt(filename: str) -> str:
    return (_SCRIPT_DIR / filename).read_text(encoding="utf-8")


def clean_json_response(raw: str) -> str:
    raw = raw.strip()
    # Priority 1: JSON inside a code block
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", raw)
    if match:
        return match.group(1).strip()
    # Priority 2: first {...} block (handles text prefix/suffix from model)
    match = re.search(r"(\{[\s\S]*\})", raw)
    if match:
        return match.group(1).strip()
    return raw


def _load_all_prompts() -> dict:
    prompts = {"id": load_prompt("Supplier_Identification_Prompt.txt")}
    for supplier_id, filename in SUPPLIER_PROMPT_MAP.items():
        prompts[supplier_id] = load_prompt(filename)
    return prompts


# ---------------------------------------------------------------------------
# Gemini API helpers (mirrors amber-automation pattern)
# ---------------------------------------------------------------------------

def _upload_and_wait(pdf_path: str, log_fn) -> object:
    """Upload PDF and wait until Gemini marks it ACTIVE."""
    log_fn("  מעלה PDF ל-Gemini...")
    uploaded = genai.upload_file(str(pdf_path), mime_type="application/pdf")
    while uploaded.state.name != "ACTIVE":
        time.sleep(3)
        uploaded = genai.get_file(uploaded.name)
    return uploaded


def call_gemini_with_retry(pdf_path: str, prompt: str, log_fn) -> str:
    """
    Upload PDF, try each model up to MAX_RETRIES times.
    Always deletes the uploaded file.
    Returns response text on success; raises on total failure.
    """
    uploaded_file = _upload_and_wait(pdf_path, log_fn)

    try:
        for model_name in MODELS:
            model = genai.GenerativeModel(model_name)
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    log_fn(f"  שולח ל-Gemini ({model_name}, ניסיון {attempt}/{MAX_RETRIES})...")
                    response = model.generate_content(
                        [uploaded_file, prompt],
                        request_options={"timeout": 600},
                    )
                    log_fn(f"  מודל פעיל: {model_name}")
                    return response.text
                except _GEMINI_TIMEOUT:
                    if attempt == MAX_RETRIES:
                        raise
                    log_fn(f"  timeout – מנסה שוב בעוד 5 שניות...")
                    time.sleep(5)
                except Exception as exc:
                    log_fn(f"  מודל {model_name} נכשל: {exc}")
                    break  # try next model

        raise RuntimeError("כל מודלי Gemini נכשלו — בדוק מפתח API והרשאות חשבון")

    finally:
        try:
            genai.delete_file(uploaded_file.name)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Two-step processing
# ---------------------------------------------------------------------------

def identify_supplier(pdf_path: str, id_prompt: str, log_fn) -> str:
    raw = call_gemini_with_retry(pdf_path, id_prompt, log_fn)
    data = json.loads(clean_json_response(raw))
    return data.get("supplier_id", "UNKNOWN")


def extract_invoice_data(pdf_path: str, supplier_prompt: str, log_fn) -> list:
    last_exc = None
    for json_attempt in range(1, 4):
        raw = call_gemini_with_retry(pdf_path, supplier_prompt, log_fn)
        cleaned = clean_json_response(raw)
        try:
            data = json.loads(cleaned)
            return data["rows"]
        except json.JSONDecodeError as exc:
            last_exc = exc
            log_fn(f"  [אזהרה] JSON לא תקין (ניסיון {json_attempt}/3): {exc}")
            log_fn(f"  תגובה גולמית (200 תווים): {raw[:200]!r}")
            if json_attempt < 3:
                log_fn("  מנסה שוב...")
                time.sleep(3)
    raise last_exc


# ---------------------------------------------------------------------------
# Per-PDF orchestration
# ---------------------------------------------------------------------------

def process_single_pdf(
    pdf_path: Path,
    prompts: dict,
    archive_dir: Path,
    log_fn,
) -> list | None:
    errors_dir = archive_dir / "errors"

    # Step 1: identify supplier
    try:
        log_fn("  זיהוי ספק...")
        supplier_id = identify_supplier(str(pdf_path), prompts["id"], log_fn)
    except Exception as exc:
        log_fn(f"  [שגיאה] זיהוי ספק נכשל: {exc}")
        _move(pdf_path, errors_dir)
        return None

    if supplier_id not in SUPPLIER_PROMPT_MAP:
        log_fn(f"  [דילוג] ספק לא זוהה (תשובה: {supplier_id!r})")
        _move(pdf_path, errors_dir)
        return None

    log_fn(f"  ספק זוהה: {supplier_id}")

    # Step 2: extract data
    try:
        log_fn("  חילוץ נתונים...")
        rows = extract_invoice_data(str(pdf_path), prompts[supplier_id], log_fn)
    except Exception as exc:
        log_fn(f"  [שגיאה] חילוץ נתונים נכשל: {exc}")
        _move(pdf_path, errors_dir)
        return None

    log_fn(f"  {len(rows)} שורות חולצו בהצלחה")
    _move(pdf_path, archive_dir / supplier_id)
    return rows


def _move(src: Path, dest_dir: Path):
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    shutil.move(str(src), str(dest))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_folder(
    folder_path: str,
    api_key: str,
    log_fn,
    progress_fn,
    stop_event: threading.Event | None = None,
) -> str | None:
    """
    Process all PDFs in folder_path.
    Returns path to the saved Excel file, or None if no data extracted.
    Stops early if stop_event is set.
    """
    folder = Path(folder_path)
    archive_dir = folder / "processed"

    genai.configure(api_key=api_key)

    prompts = _load_all_prompts()

    pdfs = sorted(folder.glob("*.pdf"))
    total = len(pdfs)

    if total == 0:
        log_fn("לא נמצאו קבצי PDF בתיקייה.")
        progress_fn(0, 0)
        return None

    log_fn(f"נמצאו {total} קבצי PDF. מתחיל עיבוד...\n")

    all_rows = []
    success_count = 0
    error_count = 0

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_path = folder / f"invoices_{timestamp}.xlsx"

    for i, pdf_path in enumerate(pdfs, start=1):
        if stop_event and stop_event.is_set():
            log_fn("עיבוד הופסק על ידי המשתמש.")
            break

        log_fn(f"[{i}/{total}] {pdf_path.name}")
        rows = process_single_pdf(pdf_path, prompts, archive_dir, log_fn)
        if rows is not None:
            all_rows.extend(rows)
            success_count += 1
            save_excel(all_rows, str(output_path))  # incremental save after each success
        else:
            error_count += 1
        progress_fn(i, total)
        log_fn("")

    # Summary
    log_fn("=" * 40)
    log_fn("סיכום")
    log_fn(f"סה\"כ קבצים:       {total}")
    log_fn(f"פוענחו בהצלחה:    {success_count}")
    log_fn(f"נכשלו:             {error_count}")

    if not all_rows:
        log_fn("לא חולצו נתונים – לא נוצר קובץ אקסל.")
        return None

    log_fn(f"קובץ פירוט נשמר:   {output_path}")
    summary_path = str(output_path).replace(".xlsx", "_summary.xlsx")
    if Path(summary_path).exists():
        log_fn(f"קובץ סיכום נשמר:   {summary_path}")
    log_fn("=" * 40)

    return str(output_path)


# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------

_SUMMARY_LABELS = {"סה\"כ נטו", "מע\"מ", "סה\"כ לתשלום", "עיגול"}


def save_excel(all_rows: list, output_path: str) -> None:
    df = pd.DataFrame(all_rows, columns=EXPECTED_COLUMNS)

    is_summary = df["תיאור_מוצר"].isin(_SUMMARY_LABELS)
    detail_df  = df[~is_summary]
    summary_df = df[is_summary]

    # Detail file — line items only, no summary rows
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        detail_df.to_excel(writer, index=False, sheet_name="Invoices")

    # Summary file — summary rows only, saved alongside detail file
    if not summary_df.empty:
        summary_path = output_path.replace(".xlsx", "_summary.xlsx")
        with pd.ExcelWriter(summary_path, engine="openpyxl") as writer:
            summary_df.to_excel(writer, index=False, sheet_name="Summary")
