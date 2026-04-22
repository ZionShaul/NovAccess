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

import pypdf

from google import genai
from google.genai import types
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
    "DESHEN":   "Prompt_DESHEN.txt",
}

# שם תצוגה קבוע לכל ספק — נקבע לפי זיהוי הספק, לא לפי מה שבחשבונית
SUPPLIER_DISPLAY_NAMES = {
    "ALEX":     "אלאקס",
    "MASHBIR":  "המשביר לחקלאי",
    "KOL_BO":   "כל בו לחקלאי",
    "AMIR":     "עמיר שיווק",
    "TIV_CHIM": "טיב כימיקלים",
    "DESHEN":   "דשן הצפון",
}

EXPECTED_COLUMNS = [
    "ספק", "לקוח", "מספר_חשבונית", "תאריך_חשבונית",
    "מספר_תעודת_משלוח", "תאריך_תעודה", "מקט",
    "תיאור_מוצר", "כמות", "מחיר_ליחידה", "סהכ_מחיר",
    "מודל_חילוץ", "שם_קובץ",
]

# מודלים לזיהוי ספק — משימה פשוטה, מודל זול מספיק
ID_MODELS = ["gemini-flash-latest", "gemini-2.0-flash-lite"]

# מודלים לחילוץ נתונים — דורש reasoning מעמיק
EXTRACTION_MODELS = [
    "gemini-2.5-flash",       # stable thinking model — עיקרי
    "gemini-3-flash-preview", # preview thinking model — fallback
    "gemini-flash-latest",    # fallback מהיר (ללא thinking)
]

# מודלים שתומכים ב-thinking config
THINKING_MODELS = {
    "gemini-2.5-flash",
    "gemini-3-flash-preview",
}
# thinking_budget=-1 = AUTOMATIC (המודל מחליט כמה לחשוב לפי מורכבות הבקשה)
THINKING_BUDGET = -1

MAX_RETRIES = 3

# מספר מרבי של לקוחות שיוזרקו לפרומפט (כדי לא להעמיס יותר מדי)
MAX_CUSTOMERS_IN_PROMPT = 300

# Timeout exception types for the old SDK
_GEMINI_TIMEOUT = (TimeoutError, google.api_core.exceptions.DeadlineExceeded)

_SCRIPT_DIR = Path(__file__).parent
_client: genai.Client | None = None

# נתיב קבוע לקובץ רשימת הלקוחות
CUSTOMERS_FILE = _SCRIPT_DIR / "לקוחות.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_prompt(filename: str) -> str:
    return (_SCRIPT_DIR / filename).read_text(encoding="utf-8")


def load_customer_list(xlsx_path: str) -> list[str]:
    """
    טוען רשימת לקוחות מקובץ Excel.
    מחפש עמודה בשם 'שם_לקוח', 'לקוח' או 'שם לקוח'; אחרת לוקח את העמודה הראשונה.
    מחזיר רשימת שמות ללא ערכים ריקים.
    """
    df = pd.read_excel(xlsx_path)
    for col in ["שם_לקוח", "לקוח", "שם לקוח"]:
        if col in df.columns:
            return df[col].dropna().astype(str).str.strip().tolist()
    # fallback — עמודה ראשונה
    return df.iloc[:, 0].dropna().astype(str).str.strip().tolist()


def _build_customer_appendix(customers: list[str]) -> str:
    """בונה הנחיה לצירוף לפרומפט עם רשימת הלקוחות."""
    trimmed = customers[:MAX_CUSTOMERS_IN_PROMPT]
    lines = "\n".join(f"- {c}" for c in trimmed)
    return (
        "\n\n"
        "רשימת לקוחות ידועים — בחר את ההתאמה הקרובה ביותר לשם המופיע בחשבונית:\n"
        f"{lines}\n"
        "אם השם בחשבונית קרוב לאחד מהרשימה, השתמש בשם המדויק מהרשימה. "
        "אם לא קיימת התאמה סבירה, השתמש בשם כפי שמופיע בחשבונית."
    )


def _fix_hebrew_gershayim(s: str) -> str:
    """
    תיקון גרשיים עבריים שאינם מוסקפים בתוך ערכי JSON.
    למשל: אגש"ח → אגש'ח , בע"מ → בע'מ
    מחליף " שמופיע בין תווים עבריים בגרש בודד '.
    """
    return re.sub(r'(?<=[א-ת])"(?=[א-תa-zA-Z])', "'", s)


def clean_json_response(raw: str) -> str:
    raw = raw.strip()
    # Priority 1: JSON inside a code block
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", raw)
    if match:
        candidate = match.group(1).strip()
    else:
        # Priority 2: first {...} block (handles text prefix/suffix from model)
        match = re.search(r"(\{[\s\S]*\})", raw)
        candidate = match.group(1).strip() if match else raw

    # Fix unescaped Hebrew gershayim (e.g., אגש"ח, בע"מ) that break JSON parsing
    try:
        json.loads(candidate)
        return candidate          # already valid — no fix needed
    except json.JSONDecodeError:
        fixed = _fix_hebrew_gershayim(candidate)
        return fixed


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
    # פותחים כ-binary stream כדי לעקוף שגיאת ASCII encoding בשמות קבצים עבריים
    with open(pdf_path, "rb") as f:
        uploaded = _client.files.upload(
            file=f,
            config=types.UploadFileConfig(mime_type="application/pdf"),
        )
    while uploaded.state.name != "ACTIVE":
        time.sleep(3)
        uploaded = _client.files.get(name=uploaded.name)
    return uploaded


def call_gemini_with_retry(pdf_path: str, prompt: str, log_fn, models=None) -> str:
    """
    Upload PDF, try each model up to MAX_RETRIES times.
    Always deletes the uploaded file.
    Returns response text on success; raises on total failure.
    models: רשימת מודלים לניסיון (ברירת מחדל: EXTRACTION_MODELS)
    """
    if models is None:
        models = EXTRACTION_MODELS

    uploaded_file = _upload_and_wait(pdf_path, log_fn)

    try:
        for model_name in models:
            use_thinking = model_name in THINKING_MODELS
            gen_config = (
                types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET)
                )
                if use_thinking else None
            )
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    thinking_label = " [thinking]" if use_thinking else ""
                    log_fn(f"  שולח ל-Gemini ({model_name}{thinking_label}, ניסיון {attempt}/{MAX_RETRIES})...")
                    kwargs = {}
                    if gen_config:
                        kwargs["config"] = gen_config
                    response = _client.models.generate_content(
                        model=model_name,
                        contents=[uploaded_file, prompt],
                        **kwargs,
                    )
                    log_fn(f"  מודל פעיל: {model_name}{thinking_label}")
                    label = "thinking" if use_thinking else "fast"
                    return response.text, label
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
            _client.files.delete(name=uploaded_file.name)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Two-step processing
# ---------------------------------------------------------------------------

def identify_supplier(pdf_path: str, id_prompt: str, log_fn) -> str:
    raw, _ = call_gemini_with_retry(pdf_path, id_prompt, log_fn, models=ID_MODELS)
    data = json.loads(clean_json_response(raw))
    return data.get("supplier_id", "UNKNOWN")


_HEADER_FIELDS = ["ספק", "לקוח", "מספר_חשבונית", "תאריך_חשבונית"]


def _fill_missing_header_fields(rows: list, pdf_path: str = "") -> list:
    """
    אם המודל השמיט שדות header חוזרים (ספק/לקוח/מספר_חשבונית/תאריך_חשבונית),
    ממצא את ערכיהם מהשורות שכן כוללות אותם וממלא את השאר.
    fallback: מספר_חשבונית נלקח משם הקובץ אם לא נמצא בשורות.
    """
    collected = {}
    for row in rows:
        for field in _HEADER_FIELDS:
            if field not in collected and row.get(field):
                collected[field] = row[field]
        if len(collected) == len(_HEADER_FIELDS):
            break

    for row in rows:
        for field, value in collected.items():
            if not row.get(field):
                row[field] = value

    # fallback: מספר חשבונית משם הקובץ
    if pdf_path and not collected.get("מספר_חשבונית"):
        inv_num = Path(pdf_path).stem
        for row in rows:
            if not row.get("מספר_חשבונית"):
                row["מספר_חשבונית"] = inv_num

    return rows


def extract_invoice_data(
    pdf_path: str,
    supplier_prompt: str,
    log_fn,
    customer_list: list | None = None,
) -> list:
    prompt = supplier_prompt
    if customer_list:
        prompt = prompt + _build_customer_appendix(customer_list)

    last_exc = None
    for json_attempt in range(1, 4):
        raw, model_label = call_gemini_with_retry(pdf_path, prompt, log_fn)
        cleaned = clean_json_response(raw)
        try:
            data = json.loads(cleaned)
            rows = data if isinstance(data, list) else data["rows"]
            rows = _fill_missing_header_fields(rows, pdf_path=pdf_path)
            return rows, model_label
        except json.JSONDecodeError as exc:
            last_exc = exc
            log_fn(f"  [אזהרה] JSON לא תקין (ניסיון {json_attempt}/3): {exc}")
            log_fn(f"  תגובה גולמית (200 תווים): {raw[:200]!r}")
            if json_attempt < 3:
                log_fn("  מנסה שוב...")
                time.sleep(3)
    raise last_exc


# ---------------------------------------------------------------------------
# MSG extraction
# ---------------------------------------------------------------------------

def extract_pdfs_from_msg(msg_path: Path, dest_folder: Path, log_fn) -> list:
    """חולץ קבצי PDF מקובץ MSG ומחזיר רשימת נתיבים."""
    import extract_msg
    extracted = []
    try:
        with extract_msg.openMsg(str(msg_path)) as msg:
            for att in msg.attachments:
                name = (att.longFilename or att.shortFilename or "").rstrip("\x00").strip()
                if name.lower().endswith(".pdf"):
                    out_path = dest_folder / name
                    if out_path.exists():
                        out_path = dest_folder / f"{out_path.stem}_from_{msg_path.stem}.pdf"
                    att.save(customPath=str(dest_folder), customFilename=out_path.name.rstrip("\x00"))
                    extracted.append(out_path)
                    log_fn(f"  [MSG] חולץ: {out_path.name} מתוך {msg_path.name}")
    except Exception as e:
        log_fn(f"  [MSG] שגיאה בחילוץ {msg_path.name}: {e}")
    return extracted


# ---------------------------------------------------------------------------
# EML extraction
# ---------------------------------------------------------------------------

def extract_pdfs_from_eml(eml_path: Path, dest_folder: Path, log_fn) -> list:
    """חולץ קבצי PDF מקובץ EML ומחזיר רשימת נתיבים."""
    import email
    from email import policy as email_policy

    extracted = []
    try:
        with open(eml_path, "rb") as f:
            msg = email.message_from_binary_file(f, policy=email_policy.default)
        for part in msg.walk():
            name = part.get_filename() or ""
            if name.lower().endswith(".pdf"):
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                dest_folder.mkdir(parents=True, exist_ok=True)
                out_path = dest_folder / name
                if out_path.exists():
                    out_path = dest_folder / f"{out_path.stem}_from_{eml_path.stem}.pdf"
                out_path.write_bytes(payload)
                extracted.append(out_path)
                log_fn(f"  [EML] חולץ: {out_path.name} מתוך {eml_path.name}")
    except Exception as e:
        log_fn(f"  [EML] שגיאה בחילוץ {eml_path.name}: {e}")
    return extracted


# ---------------------------------------------------------------------------
# Multi-invoice PDF splitting (המשביר SPS1 format)
# ---------------------------------------------------------------------------

_SPS1_RE   = re.compile(r"SPS1:(\d+):")
_DESHEN_RE = re.compile(r"SI266(\d{6})")


def _invoice_num_from_page(page) -> str | None:
    """Extract invoice number from page — supports SPS1 (המשביר) and SI266 (דשן הצפון)."""
    text = page.extract_text() or ""
    m = _SPS1_RE.search(text)
    if m:
        return m.group(1)
    m = _DESHEN_RE.search(text)
    if m:
        return f"SI266{m.group(1)}"
    return None


def split_multi_invoice_pdf(pdf_path: Path, dest_folder: Path, log_fn) -> list:
    """
    אם ה-PDF מכיל מספר חשבוניות (לפי SPS1 / SI266 header), מפרק לקבצים נפרדים.
    מחזיר רשימת נתיבים לקבצים שנוצרו.
    אם ה-PDF מכיל חשבונית אחת בלבד, מחזיר רשימה ריקה (אין צורך בפירוק).
    """
    reader = pypdf.PdfReader(str(pdf_path))

    # Build groups: list of [invoice_num, [page_indices]]
    groups = []
    for i, page in enumerate(reader.pages):
        inv = _invoice_num_from_page(page)
        if groups and groups[-1][0] == inv:
            groups[-1][1].append(i)
        else:
            groups.append([inv, [i]])

    if len(groups) <= 1:
        return []  # single invoice or unrecognized format — no split needed

    # במסמך SI266 (דשן): עמוד ללא מספר = עמוד המשך — נצמד לקבוצה הקודמת
    is_si266_doc = any(inv and inv.startswith("SI266") for inv, _ in groups)
    if is_si266_doc:
        merged = []
        for inv, pages in groups:
            if inv is None and merged:
                merged[-1][1].extend(pages)
            else:
                merged.append([inv, pages])
        groups = merged

    dest_folder.mkdir(parents=True, exist_ok=True)
    created = []
    skipped = 0
    for inv_num, page_indices in groups:
        if inv_num is None:
            skipped += len(page_indices)
            continue  # דפי כיסוי / תנאים ללא מספר חשבונית — מדלגים
        out_path = dest_folder / f"{inv_num}.pdf"
        writer = pypdf.PdfWriter()
        for idx in page_indices:
            writer.add_page(reader.pages[idx])
        with open(out_path, "wb") as f:
            writer.write(f)
        created.append(out_path)

    skip_note = f", דולגו {skipped} עמ' ללא כותרת" if skipped else ""
    log_fn(f"  [פירוק] {pdf_path.name}: {len(created)} חשבוניות{skip_note} → {dest_folder.name}/")
    return created


# ---------------------------------------------------------------------------
# Per-PDF orchestration
# ---------------------------------------------------------------------------

def process_single_pdf(
    pdf_path: Path,
    prompts: dict,
    archive_dir: Path,
    log_fn,
    customer_list: list | None = None,
) -> list | None:
    if not pdf_path.exists():
        log_fn(f"  [דילוג] קובץ לא נמצא (כנראה כבר עובד): {pdf_path.name}")
        return None

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
        rows, model_label = extract_invoice_data(str(pdf_path), prompts[supplier_id], log_fn, customer_list=customer_list)
    except Exception as exc:
        log_fn(f"  [שגיאה] חילוץ נתונים נכשל: {exc}")
        _move(pdf_path, errors_dir)
        return None

    # שם ספק נקבע מהזיהוי — לא ממה שהמודל חולץ מהחשבונית
    display_name = SUPPLIER_DISPLAY_NAMES.get(supplier_id, "")
    for row in rows:
        row["ספק"] = display_name or row.get("ספק") or supplier_id
        row["מודל_חילוץ"] = model_label
        row["שם_קובץ"] = pdf_path.name

    log_fn(f"  {len(rows)} שורות חולצו בהצלחה")
    _move(pdf_path, archive_dir / supplier_id)
    return rows


def _move(src: Path, dest_dir: Path):
    if not src.exists():
        return
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
    customers_path: str | None = None,
) -> str | None:
    """
    Process all PDFs in folder_path.
    Returns path to the saved Excel file, or None if no data extracted.
    Stops early if stop_event is set.
    """
    folder = Path(folder_path)
    archive_dir = folder / "processed"

    global _client
    _client = genai.Client(api_key=api_key)

    prompts = _load_all_prompts()

    # טעינת רשימת לקוחות (אופציונלי)
    customer_list: list | None = None
    if customers_path and Path(customers_path).exists():
        try:
            customer_list = load_customer_list(customers_path)
            log_fn(f"נטענה רשימת לקוחות: {len(customer_list)} לקוחות מ-{Path(customers_path).name}")
        except Exception as e:
            log_fn(f"[אזהרה] לא ניתן לטעון רשימת לקוחות: {e}")

    # שלב 1: חלץ PDFים מקבצי MSG / EML
    mail_files = sorted(folder.glob("*.msg")) + sorted(folder.glob("*.eml"))
    if mail_files:
        tmp_extract_dir = folder / "_msg_extracted"
        tmp_extract_dir.mkdir(exist_ok=True)
        for mail_file in mail_files:
            log_fn(f"מעבד קובץ מייל: {mail_file.name}")
            if mail_file.suffix.lower() == ".msg":
                extract_pdfs_from_msg(mail_file, tmp_extract_dir, log_fn)
            else:
                extract_pdfs_from_eml(mail_file, tmp_extract_dir, log_fn)
        log_fn("")
    else:
        tmp_extract_dir = None

    # שלב 2: פרק PDFs עם מרובה חשבוניות (SPS1 format)
    candidate_pdfs = list(sorted(folder.glob("*.pdf")))
    if tmp_extract_dir:
        candidate_pdfs += list(sorted(tmp_extract_dir.glob("*.pdf")))

    split_dir = folder / "_split"
    final_pdfs = []
    seen_stems = set()
    for pdf in candidate_pdfs:
        split_results = split_multi_invoice_pdf(pdf, split_dir, log_fn)
        candidates = split_results if split_results else [pdf]
        for p in candidates:
            if p.stem in seen_stems:
                log_fn(f"  [כפול] {p.name} כבר בתור — מדלג")
            else:
                seen_stems.add(p.stem)
                final_pdfs.append(p)

    pdfs = final_pdfs
    total = len(pdfs)

    if total == 0:
        log_fn("לא נמצאו קבצי PDF בתיקייה (גם לא בתוך קבצי MSG).")
        progress_fn(0, 0)
        return None

    log_fn(f"נמצאו {total} קבצי PDF לעיבוד. מתחיל...\n")

    all_rows = []
    duplicate_rows = []
    seen_invoice_numbers: set[str] = set()
    success_count = 0
    error_count = 0
    duplicate_count = 0

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_path = folder / f"invoices_{timestamp}.xlsx"
    duplicate_path = folder / f"invoices_{timestamp}_duplicates.xlsx"

    for i, pdf_path in enumerate(pdfs, start=1):
        if stop_event and stop_event.is_set():
            log_fn("עיבוד הופסק על ידי המשתמש.")
            break

        log_fn(f"[{i}/{total}] {pdf_path.name}")
        rows = process_single_pdf(pdf_path, prompts, archive_dir, log_fn, customer_list=customer_list)
        if rows is not None:
            inv_num = str(rows[0].get("מספר_חשבונית") or "").strip() if rows else ""
            if inv_num and inv_num in seen_invoice_numbers:
                duplicate_rows.extend(rows)
                duplicate_count += 1
                log_fn(f"  [כפילות] חשבונית {inv_num} כבר קיימת — הועברה לקובץ כפילויות")
            else:
                if inv_num:
                    seen_invoice_numbers.add(inv_num)
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
    if duplicate_count:
        log_fn(f"כפילויות:          {duplicate_count} חשבוניות")

    dup_out: str | None = None
    if duplicate_rows:
        save_excel(duplicate_rows, str(duplicate_path))
        dup_out = str(duplicate_path)
        log_fn(f"קובץ כפילויות:     {duplicate_path}")

    if not all_rows:
        log_fn("לא חולצו נתונים – לא נוצר קובץ אקסל.")
        log_fn("=" * 40)
        return None, dup_out

    log_fn(f"קובץ פירוט נשמר:   {output_path}")
    log_fn("=" * 40)

    return str(output_path), dup_out


# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------

# regex לזיהוי שורות סיכום — מכסה וריאציות כמו סה"כ / סהכ / מע"מ / מעמ
_SUMMARY_PATTERN = r'סה.?כ|מע.?מ|עיגול'


def save_excel(all_rows: list, output_path: str) -> None:
    df = pd.DataFrame(all_rows, columns=EXPECTED_COLUMNS)

    # הסרת שורות סיכום לחלוטין — לא בפירוט ולא בקובץ נפרד
    is_summary = df["תיאור_מוצר"].str.contains(_SUMMARY_PATTERN, na=False, regex=True)
    detail_df = df[~is_summary]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        detail_df.to_excel(writer, index=False, sheet_name="Invoices")


# ---------------------------------------------------------------------------
# Excel merge
# ---------------------------------------------------------------------------

def merge_excel_files(
    file_paths: list,
    output_path: str,
    log_fn=None,
) -> str:
    """
    מאחד מספר קבצי Excel (גיליון Invoices) לקובץ אחד.
    מחזיר את נתיב הקובץ המאוחד.
    """
    if not file_paths:
        raise ValueError("לא נבחרו קבצים לאיחוד")

    frames = []
    for p in file_paths:
        if not Path(p).exists():
            raise FileNotFoundError(f"קובץ לא נמצא: {p}")
        try:
            df = pd.read_excel(p, sheet_name="Invoices")
        except Exception:
            raise ValueError(f"קובץ לא תקין או חסר גיליון 'Invoices': {Path(p).name}")
        frames.append(df)
        if log_fn:
            log_fn(f"  נקרא: {Path(p).name} — {len(df)} שורות")

    merged = pd.concat(frames, ignore_index=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as w:
        merged.to_excel(w, index=False, sheet_name="Invoices")

    if log_fn:
        log_fn(f"נכתב: {Path(output_path).name} ({len(merged)} שורות סה\"כ)")

    return output_path
