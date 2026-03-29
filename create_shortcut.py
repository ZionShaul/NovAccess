"""
create_shortcut.py – הרץ פעם אחת ליצירת קיצור דרך בשולחן העבודה.
"""

import sys
from pathlib import Path

try:
    import winshell
    import win32com.client
except ImportError:
    print("מתקין תלויות נדרשות...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pywin32", "winshell"])
    import winshell
    import win32com.client

desktop = Path(winshell.desktop())
shortcut_path = desktop / "NovAccess.lnk"

pythonw = Path(sys.executable).parent / "pythonw.exe"
if not pythonw.exists():
    pythonw = Path(sys.executable)  # fallback to python.exe

main_script = Path(__file__).parent / "main.py"

shell = win32com.client.Dispatch("WScript.Shell")
shortcut = shell.CreateShortCut(str(shortcut_path))
shortcut.Targetpath = str(pythonw)
shortcut.Arguments = f'"{main_script}"'
shortcut.WorkingDirectory = str(main_script.parent)
shortcut.IconLocation = str(pythonw)
shortcut.Description = "NovAccess – פענוח חשבוניות אוטומטי"
shortcut.save()

print(f"קיצור דרך נוצר בהצלחה:\n{shortcut_path}")
