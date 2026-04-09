"""
CRM Data Cleaner — Folder Watcher
===================================
Drop any CRM export (CSV, Excel, JSON) into the watch_inbox/ folder.
The cleaner runs automatically and puts the results in watch_output/<timestamp>_<filename>/.

Usage
-----
  python folder_watcher.py                     # watches ./watch_inbox/
  python folder_watcher.py --inbox my_folder   # custom inbox folder
  python folder_watcher.py --format workbook   # output as Excel workbook

Dependencies
------------
  pip install watchdog pandas openpyxl rapidfuzz phonenumbers

Stop with Ctrl-C.
"""

import argparse
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    print("Error: watchdog is required. Run: pip install watchdog")
    sys.exit(1)

from crm_cleaner import clean

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DEFAULT_INBOX  = "watch_inbox"
DEFAULT_OUTPUT = "watch_output"
SUPPORTED_EXTS = {".csv", ".xlsx", ".xls", ".json"}

# How long to wait after a file appears before processing it
# (gives time for the file to finish copying/writing)
SETTLE_SECONDS = 2


# ─── EVENT HANDLER ────────────────────────────────────────────────────────────

class CRMFileHandler(FileSystemEventHandler):

    def __init__(self, output_root: str, out_format: str):
        self.output_root = output_root
        self.out_format  = out_format
        self._processing = set()   # prevent double-trigger

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in SUPPORTED_EXTS:
            return
        if str(path) in self._processing:
            return
        self._processing.add(str(path))
        self._process(path)

    def on_moved(self, event):
        """Also fires when apps save a temp file then rename it into the inbox."""
        if event.is_directory:
            return
        path = Path(event.dest_path)
        if path.suffix.lower() not in SUPPORTED_EXTS:
            return
        if str(path) in self._processing:
            return
        self._processing.add(str(path))
        self._process(path)

    def _process(self, path: Path):
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        stem = path.stem
        run_dir = os.path.join(self.output_root, f"{ts}_{stem}")
        os.makedirs(run_dir, exist_ok=True)

        print(f"\n[{ts}] New file detected: {path.name}")
        print(f"  Output folder: {run_dir}")

        # Wait for file to settle (finish writing)
        time.sleep(SETTLE_SECONDS)

        ext = ".xlsx" if self.out_format in ("xlsx", "workbook") else ".csv"

        out_clean   = os.path.join(run_dir, f"crm_cleaned{ext}")
        out_flagged = os.path.join(run_dir, f"crm_flagged{ext}")
        out_dupl    = os.path.join(run_dir, f"crm_duplicates{ext}")
        out_report  = os.path.join(run_dir, "crm_report.html")

        try:
            result = clean(
                input_path=str(path),
                out_clean=out_clean,
                out_flagged=out_flagged,
                out_dupl=out_dupl,
                out_report=out_report,
                generate_report=True,
                verbose=True,
                out_format=self.out_format,
            )
            print(f"  Done — {len(result['clean'])} clean, "
                  f"{len(result['flagged'])} flagged, "
                  f"{len(result['duplicates'])} duplicates removed.")
        except Exception as exc:
            print(f"  ERROR processing {path.name}: {exc}")
            # Write an error log so you can diagnose later
            with open(os.path.join(run_dir, "error.log"), "w") as fh:
                import traceback
                fh.write(traceback.format_exc())

        self._processing.discard(str(path))


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CRM Folder Watcher — auto-clean any CRM export dropped into a folder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--inbox",  default=DEFAULT_INBOX,
        help=f"Folder to watch for new CRM exports (default: ./{DEFAULT_INBOX}/)",
    )
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT,
        help=f"Root folder for cleaned outputs (default: ./{DEFAULT_OUTPUT}/)",
    )
    parser.add_argument(
        "--format", dest="out_format",
        choices=["csv", "xlsx", "workbook"], default="workbook",
        help="Output format for cleaned files (default: workbook)",
    )
    args = parser.parse_args()

    # Create folders if they don't exist
    os.makedirs(args.inbox,  exist_ok=True)
    os.makedirs(args.output, exist_ok=True)

    print(f"\n{'=' * 60}")
    print(f"  CRM Folder Watcher")
    print(f"{'=' * 60}")
    print(f"  Watching : {os.path.abspath(args.inbox)}")
    print(f"  Outputs  : {os.path.abspath(args.output)}")
    print(f"  Format   : {args.out_format}")
    print(f"  Accepted : {', '.join(sorted(SUPPORTED_EXTS))}")
    print(f"\n  Drop a CRM export into the inbox folder.")
    print(f"  Press Ctrl-C to stop.\n")

    handler  = CRMFileHandler(output_root=args.output, out_format=args.out_format)
    observer = Observer()
    observer.schedule(handler, path=args.inbox, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher stopped.")

    observer.join()


if __name__ == "__main__":
    main()
