#!/usr/bin/env python3
"""
Photo Card Cleaner - Interactive tool to clean old photos from memory cards
Organized by day with preview and batch delete capabilities
"""

import os
import sys
import random
import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple

from textual.app import App, ComposeResult
from textual.containers import Container
from textual.widgets import Header, Footer, DataTable, Static, Label
from textual.binding import Binding
from textual.screen import ModalScreen
from textual import work


# Supported image extensions
IMAGE_EXTENSIONS = {'.cr3', '.mov', '.mp4', '.tif', '.tiff', '.jpg', '.jpeg', '.raw'}


class ConfirmDialog(ModalScreen):
    """Modal dialog for delete confirmation"""

    def __init__(self, message: str, date: str, count: int):
        super().__init__()
        self.message = message
        self.date = date
        self.count = count
        self.confirmed = False

    def compose(self) -> ComposeResult:
        with Container(id="dialog"):
            yield Label(self.message, id="question")
            if self.count > 0:
                yield Label(f"Date: {self.date} ({self.count} files)", id="details")
            else:
                yield Label(f"{self.date}", id="details")
            yield Label("Press 'y' to confirm, 'n' to cancel", id="hint")

    def on_key(self, event):
        if event.key == "y":
            self.confirmed = True
            self.dismiss(True)
        elif event.key == "n":
            self.confirmed = False
            self.dismiss(False)


class PhotoCleanerApp(App):
    """Main Textual app for photo cleaning"""

    CSS = """
    Screen {
        background: $surface;
    }

    #info {
        height: 3;
        background: $panel;
        padding: 1;
    }

    DataTable {
        height: 1fr;
    }

    #status {
        height: 3;
        background: $panel;
        padding: 1;
    }

    #dialog {
        width: 60;
        height: 11;
        border: thick $primary;
        background: $surface;
        padding: 1;
    }

    #question {
        width: 100%;
        height: 3;
        content-align: center middle;
        text-style: bold;
    }

    #details {
        width: 100%;
        height: 3;
        content-align: center middle;
    }

    #hint {
        width: 100%;
        height: 3;
        content-align: center middle;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("p", "preview", "Preview"),
        Binding("space", "preview", "Preview", show=False),
        Binding("d", "delete", "Delete"),
        Binding("backspace", "delete", "Delete", show=False),
        Binding("delete", "delete", "Delete", show=False),
        Binding("r", "rescan", "Rescan"),
        Binding("e", "eject", "Eject"),
    ]

    def __init__(self, scan_path: Path):
        super().__init__()
        self.scan_path = scan_path
        self.photo_data: Dict[str, List[Tuple[Path, int]]] = {}
        self.total_files = 0
        self.total_size = 0
        self.known_cards = set()  # Track known camera cards for auto-detection

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(f"Scanning: {self.scan_path}", id="info")
        yield DataTable(id="photo_table")
        yield Static("Loading...", id="status")
        yield Footer()

    def on_mount(self) -> None:
        """Set up the data table and scan for photos"""
        table = self.query_one(DataTable)
        table.add_columns("Date", "Count", "Size", "Sample File")
        table.cursor_type = "row"

        # Initialize known cards
        self.known_cards = set(card.name for card in find_camera_cards())

        self.update_disk_info()
        self._do_scan()

        # Start background worker to monitor for new cards
        self.run_worker(self._monitor_new_cards(), exclusive=False)

    def update_disk_info(self):
        """Update the disk space information display"""
        try:
            disk = shutil.disk_usage(self.scan_path)
            used_gb = disk.used / (1024**3)
            free_gb = disk.free / (1024**3)
            total_gb = disk.total / (1024**3)
            used_pct = (disk.used / disk.total * 100) if disk.total > 0 else 0

            info = self.query_one("#info", Static)
            info.update(
                f"{self.scan_path} | "
                f"Used: {used_gb:.1f} GB ({used_pct:.1f}%) | "
                f"Free: {free_gb:.1f} GB"
            )
        except Exception as e:
            # If we can't get disk info, just show the path
            info = self.query_one("#info", Static)
            info.update(f"{self.scan_path}")

    @work(thread=True, exclusive=True, group="scan")
    def _do_scan(self):
        """Worker that runs scan_photos in a thread so UI stays responsive"""
        self.call_from_thread(self._start_scan_ui)

        # Check read access before walking — os.walk silently yields nothing on EPERM
        try:
            os.listdir(self.scan_path)
        except FileNotFoundError:
            self.call_from_thread(
                self._show_scan_error,
                f"Path not found: {self.scan_path} — card may not be mounted yet"
            )
            return
        except PermissionError:
            terminal = os.environ.get("TERM_PROGRAM", "Terminal")
            self.call_from_thread(
                self._show_scan_error,
                f"Permission denied: cannot read {self.scan_path}\n"
                f"Grant '{terminal}' access to removable volumes in:\n"
                f"System Settings → Privacy & Security → Full Disk Access"
            )
            return

        photo_data: Dict[str, List[Tuple[Path, int]]] = defaultdict(list)
        total_files = 0
        total_size = 0
        files_since_update = 0

        for root, dirs, files in os.walk(self.scan_path):
            for filename in files:
                if filename.startswith('._'):
                    continue

                file_path = Path(root) / filename
                if file_path.suffix.lower() in IMAGE_EXTENSIONS:
                    try:
                        stat_info = file_path.stat()
                        date_str = datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d')
                        size = stat_info.st_size

                        photo_data[date_str].append((file_path, size))
                        total_files += 1
                        total_size += size
                        files_since_update += 1

                        if files_since_update >= 50:
                            files_since_update = 0
                            n_dates = len(photo_data)
                            self.call_from_thread(
                                self._update_scan_progress,
                                total_files, n_dates, total_size,
                            )
                    except (FileNotFoundError, OSError):
                        pass

        self.photo_data = photo_data
        self.total_files = total_files
        self.total_size = total_size
        self.call_from_thread(self._finish_scan)

    def _show_scan_error(self, message):
        status = self.query_one("#status", Static)
        status.update(message)

    def _start_scan_ui(self):
        status = self.query_one("#status", Static)
        status.update("Scanning files...")

    def _update_scan_progress(self, total_files, n_dates, total_size):
        status = self.query_one("#status", Static)
        status.update(
            f"Scanning... {total_files} files in {n_dates} days "
            f"({self.format_size(total_size)}) so far"
        )

    def _finish_scan(self):
        self.populate_table()
        status = self.query_one("#status", Static)
        status.update(f"Found {self.total_files} files in {len(self.photo_data)} days | "
                     f"Total: {self.format_size(self.total_size)}")

    def populate_table(self):
        """Populate the data table with photo data"""
        table = self.query_one(DataTable)
        table.clear()

        # Sort by date (newest first)
        sorted_dates = sorted(self.photo_data.keys(), reverse=True)

        for date in sorted_dates:
            entries = self.photo_data[date]
            if not entries:
                continue

            # Sum pre-stored sizes — no extra stat calls
            size = sum(s for _, s in entries)
            count = len(entries)
            sample = entries[0][0].name
            table.add_row(date, str(count), self.format_size(size), sample)

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format bytes to human-readable size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"

    def action_preview(self):
        """Preview a random photo from selected day"""
        table = self.query_one(DataTable)
        if table.cursor_row is None:
            return

        cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        row = table.get_row(cell_key.row_key)
        date = row[0]

        entries = self.photo_data[date]
        if not entries:
            return

        # Pick a random file
        random_file = random.choice(entries)[0]

        status = self.query_one("#status", Static)

        # Try to preview
        success, message = self.preview_image(random_file)
        if success:
            status.update(f"Previewing: {random_file}")
        else:
            status.update(f"Preview failed: {message}")

    def preview_image(self, path: Path) -> tuple[bool, str]:
        """Preview an image file. Returns (success, message)"""
        # Check if file exists
        if not path.exists():
            return False, f"File not found: {path}"

        # Check if this is a video file
        video_extensions = {'.mov', '.mp4'}
        is_video = path.suffix.lower() in video_extensions

        try:
            if sys.platform == 'darwin':
                if is_video:
                    # Use default app (QuickTime) for videos - Quick Look doesn't handle videos well
                    subprocess.Popen(['open', str(path)],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
                    return True, f"Opening video: {path.name}"
                else:
                    # Use Quick Look for images/RAW files (lightweight preview)
                    # Kill any existing qlmanage processes to avoid showing stale previews
                    try:
                        subprocess.run(['killall', 'qlmanage'],
                                     capture_output=True,
                                     stderr=subprocess.DEVNULL)
                        # Small delay to ensure old process is fully terminated
                        time.sleep(0.05)
                    except:
                        pass

                    # Open new Quick Look window
                    subprocess.Popen(['qlmanage', '-p', str(path)],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)

                    # Bring Quick Look to foreground using AppleScript
                    # Small delay to let qlmanage start
                    time.sleep(0.1)
                    try:
                        applescript = '''
                        tell application "System Events"
                            set qlProcess to first process whose name is "qlmanage"
                            set frontmost of qlProcess to true
                        end tell
                        '''
                        subprocess.run(['osascript', '-e', applescript],
                                     capture_output=True,
                                     timeout=1)
                    except:
                        pass

                    return True, f"Quick Look: {path.name} (Click or press Space to close)"
            elif sys.platform == 'linux':
                subprocess.Popen(['xdg-open', str(path)],
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
                return True, "Opened in system viewer"
            elif sys.platform == 'win32':
                os.startfile(str(path))
                return True, "Opened in system viewer"
            else:
                return False, f"Unsupported platform: {sys.platform}"
        except Exception as e:
            return False, f"Failed to open: {e}"

    def action_delete(self):
        """Delete all files from selected day"""
        table = self.query_one(DataTable)
        if table.cursor_row is None:
            return

        cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        row = table.get_row(cell_key.row_key)
        date = row[0]
        count = int(row[1])

        # Show confirmation dialog and handle result
        self.run_worker(self._confirm_and_delete(date, count))

    async def _confirm_and_delete(self, date: str, count: int):
        """Worker to show confirmation dialog and delete files"""
        result = await self.push_screen_wait(
            ConfirmDialog(
                "Are you sure you want to delete all files from this day?",
                date,
                count
            )
        )

        if result:
            self._do_delete(date)

    @work(thread=True, exclusive=True, group="delete")
    def _do_delete(self, date: str):
        """Delete all files from a specific date in a background thread"""
        entries = self.photo_data[date]
        total = len(entries)

        deleted = 0
        failed = 0

        for file_path, _ in entries:
            try:
                file_path.unlink()
                deleted += 1
            except Exception:
                failed += 1

            if deleted % 20 == 0 or deleted == total:
                self.call_from_thread(
                    self._update_delete_progress, date, deleted, failed, total,
                )

        del self.photo_data[date]
        self.call_from_thread(self._finish_delete, date, deleted, failed)

    def _update_delete_progress(self, date, deleted, failed, total):
        status = self.query_one("#status", Static)
        status.update(f"Deleting {date}... {deleted}/{total}")

    def _finish_delete(self, date, deleted, failed):
        self.total_files -= deleted
        self.populate_table()
        self.update_disk_info()
        status = self.query_one("#status", Static)
        status.update(f"Deleted {deleted} files from {date}" +
                     (f" ({failed} failed)" if failed else ""))

    def action_rescan(self):
        """Rescan the directory"""
        self.update_disk_info()
        self._do_scan()

    def action_eject(self):
        """Eject the current volume"""
        # Only allow ejecting if we're on a volume (not a regular directory)
        if not str(self.scan_path).startswith('/Volumes/'):
            status = self.query_one("#status", Static)
            status.update("Cannot eject: not a mounted volume")
            return

        # Show confirmation and eject
        self.run_worker(self._confirm_and_eject())

    async def _confirm_and_eject(self):
        """Worker to confirm and eject volume"""
        result = await self.push_screen_wait(
            ConfirmDialog(
                "Eject this volume?",
                str(self.scan_path),
                0
            )
        )

        if result:
            self._do_eject()

    def _do_eject(self):
        """Perform the actual eject operation"""
        status = self.query_one("#status", Static)
        status.update(f"Ejecting {self.scan_path}...")

        try:
            if sys.platform == 'darwin':
                # Use diskutil on macOS
                result = subprocess.run(
                    ['diskutil', 'eject', str(self.scan_path)],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    status.update(f"Successfully ejected {self.scan_path}. Press 'q' to quit.")
                    # Clear the table since the volume is gone
                    self.photo_data.clear()
                    table = self.query_one(DataTable)
                    table.clear()
                else:
                    status.update(f"Eject failed: {result.stderr}")
            else:
                status.update("Eject is only supported on macOS")
        except subprocess.TimeoutExpired:
            status.update("Eject timed out")
        except Exception as e:
            status.update(f"Eject error: {e}")

    async def _monitor_new_cards(self):
        """Background worker to monitor for newly inserted camera cards"""
        import asyncio
        while True:
            await asyncio.sleep(2)  # Check every 2 seconds

            current_card_objs = find_camera_cards()
            current_cards = set(card.name for card in current_card_objs)
            new_cards = current_cards - self.known_cards

            if new_cards:
                # New card(s) detected - automatically switch to the first one
                self.known_cards = current_cards

                # Find the Path object for the first new card
                new_card_path = None
                for card in current_card_objs:
                    if card.name in new_cards:
                        new_card_path = card
                        break

                if new_card_path:
                    status = self.query_one("#status", Static)
                    status.update(f"New card detected: {new_card_path.name} - Switching...")

                    # Switch to the new card
                    self.scan_path = new_card_path
                    self.update_disk_info()
                    self._do_scan()


def find_camera_cards() -> List[Path]:
    """Find common camera card mount points"""
    common_names = [
        'EOS_DIGITAL',  # Canon
        'NIKON',        # Nikon
        'NO NAME',      # Generic SD cards
        'SDCARD',       # Generic
        'Untitled',     # macOS default
    ]

    found_cards = []
    volumes_path = Path('/Volumes')

    if volumes_path.exists():
        for entry in volumes_path.iterdir():
            if entry.is_dir() and entry.name in common_names:
                found_cards.append(entry)

    return found_cards


def select_scan_path() -> Path:
    """Determine which path to scan"""
    # Command-line argument takes precedence
    if len(sys.argv) > 1:
        return Path(sys.argv[1])

    # Check for camera cards
    cards = find_camera_cards()

    if not cards:
        # No cards found, use current directory
        print("No camera cards detected. Scanning current directory.")
        return Path.cwd()

    if len(cards) == 1:
        # One card found, use it
        print(f"Found camera card: {cards[0]}")
        return cards[0]

    # Multiple cards found, let user choose
    print("Multiple camera cards detected:")
    for i, card in enumerate(cards, 1):
        print(f"  {i}. {card}")
    print(f"  {len(cards) + 1}. Current directory ({Path.cwd()})")

    while True:
        try:
            choice = input("\nSelect a location (or press Enter for current directory): ").strip()
            if not choice:
                return Path.cwd()

            choice_num = int(choice)
            if 1 <= choice_num <= len(cards):
                return cards[choice_num - 1]
            elif choice_num == len(cards) + 1:
                return Path.cwd()
            else:
                print(f"Invalid choice. Please enter 1-{len(cards) + 1}")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\nCancelled.")
            sys.exit(0)


def main():
    """Main entry point"""
    scan_path = select_scan_path()

    if not scan_path.exists():
        print(f"Error: Path does not exist: {scan_path}")
        sys.exit(1)

    if not scan_path.is_dir():
        print(f"Error: Path is not a directory: {scan_path}")
        sys.exit(1)

    app = PhotoCleanerApp(scan_path)
    app.run()


if __name__ == "__main__":
    main()
