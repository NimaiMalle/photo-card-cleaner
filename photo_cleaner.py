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
import signal
import threading
import plistlib
import hashlib
import re
import time
import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from textual.app import App, ComposeResult
from textual.containers import Container, Vertical, Horizontal
from textual.widgets import Header, Footer, DataTable, Static, Label, ProgressBar
from textual.binding import Binding
from textual.screen import ModalScreen, Screen
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


# ---------------------------------------------------------------------------
# Card health check
# ---------------------------------------------------------------------------

HEALTH_CHECK_NAMES = [
    "Volume accessible",
    "Mount info",
    "Filesystem integrity",
    "SMART probe",
    "Speed baseline",
    "Full surface read",
    "Random file sample",
]

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_PASS = "pass"
STATUS_WARN = "warn"
STATUS_FAIL = "fail"
STATUS_NA = "na"
STATUS_CANCELLED = "cancelled"
STATUS_SKIPPED = "skipped"

STATUS_ICONS = {
    STATUS_QUEUED: "[ ]",
    STATUS_RUNNING: "[~]",
    STATUS_PASS: "[✓]",
    STATUS_WARN: "[!]",
    STATUS_FAIL: "[✗]",
    STATUS_NA: "[–]",
    STATUS_CANCELLED: "[–]",
    STATUS_SKIPPED: "[ ]",
}

SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


@dataclass
class CheckResult:
    status: str = STATUS_QUEUED
    detail: str = ""
    # Live progress values the UI can render. progress_frac in [0, 1] or None.
    progress_frac: Optional[float] = None
    progress_text: str = ""


@dataclass
class HealthContext:
    scan_path: Path
    cancel_event: threading.Event
    device: Optional[str] = None          # e.g. /dev/disk4
    raw_device: Optional[str] = None      # e.g. /dev/rdisk4
    device_size: Optional[int] = None     # bytes
    filesystem: str = ""
    media_name: str = ""
    baseline_mbps: Optional[float] = None
    speed_threshold_mbps: Optional[float] = None
    have_smartctl: bool = False
    missing_tools: List[str] = field(default_factory=list)


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} TB"


def _parse_dd_progress(line: str) -> Optional[Tuple[int, float]]:
    """Parse a dd SIGINFO line. Returns (bytes_transferred, seconds) or None.

    macOS dd prints, on SIGINFO:
      '12345+0 records in'
      '12345+0 records out'
      '12345678 bytes transferred in 0.567890 secs (21742690 bytes/sec)'
    """
    m = re.search(r"(\d+)\s+bytes transferred in\s+([\d.]+)\s+secs", line)
    if not m:
        return None
    return int(m.group(1)), float(m.group(2))


def _run_dd_read(
    raw_device: str,
    count_bytes: Optional[int],
    skip_bytes: int,
    bs: int,
    cancel: threading.Event,
    on_progress,
) -> Tuple[int, float, int]:
    """Run dd reading from raw_device, streaming progress via SIGINFO.

    Returns (bytes_read, seconds, error_count).
    """
    # iflag=skip_bytes lets us skip in bytes; macOS dd uses 'iseek' for that.
    # Use bs in MB and iseek in bytes.
    args = ["dd", f"if={raw_device}", "of=/dev/null", f"bs={bs}"]
    if skip_bytes:
        # macOS dd supports 'iseek=N' (in bs units) or with 'iseek' + bs.
        # We set iseek in bs units = skip_bytes // bs.
        args.append(f"iseek={skip_bytes // bs}")
    if count_bytes is not None:
        args.append(f"count={count_bytes // bs}")

    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    last_bytes = 0
    last_secs = 0.0
    errors = 0

    # Poller thread: send SIGINFO ~1Hz; parse stderr as it arrives.
    stop_poll = threading.Event()

    def poll_siginfo():
        while not stop_poll.is_set():
            if proc.poll() is not None:
                return
            try:
                proc.send_signal(signal.SIGINFO)
            except (ProcessLookupError, OSError):
                return
            stop_poll.wait(1.0)

    poller = threading.Thread(target=poll_siginfo, daemon=True)
    poller.start()

    try:
        assert proc.stderr is not None
        for line in proc.stderr:
            if cancel.is_set():
                proc.terminate()
                break
            if "Input/output error" in line or "I/O error" in line:
                errors += 1
            parsed = _parse_dd_progress(line)
            if parsed:
                last_bytes, last_secs = parsed
                on_progress(last_bytes, last_secs)
    finally:
        stop_poll.set()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    # On normal completion dd prints final summary line to stderr which we
    # already consumed. If the process exited non-zero and we saw no errors,
    # count it as one error.
    if proc.returncode not in (0, -signal.SIGTERM, -signal.SIGKILL) and errors == 0 and not cancel.is_set():
        errors = 1

    return last_bytes, last_secs, errors


def _get_mount_info(path: Path) -> dict:
    """Return parsed diskutil info plist for a mount path."""
    result = subprocess.run(
        ["diskutil", "info", "-plist", str(path)],
        capture_output=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", "replace").strip() or "diskutil info failed")
    return plistlib.loads(result.stdout)


def check_volume_accessible(ctx: HealthContext, update) -> CheckResult:
    update(progress_text="Checking mount point…")
    try:
        entries = os.listdir(ctx.scan_path)
    except FileNotFoundError:
        return CheckResult(STATUS_FAIL, f"Path not found: {ctx.scan_path}")
    except PermissionError as e:
        return CheckResult(STATUS_FAIL, f"Permission denied: {e}")
    return CheckResult(STATUS_PASS, f"Mounted, {len(entries)} top-level entries")


def check_mount_info(ctx: HealthContext, update) -> CheckResult:
    update(progress_text="Querying diskutil…")
    try:
        info = _get_mount_info(ctx.scan_path)
    except Exception as e:
        return CheckResult(STATUS_FAIL, f"diskutil info failed: {e}")

    device_node = info.get("DeviceNode") or ""   # /dev/disk4s1
    # Map partition -> whole disk (strip sN suffix)
    whole = re.sub(r"s\d+$", "", device_node) if device_node else ""
    if whole:
        ctx.device = whole
        ctx.raw_device = whole.replace("/dev/disk", "/dev/rdisk")

    ctx.device_size = info.get("TotalSize") or info.get("Size") or None
    ctx.filesystem = info.get("FilesystemName") or info.get("FilesystemType") or ""
    ctx.media_name = info.get("MediaName") or info.get("IORegistryEntryName") or ""

    if not ctx.raw_device or not ctx.device_size:
        return CheckResult(STATUS_WARN, f"Partial info: device={device_node}")

    size_gb = ctx.device_size / (1024**3)
    detail = f"{ctx.media_name or 'Unknown media'} · {ctx.filesystem or '?'} · {size_gb:.1f} GB · {ctx.device}"
    return CheckResult(STATUS_PASS, detail)


def check_verify_volume(ctx: HealthContext, update) -> CheckResult:
    update(progress_text="Running diskutil verifyVolume…")
    try:
        proc = subprocess.Popen(
            ["diskutil", "verifyVolume", str(ctx.scan_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except FileNotFoundError:
        return CheckResult(STATUS_NA, "diskutil not available")

    last_line = ""
    assert proc.stdout is not None
    for line in proc.stdout:
        if ctx.cancel_event.is_set():
            proc.terminate()
            proc.wait()
            return CheckResult(STATUS_CANCELLED, "Cancelled")
        line = line.strip()
        if line:
            last_line = line
            update(progress_text=line[:80])

    proc.wait()
    if proc.returncode == 0:
        return CheckResult(STATUS_PASS, last_line or "Volume verified")
    return CheckResult(STATUS_FAIL, last_line or f"verifyVolume exited {proc.returncode}")


def check_smart(ctx: HealthContext, update) -> CheckResult:
    if not ctx.have_smartctl:
        return CheckResult(STATUS_NA, "smartctl not installed")
    if not ctx.device:
        return CheckResult(STATUS_NA, "No block device available")

    update(progress_text=f"smartctl -a {ctx.device}…")
    # Try default, then -d sat
    for extra in ([], ["-d", "sat"]):
        try:
            r = subprocess.run(
                ["smartctl", "-a", *extra, ctx.device],
                capture_output=True, text=True, timeout=15,
            )
        except subprocess.TimeoutExpired:
            return CheckResult(STATUS_FAIL, "smartctl timed out")
        out = (r.stdout or "") + (r.stderr or "")
        if "SMART support is: Enabled" in out or "SMART overall-health" in out:
            # Try to extract the overall health line.
            m = re.search(r"SMART overall-health self-assessment test result:\s*(\S+)", out)
            if m:
                val = m.group(1)
                return CheckResult(
                    STATUS_PASS if val.upper() == "PASSED" else STATUS_FAIL,
                    f"Overall health: {val}",
                )
            return CheckResult(STATUS_PASS, "SMART supported (no overall-health line parsed)")
        if "Unknown USB bridge" in out or "Unsupported" in out:
            continue

    return CheckResult(STATUS_NA, "Device does not expose SMART (typical for SD readers)")


def check_speed_baseline(ctx: HealthContext, update) -> CheckResult:
    if not ctx.raw_device or not ctx.device_size:
        return CheckResult(STATUS_NA, "Raw device not available")

    sample = 128 * 1024 * 1024  # 128 MB
    bs = 4 * 1024 * 1024
    total = ctx.device_size
    # Three offsets: start, middle, end-128MB
    offsets = [0, max(0, total // 2 - sample // 2), max(0, total - sample)]
    labels = ["start", "middle", "end"]

    rates: List[float] = []
    for i, (off, label) in enumerate(zip(offsets, labels)):
        if ctx.cancel_event.is_set():
            return CheckResult(STATUS_CANCELLED, "Cancelled")

        # Align offset down to bs boundary
        off_aligned = (off // bs) * bs

        def on_progress(b, s, idx=i, lbl=label):
            mbps = (b / s / 1_000_000) if s > 0 else 0
            update(
                progress_frac=(idx + (b / sample if sample else 1)) / len(offsets),
                progress_text=f"Sampling {lbl} · {_human_bytes(b)} · {mbps:.1f} MB/s",
            )

        b, s, errs = _run_dd_read(
            ctx.raw_device, sample, off_aligned, bs, ctx.cancel_event, on_progress
        )
        if ctx.cancel_event.is_set():
            return CheckResult(STATUS_CANCELLED, "Cancelled")
        if errs or b == 0 or s <= 0:
            return CheckResult(STATUS_FAIL, f"Sample read error at {label} (offset {off_aligned})")
        rates.append(b / s / 1_000_000)

    avg = sum(rates) / len(rates)
    ctx.baseline_mbps = avg
    ctx.speed_threshold_mbps = avg * 0.70
    detail = (
        f"start {rates[0]:.1f} · mid {rates[1]:.1f} · end {rates[2]:.1f} MB/s · "
        f"avg {avg:.1f} MB/s · threshold {ctx.speed_threshold_mbps:.1f} MB/s"
    )
    return CheckResult(STATUS_PASS, detail)


def check_surface_read(ctx: HealthContext, update) -> CheckResult:
    if not ctx.raw_device or not ctx.device_size:
        return CheckResult(STATUS_NA, "Raw device not available")

    total = ctx.device_size
    bs = 4 * 1024 * 1024
    start_time = time.monotonic()

    def on_progress(b, s):
        frac = b / total if total else 0
        mbps = (b / s / 1_000_000) if s > 0 else 0
        remaining = ((total - b) / (b / s)) if (b > 0 and s > 0) else 0
        eta = f"{int(remaining // 60):02d}:{int(remaining % 60):02d}"
        update(
            progress_frac=min(frac, 1.0),
            progress_text=f"{_human_bytes(b)} / {_human_bytes(total)} · {mbps:.1f} MB/s · ETA {eta}",
        )

    b, s, errs = _run_dd_read(ctx.raw_device, None, 0, bs, ctx.cancel_event, on_progress)
    elapsed = time.monotonic() - start_time

    if ctx.cancel_event.is_set():
        return CheckResult(STATUS_CANCELLED, f"Cancelled after {_human_bytes(b)}")

    if errs:
        return CheckResult(STATUS_FAIL, f"{errs} I/O error(s) during surface read")

    if b < total * 0.99:
        return CheckResult(
            STATUS_FAIL,
            f"Only {_human_bytes(b)} of {_human_bytes(total)} read",
        )

    avg_mbps = (b / s / 1_000_000) if s > 0 else 0
    threshold = ctx.speed_threshold_mbps or 0
    if threshold and avg_mbps < threshold:
        return CheckResult(
            STATUS_WARN,
            f"{_human_bytes(b)} read · {avg_mbps:.1f} MB/s avg (below {threshold:.1f} MB/s threshold)",
        )
    return CheckResult(
        STATUS_PASS,
        f"{_human_bytes(b)} read in {elapsed:.0f}s · {avg_mbps:.1f} MB/s avg · 0 errors",
    )


def check_random_file_sample(ctx: HealthContext, update, photo_files: List[Path]) -> CheckResult:
    if not photo_files:
        return CheckResult(STATUS_NA, "No photos to sample")

    budget_s = 10.0
    pool = list(photo_files)
    random.shuffle(pool)
    spinner_i = 0
    last_spin = 0.0

    start = time.monotonic()
    files_read = 0
    bytes_read = 0
    errors = 0

    pool_size = len(pool)
    exhausted = False
    for path in pool:
        now = time.monotonic()
        if now - start >= budget_s:
            break
        if ctx.cancel_event.is_set():
            return CheckResult(STATUS_CANCELLED, f"Cancelled after {files_read} files")

        try:
            h = hashlib.sha1()
            file_bytes = 0
            with open(path, "rb") as f:
                while True:
                    if ctx.cancel_event.is_set():
                        return CheckResult(STATUS_CANCELLED, f"Cancelled after {files_read} files")
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
                    file_bytes += len(chunk)

                    now2 = time.monotonic()
                    if now2 - last_spin > 0.1:
                        last_spin = now2
                        spinner_i = (spinner_i + 1) % len(SPINNER_FRAMES)
                        frame = SPINNER_FRAMES[spinner_i]
                        elapsed = now2 - start
                        name = path.name if len(path.name) <= 28 else path.name[:25] + "…"
                        update(
                            progress_frac=min(elapsed / budget_s, 1.0),
                            progress_text=(
                                f"{frame} {name} · read {files_read} files · "
                                f"{_human_bytes(bytes_read + file_bytes)} · {errors} errors"
                            ),
                        )
            files_read += 1
            bytes_read += file_bytes
        except (OSError, IOError):
            errors += 1
    else:
        exhausted = True

    elapsed = max(time.monotonic() - start, 0.001)
    mbps = bytes_read / elapsed / 1_000_000
    suffix = f" (pool exhausted: {pool_size})" if exhausted else ""
    detail = (
        f"{files_read} files · {_human_bytes(bytes_read)} · "
        f"{mbps:.1f} MB/s avg · {errors} errors{suffix}"
    )
    if errors:
        return CheckResult(STATUS_FAIL, detail)
    if files_read == 0:
        return CheckResult(STATUS_WARN, "No files read within budget")
    return CheckResult(STATUS_PASS, detail)


# ---------------------------------------------------------------------------
# Gate modals
# ---------------------------------------------------------------------------


def _copy_to_clipboard(text: str) -> bool:
    """Copy text to the macOS clipboard via pbcopy. Returns True on success."""
    if sys.platform != "darwin":
        return False
    try:
        proc = subprocess.run(
            ["pbcopy"], input=text.encode("utf-8"), timeout=2, check=False
        )
        return proc.returncode == 0
    except Exception:
        return False


class SudoRequiredScreen(ModalScreen):
    """Shown when health check launched without root privileges."""

    def __init__(self, relaunch_cmd: str):
        super().__init__()
        self.relaunch_cmd = relaunch_cmd
        self.copied = False

    def compose(self) -> ComposeResult:
        with Container(id="sudo_dialog"):
            yield Label("Card Health Check requires root", id="sudo_title")
            yield Label(
                "Raw-device reads need elevated privileges.\n"
                "Please quit and relaunch with sudo:",
                id="sudo_body",
            )
            yield Label(f"    {self.relaunch_cmd}", id="sudo_cmd")
            yield Label("", id="sudo_copy_hint")
            yield Label("Press any key to close", id="sudo_hint")

    def on_mount(self) -> None:
        self.copied = _copy_to_clipboard(self.relaunch_cmd)
        hint = self.query_one("#sudo_copy_hint", Label)
        if self.copied:
            hint.update("✓ Copied to clipboard — paste with ⌘V")
        else:
            hint.update("Select manually: hold ⌥ while dragging, then ⌘C")

    def on_key(self, event):
        self.dismiss(None)


class MissingToolsScreen(ModalScreen):
    """Shown when optional CLI tools are missing."""

    def __init__(self, missing: List[Tuple[str, str]]):
        super().__init__()
        # missing: list of (tool, install_cmd)
        self.missing = missing

    def compose(self) -> ComposeResult:
        with Container(id="tools_dialog"):
            yield Label("Optional tools not installed", id="tools_title")
            yield Label(
                "These checks will be skipped (marked N/A).",
                id="tools_body",
            )
            for tool, cmd in self.missing:
                yield Label(f"• {tool}", classes="tools_item")
                yield Label(f"    {cmd}", classes="tools_cmd")
            yield Label("", id="tools_copy_hint")
            yield Label(r"\[c] continue without  ·  \[q] quit to install", id="tools_hint")

    def on_mount(self) -> None:
        combined = "\n".join(cmd for _, cmd in self.missing)
        copied = _copy_to_clipboard(combined)
        hint = self.query_one("#tools_copy_hint", Label)
        if copied and len(self.missing) == 1:
            hint.update("✓ Copied to clipboard — paste with ⌘V")
        elif copied:
            hint.update(f"✓ {len(self.missing)} commands copied — paste with ⌘V")
        else:
            hint.update("Select manually: hold ⌥ while dragging, then ⌘C")

    def on_key(self, event):
        if event.key == "c":
            self.dismiss(True)
        elif event.key == "q":
            self.dismiss(False)


def _resolve_device_for_mount(mount_path: Path) -> Optional[str]:
    """Resolve /Volumes/X to its underlying /dev/diskN (whole disk)."""
    try:
        result = subprocess.run(
            ["diskutil", "info", "-plist", str(mount_path)],
            capture_output=True, timeout=5,
        )
        if result.returncode != 0:
            return None
        info = plistlib.loads(result.stdout)
        node = info.get("DeviceNode") or ""  # e.g. /dev/disk5s1
        if not node:
            return None
        # Whole disk: strip partition suffix
        return re.sub(r"s\d+$", "", node)
    except Exception:
        return None


def _parse_lsof_F(stdout: str) -> Dict[int, Tuple[str, str]]:
    """Parse lsof -F pcn output into {pid: (cmd, sample_path)}."""
    seen: Dict[int, Tuple[str, str]] = {}
    pid: Optional[int] = None
    cmd = ""
    for line in stdout.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:]
        if tag == "p":
            try:
                pid = int(value)
            except ValueError:
                pid = None
            cmd = ""
        elif tag == "c":
            cmd = value
            if pid is not None and pid not in seen:
                seen[pid] = (cmd, "")
        elif tag == "n" and pid is not None:
            existing_cmd, existing_path = seen.get(pid, (cmd, ""))
            if not existing_path:
                seen[pid] = (existing_cmd or cmd, value)
    return seen


def _find_blocking_processes(mount_path: Path) -> List[str]:
    """Return human-readable lines describing processes holding the volume open.

    Combines three queries:
      1. lsof +D <mount>           — open files under the mount path
      2. lsof <mount>              — the mount root itself (Lightroom watcher)
      3. lsof <whole device node>  — raw block device handles
    Empty list means nothing detected (or lsof unavailable / no permission).
    """
    queries: List[List[str]] = [
        ["lsof", "-F", "pcn", "+D", str(mount_path)],
        ["lsof", "-F", "pcn", str(mount_path)],
    ]
    device = _resolve_device_for_mount(mount_path)
    if device:
        # Both whole-disk and any partition under it
        queries.append(["lsof", "-F", "pcn", device])
        queries.append(["lsof", "-F", "pcn", f"{device}s1"])

    merged: Dict[int, Tuple[str, str]] = {}
    for cmd_args in queries:
        try:
            r = subprocess.run(cmd_args, capture_output=True, text=True, timeout=8)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        # lsof exits non-zero when nothing matches — that's fine, parse stdout anyway.
        for pid, (cmd, path) in _parse_lsof_F(r.stdout).items():
            existing = merged.get(pid)
            if existing is None or (not existing[1] and path):
                merged[pid] = (cmd, path)

    # Filter out our own pid — we may have an fd into the mount via the scan.
    merged.pop(os.getpid(), None)

    if not merged:
        return []

    lines = []
    for pid_val, (cmd_val, path_val) in sorted(merged.items()):
        if path_val:
            lines.append(f"  • {cmd_val} (pid {pid_val}) — {path_val}")
        else:
            lines.append(f"  • {cmd_val} (pid {pid_val})")
    return lines


class EjectErrorScreen(ModalScreen):
    """Shown when Finder eject fails — typically because a process holds
    the volume open."""

    def __init__(self, mount_path: Path, raw_error: str, blockers: List[str]):
        super().__init__()
        self.mount_path = mount_path
        self.raw_error = raw_error
        self.blockers = blockers

    def compose(self) -> ComposeResult:
        with Container(id="eject_dialog"):
            yield Label(f"Could not eject {self.mount_path}", id="eject_title")
            error_text = self.raw_error.strip() or "Finder reported a failure but printed no message."
            yield Static(error_text, id="eject_body")
            if self.blockers:
                yield Label(
                    "Open by:\n" + "\n".join(self.blockers),
                    id="eject_blockers",
                )
                yield Label(
                    "Quit those apps (or close the offending file) and press 'e' again.",
                    id="eject_advice",
                )
            else:
                yield Static(
                    "No specific process detected via lsof. Try ejecting from "
                    "Finder directly — it sometimes prompts for a force-eject "
                    "where this scripted call cannot.",
                    id="eject_advice",
                )
            yield Label("Press any key to close", id="eject_hint")

    def on_key(self, event):
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Tidy macOS metadata
# ---------------------------------------------------------------------------

METADATA_NEVER_INDEX = ".metadata_never_index"

TIDY_TOP_LEVEL_DIRS = [
    (".Spotlight-V100", "Spotlight index"),
    (".fseventsd", "filesystem events"),
    (".Trashes", "macOS trash"),
    ("System Volume Information", "Windows index"),
]

# Files scanned recursively (names, not paths)
TIDY_NOISE_FILE_PREDICATES = [
    (lambda name: name == ".DS_Store", ".DS_Store"),
    (lambda name: name.startswith("._"), "._* AppleDouble"),
]


@dataclass
class TidyReport:
    top_dirs: List[Tuple[Path, str, int]] = field(default_factory=list)
    # (path, description, size)
    noise_files: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    # label -> (count, total_bytes)
    noise_paths: List[Path] = field(default_factory=list)
    already_marked: bool = False
    total_bytes: int = 0


def _dir_size(path: Path) -> int:
    total = 0
    try:
        for root, _dirs, files in os.walk(path, onerror=lambda _e: None):
            for f in files:
                try:
                    total += (Path(root) / f).lstat().st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def scan_metadata(volume: Path, cancel: Optional[threading.Event] = None) -> TidyReport:
    """Walk the volume gathering what tidy would remove."""
    report = TidyReport()

    # Top-level metadata dirs
    for name, desc in TIDY_TOP_LEVEL_DIRS:
        if cancel is not None and cancel.is_set():
            return report
        p = volume / name
        if p.exists() and p.is_dir():
            size = _dir_size(p)
            report.top_dirs.append((p, desc, size))
            report.total_bytes += size

    # Noise files, recursive
    counts: Dict[str, List[int]] = {label: [0, 0] for _pred, label in TIDY_NOISE_FILE_PREDICATES}
    skip_dir_names = {name for name, _ in TIDY_TOP_LEVEL_DIRS}
    for root, dirs, files in os.walk(volume, onerror=lambda _e: None):
        if cancel is not None and cancel.is_set():
            return report
        # Don't recurse into the top-level dirs we're already accounting for whole
        dirs[:] = [d for d in dirs if d not in skip_dir_names]
        for name in files:
            for pred, label in TIDY_NOISE_FILE_PREDICATES:
                if pred(name):
                    fp = Path(root) / name
                    try:
                        size = fp.lstat().st_size
                    except OSError:
                        continue
                    counts[label][0] += 1
                    counts[label][1] += size
                    report.noise_paths.append(fp)
                    report.total_bytes += size
                    break

    report.noise_files = {label: (c[0], c[1]) for label, c in counts.items() if c[0] > 0}
    report.already_marked = (volume / METADATA_NEVER_INDEX).exists()
    return report


def _remove_tree(path: Path) -> Tuple[int, int]:
    """Recursively unlink a directory; returns (files_removed, errors)."""
    removed, errors = 0, 0
    for root, dirs, files in os.walk(path, topdown=False, onerror=lambda _e: None):
        for name in files:
            try:
                (Path(root) / name).unlink()
                removed += 1
            except OSError:
                errors += 1
        for name in dirs:
            try:
                (Path(root) / name).rmdir()
            except OSError:
                errors += 1
    try:
        path.rmdir()
    except OSError:
        errors += 1
    return removed, errors


def run_tidy(
    volume: Path,
    report: TidyReport,
    cancel: threading.Event,
    on_progress,
) -> Tuple[int, int, int]:
    """Execute the tidy plan. Returns (items_removed, bytes_freed, errors)."""
    items_removed, bytes_freed, errors = 0, 0, 0

    for p, desc, size in report.top_dirs:
        if cancel.is_set():
            return items_removed, bytes_freed, errors
        on_progress(f"Removing {p.name}/ ({desc})…")
        r, e = _remove_tree(p)
        items_removed += r
        errors += e
        if e == 0:
            bytes_freed += size

    total_files = sum(c for c, _ in report.noise_files.values())
    done = 0
    for fp in report.noise_paths:
        if cancel.is_set():
            return items_removed, bytes_freed, errors
        try:
            size = fp.lstat().st_size
            fp.unlink()
            items_removed += 1
            bytes_freed += size
        except OSError:
            errors += 1
        done += 1
        if done % 100 == 0:
            on_progress(f"Removing noise files… {done}/{total_files}")

    # Write the sentinel (idempotent)
    marker = volume / METADATA_NEVER_INDEX
    if not marker.exists():
        try:
            marker.touch()
        except OSError:
            errors += 1

    return items_removed, bytes_freed, errors


class TidyConfirmDialog(ModalScreen):
    """Confirmation dialog showing what 'tidy' will remove and create."""

    def __init__(self, volume: Path, report: TidyReport):
        super().__init__()
        self.volume = volume
        self.report = report

    def compose(self) -> ComposeResult:
        r = self.report
        lines: List[str] = []
        if r.top_dirs:
            for path, desc, size in r.top_dirs:
                lines.append(f"  {path.name}/   {_human_bytes(size):>10}   ({desc})")
        for label, (count, size) in r.noise_files.items():
            lines.append(f"  {count} × {label}   {_human_bytes(size):>10}")
        if not lines:
            lines.append("  (nothing to remove)")
        create_line = (
            f"Already marked: {METADATA_NEVER_INDEX} exists."
            if r.already_marked
            else f"Will create: {METADATA_NEVER_INDEX}   (prevents future Spotlight indexing)"
        )
        total_line = f"Total to free: {_human_bytes(r.total_bytes)}"

        with Container(id="tidy_dialog"):
            yield Label(f"Tidy macOS metadata on {self.volume}?", id="tidy_title")
            yield Static("Will remove:", id="tidy_will_remove")
            yield Static("\n".join(lines), id="tidy_list")
            yield Static(create_line, id="tidy_create")
            yield Static(total_line, id="tidy_total")
            yield Label(r"\[y] proceed  ·  \[n] cancel", id="tidy_hint")

    def on_key(self, event):
        if event.key == "y":
            self.dismiss(True)
        elif event.key in ("n", "escape"):
            self.dismiss(False)


# ---------------------------------------------------------------------------
# HealthCheckScreen
# ---------------------------------------------------------------------------


class HealthCheckScreen(Screen):
    """Full-screen card health check with live progress and report card."""

    BINDINGS = [
        Binding("c", "cancel", "Cancel", show=True),
        Binding("y", "copy_report", "Copy Report", show=True),
        Binding("escape", "close", "Close", show=True),
        Binding("q", "close", "Close", show=False),
    ]

    CSS = """
    HealthCheckScreen {
        background: $surface;
    }
    #hc_title {
        height: 3;
        background: $panel;
        padding: 1;
        text-style: bold;
    }
    #hc_rows {
        height: 1fr;
        padding: 1 2;
    }
    .hc_row {
        height: 2;
    }
    .hc_row_detail {
        color: $text-muted;
        padding-left: 4;
    }
    #hc_overall {
        height: 4;
        background: $panel;
        padding: 1;
    }
    #hc_verdict {
        text-style: bold;
    }
    """

    def __init__(self, scan_path: Path, photo_files: List[Path], ctx: HealthContext):
        super().__init__()
        self.scan_path = scan_path
        self.photo_files = photo_files
        self.ctx = ctx
        self.results: List[CheckResult] = [CheckResult() for _ in HEALTH_CHECK_NAMES]
        self.done = False
        self.verdict = ""
        self.recommendation = ""
        self._current_index = 0

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            f"Card Health Check — {self.scan_path}",
            id="hc_title",
        )
        with Vertical(id="hc_rows"):
            for i, name in enumerate(HEALTH_CHECK_NAMES):
                yield Static(self._render_row_line(i), id=f"hc_row_{i}", classes="hc_row")
                yield Static("", id=f"hc_row_detail_{i}", classes="hc_row_detail")
        with Vertical(id="hc_overall"):
            yield ProgressBar(total=len(HEALTH_CHECK_NAMES), id="hc_progress", show_eta=False)
            yield Static("", id="hc_verdict")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#hc_progress", ProgressBar).update(progress=0)
        self._run_pipeline()

    def _render_row_line(self, i: int) -> str:
        result = self.results[i]
        icon = STATUS_ICONS.get(result.status, "[ ]")
        name = HEALTH_CHECK_NAMES[i]
        return f"{icon} {name}"

    def _update_row(self, i: int) -> None:
        try:
            row = self.query_one(f"#hc_row_{i}", Static)
            detail = self.query_one(f"#hc_row_detail_{i}", Static)
        except Exception:
            return
        row.update(self._render_row_line(i))
        r = self.results[i]
        text = r.progress_text or r.detail
        detail.update(text)

    def _set_progress_fraction(self, i: int, frac: Optional[float]) -> None:
        bar = self.query_one("#hc_progress", ProgressBar)
        completed = float(i) + (frac if frac is not None else 0.0)
        bar.update(progress=min(completed, len(HEALTH_CHECK_NAMES)))

    @work(thread=True, exclusive=True, group="health")
    def _run_pipeline(self) -> None:
        ctx = self.ctx
        photo_files = self.photo_files

        def make_updater(index: int):
            def update(**kwargs):
                # Merge live progress into the current result
                r = self.results[index]
                if "progress_frac" in kwargs:
                    r.progress_frac = kwargs["progress_frac"]
                if "progress_text" in kwargs:
                    r.progress_text = kwargs["progress_text"]
                self.app.call_from_thread(self._update_row, index)
                self.app.call_from_thread(
                    self._set_progress_fraction, index, r.progress_frac
                )
            return update

        # Ordered pipeline
        pipeline = [
            lambda u: check_volume_accessible(ctx, u),
            lambda u: check_mount_info(ctx, u),
            lambda u: check_verify_volume(ctx, u),
            lambda u: check_smart(ctx, u),
            lambda u: check_speed_baseline(ctx, u),
            lambda u: check_surface_read(ctx, u),
            lambda u: check_random_file_sample(ctx, u, photo_files),
        ]

        for i, fn in enumerate(pipeline):
            self._current_index = i
            if ctx.cancel_event.is_set():
                self.results[i] = CheckResult(STATUS_CANCELLED, "Cancelled")
                self.app.call_from_thread(self._update_row, i)
                break

            self.results[i].status = STATUS_RUNNING
            self.app.call_from_thread(self._update_row, i)

            try:
                result = fn(make_updater(i))
            except Exception as e:  # keep pipeline alive on unexpected errors
                result = CheckResult(STATUS_FAIL, f"Error: {e}")

            # Preserve progress_text if the check populated one and detail is empty
            if not result.detail and result.progress_text:
                result.detail = result.progress_text
            # Clear transient progress text now that the row is finalized
            result.progress_text = ""
            result.progress_frac = None
            self.results[i] = result
            self.app.call_from_thread(self._update_row, i)
            self.app.call_from_thread(self._set_progress_fraction, i + 1, 0.0)

            # Gate: volume access failure = abort
            if i == 0 and result.status == STATUS_FAIL:
                # mark remaining as skipped
                for j in range(i + 1, len(pipeline)):
                    self.results[j] = CheckResult(STATUS_SKIPPED, "Skipped")
                    self.app.call_from_thread(self._update_row, j)
                break

        # Mark remaining as skipped if we exited early via cancel
        if ctx.cancel_event.is_set():
            for j in range(self._current_index + 1, len(pipeline)):
                if self.results[j].status == STATUS_QUEUED:
                    self.results[j] = CheckResult(STATUS_SKIPPED, "Skipped")
                    self.app.call_from_thread(self._update_row, j)

        self.app.call_from_thread(self._finish)

    def _finish(self) -> None:
        self.done = True
        statuses = [r.status for r in self.results]
        if STATUS_CANCELLED in statuses:
            banner, rec = "CANCELLED", "Run again to complete the check."
        elif STATUS_FAIL in statuses:
            banner, rec = "FAILED", "Treat this card as untrusted — copy off and replace."
        elif STATUS_WARN in statuses:
            banner, rec = "WARNINGS", "Usable but watch for issues; consider replacing soon."
        else:
            banner, rec = "HEALTHY", "No problems detected."
        self.verdict = banner
        self.recommendation = rec
        verdict = self.query_one("#hc_verdict", Static)
        verdict.update(f"{banner}  —  {rec}   (y copy · Esc close)")
        bar = self.query_one("#hc_progress", ProgressBar)
        bar.update(progress=len(HEALTH_CHECK_NAMES))

    def action_cancel(self) -> None:
        if self.done:
            return
        self.ctx.cancel_event.set()

    def action_close(self) -> None:
        # If still running, signal cancel and let the pipeline wind down before closing.
        if not self.done:
            self.ctx.cancel_event.set()
            return
        self.app.pop_screen()

    _STATUS_LABELS = {
        STATUS_QUEUED: "QUEUED",
        STATUS_RUNNING: "RUNNING",
        STATUS_PASS: "PASS",
        STATUS_WARN: "WARN",
        STATUS_FAIL: "FAIL",
        STATUS_NA: "N/A",
        STATUS_CANCELLED: "CANCELLED",
        STATUS_SKIPPED: "SKIPPED",
    }

    def _build_report_text(self) -> str:
        lines = []
        lines.append("Card Health Check Report")
        lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Path:      {self.scan_path}")
        if self.ctx.device:
            lines.append(f"Device:    {self.ctx.device}")
        if self.ctx.media_name:
            lines.append(f"Media:     {self.ctx.media_name}")
        if self.ctx.device_size:
            lines.append(f"Size:      {_human_bytes(self.ctx.device_size)}")
        if self.ctx.filesystem:
            lines.append(f"FS:        {self.ctx.filesystem}")
        lines.append("")
        lines.append(f"Verdict: {self.verdict or 'IN PROGRESS'} — {self.recommendation or ''}".rstrip(" —"))
        lines.append("")
        lines.append("Checks:")
        for name, result in zip(HEALTH_CHECK_NAMES, self.results):
            label = self._STATUS_LABELS.get(result.status, result.status.upper())
            detail = result.detail or ""
            lines.append(f"  [{label:<9}] {name}")
            if detail:
                lines.append(f"             {detail}")
        return "\n".join(lines) + "\n"

    def action_copy_report(self) -> None:
        if not self.done:
            return
        text = self._build_report_text()
        ok = _copy_to_clipboard(text)
        verdict = self.query_one("#hc_verdict", Static)
        banner = self.verdict or ""
        rec = self.recommendation or ""
        if ok:
            verdict.update(f"{banner}  —  {rec}   (✓ report copied to clipboard)")
        else:
            verdict.update(f"{banner}  —  {rec}   (copy failed — pbcopy unavailable)")


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

    SudoRequiredScreen, MissingToolsScreen, EjectErrorScreen, TidyConfirmDialog {
        align: center middle;
    }

    #sudo_dialog, #tools_dialog, #eject_dialog, #tidy_dialog {
        width: 78;
        height: auto;
        max-height: 90%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #tidy_title {
        text-style: bold;
        padding-bottom: 1;
    }

    #tidy_will_remove {
        color: $text-muted;
    }

    #tidy_list {
        padding: 0 0 1 0;
    }

    #tidy_create {
        color: $text-muted;
        padding-bottom: 1;
    }

    #tidy_total {
        text-style: bold;
        padding-bottom: 1;
    }

    #tidy_hint {
        color: $text-muted;
    }

    #eject_title {
        text-style: bold;
        padding-bottom: 1;
    }

    #eject_body {
        color: $warning;
        padding-bottom: 1;
        width: 100%;
        height: auto;
    }

    #eject_blockers {
        padding: 1 0;
    }

    #eject_advice {
        color: $text-muted;
        padding-top: 1;
        width: 100%;
        height: auto;
    }

    #eject_hint {
        color: $text-muted;
        padding-top: 1;
    }

    #sudo_title, #tools_title {
        text-style: bold;
        padding-bottom: 1;
    }

    #sudo_cmd, .tools_cmd {
        color: $warning;
        padding: 0 0 0 0;
    }

    .tools_item {
        padding-top: 1;
    }

    #tools_copy_hint, #sudo_copy_hint {
        color: $text-muted;
        padding-top: 1;
    }

    #sudo_hint, #tools_hint {
        color: $text-muted;
        padding-top: 1;
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
        Binding("t", "tidy_metadata", "Tidy"),
        Binding("e", "eject", "Eject"),
        Binding("h", "health_check", "Health Check"),
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
        if not self._is_main_screen_active():
            return
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
        if not self._is_main_screen_active():
            return
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

    def _is_main_screen_active(self) -> bool:
        """True only when no modal/health screen is on top of the default one."""
        try:
            return len(self.screen_stack) <= 1
        except Exception:
            return True

    def action_rescan(self):
        """Rescan the directory"""
        if not self._is_main_screen_active():
            return
        self.update_disk_info()
        self._do_scan()

    def action_eject(self):
        """Eject the current volume"""
        if not self._is_main_screen_active():
            return
        # Only allow ejecting if we're on a volume (not a regular directory)
        if not str(self.scan_path).startswith('/Volumes/'):
            status = self.query_one("#status", Static)
            status.update("Cannot eject: not a mounted volume")
            return

        # Show confirmation and eject
        self.run_worker(self._confirm_and_eject())

    def action_tidy_metadata(self):
        """Tidy macOS metadata (Spotlight, fseventsd, .DS_Store, etc.) on the card."""
        if not self._is_main_screen_active():
            return
        if not str(self.scan_path).startswith("/Volumes/"):
            self.query_one("#status", Static).update("Tidy only works on mounted volumes.")
            return
        self.run_worker(self._tidy_flow(), exclusive=True, group="tidy")

    async def _tidy_flow(self, auto_eject_after: bool = False) -> bool:
        """Scan, confirm, and execute tidy. Returns True if tidy ran."""
        status = self.query_one("#status", Static)
        volume = self.scan_path

        status.update(f"Scanning metadata on {volume}…")
        report = await asyncio.to_thread(scan_metadata, volume)

        if report.total_bytes == 0 and not report.top_dirs and not report.noise_files:
            status.update(
                f"Nothing to tidy on {volume}. "
                + ("Already marked." if report.already_marked else "")
            )
            # Still write the marker if missing
            if not report.already_marked:
                try:
                    (volume / METADATA_NEVER_INDEX).touch()
                    status.update(f"Nothing to tidy. Wrote {METADATA_NEVER_INDEX}.")
                except OSError as e:
                    status.update(f"Nothing to tidy. Could not write marker: {e}")
            return False

        confirmed = await self.push_screen_wait(TidyConfirmDialog(volume, report))
        if not confirmed:
            status.update("Tidy cancelled.")
            return False

        status.update(f"Tidying {volume}…")
        cancel = threading.Event()

        def on_progress(msg: str) -> None:
            self.call_from_thread(status.update, f"Tidying… {msg}")

        items, bytes_freed, errors = await asyncio.to_thread(
            run_tidy, volume, report, cancel, on_progress
        )
        suffix = f" ({errors} errors)" if errors else ""
        status.update(
            f"Tidied {volume}: removed {items} items, freed {_human_bytes(bytes_freed)}{suffix}"
        )
        self.update_disk_info()
        return True

    def action_health_check(self):
        """Launch Card Health Check."""
        if not self._is_main_screen_active():
            return
        self.run_worker(self._start_health_check())

    async def _start_health_check(self):
        if sys.platform != "darwin":
            status = self.query_one("#status", Static)
            status.update("Health check currently supports macOS only.")
            return

        # Privilege gate
        if os.geteuid() != 0:
            if sys.argv and sys.argv[0].endswith("photo_cleaner.py"):
                relaunch = f"sudo {sys.executable} {sys.argv[0]} {self.scan_path}"
            else:
                relaunch = f"sudo ./run.sh {self.scan_path}"
            await self.push_screen_wait(SudoRequiredScreen(relaunch))
            return

        # Tool availability
        have_smartctl = shutil.which("smartctl") is not None
        missing: List[Tuple[str, str]] = []
        if not have_smartctl:
            missing.append(("smartctl (SMART probe)", "brew install smartmontools"))

        if missing:
            proceed = await self.push_screen_wait(MissingToolsScreen(missing))
            if not proceed:
                return

        # Build the photo file list for random-sample check.
        all_files: List[Path] = []
        for entries in self.photo_data.values():
            all_files.extend(p for p, _ in entries)

        ctx = HealthContext(
            scan_path=self.scan_path,
            cancel_event=threading.Event(),
            have_smartctl=have_smartctl,
        )
        await self.push_screen_wait(HealthCheckScreen(self.scan_path, all_files, ctx))

    async def _confirm_and_eject(self):
        """Worker to confirm and eject volume"""
        # Pre-eject hygiene: if the volume has Spotlight index / noise but no
        # .metadata_never_index marker yet, offer to tidy first — this avoids
        # the common "eject hangs while Spotlight negotiates" failure.
        volume = self.scan_path
        needs_tidy_prompt = (
            str(volume).startswith("/Volumes/")
            and not (volume / METADATA_NEVER_INDEX).exists()
            and (
                (volume / ".Spotlight-V100").exists()
                or (volume / ".fseventsd").exists()
                or (volume / ".Trashes").exists()
            )
        )
        if needs_tidy_prompt:
            choice = await self.push_screen_wait(
                ConfirmDialog(
                    "Tidy macOS metadata before ejecting?",
                    "Spotlight indexes can cause eject to hang.",
                    0,
                )
            )
            if choice:
                did_tidy = await self._tidy_flow()
                if not did_tidy:
                    return  # user cancelled tidy

        result = await self.push_screen_wait(
            ConfirmDialog(
                "Eject this volume?",
                str(self.scan_path),
                0
            )
        )

        if result:
            self.run_worker(self._do_eject(), exclusive=True, group="eject")

    async def _do_eject(self):
        """Ask Finder to eject the volume; poll for actual unmount."""
        status = self.query_one("#status", Static)

        if sys.platform != "darwin":
            status.update("Eject is only supported on macOS")
            return

        mount_path = self.scan_path
        volume_name = mount_path.name

        # Snapshot blockers up front — if Finder also fails, the offending
        # process may release/re-acquire by the time we'd query post-failure.
        status.update(f"Checking what has {mount_path} open...")
        pre_blockers = await asyncio.to_thread(_find_blocking_processes, mount_path)

        status.update(f"Asking Finder to eject {mount_path}...")

        # Address the disk by name. POSIX-file/alias coercion fails once
        # Finder has already started unmounting, but `disk "<name>"` is
        # tolerant of partial state.
        applescript = f'tell application "Finder" to eject disk "{volume_name}"'
        proc = await asyncio.create_subprocess_exec(
            "osascript", "-e", applescript,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # osascript returns once Finder *accepts* the eject, but the actual
        # unmount runs asynchronously. Poll the mountpoint either way.
        deadline = time.monotonic() + 30
        stdout_b = b""
        stderr_b = b""
        try:
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        except asyncio.TimeoutError:
            pass  # Finder may be showing a force-eject prompt; keep polling.

        while mount_path.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        if proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

        if not mount_path.exists():
            status.update(f"Successfully ejected {mount_path}. Press 'q' to quit.")
            self.photo_data.clear()
            table = self.query_one(DataTable)
            table.clear()
            return

        # Still mounted → Finder couldn't eject it. Build a useful error.
        raw_error = (stderr_b.decode("utf-8", "replace") or stdout_b.decode("utf-8", "replace")).strip()
        if not raw_error:
            raw_error = (
                "Finder did not unmount the volume within 30s. It may be "
                "showing a confirmation/force-eject dialog, or a process is "
                "holding the volume open."
            )
        post_blockers = await asyncio.to_thread(_find_blocking_processes, mount_path)
        blockers = post_blockers or pre_blockers
        status.update(f"Eject failed for {mount_path} — see dialog")
        await self.push_screen_wait(EjectErrorScreen(mount_path, raw_error, blockers))

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
