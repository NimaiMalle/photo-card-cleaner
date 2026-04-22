# Photo Card Cleaner

Interactive CLI tool for cleaning old photos from memory cards.

## Features

- **Lightning-fast scanning**: Efficiently scans directories for image files (CR3, RAW, MOV, MP4, TIF, JPG, JPEG)
- **Group by day**: Organizes photos by file modification date
- **Auto-detect camera cards**: Automatically finds and offers common camera card mount points (EOS_DIGITAL, NIKON, etc.)
- **Auto-detect new cards**: Monitors for newly inserted cards while running
- **Disk space display**: Shows used/free space on the volume with live updates
- **Random preview**: View a random photo from any day to verify processing with Quick Look
- **Batch delete**: Delete all photos from a specific day with confirmation
- **Safe eject**: Eject the volume directly from the app when done
- **Tidy macOS metadata**: Remove Spotlight index, `.DS_Store`, `._*`, and other macOS junk from cards; write `.metadata_never_index` to prevent future indexing (and eject hangs)
- **Card Health Check**: Non-destructive checks (filesystem, SMART, full surface read, speed baseline, random-file sample) with a live report card
- **Interactive TUI**: Beautiful terminal interface with keyboard navigation

## Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# Auto-detect camera cards
./run.sh

# Scan specific path
./run.sh /Volumes/EOS_DIGITAL
```

## Keyboard Controls

- **Arrow keys**: Navigate through dates
- **p** or **Space**: Preview random photo from selected day
- **d** or **Backspace/Delete**: Delete all files from selected day (with confirmation)
- **e**: Eject the current volume
- **r**: Rescan directory
- **t**: Tidy macOS metadata (see below)
- **h**: Run Card Health Check (see below)
- **q**: Quit

## Performance

Optimized for maximum speed:
- Uses only filesystem metadata (no file I/O during scan)
- Scans in a background thread with live progress
- Minimal memory footprint

## Image Preview

- **macOS**: Uses Quick Look (`qlmanage`) for instant, lightweight preview window
- **Linux**: Uses `xdg-open`
- **Windows**: Uses default file handler

Quick Look automatically comes to the foreground and can be closed by clicking or pressing Space/Esc.

## Tidy macOS metadata (macOS)

Press **t** to clean up macOS-specific junk that accumulates on memory cards and can cause slow or hung ejects. Shows a confirmation listing exactly what will be removed and how much space that frees.

### Removes

- `.Spotlight-V100/` — Spotlight index (often hundreds of MB; the main culprit behind eject hangs)
- `.fseventsd/` — macOS filesystem event log
- `.Trashes/` — macOS trash folders
- `System Volume Information/` — Windows equivalent, if present
- `.DS_Store` files, recursive — Finder view metadata
- `._*` files, recursive — AppleDouble resource forks

### Creates

- `.metadata_never_index` at the volume root — Spotlight respects this file and will not index the volume on future mounts, preventing the problem from recurring.

### Eject integration

Before ejecting, if the volume has Spotlight index or other metadata present but no `.metadata_never_index` marker, you'll be offered a pre-eject tidy. Accepting this avoids the macOS behavior where `diskutil eject` hangs for minutes while negotiating with Spotlight's indexer.

## Card Health Check (macOS)

Press **h** to run a non-destructive diagnostic against the inserted card. Useful after a card has been dropped, gotten wet, or is behaving oddly.

### What it checks

1. **Volume accessible** — mount point reads cleanly
2. **Mount info** — resolves the underlying block device, filesystem, size, media name
3. **Filesystem integrity** — `diskutil verifyVolume`
4. **SMART probe** — `smartctl -a` (most SD readers don't expose SMART; this shows as N/A when unsupported, which is normal)
5. **Speed baseline** — short raw reads at start/middle/end of the card to derive a realistic sustained-read threshold (70% of observed average)
6. **Full surface read** — `dd` of the entire raw device, monitored for I/O errors and throughput regressions against the baseline
7. **Random file sample** — 10-second timeboxed read-and-hash of random photos

Results are shown live as a report card (pass / warn / fail / N/A), with a final verdict banner.

### Requirements

Raw-device reads require root. Launch with sudo when running the health check:

```bash
sudo ./run.sh /Volumes/EOS_DIGITAL
```

If you launched without sudo, pressing `h` will show the exact relaunch command and exit.

### Optional tools

Install this via Homebrew for full coverage — the SMART probe is skipped (marked N/A) if missing:

```bash
brew install smartmontools   # enables SMART probe
```

Note: most USB SD card readers do not expose SMART to the host, so this check commonly reports N/A even when `smartctl` is installed. That's expected — it's a limitation of the reader, not the card.

Capacity-fraud verification (`f3probe`) is not available on macOS; the Homebrew `f3` bottle ships only `f3read`/`f3write`, which require an empty card and a destructive fill-and-verify pass. The full surface read above already detects read-side damage on existing data, which is the relevant failure mode for cards in active use.

### Controls during a check

- **c** — cancel (partial report is retained)
- **Esc** / **q** — close the report card when done

## Workflow Tips

1. **Start the app** - It will auto-detect camera cards like `/Volumes/EOS_DIGITAL`
2. **Review by date** - Navigate with arrow keys and press Space to preview random photos
3. **Delete old dates** - Press Backspace/Delete to remove entire days with confirmation
4. **Watch disk space** - The top bar shows live disk usage and free space
5. **Insert new cards** - The app automatically detects newly inserted cards
6. **Safe eject** - Press 'e' to safely eject the card when done
