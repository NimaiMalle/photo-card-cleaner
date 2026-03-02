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

## Workflow Tips

1. **Start the app** - It will auto-detect camera cards like `/Volumes/EOS_DIGITAL`
2. **Review by date** - Navigate with arrow keys and press Space to preview random photos
3. **Delete old dates** - Press Backspace/Delete to remove entire days with confirmation
4. **Watch disk space** - The top bar shows live disk usage and free space
5. **Insert new cards** - The app automatically detects newly inserted cards
6. **Safe eject** - Press 'e' to safely eject the card when done
