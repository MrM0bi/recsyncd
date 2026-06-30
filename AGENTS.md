# AGENTS.md

This file provides guidance to AI Coding Agents when working with code in this repository.

## What this is

Python asyncio daemon that polls an Innovaphone PBX WebDAV endpoint for completed call recordings (.pcap), converts them to audio (WAV/MP3), and moves them to a network share. Runs as a systemd service on Linux.

## Setup & running

```bash
# Install system dependencies (Debian/Ubuntu)
apt install tshark ffmpeg

# Python environment
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with real credentials and paths

# Run directly
.venv/bin/python recsyncd.py config.yaml

# Install as service
cp recsyncd.service /etc/systemd/system/
systemctl enable --now recsyncd
journalctl -fu recsyncd
```

## Architecture

Single file (`recsyncd.py`) with three classes:

- **`Config`** — dataclass loaded from YAML. All tunable parameters live here.
- **`StateDB`** — SQLite wrapper. Tracks `href → status` so processed files are never re-downloaded after restart.
- **`Daemon`** — asyncio poll loop. Maintains `size_history` dict in memory (href → rolling list of observed sizes) to detect when a recording is complete.

### "File complete" detection

Innovaphone files appear on WebDAV at 0 bytes while recording, then reach final size when the call ends. The daemon observes `stable_checks` (default: 2) consecutive polls with identical non-zero size before treating a file as ready. Increase `stable_checks` if false positives occur on slow/large recordings.

### Conversion pipeline

`_convert()` runs synchronously in a thread executor (non-blocking to the poll loop):
1. `tshark` extracts RTP payload as hex fields
2. Python strips colon separators and decodes to raw bytes
3. `ffmpeg` converts raw PCM/µ-law/A-law bytes to WAV or MP3

Assumes one RTP stream per PCAP (one call = one file). For conference recordings with multiple streams, `_convert()` needs rework.

### URL construction

`_origin` is derived from `webdav_url` (scheme + host only). WebDAV PROPFIND returns absolute-path hrefs (e.g. `/recordings/call.pcap`). Download URL = `_origin + href`. If Innovaphone returns relative hrefs, `_download()` needs adjustment.

## Key config parameters

| Parameter | Default | Notes |
|---|---|---|
| `poll_interval` | 10s | Don't go below 5s — WebDAV is unstable under load |
| `stable_checks` | 2 | Polls needed to confirm file complete |
| `audio_codec` | `mulaw` | G.711 µ-law; use `alaw` for G.711 A-law |
| `tls_verify` | `true` | Set `false` only for self-signed PBX certs |

## Failed files

Files marked `status='failed'` in SQLite are not retried automatically. To retry:
```sql
DELETE FROM files WHERE status='failed';
```

or using sqlite3:

```bash
sqlite3 state.db "DELETE FROM files WHERE status='failed'"
```

## Reset Database completly

```sql
DELETE FROM files;
```

or using sqlite3:

```bash
sqlite3 state.db "DELETE FROM files"
```
