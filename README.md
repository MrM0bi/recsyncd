# recsyncd

Polls an **Innovaphone PBX AP Recording App** WebDAV endpoint for completed call recordings (`.pcap`), extracts RTP audio, converts to **WAV**, and moves them to a network share. Runs as a systemd service on Linux.

## How it works

```
WebDAV poll ─► size stable for N checks? ─► download .pcap ─► tshark extracts RTP payload
                                                              └► ffmpeg converts to WAV
                                                              └► shutil.move to destination
```

Files appear at 0 bytes while recording, reach final size when call ends. Daemon waits `stable_checks` (default: 2) consecutive polls with identical non-zero size before processing.

Supported codecs: **G.711 µ-law**, **G.711 A-law**, **G.722**, **G.729**. Auto-detected from RTP payload type or SDP negotiation.

## Quickstart

```bash
# Dependencies
apt install tshark ffmpeg python3 python3-venv

# Setup
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp config.example.yaml config.yaml
# edit config.yaml — set webdav_url, credentials, destination_path

# Run
.venv/bin/python recsyncd.py config.yaml
```

## Install as service

```bash
# copy to /opt/recsyncd/
mkdir -p /opt/recsyncd
cp recsyncd.py config.yaml requirements.txt /opt/recsyncd/
cp -r .venv /opt/recsyncd/

# create user
useradd -r -s /bin/false recdaemon
mkdir -p /var/lib/recsyncd /var/log/recsyncd
chown recdaemon:recdaemon /var/lib/recsyncd /var/log/recsyncd

# install service
cp recsyncd.service /etc/systemd/system/
# edit /etc/systemd/system/recsyncd.service — adjust paths and user
systemctl daemon-reload
systemctl enable --now recsyncd
journalctl -fu recsyncd
```

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `webdav_url` | — | WebDAV endpoint (e.g. `https://pbx.example.com/recordings/`) |
| `webdav_user` / `webdav_password` | — | Digest auth credentials |
| `destination_path` | — | Where finished WAVs land |
| `poll_interval` | 10s | WebDAV poll interval (min 5s) |
| `stable_checks` | 2 | Consecutive polls with same size = call complete |
| `orphan_checks` | 3 | Consecutive polls absent = remove from DB |
| `user_filter` | `[]` | If set, only process files matching these usernames |
| `log_dir` | `""` | Daily rotating log files directory (empty = stdout) |
| `log_retention_days` | 30 | Delete log files older than this |
| `log_level` | `INFO` | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `tls_verify` | `true` | Set `false` for self-signed PBX certs |
| `smtp_host` / `smtp_to` | `""` / `[]` | SMTP error digest (optional) |
| `smtp_interval` | 600s | Digest email interval |

## Architecture

Single file (`recsyncd.py`), three classes:

- **`Config`** — dataclass loaded from YAML
- **`StateDB`** — SQLite wrapper tracking `href → status` (done/failed/orphan/filtered)
- **`Daemon`** — asyncio poll loop with rolling `size_history` for completion detection

Conversion runs in a thread executor (non-blocking to the poll loop). Per-stream audio files are extracted via `tshark`, then merged with `ffmpeg`.

### Stereo handling

- 1 RTP stream → mono WAV
- 2 streams → stereo WAV (one direction per channel)
- 3+ streams → grouped by source IP into stereo if 2 distinct sources, otherwise mixed to stereo

## Admins

```bash
# Retry failed files
sqlite3 /var/lib/recsyncd/state.db "DELETE FROM files WHERE status='failed'"

# Reset database
sqlite3 /var/lib/recsyncd/state.db "DELETE FROM files"
```

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).
