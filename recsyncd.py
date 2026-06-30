#!/usr/bin/env python3
"""
recsyncd
========
Polls a WebDAV server (Innovaphone AP Recording App) for completed call recordings
(.pcap files), converts them to WAV audio, and moves them to a network share.

Run as:  python recsyncd.py config.yaml
"""

from __future__ import annotations
import asyncio      # for running tasks concurrently without threads
import logging      # for writing timestamped log messages to stdout
import logging.handlers
import smtplib
import sqlite3      # for the small local database that tracks processed files
import subprocess   # for calling external programs (tshark, ffmpeg)
import shutil       # for moving files across filesystems
import sys          # for reading command-line arguments and exiting
import threading
import time
from email.mime.text import MIMEText
from collections import Counter         # for finding the most common value in a list
from dataclasses import dataclass, field, fields  # for the Config settings class
from datetime import date, datetime, timedelta  # for timestamped filenames and log rotation
from pathlib import Path                # for convenient file path handling
from urllib.parse import urlparse       # for splitting a URL into its parts
import yaml         # for reading the config.yaml file
import httpx        # for making HTTP/WebDAV requests (supports async and digest auth)
import xml.etree.ElementTree as ET      # for parsing the XML that WebDAV returns


# --- Logging setup -----------------------------------------------------------
# Sets up log messages in the format:  2026-01-15 12:34:56,789 INFO Some message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


class DailyRotatingFileHandler(logging.FileHandler):
    """
    Writes log output to YYYY-MM-DD.log files inside log_dir.
    Rotates at midnight by reopening a new file and deletes files older than
    retention_days at each rotation.
    """

    def __init__(self, log_dir: Path, retention_days: int):
        self._log_dir = log_dir
        self._retention_days = retention_days
        self._current_date = datetime.now().date()
        super().__init__(self._log_path(self._current_date), encoding="utf-8")
        self._cleanup_old_logs()

    def _log_path(self, d: date) -> str:
        return str(self._log_dir / f"{d.isoformat()}.log")

    def emit(self, record: logging.LogRecord) -> None:
        # Called for every log line. Check if midnight has passed since we last
        # opened a log file; if so, switch to a new YYYY-MM-DD.log file.
        today = datetime.now().date()
        if today != self._current_date:
            self.close()
            self._current_date = today
            self.baseFilename = self._log_path(today)
            self.stream = self._open()
            self._cleanup_old_logs()
        super().emit(record)

    def _cleanup_old_logs(self) -> None:
        cutoff = datetime.now().date() - timedelta(days=self._retention_days)
        for log_file in self._log_dir.glob("????-??-??.log"):
            try:
                if date.fromisoformat(log_file.stem) < cutoff:
                    log_file.unlink()
            except ValueError:
                pass


def setup_file_logging(log_dir: str, retention_days: int) -> None:
    """Adds a daily rotating file handler to the root logger."""
    if not log_dir:
        return
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    handler = DailyRotatingFileHandler(path, retention_days)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)


class _BufferedSMTPHandler(logging.Handler):
    """
    Buffers log records in memory and sends them as a single digest email at a
    fixed interval. A daemon thread wakes every `interval` seconds to flush.
    emit() is non-blocking — it just appends to the buffer under a lock.
    """

    _FAILURE_COOLDOWN = 60  # seconds between repeated SMTP failure warnings

    def __init__(
        self,
        mailhost: str,
        mailport: int,
        fromaddr: str,
        toaddrs: list[str],
        credentials: tuple[str, str] | None,
        secure: bool,
        interval: int,
        instance: str,
    ):
        super().__init__()
        self._instance = instance
        self._mailhost = mailhost
        self._mailport = mailport
        self._fromaddr = fromaddr
        self._toaddrs = toaddrs
        self._credentials = credentials
        self._secure = secure
        self._interval = interval
        self._buffer: list[str] = []   # pre-formatted log lines
        self._lock = threading.Lock()
        # threading.local() gives each thread its own independent copy of the 'sending' flag.
        # We need this because log.info() calls inside _flush_buffer would otherwise re-enter
        # emit(), add new lines to the buffer, and trigger another send — an infinite loop.
        # The flag breaks that cycle: emit() returns early while a flush is already running.
        self._local = threading.local()
        self._last_failure_time: float = 0
        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

    def emit(self, record: logging.LogRecord) -> None:
        # Skip log records that are emitted by _flush_buffer itself (e.g. "Sending digest…").
        # Without this check those messages would land in the buffer and trigger another send.
        if getattr(self._local, "sending", False):
            return
        try:
            line = self.format(record)
            with self._lock:
                self._buffer.append(line)
        except Exception:
            self.handleError(record)

    def _flush_loop(self) -> None:
        while True:
            time.sleep(self._interval)
            self._flush_buffer()

    def flush(self) -> None:
        """Send any buffered records immediately (called on daemon shutdown)."""
        self._flush_buffer()

    def _flush_buffer(self) -> None:
        """Drain the buffer and send one digest email. Safe to call from any thread."""
        # Set the re-entrancy flag for this thread before we start logging anything
        self._local.sending = True
        try:
            with self._lock:
                lines = self._buffer[:]
                self._buffer.clear()
            if not lines:
                return
            subject = f"[recsyncd] {len(lines)} error alert(s) - {self._instance}"
            body = "\n".join(lines)
            log.info(
                "Sending error digest (%d line(s)) via SMTP to %r",
                len(lines),
                self._toaddrs,
            )
            try:
                msg = MIMEText(body)
                msg["Subject"] = subject
                msg["From"] = self._fromaddr
                msg["To"] = ", ".join(self._toaddrs)
                with smtplib.SMTP(self._mailhost, self._mailport) as smtp:
                    if self._secure:
                        smtp.ehlo()
                        smtp.starttls()
                        smtp.ehlo()
                    if self._credentials:
                        smtp.login(*self._credentials)
                    smtp.sendmail(self._fromaddr, self._toaddrs, msg.as_string())
                log.info("Error digest sent successfully to %r", self._toaddrs)
            except Exception:
                now = time.monotonic()
                if now - self._last_failure_time >= self._FAILURE_COOLDOWN:
                    self._last_failure_time = now
                    log.warning(
                        "SMTP delivery failed for %r — check smtp_host/smtp_port/credentials: %s",
                        self._toaddrs,
                        sys.exc_info()[1],
                    )
        finally:
            self._local.sending = False


def setup_smtp_logging(config) -> "_BufferedSMTPHandler | None":
    """
    Attaches a buffered SMTP handler to the root logger.
    Returns the handler so main() can call .flush() on shutdown, or None if
    SMTP is not configured.
    """
    if not config.smtp_host or not config.smtp_to:
        return None

    level = getattr(logging, config.smtp_level.upper(), logging.ERROR)
    credentials = (config.smtp_user, config.smtp_password) if config.smtp_user else None
    secure = config.smtp_use_tls

    handler = _BufferedSMTPHandler(
        mailhost=config.smtp_host,
        mailport=config.smtp_port,
        fromaddr=config.smtp_from or f"recsyncd@{config.smtp_host}",
        toaddrs=config.smtp_to,
        credentials=credentials,
        secure=secure,
        interval=config.smtp_interval,
        instance=urlparse(config.webdav_url).hostname or config.webdav_url,
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)
    return handler


# --- Constants ---------------------------------------------------------------

# The XML namespace string that WebDAV uses inside all of its responses
WEBDAV_NAMESPACE = "DAV:"

# These audio formats are raw bitstreams with no file header, so ffmpeg needs
# to be told the sample rate and channel count explicitly when it reads them.
# G.729 is different — it has its own framing and tells ffmpeg its own parameters.
FORMATS_NEEDING_SAMPLE_RATE_HINT = {"mulaw", "alaw", "g722"}

# The XML body we send to the WebDAV server to request a directory listing.
# "PROPFIND" is the WebDAV method for "list the properties of files in this folder".
WEBDAV_LIST_REQUEST = b"""<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:displayname/>
    <D:getcontentlength/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>"""

# Maps the RTP payload type number (a standard integer present in every RTP packet)
# to a tuple: (ffmpeg_format_name, sample_rate_in_hz, number_of_channels)
# These are "static" types whose numbers are fixed by the RTP standard (RFC 3551).
KNOWN_RTP_PAYLOAD_TYPES: dict[int, tuple[str, int, int]] = {
    0:  ("mulaw", 8000, 1),   # G.711 µ-law (PCMU)
    8:  ("alaw",  8000, 1),   # G.711 A-law (PCMA)
    9:  ("g722", 16000, 1),   # G.722 wideband
    18: ("g729",  8000, 1),   # G.729 compressed
}

# Maps codec name strings (as they appear in SDP negotiation embedded in the PCAP)
# to the same tuple format as above.
# Used when the RTP payload type number is in the "dynamic" range (96–127),
# which means the codec name was negotiated during the call setup and recorded in SDP.
KNOWN_CODEC_NAMES: dict[str, tuple[str, int, int]] = {
    "pcmu":  ("mulaw", 8000, 1),
    "pcma":  ("alaw",  8000, 1),
    "g711u": ("mulaw", 8000, 1),
    "g711a": ("alaw",  8000, 1),
    "g722":  ("g722", 16000, 1),
    "g729":  ("g729",  8000, 1),
}

# Regular expression that matches the Innovaphone PCAP filename format:
#   <random-hex>-<phone-serial-12-hex-chars>-<call-number>--<username>
# Example: 5f9f8974e4166a01926b0090332ffb1a-0090332ffb1a-8--benjamin.herbst
# --- Helper functions --------------------------------------------------------

def build_output_filename(recording_stem: str, file_extension: str) -> str:
    """
    Builds a human-readable output filename from the raw PCAP filename stem.

    Input stem:  <randomid>-<serial>-<number>--<username>
    Output:      YYYY-MM-DD_HHMMSS_<username>_<serial>_<number>.<ext>

    Splits on "-"; the "--" separator produces an empty string at index 3.
    Falls back to YYYY-MM-DD_HHMMSS_<full-stem>.<ext> if format not recognised.
    """
    now = datetime.now()
    date_part = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H%M%S")

    # Expected format: <randomid>-<serial>-<number>--<username>
    # The "--" separator produces an empty string at index 3 when split on "-"
    parts = recording_stem.split('-')
    if len(parts) >= 5 and parts[3] == '' and parts[2].isdigit():
        phone_serial = parts[1]
        call_number = parts[2]
        username = '-'.join(parts[4:])
        return (
            f"{date_part}_{time_part}_{username}_{phone_serial.lower()}"
            f"_{call_number}{file_extension}"
        )

    # Fallback: filename didn't match the expected Innovaphone format
    return f"{date_part}_{time_part}_{recording_stem}{file_extension}"


# --- Configuration -----------------------------------------------------------

# @dataclass is a shortcut that automatically creates an __init__ method for us.
# Think of it as a simple container for settings — like a named dictionary where
# each key has a fixed type and an optional default value.
@dataclass
class Config:
    """All settings loaded from config.yaml. Fields without a default are required."""

    webdav_url: str           # e.g. https://pbx.example.com/recordings/
    webdav_user: str
    webdav_password: str
    destination_path: str     # e.g. /mnt/share/recordings — where finished WAVs go

    temp_dir: str = "/tmp/recsyncd"    # working directory for in-progress files
    poll_interval: int = 10              # seconds between WebDAV checks
    # How many consecutive polls with the same non-zero file size confirm the
    # call has ended and the file is complete:
    stable_checks: int = 2
    db_path: str = "/var/lib/recsyncd/state.db"
    tls_verify: bool = True              # set False for self-signed PBX certificates
    user_filter: list = field(default_factory=list)  # if non-empty, only process these usernames
    # How many consecutive polls a file must be absent from WebDAV before its
    # database record is deleted:
    orphan_checks: int = 3
    log_dir: str = ""               # directory for daily log files; empty = stdout only
    log_retention_days: int = 30    # delete log files older than this many days
    log_level: str = "INFO"         # DEBUG, INFO, WARNING, ERROR, CRITICAL

    # SMTP error notifications — all fields optional; disabled if smtp_host or smtp_to is empty
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_to: list = field(default_factory=list)   # one or more recipient addresses
    smtp_level: str = "ERROR"       # minimum log level that triggers an email
    smtp_use_tls: bool = True       # use STARTTLS (port 587); set False for plain SMTP
    smtp_interval: int = 600        # seconds between digest emails (default: 10 minutes)


def load_settings(path: str) -> Config:
    """Reads config.yaml from disk and returns a Config object with all the settings."""
    with open(path) as config_file:
        raw_yaml_data = yaml.safe_load(config_file)

    # Only pass YAML keys that Config actually knows about — silently ignore unknown keys
    valid_setting_names = {f.name for f in fields(Config)}
    return Config(**{
        key: value
        for key, value in raw_yaml_data.items()
        if key in valid_setting_names
    })


# --- State database ----------------------------------------------------------

class StateDB:
    """SQLite tracker for recording file status: done, failed, filtered, orphan."""

    def __init__(self, db_file_path: str):
        Path(db_file_path).parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False needed for asyncio event loop thread
        self.connection = sqlite3.connect(db_file_path, check_same_thread=False)
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS files (
                href         TEXT PRIMARY KEY,
                status       TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.connection.commit()

    def has_status(self, webdav_path: str, status: str | None = None) -> bool:
        """True if a row exists for webdav_path. If status given, match that too."""
        if status is None:
            cursor = self.connection.execute(
                "SELECT 1 FROM files WHERE href=?", (webdav_path,)
            )
        else:
            cursor = self.connection.execute(
                "SELECT 1 FROM files WHERE href=? AND status=?", (webdav_path, status)
            )
        return cursor.fetchone() is not None

    def set_status(self, webdav_path: str, status: str):
        """Insert or update the status for a file."""
        self.connection.execute(
            "INSERT OR REPLACE INTO files(href, status) VALUES(?, ?)", (webdav_path, status)
        )
        self.connection.commit()

    def delete_record(self, webdav_path: str):
        self.connection.execute("DELETE FROM files WHERE href=?", (webdav_path,))
        self.connection.commit()

    def known_paths(self) -> set[str]:
        cursor = self.connection.execute("SELECT href FROM files")
        return {row[0] for row in cursor.fetchall()}

    def count_by_status(self) -> dict[str, int]:
        cursor = self.connection.execute(
            "SELECT status, COUNT(*) FROM files GROUP BY status"
        )
        return {row[0]: row[1] for row in cursor.fetchall()}


# --- Main daemon -------------------------------------------------------------

# The Daemon class holds all the running state (HTTP connection, size history, …)
# and contains every piece of logic: polling, orphan detection, downloading, converting.
#
# How to start it:
#   daemon = Daemon(config)
#   asyncio.run(daemon.run())    ← this blocks until the process is stopped
#
class Daemon:
    """
    The main recording daemon.

    Runs an infinite poll loop:
      1. Ask the WebDAV server for the current list of .pcap files
      2. Check whether any previously-seen files have disappeared (orphan detection)
      3. For each new non-zero-size file, record its size across multiple polls
      4. Once a file's size has been stable for stable_checks polls (= call ended),
         download it, convert it to WAV, and move it to the destination share
    """

    def __init__(self, config: Config):
        # Store the settings so all methods in this class can access them via self.config
        self.config = config
        self.database = StateDB(config.db_path)

        # Tracks file sizes observed across recent polls, one entry per file.
        # Key:   WebDAV path string (e.g. "/recordings/call.pcap")
        # Value: list of file sizes from the last N polls (oldest first)
        # Purpose: detect when a recording is complete (size stops changing)
        self.size_history: dict[str, list[int]] = {}

        # Tracks how many consecutive polls each file has been absent from WebDAV.
        # Key:   WebDAV path string
        # Value: number of polls the file has been missing
        self.missing_poll_count: dict[str, int] = {}

        # Create the temp directory for in-progress downloads if it doesn't exist
        Path(config.temp_dir).mkdir(parents=True, exist_ok=True)

        # Extract just "scheme://host" from the WebDAV URL.
        # WebDAV PROPFIND returns file paths like "/recordings/call.pcap" (no host).
        # We prepend this base URL to build the full download URL.
        # Example: "https://pbx.example.com/recordings/" → "https://pbx.example.com"
        parsed_url = urlparse(config.webdav_url)
        self.server_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # =========================================================================
    # About async / await
    # =========================================================================
    # Normal Python functions block: while they wait for a network response,
    # nothing else can run.  Async functions are different: when they hit an
    # 'await', they pause and hand control back to the event loop, which can
    # then run other async tasks.  No extra threads are needed.
    #
    # 'async def' declares an async function.
    # 'await some_function()' means "call this async function and wait for it,
    #  but let other tasks run in the meantime".
    # =========================================================================

    async def run(self):
        """Main loop: polls WebDAV every poll_interval seconds, forever."""
        log.info(
            "Daemon started — polling every %ds, stable_checks=%d",
            self.config.poll_interval, self.config.stable_checks,
        )

        # Create a shared HTTP client that handles authentication and TLS for us.
        # 'async with' is like a regular 'with' block but works with async code —
        # it ensures the HTTP connection is properly closed when we exit.
        async with httpx.AsyncClient(
            auth=httpx.DigestAuth(self.config.webdav_user, self.config.webdav_password),
            verify=self.config.tls_verify,
            timeout=30.0,
        ) as http_client:
            try:
                while True:
                    try:
                        await self.poll_once(http_client)
                    except Exception as error:
                        # Log the error but keep running — one bad poll should not
                        # stop the daemon (the PBX might be temporarily unreachable)
                        log.error("Poll cycle failed: %s", error)

                    # Wait before the next poll.
                    # 'await' here means: pause this loop and let other async tasks
                    # run while we wait, rather than freezing the whole process.
                    await asyncio.sleep(self.config.poll_interval)

            except asyncio.CancelledError:
                # Raised when something asks the daemon to shut down gracefully
                log.info("Daemon shutting down")

    async def poll_once(self, http_client: httpx.AsyncClient):
        """
        One full iteration of the main loop:
          - Fetch the list of .pcap files currently on the WebDAV server
          - Detect any files that have disappeared since the last poll
          - Update size history for each file we see
          - Download and convert any file whose size has been stable long enough
        """
        # Ask the WebDAV server for its current file list.
        # Result is a dict:  {"/recordings/call.pcap": 123456, ...}  (path → size in bytes)
        files_on_server = await self.get_file_list(http_client)
        log.debug("WebDAV returned %d .pcap files", len(files_on_server))

        # Check whether any previously-known file is no longer on the server
        self.handle_missing_files(set(files_on_server.keys()))

        # Build the list of files that are ready to be downloaded and converted
        files_ready_to_process = []

        for webdav_path, file_size in files_on_server.items():

            # Skip files we have already successfully processed
            if self.database.has_status(webdav_path, 'done'):
                continue

            # Skip files that previously failed — the error was already logged and
            # emailed once when it first failed, so we must not retry (and re-alert)
            # on every poll. Drop any stale size history so we stop tracking it.
            if self.database.has_status(webdav_path, 'failed'):
                self.size_history.pop(webdav_path, None)
                continue

            # If user_filter is set, skip files whose name doesn't contain
            # any of the configured usernames
            if self.config.user_filter:
                filename = Path(webdav_path).name
                username_matches = any(
                    username.lower() in filename.lower()
                    for username in self.config.user_filter
                )
                if not username_matches:
                    if not self.database.has_status(webdav_path):
                        log.info("Filtered (not in user_filter): %s", filename)
                        self.database.set_status(webdav_path, 'filtered')
                    continue

            # A file at 0 bytes means the call is still in progress — skip it.
            # Also clear any previous size history for this path (edge case: a new
            # call reusing a path that had a previous partial history).
            if file_size == 0:
                self.size_history.pop(webdav_path, None)
                continue

            # Add the current size to this file's rolling size history
            size_readings = self.size_history.setdefault(webdav_path, [])
            size_readings.append(file_size)
            # Keep only the most recent readings (we don't need older ones)
            if len(size_readings) > self.config.stable_checks + 1:
                size_readings.pop(0)   # remove the oldest reading

            # The file is "stable" (call ended) when the last N readings are all
            # the same non-zero size — meaning nothing new was written to the file.
            recent_readings = size_readings[-self.config.stable_checks:]
            size_is_stable = (
                len(recent_readings) == self.config.stable_checks
                and len(set(recent_readings)) == 1    # all values in the set are identical
            )
            if size_is_stable:
                files_ready_to_process.append(webdav_path)

        # Download and convert each ready file one at a time
        for webdav_path in files_ready_to_process:
            await self.process_recording(http_client, webdav_path)

        counts = self.database.count_by_status()
        log.info(
            "Poll OK — WebDAV: %d file(s) | done: %d | filtered: %d | orphaned: %d | failed: %d",
            len(files_on_server),
            counts.get("done", 0),
            counts.get("filtered", 0),
            counts.get("orphan", 0),
            counts.get("failed", 0),
        )

    def handle_missing_files(self, files_currently_on_server: set[str]):
        """
        Detects files that were previously known to us but have vanished from WebDAV.

        On the first poll where a file is missing:
            → marks it as 'orphan' in the database and clears its size history

        After orphan_checks consecutive polls where it's still missing:
            → deletes its database record entirely
        """
        # The full set of files we should be watching:
        # everything currently in size_history (being monitored) +
        # everything recorded in the database (previously seen)
        all_known_files = (
            set(self.size_history.keys()) | self.database.known_paths()
        )

        for webdav_path in all_known_files:
            if webdav_path in files_currently_on_server:
                # File is present on the server — reset its "missing" counter
                self.missing_poll_count.pop(webdav_path, None)
                continue

            # File is not on the server this poll — increment the missing counter
            times_missing = self.missing_poll_count.get(webdav_path, 0) + 1
            self.missing_poll_count[webdav_path] = times_missing

            if times_missing == 1:
                # First time we notice it's gone: mark it and log it
                log.info(
                    "File absent from WebDAV, marked orphan: %s",
                    Path(webdav_path).name,
                )
                self.database.set_status(webdav_path, 'orphan')
                self.size_history.pop(webdav_path, None)

            if times_missing >= self.config.orphan_checks:
                # It has been missing for enough consecutive polls — clean it up
                log.info(
                    "Orphan absent for %d polls, removing from DB: %s",
                    times_missing, Path(webdav_path).name,
                )
                self.database.delete_record(webdav_path)
                del self.missing_poll_count[webdav_path]

    async def get_file_list(self, http_client: httpx.AsyncClient) -> dict[str, int]:
        """
        Sends a WebDAV PROPFIND request to the configured URL and returns the
        list of .pcap files found there, as a dict mapping path → size in bytes.
        """
        response = await http_client.request(
            "PROPFIND",
            self.config.webdav_url,
            content=WEBDAV_LIST_REQUEST,
            headers={"Depth": "1", "Content-Type": "application/xml"},
        )
        response.raise_for_status()
        return self.parse_file_list(response.text)

    def parse_file_list(self, xml_text: str) -> dict[str, int]:
        """
        Parses the XML response body from a WebDAV PROPFIND request.
        Returns a dict mapping each .pcap file's server path to its size in bytes.
        """
        result: dict[str, int] = {}

        try:
            xml_root = ET.fromstring(xml_text)
        except ET.ParseError as error:
            log.error("WebDAV XML parse error: %s", error)
            return result

        # The XML contains one <response> element per file/directory in the folder
        for xml_entry in xml_root.findall(f"{{{WEBDAV_NAMESPACE}}}response"):

            # Read the file path from the <href> element
            path_element = xml_entry.find(f"{{{WEBDAV_NAMESPACE}}}href")
            if path_element is None or not path_element.text:
                continue
            webdav_path = path_element.text

            # Some servers return paths with double slashes — normalise them
            while "//" in webdav_path:
                webdav_path = webdav_path.replace("//", "/")

            # We only care about .pcap files
            if not webdav_path.lower().endswith(".pcap"):
                continue

            # Skip directory entries — they contain a <collection/> element
            if xml_entry.find(f".//{{{WEBDAV_NAMESPACE}}}collection") is not None:
                continue

            # Read the file size from <getcontentlength>
            size_element = xml_entry.find(f".//{{{WEBDAV_NAMESPACE}}}getcontentlength")
            if size_element is None or not size_element.text:
                continue

            try:
                result[webdav_path] = int(size_element.text)
            except ValueError:
                pass    # ignore entries with non-numeric size values

        return result

    async def process_recording(self, http_client: httpx.AsyncClient, webdav_path: str):
        """
        Full pipeline for one completed recording file:
          1. Download the .pcap from WebDAV into the temp directory
          2. Extract audio from the .pcap and convert it to WAV
             (this runs in a background thread so the event loop stays responsive)
          3. Move the WAV to the destination share with a readable filename
          4. Mark the file as 'done' in the database
        """
        filename = Path(webdav_path).name
        temp_pcap_file = Path(self.config.temp_dir) / filename
        log.info("Processing: %s", filename)

        try:
            # Step 1: download
            await self.download_recording(http_client, webdav_path, temp_pcap_file)

            # Step 2: convert PCAP → WAV
            # convert_pcap_to_wav() calls tshark and ffmpeg, which are normal blocking
            # programs.  run_in_executor() runs it in a thread pool so that the async
            # event loop is free to do other things (e.g. handle another poll) while
            # the conversion is running.
            event_loop = asyncio.get_event_loop()
            output_wav_file = await event_loop.run_in_executor(
                None, self.convert_pcap_to_wav, temp_pcap_file
            )

            # Step 3: move WAV to destination with a human-readable name
            output_filename = build_output_filename(
                Path(webdav_path).stem, output_wav_file.suffix
            )
            destination = Path(self.config.destination_path) / output_filename
            shutil.move(str(output_wav_file), str(destination))
            log.info("Done: %s → %s", filename, destination.name)

            # Step 4: record success so we never process this file again
            self.database.set_status(webdav_path, 'done')
            self.size_history.pop(webdav_path, None)

        except Exception as error:
            log.error("Failed to process %s: %s", filename, error)
            self.database.set_status(webdav_path, 'failed')

        finally:
            # Always delete the temp PCAP, whether we succeeded or failed
            temp_pcap_file.unlink(missing_ok=True)

    async def download_recording(
        self, http_client: httpx.AsyncClient, webdav_path: str, save_to: Path
    ):
        """Downloads one file from the WebDAV server and writes it to a local path."""
        download_url = self.server_base_url + webdav_path

        # stream() downloads in chunks instead of loading the whole file into memory first
        async with http_client.stream("GET", download_url) as response:
            response.raise_for_status()
            with open(save_to, "wb") as output_file:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    output_file.write(chunk)

        log.debug("Downloaded %s (%d bytes)", save_to.name, save_to.stat().st_size)

    # =========================================================================
    # Audio conversion (regular blocking functions — not async)
    # These call tshark and ffmpeg via subprocess and may take several seconds.
    # They are run inside a thread (via run_in_executor above) so they don't
    # block the async event loop.
    # =========================================================================

    def detect_audio_codec(self, pcap_file: Path) -> tuple[str, int, int]:
        """
        Inspects the RTP packets in a PCAP file to determine which audio codec was used.
        Returns a tuple of:  (ffmpeg_format_name, sample_rate_in_hz, number_of_channels)

        How it works:
        Every RTP packet contains a small integer called the "payload type" that
        identifies the codec.  Numbers 0–95 are fixed by the RTP standard.
        Numbers 96–127 are "dynamic" — the codec was negotiated during call setup
        via SDP (Session Description Protocol), which is also stored in the PCAP.
        """
        # Ask tshark to list the payload type number from every RTP packet
        tshark_output = self.run_command([
            "tshark", "-r", str(pcap_file),
            "-Y", "rtp",            # filter: show only RTP packets
            "-T", "fields",         # output format: individual fields, one per line
            "-e", "rtp.p_type",     # field: the payload type number
        ])

        payload_type_numbers = [
            int(line)
            for line in tshark_output.stdout.decode(errors="replace").splitlines()
            if line.strip().isdigit()
        ]

        if not payload_type_numbers:
            raise RuntimeError("No RTP packets in PCAP")

        # Use the most common payload type — a few control packets with a different
        # type shouldn't confuse us into picking the wrong codec
        dominant_payload_type = Counter(payload_type_numbers).most_common(1)[0][0]
        log.debug("RTP payload type: %d", dominant_payload_type)

        # Look up the payload type in the well-known static table first
        if dominant_payload_type in KNOWN_RTP_PAYLOAD_TYPES:
            return KNOWN_RTP_PAYLOAD_TYPES[dominant_payload_type]

        # Dynamic payload type — look up the codec name from the SDP data in the PCAP.
        # SDP rtpmap attribute lines look like: "96 G729/8000"
        sdp_tshark_output = subprocess.run(
            [
                "tshark", "-r", str(pcap_file),
                "-Y", "sdp",
                "-T", "fields",
                "-e", "sdp.attribute.value",
            ],
            capture_output=True,
        )
        for sdp_line in sdp_tshark_output.stdout.decode(errors="replace").splitlines():
            for sdp_entry in sdp_line.split(","):
                sdp_entry = sdp_entry.strip()
                # Check if this SDP entry describes our dynamic payload type number
                if sdp_entry.startswith(f"{dominant_payload_type} "):
                    # Extract the codec name — e.g. "96 G729/8000" → "g729"
                    codec_name = sdp_entry.split(" ", 1)[1].split("/")[0].lower()
                    if codec_name in KNOWN_CODEC_NAMES:
                        return KNOWN_CODEC_NAMES[codec_name]

        log.warning(
            "Unknown RTP payload type %d — falling back to G.711 µ-law",
            dominant_payload_type,
        )
        return ("mulaw", 8000, 1)

    def convert_pcap_to_wav(self, pcap_file: Path) -> Path:
        """
        Converts a PCAP recording file to a WAV audio file.
        Returns the path to the resulting WAV file.

        The conversion pipeline is:
          PCAP  →  raw audio bytes per RTP stream (.bin files)  →  merged WAV
        The intermediate .bin files are deleted automatically at the end.
        """
        output_wav_file = pcap_file.with_suffix(".wav")
        # Intermediate path used as a base name for per-stream binary files.
        # Actual files will be named like: <base>.c4720600.bin, <base>.c1aeba9c.bin, …
        intermediate_bin_base = pcap_file.with_suffix(".bin")

        try:
            audio_format, sample_rate, channel_count = self.detect_audio_codec(pcap_file)
            log.info("Codec: %s  %dHz  %dch", audio_format, sample_rate, channel_count)
            self.convert_streams_to_wav(
                pcap_file,
                intermediate_bin_base,
                audio_format,
                sample_rate,
                channel_count,
                output_wav_file,
            )
        finally:
            # Clean up the base .bin file path (individual stream files are cleaned
            # up inside convert_streams_to_wav)
            intermediate_bin_base.unlink(missing_ok=True)

        return output_wav_file

    def get_stream_ids(self, pcap_file: Path) -> list[str]:
        """
        Returns the list of unique RTP SSRC values (stream source identifiers) in the PCAP.

        A typical phone call has two RTP streams: one for each direction
        (caller → callee and callee → caller).  Each stream has a unique SSRC number.
        We extract them separately so we can later merge them into stereo audio
        (one direction per channel).
        """
        tshark_output = self.run_command([
            "tshark", "-r", str(pcap_file),
            "-Y", "rtp",
            "-T", "fields",
            "-e", "rtp.ssrc",   # the per-stream unique identifier
        ])

        # Collect unique SSRCs while preserving the order they first appear
        seen_ssrcs: set[str] = set()
        unique_stream_ids: list[str] = []
        for line in tshark_output.stdout.decode(errors="replace").splitlines():
            ssrc = line.strip()
            if ssrc and ssrc not in seen_ssrcs:
                seen_ssrcs.add(ssrc)
                unique_stream_ids.append(ssrc)

        log.debug("RTP stream IDs (SSRCs): %s", unique_stream_ids)
        return unique_stream_ids

    def get_stream_source_ips(self, pcap_file: Path) -> dict[str, str]:
        """
        Returns a mapping of RTP SSRC → source IP address (the IP that sent the stream).

        Used to group the streams of a multi-party recording by who sent them, so
        we can fold several streams down to a two-channel (stereo) recording with
        one side per source. tshark separates the requested fields with a tab.
        """
        tshark_output = self.run_command([
            "tshark", "-r", str(pcap_file),
            "-Y", "rtp",
            "-T", "fields",
            "-e", "rtp.ssrc",
            "-e", "ip.src",
        ])

        ssrc_to_ip: dict[str, str] = {}
        for line in tshark_output.stdout.decode(errors="replace").splitlines():
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            ssrc, source_ip = parts[0].strip(), parts[1].strip()
            # Keep the first IP we see for each SSRC (it never changes within a stream)
            if ssrc and source_ip and ssrc not in ssrc_to_ip:
                ssrc_to_ip[ssrc] = source_ip

        log.debug("RTP SSRC → source IP: %s", ssrc_to_ip)
        return ssrc_to_ip

    @staticmethod
    def run_command(command: list[str]) -> subprocess.CompletedProcess:
        """
        Runs an external command (tshark or ffmpeg) and returns its output.
        If the command fails (non-zero exit code), logs the full error output
        and raises an exception so the caller knows something went wrong.

        @staticmethod means this function doesn't need access to 'self' — it's
        just a utility function grouped inside the class for organisation.
        """
        result = subprocess.run(command, capture_output=True)

        if result.returncode != 0:
            error_output = result.stderr.decode(errors="replace").strip()

            # tshark exits 2 when a capture is truncated ("appears to have been cut
            # short in the middle of a packet"), but it still emits every packet it
            # managed to read before the truncation. Innovaphone occasionally leaves
            # a recording cut short on abnormal call teardown — the audio up to that
            # point is still usable, so salvage the partial output instead of failing.
            if command[0] == "tshark" and "cut short" in error_output and result.stdout:
                log.warning(
                    "tshark read a truncated capture (exit %d) — salvaging the "
                    "%d row(s) read before the cut",
                    result.returncode,
                    result.stdout.decode(errors="replace").count("\n"),
                )
                return result

            log.error(
                "Command failed (exit %d): %s\n%s",
                result.returncode, command[0], error_output,
            )
            raise subprocess.CalledProcessError(
                result.returncode, command, result.stdout, result.stderr
            )

        return result

    def convert_streams_to_wav(
        self,
        pcap_file: Path,
        intermediate_bin_base: Path,
        audio_format: str,
        sample_rate: int,
        channel_count: int,
        output_wav_file: Path,
    ) -> None:
        """
        Extracts the raw audio payload from each RTP stream in the PCAP,
        writes each stream to a temporary binary file, then uses ffmpeg to
        combine all streams into a single WAV file.

        1 stream  → mono WAV
        2 streams → stereo WAV (one direction per channel — most common case)
        3+ streams → grouped by source IP into a stereo WAV when there are exactly
                     two distinct sources (one side per source); otherwise all
                     streams are mixed down to stereo.
        """
        stream_ids = self.get_stream_ids(pcap_file)
        if not stream_ids:
            raise RuntimeError("No RTP packets in PCAP")

        # Raw bitstream formats (G.711, G.722) have no file header, so ffmpeg needs
        # to be told the sample rate and channel count explicitly via command-line flags.
        # Compressed formats like G.729 carry this information inside their own framing.
        format_needs_explicit_hints = audio_format in FORMATS_NEEDING_SAMPLE_RATE_HINT

        def build_ffmpeg_input_args(stream_audio_file: Path) -> list[str]:
            """Returns the ffmpeg input arguments for one raw audio stream file."""
            if format_needs_explicit_hints:
                return [
                    "-f", audio_format,
                    "-ar", str(sample_rate),
                    "-ac", str(channel_count),
                    "-i", str(stream_audio_file),
                ]
            # Compressed formats know their own parameters
            return ["-f", audio_format, "-i", str(stream_audio_file)]

        # Temporary per-stream audio files as (SSRC, path) pairs; tracked so we can
        # group them by source IP later and delete them at the end.
        stream_audio_files: list[tuple[str, Path]] = []

        try:
            # Extract the raw audio payload bytes for each RTP stream separately.
            # We separate streams by SSRC so we can merge them with correct stereo panning.
            for stream_id in stream_ids:
                # Give each stream's file a unique name based on the SSRC
                stream_audio_file = intermediate_bin_base.with_suffix(
                    f".{stream_id.replace('0x', '')}.bin"
                )

                # Ask tshark for the raw RTP payload bytes of this specific stream,
                # formatted as hex with colon separators (e.g. "d5:a3:7f:22:…")
                tshark_output = self.run_command([
                    "tshark", "-r", str(pcap_file),
                    "-Y", f"rtp && rtp.ssrc == {stream_id}",  # only this stream
                    "-T", "fields",
                    "-e", "rtp.payload",
                ])

                # Remove the colon separators tshark adds and decode the hex to raw bytes
                audio_hex_data = (
                    tshark_output.stdout
                    .decode(errors="replace")
                    .replace(":", "")
                    .replace("\n", "")
                    .strip()
                )

                if audio_hex_data:
                    stream_audio_file.write_bytes(bytes.fromhex(audio_hex_data))
                    stream_audio_files.append((stream_id, stream_audio_file))

            if not stream_audio_files:
                raise RuntimeError("No RTP payload found in PCAP")

            stream_count = len(stream_audio_files)
            audio_paths = [path for _, path in stream_audio_files]

            # Build the ffmpeg command depending on how many streams there are
            if stream_count == 1:
                # Single stream: straightforward mono conversion
                ffmpeg_command = (
                    ["ffmpeg", "-y"]
                    + build_ffmpeg_input_args(audio_paths[0])
                    + [str(output_wav_file)]
                )

            elif stream_count == 2:
                # Two streams (the typical case): merge into stereo.
                # amerge puts stream 0 on the left channel and stream 1 on the right.
                ffmpeg_command = (
                    ["ffmpeg", "-y"]
                    + build_ffmpeg_input_args(audio_paths[0])
                    + build_ffmpeg_input_args(audio_paths[1])
                    + ["-filter_complex", "amerge=inputs=2", "-ac", "2", str(output_wav_file)]
                )

            else:
                # More than two streams (e.g. a conference). Try to group them by
                # source IP so each side of the conversation lands on one stereo
                # channel; fall back to mixing everything together if that isn't
                # possible.
                ffmpeg_command = self.build_multistream_ffmpeg_command(
                    pcap_file, stream_audio_files, build_ffmpeg_input_args, output_wav_file
                )

            self.run_command(ffmpeg_command)

        finally:
            # Always delete the temporary per-stream binary files
            for _, stream_audio_file in stream_audio_files:
                stream_audio_file.unlink(missing_ok=True)

    def build_multistream_ffmpeg_command(
        self,
        pcap_file: Path,
        stream_audio_files: list[tuple[str, Path]],
        build_ffmpeg_input_args,
        output_wav_file: Path,
    ) -> list[str]:
        """
        Builds the ffmpeg command for a recording with three or more RTP streams.

        If the streams come from exactly two source IPs, each source is mixed onto
        its own stereo channel (left = source A, right = source B). Otherwise every
        stream is mixed down together into a stereo file.

        Note: we deliberately do NOT use amix's `normalize=0` option — it only exists
        in ffmpeg ≥ 4.4 and the target host runs 4.3. Instead we let amix average the
        inputs (its default) and multiply the result back up with `volume=<inputs>`,
        which restores the original loudness and works on every ffmpeg version.
        """
        stream_count = len(stream_audio_files)

        # Group the per-stream files by the IP that sent them
        ssrc_to_ip = self.get_stream_source_ips(pcap_file)
        groups: dict[str, list[Path]] = {}
        for ssrc, path in stream_audio_files:
            source_ip = ssrc_to_ip.get(ssrc, "unknown")
            groups.setdefault(source_ip, []).append(path)

        # Helper: emit the filter that mixes a group of inputs (referenced by their
        # input-stream labels) down to a single mono signal at full loudness.
        def mix_group_to_mono(input_labels: list[str], output_label: str) -> str:
            count = len(input_labels)
            joined = "".join(input_labels)
            if count == 1:
                # Nothing to mix — just relabel the single input
                return f"{joined}anull{output_label}"
            return (
                f"{joined}amix=inputs={count}:duration=longest,"
                f"volume={count}{output_label}"
            )

        if len(groups) == 2:
            # One source per stereo channel
            log.info(
                "PCAP has %d RTP streams from 2 source IPs — grouping into stereo "
                "(one source per channel)", stream_count
            )
            (left_paths, right_paths) = list(groups.values())

            ffmpeg_command = ["ffmpeg", "-y"]
            input_index = 0
            left_labels: list[str] = []
            right_labels: list[str] = []
            for path in left_paths:
                ffmpeg_command += build_ffmpeg_input_args(path)
                left_labels.append(f"[{input_index}:a]")
                input_index += 1
            for path in right_paths:
                ffmpeg_command += build_ffmpeg_input_args(path)
                right_labels.append(f"[{input_index}:a]")
                input_index += 1

            filters = [
                mix_group_to_mono(left_labels, "[left]"),
                mix_group_to_mono(right_labels, "[right]"),
                "[left][right]amerge=inputs=2[out]",
            ]
            ffmpeg_command += [
                "-filter_complex", ";".join(filters),
                "-map", "[out]",
                "-ac", "2",
                str(output_wav_file),
            ]
            return ffmpeg_command

        # 1 source, or 3+ sources: no clean stereo split — mix everything down.
        log.warning(
            "PCAP has %d RTP streams from %d source IP(s) — mixing all down to stereo",
            stream_count, len(groups),
        )
        ffmpeg_command = ["ffmpeg", "-y"]
        for _, path in stream_audio_files:
            ffmpeg_command += build_ffmpeg_input_args(path)
        ffmpeg_command += [
            "-filter_complex",
            f"amix=inputs={stream_count}:duration=longest,volume={stream_count}",
            "-ac", "2",
            str(output_wav_file),
        ]
        return ffmpeg_command


# --- Entry point -------------------------------------------------------------

def main():
    """Script entry point: reads config path from the command line and starts the daemon."""
    if len(sys.argv) < 2:
        print("Usage: recsyncd.py <config.yaml>", file=sys.stderr)
        sys.exit(1)

    config = load_settings(sys.argv[1])

    logging.getLogger().setLevel(
        getattr(logging, config.log_level.upper(), logging.INFO)
    )
    if logging.getLogger().level > logging.DEBUG:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    setup_file_logging(config.log_dir, config.log_retention_days)
    smtp_handler = setup_smtp_logging(config)

    try:
        # asyncio.run() starts the async event loop and runs the daemon until
        # Ctrl+C is pressed or the process receives a stop signal.
        asyncio.run(Daemon(config).run())
    except KeyboardInterrupt:
        pass   # clean exit — no error message needed for Ctrl+C
    finally:
        if smtp_handler:
            smtp_handler.flush()   # send any buffered alerts before exit


if __name__ == "__main__":
    main()

