"""
transfer_db.py — async SQLite layer (aiosqlite).
Tracks:
  transfer_sessions   — one row per rsync run
  connectivity_events — drop / reconnect timeline
  partial_files       — snapshot of .rsync-partial on target after a failure
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiosqlite

DB_PATH = Path("/home/user/workspace/moodle-dr-dashboard-v11/data/transfers.db")

# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transfer_sessions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger             TEXT    NOT NULL DEFAULT 'operator',
    started_at          TEXT    NOT NULL,
    ended_at            TEXT,
    duration_secs       REAL,
    files_checked       INTEGER,
    files_transferred   INTEGER,
    bytes_sent          INTEGER,
    bytes_sent_human    TEXT,
    exit_code           INTEGER,
    status              TEXT    NOT NULL DEFAULT 'running',
    error_msg           TEXT,
    src_path            TEXT,
    dest_path           TEXT,
    target_ip           TEXT
);

CREATE TABLE IF NOT EXISTS connectivity_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type          TEXT    NOT NULL,
    timestamp           TEXT    NOT NULL,
    target_ip           TEXT,
    downtime_secs       REAL,
    triggered_catchup   INTEGER DEFAULT 0,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS partial_files (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at         TEXT    NOT NULL,
    session_id          INTEGER,
    file_path           TEXT    NOT NULL,
    size_bytes          INTEGER,
    size_human          TEXT,
    modified_at         TEXT,
    resolved            INTEGER DEFAULT 0
);
"""


async def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_SCHEMA)
        await db.commit()


# ── Transfer sessions ─────────────────────────────────────────────────────────

async def start_session(
    trigger: str,
    src: str,
    dest: str,
    target_ip: str,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO transfer_sessions
               (trigger, started_at, status, src_path, dest_path, target_ip)
               VALUES (?,?,?,?,?,?)""",
            (trigger, datetime.now().isoformat(), "running", src, dest, target_ip),
        )
        await db.commit()
        return cur.lastrowid


async def end_session(
    session_id: int,
    exit_code: int,
    files_checked: int      = 0,
    files_transferred: int  = 0,
    bytes_sent: int         = 0,
    bytes_sent_human: str   = "",
    error_msg: Optional[str] = None,
):
    ended = datetime.now().isoformat()
    status = "success" if exit_code == 0 else "error"

    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute(
            "SELECT started_at FROM transfer_sessions WHERE id=?", (session_id,)
        )).fetchone()
        duration = None
        if row:
            try:
                duration = (
                    datetime.fromisoformat(ended) - datetime.fromisoformat(row[0])
                ).total_seconds()
            except Exception:
                pass

        await db.execute(
            """UPDATE transfer_sessions SET
               ended_at=?, duration_secs=?, files_checked=?, files_transferred=?,
               bytes_sent=?, bytes_sent_human=?, exit_code=?, status=?, error_msg=?
               WHERE id=?""",
            (
                ended, duration, files_checked, files_transferred,
                bytes_sent, bytes_sent_human, exit_code, status, error_msg,
                session_id,
            ),
        )
        await db.commit()


async def get_sessions(limit: int = 60) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM transfer_sessions ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """SELECT
               COUNT(*),
               COUNT(CASE WHEN status='success'  THEN 1 END),
               COUNT(CASE WHEN status='error'    THEN 1 END),
               COUNT(CASE WHEN status='running'  THEN 1 END),
               COALESCE(SUM(bytes_sent),0),
               COALESCE(SUM(files_transferred),0)
               FROM transfer_sessions"""
        )
        total, success, errors, running, total_bytes, total_files = (
            await cur.fetchone()
        )
        # Last error
        cur2 = await db.execute(
            "SELECT started_at, error_msg, target_ip FROM transfer_sessions "
            "WHERE status='error' ORDER BY id DESC LIMIT 1"
        )
        last_err = await cur2.fetchone()
        # Last success
        cur3 = await db.execute(
            "SELECT ended_at, bytes_sent_human FROM transfer_sessions "
            "WHERE status='success' ORDER BY id DESC LIMIT 1"
        )
        last_ok = await cur3.fetchone()

        return {
            "total_sessions":      total or 0,
            "successful":          success or 0,
            "failed":              errors or 0,
            "running":             running or 0,
            "total_bytes_sent":    total_bytes or 0,
            "total_files_transferred": total_files or 0,
            "last_error_at":       last_err[0] if last_err else None,
            "last_error_msg":      last_err[1] if last_err else None,
            "last_success_at":     last_ok[0] if last_ok else None,
            "last_success_bytes":  last_ok[1] if last_ok else None,
        }


# ── Connectivity events ───────────────────────────────────────────────────────

async def log_connectivity(
    event_type: str,
    target_ip: str,
    downtime_secs: Optional[float] = None,
    triggered_catchup: bool = False,
    notes: Optional[str] = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO connectivity_events
               (event_type, timestamp, target_ip, downtime_secs, triggered_catchup, notes)
               VALUES (?,?,?,?,?,?)""",
            (
                event_type,
                datetime.now().isoformat(),
                target_ip,
                downtime_secs,
                1 if triggered_catchup else 0,
                notes,
            ),
        )
        await db.commit()


async def get_connectivity_events(limit: int = 50) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM connectivity_events ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cur.fetchall()]


# ── Partial files ─────────────────────────────────────────────────────────────

def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


async def save_partial_snapshot(session_id: Optional[int], files: list[dict]):
    if not files:
        return
    captured = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT INTO partial_files
               (captured_at, session_id, file_path, size_bytes, size_human, modified_at)
               VALUES (?,?,?,?,?,?)""",
            [
                (
                    captured,
                    session_id,
                    f["path"],
                    f["size_bytes"],
                    _human_bytes(f["size_bytes"]),
                    f.get("modified_at"),
                )
                for f in files
            ],
        )
        await db.commit()


async def mark_partials_resolved(session_id: Optional[int] = None):
    """Mark partial file records as resolved (after a successful catch-up rsync)."""
    async with aiosqlite.connect(DB_PATH) as db:
        if session_id:
            await db.execute(
                "UPDATE partial_files SET resolved=1 WHERE session_id=?",
                (session_id,),
            )
        else:
            await db.execute("UPDATE partial_files SET resolved=1 WHERE resolved=0")
        await db.commit()


async def get_unresolved_partials() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT DISTINCT file_path, MAX(size_bytes) as size_bytes,
               MAX(size_human) as size_human, MAX(captured_at) as captured_at,
               MAX(modified_at) as modified_at
               FROM partial_files
               WHERE resolved=0
               GROUP BY file_path
               ORDER BY size_bytes DESC"""
        )
        return [dict(r) for r in await cur.fetchall()]


# ── rsync output parsing ──────────────────────────────────────────────────────

def parse_rsync_stats(output_lines: list[str]) -> dict:
    """
    Parse --info=stats2 output lines.
    Returns: files_checked, files_transferred, bytes_sent, bytes_sent_human
    """
    text = "\n".join(output_lines)

    def extract(pattern: str, default: int = 0) -> int:
        m = re.search(pattern, text)
        if m:
            return int(m.group(1).replace(",", ""))
        return default

    files_checked     = extract(r"Number of files:\s+([\d,]+)")
    files_transferred = extract(r"Number of regular files transferred:\s+([\d,]+)")
    bytes_sent        = extract(r"Total bytes sent:\s+([\d,]+)")

    return {
        "files_checked":     files_checked,
        "files_transferred": files_transferred,
        "bytes_sent":        bytes_sent,
        "bytes_sent_human":  _human_bytes(bytes_sent) if bytes_sent else "0 B",
    }
