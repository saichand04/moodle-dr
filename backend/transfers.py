"""
transfers.py — FastAPI router for transfer tracking endpoints.

Endpoints:
  GET  /api/transfers                  — session history (last N runs)
  GET  /api/transfers/stats            — aggregate stats
  GET  /api/transfers/partials         — live scan of .rsync-partial on target
  GET  /api/transfers/partials/history — DB-persisted partial file records
  POST /api/transfers/resume           — force a catch-up rsync now
  GET  /api/connectivity               — connectivity event log
  GET  /api/connectivity/state         — current watchdog state
  POST /api/connectivity/watchdog/start
  POST /api/connectivity/watchdog/stop
"""

import subprocess
from datetime import datetime

from fastapi import APIRouter

import state
import transfer_db

router = APIRouter(tags=["transfers"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh_cmd(*remote_args: str) -> list[str]:
    return [
        "ssh",
        "-i", state.SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=8",
        "-o", "BatchMode=yes",
        f"{state.AZURE_VM_USER}@{state.AZURE_VM_IP}",
        *remote_args,
    ]


def _human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _age_human(ts_str: str) -> str:
    """Return human-readable age from an ISO timestamp string."""
    try:
        dt = datetime.fromisoformat(ts_str)
        secs = int((datetime.now() - dt).total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            h, m = divmod(secs // 60, 60)
            return f"{h}h {m}m ago"
        return f"{secs // 86400}d ago"
    except Exception:
        return "unknown"


# ── Transfer session history ──────────────────────────────────────────────────

@router.get("/api/transfers")
async def get_transfers(limit: int = 60):
    sessions = await transfer_db.get_sessions(limit=limit)
    # Enrich with computed fields
    for s in sessions:
        if s.get("started_at"):
            s["started_at_age"] = _age_human(s["started_at"])
        if s.get("duration_secs") is not None:
            secs = int(s["duration_secs"])
            if secs < 60:
                s["duration_human"] = f"{secs}s"
            elif secs < 3600:
                s["duration_human"] = f"{secs // 60}m {secs % 60}s"
            else:
                h, rem = divmod(secs, 3600)
                s["duration_human"] = f"{h}h {rem // 60}m"
        else:
            s["duration_human"] = "—"
    return {"sessions": sessions, "count": len(sessions)}


@router.get("/api/transfers/stats")
async def get_transfer_stats():
    stats = await transfer_db.get_stats()
    if stats["total_bytes_sent"]:
        stats["total_bytes_sent_human"] = _human(stats["total_bytes_sent"])
    else:
        stats["total_bytes_sent_human"] = "0 B"
    if stats["total_sessions"] > 0:
        stats["success_rate_pct"] = round(
            stats["successful"] / stats["total_sessions"] * 100, 1
        )
    else:
        stats["success_rate_pct"] = 0.0
    return stats


# ── Partial files — live SSH scan ─────────────────────────────────────────────

@router.get("/api/transfers/partials")
async def get_partial_files():
    """
    SSH into target and list .rsync-partial/ contents.
    Returns live data — what is actually stuck right now.
    """
    target_dir = state.TARGET_PATH.rstrip("/")
    partial_dir = f"{target_dir}/.rsync-partial"

    # Check if directory exists
    exist_r = subprocess.run(
        _ssh_cmd(f"test -d {partial_dir} && echo __exists__ || echo __empty__"),
        capture_output=True, text=True, timeout=12,
    )

    if not exist_r.ok or "__exists__" not in exist_r.stdout:
        return {
            "ok": True,
            "files": [],
            "total_bytes": 0,
            "total_human": "0 B",
            "partial_dir": partial_dir,
            "message": "No partial files — .rsync-partial/ is empty or does not exist",
        }

    # List files: path \t size_bytes \t mtime_epoch
    list_r = subprocess.run(
        _ssh_cmd(
            f"find {partial_dir} -type f "
            r"-printf '%P\t%s\t%TY-%Tm-%TdT%TH:%TM:%.2TS\n' 2>/dev/null "
            f"| sort -t$'\\t' -k2 -rn"
        ),
        capture_output=True, text=True, timeout=20,
    )

    files = []
    total_bytes = 0

    for line in list_r.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        rel_path = parts[0]
        try:
            size = int(parts[1])
        except ValueError:
            size = 0
        mtime = parts[2] if len(parts) > 2 else None

        total_bytes += size
        files.append({
            "path":          rel_path,
            "full_path":     f"{partial_dir}/{rel_path}",
            "size_bytes":    size,
            "size_human":    _human(size),
            "modified_at":   mtime,
            "age_human":     _age_human(mtime) if mtime else "unknown",
        })

    # Persist to DB for history
    if files:
        session_id = state.rsync_job.get("session_id")
        await transfer_db.save_partial_snapshot(session_id, files)

    return {
        "ok":          True,
        "files":       files,
        "count":       len(files),
        "total_bytes": total_bytes,
        "total_human": _human(total_bytes),
        "partial_dir": partial_dir,
        "scanned_at":  datetime.now().isoformat(),
    }


@router.get("/api/transfers/partials/history")
async def get_partials_history():
    """Return DB-persisted partial file records (survive page reload)."""
    unresolved = await transfer_db.get_unresolved_partials()
    for f in unresolved:
        if f.get("captured_at"):
            f["age_human"] = _age_human(f["captured_at"])
        if f.get("size_bytes"):
            f["size_human"] = _human(f["size_bytes"])
    return {"files": unresolved, "count": len(unresolved)}


@router.post("/api/transfers/resume")
async def force_resume():
    """
    Force a catch-up rsync immediately, regardless of watchdog state.
    Useful when the connection is back but the watchdog hasn't fired yet.
    """
    if state.rsync_job.get("running"):
        return {"ok": False, "message": "rsync is already running"}

    if not state.trigger_rsync_fn:
        return {"ok": False, "message": "rsync trigger function not initialised"}

    import asyncio
    asyncio.ensure_future(state.trigger_rsync_fn("operator"))
    return {"ok": True, "message": "Catch-up rsync triggered"}


# ── Connectivity events ───────────────────────────────────────────────────────

@router.get("/api/connectivity")
async def get_connectivity(limit: int = 50):
    events = await transfer_db.get_connectivity_events(limit=limit)
    for e in events:
        if e.get("timestamp"):
            e["age_human"] = _age_human(e["timestamp"])
        if e.get("downtime_secs") is not None:
            s = int(e["downtime_secs"])
            e["downtime_human"] = (
                f"{s}s" if s < 60
                else f"{s // 60}m {s % 60}s" if s < 3600
                else f"{s // 3600}h {(s % 3600) // 60}m"
            )
    return {"events": events, "count": len(events)}


@router.get("/api/connectivity/state")
async def get_connectivity_state():
    if state.watchdog:
        return state.watchdog.state_dict
    return {
        "running":   False,
        "connected": None,
        "message":   "Watchdog not initialised",
    }


@router.post("/api/connectivity/watchdog/start")
async def watchdog_start():
    if not state.watchdog:
        return {"ok": False, "message": "Watchdog not initialised"}
    started = state.watchdog.start()
    return {"ok": True, "started": started,
            "message": "Watchdog started" if started else "Watchdog was already running"}


@router.post("/api/connectivity/watchdog/stop")
async def watchdog_stop():
    if not state.watchdog:
        return {"ok": False, "message": "Watchdog not initialised"}
    state.watchdog.stop()
    return {"ok": True, "message": "Watchdog stopped"}
