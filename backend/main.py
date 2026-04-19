"""
Moodle DR Dashboard — FastAPI Backend v11
Production-grade disaster recovery for Moodle LMS.
Supports: file replication (rsync+lsyncd) + MySQL GTID replication.

Startup sequence:
  1. Populate state.* config from environment
  2. Init SQLite DB (transfer_db + db_replication_db)
  3. Register state.trigger_rsync_fn callback
  4. Start connectivity watchdog
  5. Mount all routers + static frontend
"""

import asyncio
import json
import os
import re
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── Custom modules ─────────────────────────────────────────────────────────────
import state
import transfer_db
import db_replication_db
from setup import router as setup_router
from transfers import router as transfers_router
from db_replication import router as db_replication_router
from public_api import router as public_api_router
from integrations import router as integrations_router
from watchdog import ConnectivityWatchdog

# ── Runtime config from environment ───────────────────────────────────────────
state.AZURE_VM_IP   = os.environ.get("AZURE_VM_IP", "")
state.AZURE_VM_USER = os.getenv("AZURE_VM_USER", "moodlesync")
state.ADMIN_VM_USER = os.getenv("ADMIN_VM_USER", "admmoodle")
state.SSH_KEY_PATH  = os.getenv("SSH_KEY_PATH", "/root/.ssh/moodle_rsync_ed25519")
state.SOURCE_PATH   = os.getenv("SOURCE_PATH", "/var/www/Azure-MoodleData/")
state.TARGET_PATH   = os.getenv("TARGET_PATH", "/moodledata/")

LSYNCD_LOG    = os.getenv("LSYNCD_LOG",    "/var/log/lsyncd/lsyncd.log")
LSYNCD_STATUS = os.getenv("LSYNCD_STATUS", "/var/log/lsyncd/lsyncd-status.log")


def _sync_state_from_file():
    """Pull fresh values from setup-state.json if env vars are blank."""
    default_path = "/etc/moodle-dr/setup-state.json"
    p = Path(os.getenv("STATE_FILE", default_path))
    if not p.exists():
        return
    try:
        saved = json.loads(p.read_text())
        if not state.AZURE_VM_IP and saved.get("target_ip"):
            state.AZURE_VM_IP = saved["target_ip"]
        if saved.get("admin_ssh_user"):
            state.ADMIN_VM_USER = saved["admin_ssh_user"]
        if saved.get("sync_user"):
            state.AZURE_VM_USER = saved["sync_user"]
        if saved.get("ssh_key_path"):
            state.SSH_KEY_PATH = saved["ssh_key_path"]
        if saved.get("src_path"):
            state.SOURCE_PATH = saved["src_path"]
        if saved.get("target_path"):
            state.TARGET_PATH = saved["target_path"]
        # Load setup flags
        state.setup_mode = saved.get("setup_mode", "")
        state.welcome_seen = saved.get("welcome_seen", False)
        state.file_setup_complete = saved.get("file_setup_complete", False)
        state.db_setup_complete = saved.get("db_setup_complete", False)
    except Exception:
        pass


# ── Startup / shutdown ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Init file sync DB
    await transfer_db.init_db()

    # 2. Init DB replication tables
    await db_replication_db.init_db_replication()
    await db_replication_db.init_integrations_db()

    # 3. Load setup state from file
    _sync_state_from_file()
    # Load admin_ssh_user from DB config (highest priority)
    try:
        import db_replication_db as _drdb
        _db_cfg = _drdb.get_raw_db_config() or {}
        if _db_cfg.get("admin_ssh_user"):
            state.ADMIN_VM_USER = _db_cfg["admin_ssh_user"]
    except Exception:
        pass

    # 4. Register rsync trigger callback
    state.trigger_rsync_fn = _trigger_rsync_job

    # 5. Auto-detect seed completion: if replication is already healthy,
    #    mark seed job as done so the UI shows completed state on restart.
    if state.db_setup_complete and not state.db_seed_job.get("result"):
        try:
            from db_replication import _get_mysql_conn, _query_replica_status
            _db_cfg2 = (db_replication_db.get_raw_db_config() or {})
            if _db_cfg2:
                rep_status = _query_replica_status(_db_cfg2)
                io_ok  = rep_status.get("io_running") == "Yes"
                sql_ok = rep_status.get("sql_running") == "Yes"
                if io_ok and sql_ok:
                    from datetime import datetime
                    state.db_seed_job.update({
                        "running": False,
                        "phase": "done",
                        "progress": 100,
                        "result": "ok",
                        "error": "",
                        "output": ["[Auto-detected] Replication healthy — seed previously completed outside app."],
                        "ended_at": datetime.utcnow().isoformat(),
                    })
        except Exception:
            pass

    # 6. Start connectivity watchdog
    state.watchdog = ConnectivityWatchdog(check_interval=30)
    state.watchdog.start()

    yield

    # Shutdown
    if state.watchdog:
        state.watchdog.stop()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Moodle DR Sync API", version="11.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(setup_router)
app.include_router(transfers_router)
app.include_router(db_replication_router)
app.include_router(public_api_router)
app.include_router(integrations_router)


# ── Helpers ────────────────────────────────────────────────────────────────────

def run_cmd(cmd: list, timeout: int = 10) -> dict:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"ok": r.returncode == 0, "returncode": r.returncode,
                "stdout": r.stdout.strip(), "stderr": r.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"ok": False, "returncode": -1, "stdout": "", "stderr": "timed out"}
    except FileNotFoundError as e:
        return {"ok": False, "returncode": -1, "stdout": "", "stderr": str(e)}
    except Exception as e:
        return {"ok": False, "returncode": -1, "stdout": "", "stderr": str(e)}


def parse_df(raw: str) -> Optional[dict]:
    lines = raw.strip().splitlines()
    if len(lines) < 2:
        return None
    parts = lines[1].split()
    if len(parts) < 5:
        return None
    try:
        return {
            "path":     parts[5] if len(parts) > 5 else "",
            "used_gb":  int(parts[2].rstrip("G")),
            "avail_gb": int(parts[3].rstrip("G")),
            "pct":      int(parts[4].rstrip("%")),
        }
    except (ValueError, IndexError):
        return None


def _ssh_base() -> list:
    return [
        "ssh", "-i", state.SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=6",
        "-o", "BatchMode=yes",
        f"{state.AZURE_VM_USER}@{state.AZURE_VM_IP}",
    ]


# ── Config endpoint ────────────────────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    _sync_state_from_file()
    return {
        "azure_vm_ip":   state.AZURE_VM_IP,
        "azure_vm_user": state.AZURE_VM_USER,
        "source_path":   state.SOURCE_PATH,
        "target_path":   state.TARGET_PATH,
        "setup_mode":    state.setup_mode,
    }


# ── lsyncd endpoints ───────────────────────────────────────────────────────────

@app.get("/api/lsyncd/status")
def lsyncd_status():
    active_r  = run_cmd(["systemctl", "is-active",  "lsyncd"])
    enabled_r = run_cmd(["systemctl", "is-enabled", "lsyncd"])
    pid_r     = run_cmd(["pgrep", "-x", "lsyncd"])

    paused = False
    if pid_r["ok"] and pid_r["stdout"]:
        ps_r   = run_cmd(["ps", "-o", "stat=", "-p", pid_r["stdout"]])
        paused = "T" in ps_r.get("stdout", "")

    ts_r = run_cmd(["systemctl", "show", "lsyncd", "--property=ActiveEnterTimestamp"])
    timestamp = ts_r["stdout"].replace("ActiveEnterTimestamp=", "") if ts_r["ok"] else ""

    events = 0
    if os.path.exists(LSYNCD_STATUS):
        r = run_cmd(["grep", "-c", "Inotify", LSYNCD_STATUS])
        if r["ok"]:
            try:
                events = int(r["stdout"])
            except ValueError:
                pass

    status = active_r["stdout"]
    if status == "active" and paused:
        status = "paused"

    return {
        "status":       status,
        "enabled":      enabled_r["stdout"] == "enabled",
        "pid":          pid_r["stdout"] if pid_r["ok"] else None,
        "paused":       paused,
        "active_since": timestamp,
        "event_count":  events,
        "config_path":  "/etc/lsyncd/lsyncd.conf.lua",
    }


@app.get("/api/lsyncd/config")
def lsyncd_get_config():
    """Return the current generated lsyncd.conf.lua content for inspection."""
    conf_path = Path("/etc/lsyncd/lsyncd.conf.lua")
    exclude_path = Path("/etc/lsyncd/moodledata.exclude")
    cfg = conf_path.read_text() if conf_path.exists() else None
    excl = exclude_path.read_text() if exclude_path.exists() else None
    return {
        "config_path":    str(conf_path),
        "config_exists":  conf_path.exists(),
        "config_content": cfg,
        "exclude_path":   str(exclude_path),
        "exclude_content": excl,
    }


@app.post("/api/lsyncd/start")
def lsyncd_start():
    r = run_cmd(["systemctl", "start", "lsyncd"])
    if not r["ok"]:
        raise HTTPException(500, detail=r["stderr"] or "systemctl start lsyncd failed")
    return {"ok": True, "message": "lsyncd started"}


@app.post("/api/lsyncd/stop")
def lsyncd_stop():
    r = run_cmd(["systemctl", "stop", "lsyncd"])
    if not r["ok"]:
        raise HTTPException(500, detail=r["stderr"] or "systemctl stop lsyncd failed")
    return {"ok": True, "message": "lsyncd stopped"}


@app.post("/api/lsyncd/pause")
def lsyncd_pause():
    pid_r = run_cmd(["pgrep", "-x", "lsyncd"])
    if not pid_r["ok"] or not pid_r["stdout"]:
        raise HTTPException(404, detail="lsyncd is not running")
    r = run_cmd(["kill", "-SIGSTOP", pid_r["stdout"]])
    if not r["ok"]:
        raise HTTPException(500, detail=r["stderr"])
    return {"ok": True, "message": f"lsyncd paused (SIGSTOP → PID {pid_r['stdout']})"}


@app.post("/api/lsyncd/resume")
def lsyncd_resume():
    pid_r = run_cmd(["pgrep", "-x", "lsyncd"])
    if not pid_r["ok"] or not pid_r["stdout"]:
        raise HTTPException(404, detail="lsyncd is not running")
    r = run_cmd(["kill", "-SIGCONT", pid_r["stdout"]])
    if not r["ok"]:
        raise HTTPException(500, detail=r["stderr"])
    return {"ok": True, "message": f"lsyncd resumed (SIGCONT → PID {pid_r['stdout']})"}


# ── Log endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/logs")
def get_logs(lines: int = 100):
    if not os.path.exists(LSYNCD_LOG):
        return {"lines": [], "error": f"log not found: {LSYNCD_LOG}"}
    r = run_cmd(["tail", "-n", str(min(lines, 500)), LSYNCD_LOG])
    return {"lines": r["stdout"].splitlines() if r["ok"] else [], "path": LSYNCD_LOG}


@app.get("/api/logs/stream")
async def stream_logs():
    async def gen():
        if not os.path.exists(LSYNCD_LOG):
            yield f"data: {json.dumps({'error': 'log not found'})}\n\n"
            return
        proc = await asyncio.create_subprocess_exec(
            "tail", "-f", "-n", "50", LSYNCD_LOG,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            while True:
                try:
                    line = await asyncio.wait_for(proc.stdout.readline(), timeout=20)
                    if line:
                        yield f"data: {json.dumps({'line': line.decode().rstrip()})}\n\n"
                    else:
                        break
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'ping': True})}\n\n"
        finally:
            proc.kill()
            await proc.wait()

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ── rsync trigger ──────────────────────────────────────────────────────────────

@app.get("/api/rsync/status")
def get_rsync_status():
    return state.rsync_job


@app.post("/api/rsync/trigger")
async def trigger_rsync_endpoint(background_tasks: BackgroundTasks):
    _sync_state_from_file()
    if state.rsync_job["running"]:
        raise HTTPException(409, detail="rsync is already running")
    background_tasks.add_task(_trigger_rsync_job, "operator")
    return {"ok": True, "message": "rsync job dispatched"}


async def _trigger_rsync_job(trigger: str = "operator") -> bool:
    if state.rsync_job["running"]:
        return False

    session_id = await transfer_db.start_session(
        trigger=trigger,
        src=state.SOURCE_PATH,
        dest=state.TARGET_PATH,
        target_ip=state.AZURE_VM_IP,
    )

    state.rsync_job.update({
        "running":    True,
        "progress":   0,
        "speed":      "",
        "started_at": datetime.now().isoformat(),
        "output":     [],
        "last_result": None,
        "session_id": session_id,
        "trigger":    trigger,
        "files_checked":     0,
        "files_transferred": 0,
        "bytes_sent":        0,
        "bytes_sent_human":  "",
    })

    cmd = [
        "rsync", "-aHAXx",
        "--numeric-ids",
        "--delete",
        "--partial",
        "--partial-dir=.rsync-partial",
        "--delete-delay",
        "--info=progress2,stats2",
        "--human-readable",
        "--timeout=600",
        "-e",
        (f"ssh -i {state.SSH_KEY_PATH}"
         " -o StrictHostKeyChecking=accept-new"
         " -o BatchMode=yes"),
        state.SOURCE_PATH,
        f"{state.AZURE_VM_USER}@{state.AZURE_VM_IP}:{state.TARGET_PATH}",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    async for raw in proc.stdout:
        line = raw.decode().strip()
        if not line:
            continue
        state.rsync_job["output"].append(line)
        if len(state.rsync_job["output"]) > 400:
            state.rsync_job["output"] = state.rsync_job["output"][-400:]

        if "%" in line:
            for part in line.split():
                if part.endswith("%"):
                    try:
                        state.rsync_job["progress"] = int(part.rstrip("%"))
                    except ValueError:
                        pass
                if "/s" in part and not part.startswith("-"):
                    state.rsync_job["speed"] = part

    await proc.wait()
    exit_code = proc.returncode
    success   = exit_code == 0

    stats = transfer_db.parse_rsync_stats(state.rsync_job["output"])

    state.rsync_job.update({
        "running":           False,
        "last_run":          datetime.now().isoformat(),
        "returncode":        exit_code,
        "last_result":       "success" if success else "error",
        "files_checked":     stats["files_checked"],
        "files_transferred": stats["files_transferred"],
        "bytes_sent":        stats["bytes_sent"],
        "bytes_sent_human":  stats["bytes_sent_human"],
    })
    if success:
        state.rsync_job["progress"] = 100

    await transfer_db.end_session(
        session_id=session_id,
        exit_code=exit_code,
        files_checked=stats["files_checked"],
        files_transferred=stats["files_transferred"],
        bytes_sent=stats["bytes_sent"],
        bytes_sent_human=stats["bytes_sent_human"],
        error_msg=None if success else f"rsync exited with code {exit_code}",
    )

    if success:
        await transfer_db.mark_partials_resolved()

    return success


# ── Rsync dry-run validation ("are source and target in sync?") ────────────────

@app.get("/api/rsync/validate")
def get_validate_status():
    """Return current state of the dry-run validation job."""
    return state.rsync_validate_job


@app.post("/api/rsync/validate")
async def trigger_validate(background_tasks: BackgroundTasks):
    """Start a dry-run rsync --stats pass to measure source ↔ target drift."""
    _sync_state_from_file()
    if state.rsync_validate_job["running"]:
        raise HTTPException(409, detail="Validation already running")
    background_tasks.add_task(_run_rsync_validate)
    return {"ok": True, "message": "Validation started"}


async def _run_rsync_validate():
    """
    Runs:  rsync -aHAXxn --delete --stats
               -e "ssh -i <key> -o BatchMode=yes -o IdentitiesOnly=yes"
               <src> <user>@<ip>:<tgt>
    Parses the --stats output and stores result in state.rsync_validate_job.
    """
    _sync_state_from_file()

    state.rsync_validate_job.update({
        "running":             True,
        "started_at":         datetime.now().isoformat(),
        "finished_at":        None,
        "last_result":        None,
        "returncode":         None,
        "files_total":        0,
        "files_transferred":  0,
        "files_deleted":      0,
        "bytes_would_send":   0,
        "bytes_would_send_human": "",
        "total_size_human":   "",
        "output":             [],
        "error":              "",
    })

    cmd = [
        "rsync",
        "-aHAXxn",                         # -n = dry run
        "--delete",
        "--stats",
        "--human-readable",
        "--timeout=600",
        "-e",
        (
            f"ssh -i {state.SSH_KEY_PATH}"
            " -o BatchMode=yes"
            " -o IdentitiesOnly=yes"
            " -o StrictHostKeyChecking=accept-new"
        ),
        state.SOURCE_PATH,
        f"{state.AZURE_VM_USER}@{state.AZURE_VM_IP}:{state.TARGET_PATH}",
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        async for raw in proc.stdout:
            line = raw.decode(errors="replace").strip()
            if not line:
                continue
            state.rsync_validate_job["output"].append(line)
            if len(state.rsync_validate_job["output"]) > 200:
                state.rsync_validate_job["output"] = \
                    state.rsync_validate_job["output"][-200:]

        await proc.wait()
        rc = proc.returncode

    except Exception as exc:
        state.rsync_validate_job.update({
            "running":     False,
            "finished_at": datetime.now().isoformat(),
            "last_result": "error",
            "error":       str(exc),
        })
        return

    # ── Parse --stats output ──────────────────────────────────────────
    output_text = "\n".join(state.rsync_validate_job["output"])

    def _extract_int(pattern: str) -> int:
        import re
        m = re.search(pattern, output_text)
        return int(m.group(1).replace(",", "")) if m else 0

    def _extract_str(pattern: str) -> str:
        import re
        m = re.search(pattern, output_text)
        return m.group(1).strip() if m else ""

    files_total       = _extract_int(r"Number of files:\s+([\d,]+)")
    files_transferred = _extract_int(r"Number of regular files transferred:\s+([\d,]+)")
    # Count deleted entries from output lines containing "deleting "
    files_deleted     = sum(1 for l in state.rsync_validate_job["output"]
                            if l.startswith("deleting "))
    bytes_would_send  = _extract_int(r"Total transferred file size:\s+([\d,]+)")
    total_size_human  = _extract_str(r"Total file size:\s+([\d,.]+ [KMGTP]?[Bb])")

    # Determine sync status
    if rc not in (0, 24):   # 24 = vanished source files, still OK
        result = "error"
        error_msg = f"rsync exited with code {rc}"
    elif files_transferred == 0 and files_deleted == 0:
        result = "in_sync"
        error_msg = ""
    else:
        result = "out_of_sync"
        error_msg = ""

    state.rsync_validate_job.update({
        "running":             False,
        "finished_at":        datetime.now().isoformat(),
        "last_result":        result,
        "returncode":         rc,
        "files_total":        files_total,
        "files_transferred":  files_transferred,
        "files_deleted":      files_deleted,
        "bytes_would_send":   bytes_would_send,
        "bytes_would_send_human": _human_bytes(bytes_would_send),
        "total_size_human":   total_size_human,
        "error":              error_msg,
    })


def _human_bytes(n: int) -> str:
    """Convert bytes to human-readable string."""
    if n == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ── Health endpoints ───────────────────────────────────────────────────────────

@app.get("/api/health/ssh")
def health_ssh():
    _sync_state_from_file()
    r = run_cmd([*_ssh_base(), "echo ok && hostname && id"], timeout=10)
    return {"connected": r["ok"], "output": r["stdout"],
            "error": r["stderr"] if not r["ok"] else None}


@app.get("/api/health/disk")
def health_disk():
    _sync_state_from_file()
    src_r = run_cmd(["df", "-BG", state.SOURCE_PATH])
    tgt_r = run_cmd([*_ssh_base(), f"df -BG {state.TARGET_PATH}"], timeout=12)
    return {
        "source":       parse_df(src_r["stdout"]) if src_r["ok"] else None,
        "target":       parse_df(tgt_r["stdout"]) if tgt_r["ok"] else None,
        "target_error": tgt_r["stderr"] if not tgt_r["ok"] else None,
    }


@app.get("/api/status")
def full_status():
    _sync_state_from_file()
    return {
        "lsyncd": lsyncd_status(),
        "ssh":    health_ssh(),
        "disk":   health_disk(),
        "rsync":  state.rsync_job,
        "db_replication": state.db_replication_status,
        "connectivity": state.watchdog.state_dict if state.watchdog else {},
        "ts":     datetime.now().isoformat(),
    }


# ── Frontend static files ──────────────────────────────────────────────────────

_frontend = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend):
    app.mount("/", StaticFiles(directory=_frontend, html=True), name="frontend")
