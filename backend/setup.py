"""
setup.py — Setup Wizard API (v11 — file + DB DR)

New in v11:
  GET  /api/setup/mode            ← NEW: get setup mode + completion flags
  POST /api/setup/mode            ← NEW: set setup mode, welcome_seen
  GET  /api/setup/state
  POST /api/setup/state
  GET  /api/setup/prereqs
  POST /api/setup/prereqs/install
  POST /api/setup/auto-configure
  POST /api/setup/bulk-sync/start
  GET  /api/setup/bulk-sync/status
  POST /api/setup/verify
  POST /api/setup/config          ← NEW: save file+DB config together
  GET  /api/setup/config          ← NEW: get all config
  POST /api/setup/reset
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException

import state

router = APIRouter(prefix="/api/setup", tags=["setup"])

# ── Setup state persistence ────────────────────────────────────────────────────

STATE_FILE = Path("/home/user/workspace/moodle-dr-dashboard-v11/data/setup-state.json")

_DEFAULT = {
    "completed_screens": [],
    "os": "ubuntu",
    # Source (this server)
    "src_path": "/var/www/Azure-MoodleData/",
    "sync_user": "moodlesync",
    "ssh_key_type": "ed25519",
    "ssh_key_path": "/root/.ssh/moodle_rsync_ed25519",
    # Target (Azure VM)
    "target_ip": "",
    "target_admin_user": "azureuser",
    "target_admin_key_path": "",
    "target_path": "/moodledata/",
    "target_ssh_port": 22,
    # Excludes
    "excludes": ["cache/", "localcache/", "sessions/", "temp/", "*.tmp"],
    # Sync tuning
    "sync_delay": 10,
    # Status flags
    "prereqs_ok": False,
    "target_reachable": False,
    "target_setup_done": False,
    "key_generated": False,
    "ssh_tested": False,
    "config_written": False,
    "bulk_sync_done": False,
    "verified": False,
    # v11: setup mode
    "setup_mode": "",
    "welcome_seen": False,
    "file_setup_complete": False,
    "db_setup_complete": False,
    # DB config stored in db_replication_config SQLite table, but mirror here for reference
    "db_source_host": "127.0.0.1",
    "db_source_port": 3306,
    "db_replica_host": "",
    "db_replica_port": 3306,
    "db_moodle_db_name": "moodle",
    "db_seed_method": "mysqldump",
}


def load_state() -> dict:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            return {**_DEFAULT, **json.loads(STATE_FILE.read_text())}
        except Exception:
            pass
    return dict(_DEFAULT)


def save_state(updates: dict) -> dict:
    s = load_state()
    s.update(updates)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(s, indent=2))
    return s


def _run(cmd: list, timeout: int = 15, input_text: str = None) -> dict:
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout,
            input=input_text,
        )
        return {"ok": r.returncode == 0, "rc": r.returncode,
                "out": r.stdout.strip(), "err": r.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {"ok": False, "rc": -1, "out": "", "err": "command timed out"}
    except FileNotFoundError as e:
        return {"ok": False, "rc": -1, "out": "", "err": str(e)}
    except Exception as e:
        return {"ok": False, "rc": -1, "out": "", "err": str(e)}


def _admin_ssh(st: dict, *remote_args: str, timeout: int = 20) -> list:
    """Build admin SSH command list."""
    base = [
        "ssh",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"ConnectTimeout={min(timeout // 2, 10)}",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
    ]
    if st.get("target_admin_key_path"):
        base += ["-i", st["target_admin_key_path"]]
    base += [
        "-p", str(st.get("target_ssh_port", 22)),
        f"{st['target_admin_user']}@{st['target_ip']}",
        *remote_args,
    ]
    return base


def _human(n: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


# ── Setup Mode endpoints (NEW in v11) ─────────────────────────────────────────

@router.get("/mode")
def get_setup_mode():
    st = load_state()
    # Sync to state module
    state.setup_mode = st.get("setup_mode", "")
    state.welcome_seen = st.get("welcome_seen", False)
    state.file_setup_complete = st.get("file_setup_complete", False)
    state.db_setup_complete = st.get("db_setup_complete", False)
    return {
        "setup_mode": state.setup_mode,
        "welcome_seen": state.welcome_seen,
        "file_setup_complete": state.file_setup_complete,
        "db_setup_complete": state.db_setup_complete,
    }


@router.post("/mode")
def set_setup_mode(body: dict):
    updates = {}
    if "setup_mode" in body:
        updates["setup_mode"] = body["setup_mode"]
        state.setup_mode = body["setup_mode"]
    if "welcome_seen" in body:
        updates["welcome_seen"] = body["welcome_seen"]
        state.welcome_seen = body["welcome_seen"]
    if "file_setup_complete" in body:
        updates["file_setup_complete"] = body["file_setup_complete"]
        state.file_setup_complete = body["file_setup_complete"]
    if "db_setup_complete" in body:
        updates["db_setup_complete"] = body["db_setup_complete"]
        state.db_setup_complete = body["db_setup_complete"]
    saved = save_state(updates)
    return {"ok": True, "state": saved}


@router.get("/config")
def get_setup_config():
    st = load_state()
    # Also load DB config
    try:
        import db_replication_db
        db_cfg = db_replication_db.get_db_config()
    except Exception:
        db_cfg = {}
    return {"file": st, "db": db_cfg}


@router.post("/config")
def save_setup_config(body: dict):
    """Save unified file + DB config from wizard step 2."""
    file_updates = {}
    if "target_ip" in body:
        file_updates["target_ip"] = body["target_ip"]
    if "ssh_key_path" in body:
        file_updates["ssh_key_path"] = body["ssh_key_path"]
    if "sync_user" in body:
        file_updates["sync_user"] = body["sync_user"]
    if "src_path" in body:
        file_updates["src_path"] = body["src_path"]
    if "target_path" in body:
        file_updates["target_path"] = body["target_path"]

    # DB config
    db_fields = [
        "source_host", "source_port", "replica_host", "replica_port",
        "repl_user", "repl_password", "source_db_user", "source_db_password",
        "replica_db_user", "replica_db_password", "moodle_db_name",
        "ssl_ca_path", "ssl_cert_path", "ssl_key_path", "seed_method",
    ]
    db_cfg = {k: body[k] for k in db_fields if k in body}

    saved = save_state(file_updates)

    # Update state module
    if file_updates.get("target_ip"):
        state.AZURE_VM_IP = file_updates["target_ip"]
    if file_updates.get("sync_user"):
        state.AZURE_VM_USER = file_updates["sync_user"]
    if file_updates.get("ssh_key_path"):
        state.SSH_KEY_PATH = file_updates["ssh_key_path"]
    if file_updates.get("src_path"):
        state.SOURCE_PATH = file_updates["src_path"]
    if file_updates.get("target_path"):
        state.TARGET_PATH = file_updates["target_path"]

    # Save DB config
    if db_cfg:
        try:
            import db_replication_db
            db_replication_db.save_db_config(db_cfg)
        except Exception as e:
            return {"ok": False, "error": f"DB config save failed: {e}"}

    return {"ok": True, "saved": saved}


# ── State endpoints ────────────────────────────────────────────────────────────

@router.get("/state")
def get_state_endpoint():
    return load_state()


@router.post("/state")
def post_state_endpoint(body: dict):
    return save_state(body)


# ── Prerequisites ──────────────────────────────────────────────────────────────

def _chk_binary(name: str) -> dict:
    p = shutil.which(name)
    if p:
        v = _run([name, "--version"])
        return {"pass": True, "detail": v["out"].splitlines()[0] if v["out"] else "found"}
    return {"pass": False, "detail": "not installed"}


def _chk_os() -> dict:
    r = _run(["lsb_release", "-ds"])
    if r["ok"]:
        return {"pass": True, "detail": r["out"].strip('"')}
    try:
        for line in Path("/etc/os-release").read_text().splitlines():
            if line.startswith("PRETTY_NAME="):
                return {"pass": True, "detail": line.split("=", 1)[1].strip('"')}
    except Exception:
        pass
    return {"pass": False, "detail": "Could not detect OS"}


def _chk_disk(path: str = "/var") -> dict:
    r = _run(["df", "-BG", path])
    if r["ok"]:
        parts = r["out"].splitlines()
        if len(parts) >= 2:
            cols = parts[1].split()
            if len(cols) >= 4:
                try:
                    gb = int(cols[3].rstrip("G"))
                    return {"pass": gb >= 10, "detail": f"{gb} GB free on {path}"}
                except ValueError:
                    pass
    return {"pass": False, "detail": "Could not check disk space"}


def _chk_inotify() -> dict:
    try:
        v = int(Path("/proc/sys/fs/inotify/max_user_watches").read_text().strip())
        return {"pass": v >= 8192, "detail": f"max_user_watches = {v:,}"}
    except Exception:
        return {"pass": False, "detail": "Could not read inotify limit"}


def _chk_mysql_connector() -> dict:
    try:
        import mysql.connector
        return {"pass": True, "detail": f"mysql-connector-python installed"}
    except ImportError:
        return {"pass": False, "detail": "mysql-connector-python not installed"}


def _chk_mysql_running() -> dict:
    r = _run(["systemctl", "is-active", "mysql"])
    if r["ok"] and r["out"] == "active":
        return {"pass": True, "detail": "MySQL is active"}
    r2 = _run(["systemctl", "is-active", "mariadb"])
    if r2["ok"] and r2["out"] == "active":
        return {"pass": True, "detail": "MariaDB is active"}
    return {"pass": False, "detail": "MySQL/MariaDB not running"}


def _chk_pt_heartbeat() -> dict:
    p = shutil.which("pt-heartbeat")
    return {"pass": bool(p), "detail": "found" if p else "pt-heartbeat not installed (percona-toolkit)"}


def _chk_pt_checksum() -> dict:
    p = shutil.which("pt-table-checksum")
    return {"pass": bool(p), "detail": "found" if p else "pt-table-checksum not installed (percona-toolkit)"}


FILE_PREREQS = [
    {"id": "os", "name": "Operating System", "critical": True, "fn": _chk_os},
    {"id": "rsync", "name": "rsync", "critical": True, "fn": lambda: _chk_binary("rsync")},
    {"id": "lsyncd", "name": "lsyncd", "critical": True, "fn": lambda: _chk_binary("lsyncd")},
    {"id": "ssh", "name": "openssh-client", "critical": True, "fn": lambda: _chk_binary("ssh")},
    {"id": "disk", "name": "Source disk (≥10 GB free)", "critical": True, "fn": _chk_disk},
    {"id": "inotify", "name": "inotify watches (≥8192)", "critical": False, "fn": _chk_inotify},
]

DB_PREREQS = [
    {"id": "mysql_connector", "name": "mysql-connector-python", "critical": True, "fn": _chk_mysql_connector},
    {"id": "mysql_running", "name": "MySQL 8.x running on source", "critical": True, "fn": _chk_mysql_running},
    {"id": "pt_heartbeat", "name": "pt-heartbeat (percona-toolkit)", "critical": False, "fn": _chk_pt_heartbeat},
    {"id": "pt_checksum", "name": "pt-table-checksum (percona-toolkit)", "critical": False, "fn": _chk_pt_checksum},
]

INSTALL_HINTS = {
    "rsync": "apt-get install -y rsync",
    "lsyncd": "apt-get install -y lsyncd",
    "ssh": "apt-get install -y openssh-client",
    "mysql_connector": "pip install mysql-connector-python",
    "pt_heartbeat": "apt-get install -y percona-toolkit",
    "pt_checksum": "apt-get install -y percona-toolkit",
}


@router.get("/prereqs")
def get_prereqs():
    st = load_state()
    mode = st.get("setup_mode", "both")
    results = []

    checks = []
    if mode in ("file_only", "both", ""):
        checks.extend(FILE_PREREQS)
    if mode in ("db_only", "both"):
        checks.extend(DB_PREREQS)

    for p in checks:
        try:
            r = p["fn"]()
        except Exception as e:
            r = {"pass": False, "detail": str(e)}
        results.append({
            "id": p["id"],
            "name": p["name"],
            "critical": p["critical"],
            "pass": r["pass"],
            "detail": r["detail"],
            "install_hint": INSTALL_HINTS.get(p["id"]),
        })

    all_critical_ok = all(p["pass"] for p in results if p["critical"])
    save_state({"prereqs_ok": all_critical_ok})
    return {"checks": results, "all_critical_ok": all_critical_ok}


@router.post("/prereqs/install")
def install_prereqs(body: dict):
    ids = body.get("ids", [])
    results = []
    for id_ in ids:
        hint = INSTALL_HINTS.get(id_)
        if not hint:
            results.append({"id": id_, "ok": False, "error": "No install command known"})
            continue
        r = _run(["bash", "-c", hint], timeout=60)
        results.append({"id": id_, "ok": r["ok"], "output": r["out"] or r["err"]})
    return {"results": results}


# ── Auto-configure ────────────────────────────────────────────────────────────

_bulk_sync_state: dict = {
    "running": False,
    "progress": 0,
    "speed": "",
    "started_at": None,
    "ended_at": None,
    "result": None,
    "output": [],
    "files_transferred": 0,
    "bytes_sent_human": "",
}


@router.post("/auto-configure")
async def auto_configure(background_tasks: BackgroundTasks):
    """Orchestrate all file sync setup steps."""
    st = load_state()
    steps = []

    # Generate SSH key if needed
    key_path = st.get("ssh_key_path", "/root/.ssh/moodle_rsync_ed25519")
    if not Path(key_path).exists():
        r = _run(["ssh-keygen", "-t", "ed25519", "-f", key_path, "-N", ""], timeout=20)
        steps.append({"step": "Generate SSH key", "ok": r["ok"], "detail": r["out"] or r["err"]})
    else:
        steps.append({"step": "SSH key exists", "ok": True, "detail": key_path})

    # Test SSH connectivity
    target_ip = st.get("target_ip", "")
    if target_ip:
        r = _run([
            "ssh", "-i", key_path, "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=6", "-o", "StrictHostKeyChecking=accept-new",
            f"{st.get('sync_user', 'moodlesync')}@{target_ip}", "echo __ok__",
        ], timeout=12)
        steps.append({"step": "SSH connectivity test", "ok": r["ok"],
                      "detail": "Connected" if r["ok"] else r["err"]})
    else:
        steps.append({"step": "SSH connectivity test", "ok": False, "detail": "No target IP configured"})

    # Write lsyncd config
    src = st.get("src_path", "/var/www/Azure-MoodleData/")
    tgt = st.get("target_path", "/moodledata/")
    user = st.get("sync_user", "moodlesync")
    sync_delay = st.get("sync_delay", 10)
    excludes = st.get("excludes", ["cache/", "localcache/", "sessions/", "temp/", "*.tmp"])

    # Build exclude file path
    exclude_file = "/etc/lsyncd/moodledata.exclude"
    exclude_file_content = "\n".join(excludes)

    # Write exclude file
    try:
        Path(exclude_file).parent.mkdir(parents=True, exist_ok=True)
        Path(exclude_file).write_text(exclude_file_content + "\n")
    except Exception:
        pass  # non-fatal

    lsyncd_cfg = f"""-- /etc/lsyncd/lsyncd.conf.lua
-- Generated by Moodle DR Dashboard v11
-- DO NOT EDIT MANUALLY — regenerate via Dashboard > File Settings > Re-run Configure

settings {{
    logfile        = "/var/log/lsyncd/lsyncd.log",
    statusFile     = "/var/log/lsyncd/lsyncd-status.log",
    statusInterval = 20,
    nodaemon       = false,
    insist         = true,
    maxProcesses   = 1,
}}

sync {{
    default.rsyncssh,
    source      = "{src}",
    host        = "{target_ip}",
    targetdir   = "{tgt}",
    delay       = {sync_delay},
    delete      = true,
    excludeFrom = "{exclude_file}",
    rsync = {{
        archive  = true,
        compress = false,
        owner    = true,
        group    = true,
        perms    = true,
        _extra   = {{
            "--numeric-ids",
            "--partial",
            "--partial-dir=.rsync-partial",
            "--timeout=600",
        }},
    }},
    ssh = {{
        port         = 22,
        user         = "{user}",
        identityFile = "{key_path}",
        options      = {{
            StrictHostKeyChecking = "accept-new",
            BatchMode             = "yes",
            IdentitiesOnly        = "yes",
            ServerAliveInterval   = "30",
            ServerAliveCountMax   = "6",
        }},
    }},
}}
"""
    lsyncd_conf_path = Path("/etc/lsyncd/lsyncd.conf.lua")
    try:
        lsyncd_conf_path.parent.mkdir(parents=True, exist_ok=True)
        lsyncd_conf_path.write_text(lsyncd_cfg)
        r2 = _run(["mkdir", "-p", "/var/log/lsyncd"])
        steps.append({"step": "Write lsyncd config", "ok": True, "detail": str(lsyncd_conf_path)})
    except Exception as e:
        steps.append({"step": "Write lsyncd config", "ok": False, "detail": str(e)})

    # Enable lsyncd
    r3 = _run(["systemctl", "enable", "lsyncd"])
    steps.append({"step": "Enable lsyncd service", "ok": r3["ok"], "detail": r3["out"] or r3["err"]})

    ok = all(s["ok"] for s in steps)
    save_state({"config_written": ok})
    return {"ok": ok, "steps": steps}


# ── Bulk sync ──────────────────────────────────────────────────────────────────

async def _run_bulk_sync():
    global _bulk_sync_state
    st = load_state()
    _bulk_sync_state.update({
        "running": True, "progress": 0, "output": [], "result": None,
        "started_at": datetime.now().isoformat(), "ended_at": None,
        "files_transferred": 0, "bytes_sent_human": "",
    })

    key = st.get("ssh_key_path", "/root/.ssh/moodle_rsync_ed25519")
    src = st.get("src_path", "/var/www/Azure-MoodleData/")
    tgt = st.get("target_path", "/moodledata/")
    user = st.get("sync_user", "moodlesync")
    ip = st.get("target_ip", "")

    if not ip:
        _bulk_sync_state.update({"running": False, "result": "error",
                                  "ended_at": datetime.now().isoformat()})
        return

    cmd = [
        "rsync", "-aHAXx", "--numeric-ids", "--delete", "--partial",
        "--partial-dir=.rsync-partial", "--delete-delay",
        "--info=progress2,stats2", "--human-readable", "--timeout=600",
        "-e", f"ssh -i {key} -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
        src, f"{user}@{ip}:{tgt}",
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
        _bulk_sync_state["output"].append(line)
        if len(_bulk_sync_state["output"]) > 200:
            _bulk_sync_state["output"] = _bulk_sync_state["output"][-200:]
        if "%" in line:
            for part in line.split():
                if part.endswith("%"):
                    try:
                        _bulk_sync_state["progress"] = int(part.rstrip("%"))
                    except ValueError:
                        pass
                if "/s" in part:
                    _bulk_sync_state["speed"] = part

    await proc.wait()
    ok = proc.returncode == 0
    if ok:
        _bulk_sync_state["progress"] = 100
    _bulk_sync_state.update({
        "running": False,
        "result": "success" if ok else "error",
        "ended_at": datetime.now().isoformat(),
    })
    if ok:
        save_state({"bulk_sync_done": True, "file_setup_complete": True})
        state.file_setup_complete = True


@router.post("/bulk-sync/start")
async def bulk_sync_start(background_tasks: BackgroundTasks):
    if _bulk_sync_state.get("running"):
        return {"ok": False, "message": "Bulk sync already running"}
    background_tasks.add_task(_run_bulk_sync)
    return {"ok": True, "message": "Bulk sync started"}


@router.get("/bulk-sync/status")
def bulk_sync_status():
    return _bulk_sync_state


# ── Verify ────────────────────────────────────────────────────────────────────

@router.post("/verify")
def verify_sync():
    st = load_state()
    key = st.get("ssh_key_path", "/root/.ssh/moodle_rsync_ed25519")
    src = st.get("src_path", "/var/www/Azure-MoodleData/")
    tgt = st.get("target_path", "/moodledata/")
    user = st.get("sync_user", "moodlesync")
    ip = st.get("target_ip", "")

    if not ip:
        return {"ok": False, "error": "No target IP configured"}

    test_id = uuid.uuid4().hex[:8]
    test_file = f".moodle-dr-verify-{test_id}.txt"
    src_full = os.path.join(src, test_file)
    tgt_full = os.path.join(tgt, test_file)

    checks = []

    # Create test file on source
    try:
        Path(src_full).write_text(f"Moodle DR verify {test_id}")
        checks.append({"check": "Create test file on source", "ok": True})
    except Exception as e:
        checks.append({"check": "Create test file on source", "ok": False, "error": str(e)})
        return {"ok": False, "checks": checks}

    # Trigger rsync
    r = _run([
        "rsync", "-a",
        "-e", f"ssh -i {key} -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
        src_full, f"{user}@{ip}:{tgt_full}",
    ], timeout=30)
    checks.append({"check": "rsync test file to target", "ok": r["ok"]})

    # Verify on target
    time.sleep(2)
    r2 = _run([
        "ssh", "-i", key, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
        f"{user}@{ip}", f"test -f {tgt_full} && echo __found__ || echo __missing__",
    ], timeout=12)
    found = r2["ok"] and "__found__" in r2["out"]
    checks.append({"check": "Verify file on target", "ok": found,
                   "detail": "File found on target" if found else "File not found"})

    # Cleanup
    try:
        Path(src_full).unlink(missing_ok=True)
    except Exception:
        pass
    _run([
        "ssh", "-i", key, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
        f"{user}@{ip}", f"rm -f {tgt_full}",
    ], timeout=10)

    ok = all(c["ok"] for c in checks)
    if ok:
        save_state({"verified": True})
    return {"ok": ok, "checks": checks}


# ── Reset ──────────────────────────────────────────────────────────────────────

@router.post("/reset")
def reset_setup():
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    state.setup_mode = ""
    state.welcome_seen = False
    state.file_setup_complete = False
    state.db_setup_complete = False
    return {"ok": True, "message": "Setup state reset"}


# ── SSH key generation ────────────────────────────────────────────────────────

@router.get("/ssh-key/public")
def get_ssh_public_key():
    st = load_state()
    key_path = st.get("ssh_key_path", "/root/.ssh/moodle_rsync_ed25519")
    pub_path = Path(key_path + ".pub")
    if pub_path.exists():
        return {"ok": True, "public_key": pub_path.read_text().strip()}
    return {"ok": False, "error": "SSH public key not found"}
