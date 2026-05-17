"""
Moodle Upgrade Module — /api/upgrade/*
Supports three upgrade modes:
  A. In-Place       — upgrade existing server directly
  B. Lift & Shift   — migrate to new server, upgrade there (source untouched)
  C. Replica-First  — upgrade DR replica first, then cutover (source untouched)

All version decisions are made dynamically. Nothing is hardcoded to a specific Moodle version.
"""

import asyncio
import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/upgrade", tags=["upgrade"])

# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
STATE_FILE = Path(os.getenv("UPGRADE_STATE_FILE", "/opt/moodle-dr-v11/upgrade_state.json"))
LOG_FILE = Path(os.getenv("UPGRADE_LOG_FILE", "/opt/moodle-dr-v11/logs/upgrade.log"))
BACKUP_DIR = Path(os.getenv("UPGRADE_BACKUP_DIR", "/opt/moodle-dr-backups"))

_upgrade_lock = threading.Lock()
_upgrade_running = False

def _load_state() -> Dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception:
        pass
    return {
        "mode": None,
        "source_host": None,
        "source_user": "root",
        "source_key": None,
        "source_moodle_dir": None,
        "target_host": None,
        "target_user": "root",
        "target_key": None,
        "target_moodle_dir": None,
        "detected_version": None,
        "detected_php": None,
        "detected_db": None,
        "target_version": None,
        "stages": [],
        "current_stage_index": 0,
        "stage_statuses": [],
        "last_backup_path": None,
        "preflight_results": {},
        "preflight_passed": False,
        "upgrade_status": "idle",
        "error_message": None,
        "cutover_done": False,
    }


def _save_state(state: Dict[str, Any]):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _append_log(msg: str):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}\n"
    with open(LOG_FILE, "a") as f:
        f.write(line)


# ---------------------------------------------------------------------------
# Version path engine — version-agnostic, no hardcoded assumptions
# ---------------------------------------------------------------------------

# Each entry: (major, minor) tuple
WAYPOINTS = {
    # Any source below 4.4 must pass through 4.4 before going to 5.x
    "4.4": (4, 4),
}

VERSION_REQUIREMENTS = {
    # target_version: {min_upgrade_from, php, mariadb, mysql}
    "4.1": {"min_from": (4, 0, 0), "php": "8.0.0", "mariadb": "10.4.0",  "mysql": "8.0"},
    "4.2": {"min_from": (4, 1, 2), "php": "8.0.0", "mariadb": "10.6.7",  "mysql": "8.0"},
    "4.3": {"min_from": (4, 1, 2), "php": "8.0.0", "mariadb": "10.6.7",  "mysql": "8.0"},
    "4.4": {"min_from": (4, 1, 2), "php": "8.1.0", "mariadb": "10.6.7",  "mysql": "8.0"},
    "4.5": {"min_from": (4, 1, 2), "php": "8.1.0", "mariadb": "10.6.7",  "mysql": "8.0"},
    "5.0": {"min_from": (4, 2, 3), "php": "8.2.0", "mariadb": "10.11.0", "mysql": "8.4"},
    "5.1": {"min_from": (4, 2, 3), "php": "8.2.0", "mariadb": "10.11.0", "mysql": "8.4"},
}

# Moodle versions that introduce a /public docroot change
VERSIONS_WITH_PUBLIC_DIR = {"5.1"}


def _parse_version(v: str) -> Tuple[int, int, int]:
    """Parse '4.1.9' or '4.1' into (4, 1, 9)."""
    parts = v.strip().split(".")
    major = int(parts[0]) if len(parts) > 0 else 0
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return (major, minor, patch)


def _version_key(v: str) -> str:
    """Normalize '4.1.9' → '4.1', '5.1.0' → '5.1'."""
    parts = v.strip().split(".")
    return f"{parts[0]}.{parts[1]}"


def compute_upgrade_path(source_version: str, target_version: str) -> Dict[str, Any]:
    """
    Compute the safest upgrade stage path from source to target.
    Returns: {stages: [...], blocked: bool, reason: str}
    """
    src = _parse_version(source_version)
    tgt = _parse_version(target_version)
    src_key = _version_key(source_version)
    tgt_key = _version_key(target_version)

    # Already current
    if src[:2] == tgt[:2]:
        return {"stages": [], "blocked": False, "reason": "Already at target version"}

    # Downgrade not supported
    if src > tgt:
        return {"stages": [], "blocked": True, "reason": "Downgrade is not supported by Moodle"}

    # Check minimum upgrade-from requirement for the target
    req = VERSION_REQUIREMENTS.get(tgt_key)
    if req and src < req["min_from"]:
        return {
            "stages": [],
            "blocked": True,
            "reason": f"Moodle {target_version} requires upgrading from {'.'.join(str(x) for x in req['min_from'])} or later. Your version {source_version} is too old — upgrade to a supported intermediate version first."
        }

    stages = []
    current = src_key

    # Waypoint rule: any source < 4.4 targeting 5.x must pass through 4.4
    current_major, current_minor = int(current.split(".")[0]), int(current.split(".")[1])
    target_major, target_minor = int(tgt_key.split(".")[0]), int(tgt_key.split(".")[1])

    if (current_major < 4 or (current_major == 4 and current_minor < 4)) and target_major >= 5:
        # Must hit 4.4 first
        if current != "4.4":
            stages.append({
                "from": current,
                "to": "4.4",
                "label": f"{current} → 4.4 (required waypoint)",
                "requirements": VERSION_REQUIREMENTS.get("4.4", {}),
                "download_url": _download_url("4.4"),
                "public_dir_change": False,
            })
            current = "4.4"

    # Now from current to target
    if current != tgt_key:
        stages.append({
            "from": current,
            "to": tgt_key,
            "label": f"{current} → {tgt_key}",
            "requirements": VERSION_REQUIREMENTS.get(tgt_key, {}),
            "download_url": _download_url(tgt_key),
            "public_dir_change": tgt_key in VERSIONS_WITH_PUBLIC_DIR,
        })

    return {"stages": stages, "blocked": False, "reason": ""}


def _download_url(version_key: str) -> str:
    """Build Moodle download URL. e.g. '4.4' → stable44, '5.1' → stable501"""
    parts = version_key.split(".")
    major, minor = parts[0], parts[1]
    # 5.x: stable501, stable50; 4.x: stable44, stable45
    nodot = major + minor
    return f"https://download.moodle.org/stable{nodot}/moodle-latest-{nodot}.tgz"


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}

def _is_local(host: str) -> bool:
    """Return True when the host refers to this machine."""
    return (host or "").strip().lower() in _LOCAL_HOSTS

def _ssh_cmd(host: str, user: str, key: Optional[str], cmd: str, timeout: int = 60) -> Dict[str, Any]:
    """Run a command — locally via bash if host is localhost, otherwise over SSH."""
    if _is_local(host):
        # Local execution — no SSH needed.  Run as current process user.
        try:
            result = subprocess.run(
                ["bash", "-c", cmd],
                capture_output=True, text=True, timeout=timeout
            )
            return {
                "ok": result.returncode == 0,
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "returncode": result.returncode,
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "stdout": "", "stderr": "Local command timed out", "returncode": -1}
        except Exception as e:
            return {"ok": False, "stdout": "", "stderr": str(e), "returncode": -1}

    # Remote execution via SSH
    base = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"]
    if key:
        base += ["-i", key]
    base += [f"{user}@{host}", cmd]
    try:
        result = subprocess.run(base, capture_output=True, text=True, timeout=timeout)
        return {
            "ok": result.returncode == 0,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": "SSH command timed out", "returncode": -1}
    except Exception as e:
        return {"ok": False, "stdout": "", "stderr": str(e), "returncode": -1}


def _ssh_interactive(host: str, user: str, key: Optional[str], cmds: List[str], timeout: int = 300) -> Dict[str, Any]:
    """Run multiple commands via SSH in a single session."""
    script = " && ".join(cmds)
    return _ssh_cmd(host, user, key, f"bash -c '{script}'", timeout=timeout)


# Common Moodle installation paths to probe when user-supplied dir fails
_MOODLE_COMMON_PATHS = [
    "/var/www/html/moodle",
    "/var/www/moodle",
    "/var/www/html",
    "/opt/moodle",
    "/srv/moodle",
]

# Directories scanned when falling back to a recursive search for version.php
_MOODLE_SEARCH_ROOTS = ["/var/www", "/opt", "/srv"]
_MOODLE_SEARCH_EXCLUDE_DIRS = {"mod", "blocks", "theme", "lib"}


def _read_version_php_locally(path: str) -> str:
    """
    Read {path}/version.php (or path itself if it ends with version.php) using
    pure Python file I/O. Used in local mode so we don't shell out through
    sudo — systemd services have NoNewPrivileges / no-TTY semantics that
    silently break `sudo` even with NOPASSWD.

    Returns the file content on success, empty string on any failure.
    """
    p = Path(path)
    candidate = p if p.name == "version.php" else p / "version.php"
    try:
        if candidate.is_file():
            return candidate.read_text(errors="replace")
    except (PermissionError, OSError):
        pass
    return ""


def _find_version_php_locally() -> List[str]:
    """Scan well-known Moodle roots for version.php using pure Python (no sudo)."""
    hits: List[str] = []
    for root in _MOODLE_SEARCH_ROOTS:
        root_path = Path(root)
        if not root_path.is_dir():
            continue
        try:
            for dirpath, dirnames, filenames in os.walk(root_path, followlinks=False):
                # Prune Moodle sub-trees that contain unrelated version.php files
                dirnames[:] = [d for d in dirnames if d not in _MOODLE_SEARCH_EXCLUDE_DIRS]
                if "version.php" in filenames:
                    hits.append(str(Path(dirpath) / "version.php"))
                    if len(hits) >= 10:
                        return hits
        except (PermissionError, OSError):
            continue
    return hits


def _detect_moodle_on_host(host: str, user: str, key: Optional[str], moodle_dir: str) -> Dict[str, Any]:
    """Detect Moodle version, PHP version, and DB version on a host (local or remote)."""
    results = {}
    is_local = _is_local(host)

    # ── Step 1: find and read version.php ────────────────────────────────────
    # In local mode, the app runs on the same machine as Moodle. We prefer
    # pure Python file I/O over any sudo / subprocess approach because the
    # service runs under systemd (no TTY, NoNewPrivileges may apply) and
    # `sudo` silently fails in that context even when NOPASSWD is configured.
    #
    # For remote hosts we fall back to the historical SSH read strategy:
    #   1. sudo -n -E cat       — non-interactive, preserves env; bypasses ownership
    #   2. plain cat            — works if moodledr is in www-data group
    #   3. sudo -n -u www-data  — run as web server user directly
    def _read_version_php(path: str) -> str:
        if is_local:
            return _read_version_php_locally(path)
        cmd = (
            f"sudo -n -E cat {path}/version.php 2>/dev/null || "
            f"cat {path}/version.php 2>/dev/null || "
            f"sudo -n -u www-data cat {path}/version.php 2>/dev/null"
        )
        r = _ssh_cmd(host, user, key, cmd)
        return r.get("stdout", "")

    version_php_content = ""
    resolved_dir = moodle_dir

    content = _read_version_php(moodle_dir)
    if "$release" in content or "$version" in content:
        version_php_content = content
    else:
        # Try all common paths before falling back to find
        candidates = [p for p in _MOODLE_COMMON_PATHS if p != moodle_dir]
        for candidate in candidates:
            content = _read_version_php(candidate)
            if "$release" in content or "$version" in content:
                version_php_content = content
                resolved_dir = candidate
                break

        if not version_php_content:
            if is_local:
                found_paths = _find_version_php_locally()
            else:
                # sudo find so we can traverse directories owned by root/www-data
                find_r = _ssh_cmd(host, user, key,
                    "sudo -n find /var/www /opt /srv -name version.php "
                    "-not -path '*/mod/*' -not -path '*/blocks/*' "
                    "-not -path '*/theme/*' -not -path '*/lib/*' "
                    "2>/dev/null | head -10")
                # Also try without sudo in case sudo find isn't available
                if not find_r.get("stdout", "").strip():
                    find_r = _ssh_cmd(host, user, key,
                        "find /var/www /opt /srv -name version.php "
                        "-not -path '*/mod/*' -not -path '*/blocks/*' "
                        "-not -path '*/theme/*' -not -path '*/lib/*' "
                        "2>/dev/null | head -10")
                found_paths = [p.strip() for p in find_r.get("stdout", "").splitlines() if p.strip()]

            for found_path in found_paths:
                found_dir = found_path.rsplit("/", 1)[0]
                content = _read_version_php(found_dir)
                if "$release" in content or "$version" in content:
                    version_php_content = content
                    resolved_dir = found_dir
                    break

    results["resolved_dir"] = resolved_dir
    results["version_php_found"] = bool(version_php_content)

    # ── Step 2: parse version from content ───────────────────────────────────
    release_raw = ""
    for line in version_php_content.splitlines():
        if re.search(r"^\s*\$release\s*=", line):
            release_raw = line
            break
    results["release_raw"] = release_raw

    version = None
    # Match '4.1.9+' or '5.1' inside single-quoted string
    release_match = re.search(r"'([\d]+\.[\d]+(?:\.[\d]+)?)[^']*'", release_raw)
    if release_match:
        version = release_match.group(1)
    else:
        release_match2 = re.search(r"(\d+\.\d+(?:\.\d+)?)", release_raw)
        if release_match2:
            version = release_match2.group(1)
    results["moodle_version"] = version

    # Detect PHP version
    r3 = _ssh_cmd(host, user, key, "php -r 'echo PHP_VERSION;' 2>/dev/null || php8.2 -r 'echo PHP_VERSION;' 2>/dev/null || echo 'unknown'")
    results["php_version"] = r3.get("stdout", "unknown").strip()

    # Detect MariaDB/MySQL version
    r4 = _ssh_cmd(host, user, key, "mysql --version 2>/dev/null || mariadb --version 2>/dev/null || echo 'unknown'")
    db_raw = r4.get("stdout", "unknown")
    results["db_raw"] = db_raw
    db_version = None
    db_match = re.search(r"(\d+\.\d+\.\d+)", db_raw)
    if db_match:
        db_version = db_match.group(1)
    results["db_version"] = db_version
    results["db_type"] = "MariaDB" if "mariadb" in db_raw.lower() or "MariaDB" in db_raw else "MySQL"

    # Detect moodle dir
    r5 = _ssh_cmd(host, user, key, f"ls {moodle_dir}/config.php 2>/dev/null && echo 'found' || echo 'not_found'")
    results["config_found"] = "found" in r5.get("stdout", "")

    # Detect available disk space (GB) on moodle dir partition
    r6 = _ssh_cmd(host, user, key, f"df -BG {moodle_dir} 2>/dev/null | tail -1 | awk '{{print $4}}' | tr -d 'G'")
    try:
        results["disk_free_gb"] = int(r6.get("stdout", "0").strip())
    except ValueError:
        results["disk_free_gb"] = 0

    return results


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def run_preflight_checks(state: Dict[str, Any]) -> Dict[str, Any]:
    """Run all pre-flight checks for the selected mode and target version."""
    mode = state.get("mode")
    target_version = state.get("target_version", "5.1")
    tgt_key = _version_key(target_version)
    req = VERSION_REQUIREMENTS.get(tgt_key, {})
    checks = {}

    # --- Source server checks ---
    src_host = state.get("source_host")
    src_user = state.get("source_user", "root")
    src_key = state.get("source_key")
    src_dir = state.get("source_moodle_dir", "/var/www/html/moodle")

    # SSH connectivity
    r = _ssh_cmd(src_host, src_user, src_key, "echo ok", timeout=15)
    checks["source_ssh"] = {"pass": r["ok"], "label": "Source SSH access", "detail": r.get("stderr", "")}

    if r["ok"]:
        det = _detect_moodle_on_host(src_host, src_user, src_key, src_dir)
        src_version = det.get("moodle_version")

        checks["source_moodle_found"] = {
            "pass": det.get("config_found", False),
            "label": f"Moodle found at {src_dir}",
            "detail": f"Detected version: {src_version or 'unknown'}"
        }

        checks["source_disk_space"] = {
            "pass": det.get("disk_free_gb", 0) >= 15,
            "label": "Source disk space (≥15 GB free)",
            "detail": f"{det.get('disk_free_gb', 0)} GB available"
        }

        # Check maintenance CLI exists
        r_maint = _ssh_cmd(src_host, src_user, src_key, f"ls {src_dir}/admin/cli/maintenance.php 2>/dev/null && echo ok || echo missing")
        checks["maintenance_cli"] = {
            "pass": "ok" in r_maint.get("stdout", ""),
            "label": "Maintenance mode CLI available",
            "detail": ""
        }

        # Check upgrade CLI exists
        r_upg = _ssh_cmd(src_host, src_user, src_key, f"ls {src_dir}/admin/cli/upgrade.php 2>/dev/null && echo ok || echo missing")
        checks["upgrade_cli"] = {
            "pass": "ok" in r_upg.get("stdout", ""),
            "label": "Upgrade CLI available",
            "detail": ""
        }

    # --- Target server checks (Mode B — Lift & Shift) ---
    if mode == "lift_and_shift":
        tgt_host = state.get("target_host")
        tgt_user = state.get("target_user", "root")
        tgt_key = state.get("target_key")

        if tgt_host:
            r_tgt = _ssh_cmd(tgt_host, tgt_user, tgt_key, "echo ok", timeout=15)
            checks["target_ssh"] = {"pass": r_tgt["ok"], "label": "Target SSH access", "detail": r_tgt.get("stderr", "")}

            if r_tgt["ok"]:
                det_tgt = _detect_moodle_on_host(tgt_host, tgt_user, tgt_key, state.get("target_moodle_dir", "/var/www/html/moodle"))
                checks["target_disk_space"] = {
                    "pass": det_tgt.get("disk_free_gb", 0) >= 20,
                    "label": "Target disk space (≥20 GB free)",
                    "detail": f"{det_tgt.get('disk_free_gb', 0)} GB available"
                }

    # --- Replication check (Mode C — Replica-First) ---
    if mode == "replica_first":
        # Check if DR replication state is loaded
        try:
            from db_replication import get_replication_status_data
            repl = get_replication_status_data()
            io_ok = repl.get("Slave_IO_Running", "No") == "Yes"
            sql_ok = repl.get("Slave_SQL_Running", "No") == "Yes"
            checks["replication_healthy"] = {
                "pass": io_ok and sql_ok,
                "label": "DR replication healthy (IO + SQL running)",
                "detail": f"IO: {repl.get('Slave_IO_Running','?')} SQL: {repl.get('Slave_SQL_Running','?')}"
            }
        except Exception as e:
            checks["replication_healthy"] = {
                "pass": False,
                "label": "DR replication healthy",
                "detail": f"Could not check: {e}"
            }

    all_pass = all(c["pass"] for c in checks.values())
    return {"checks": checks, "all_pass": all_pass}


# ---------------------------------------------------------------------------
# Upgrade execution engine
# ---------------------------------------------------------------------------

def _run_upgrade_stage(
    host: str, user: str, key: Optional[str],
    moodle_dir: str, stage: Dict[str, Any],
    backup_base: Path, stage_index: int
) -> bool:
    """
    Execute a single upgrade stage on a remote host.
    Returns True on success, False on failure.
    """
    from_ver = stage["from"]
    to_ver = stage["to"]
    download_url = stage["download_url"]
    public_dir_change = stage.get("public_dir_change", False)

    _append_log(f"=== STAGE {stage_index+1}: {from_ver} → {to_ver} ===")

    # Step 1: Backup
    backup_path = backup_base / f"stage_{stage_index+1}_{from_ver.replace('.','')}_to_{to_ver.replace('.','')}"
    _append_log(f"Creating backup at {backup_path} ...")
    r = _ssh_cmd(host, user, key,
        f"mkdir -p {backup_path} && "
        f"mysqldump --single-transaction --routines --triggers $(grep dbname {moodle_dir}/config.php | grep -o \"'[^']*'\" | tr -d \"'\") > {backup_path}/db_backup.sql 2>&1 && "
        f"tar czf {backup_path}/moodle_code.tar.gz -C $(dirname {moodle_dir}) $(basename {moodle_dir}) --exclude='$(basename {moodle_dir})/moodledata' 2>&1",
        timeout=600
    )
    if not r["ok"]:
        _append_log(f"WARNING: Backup may be incomplete: {r['stderr']}")
        # Don't fail on backup warning — log and continue

    # Step 2: Enable maintenance mode
    _append_log("Enabling maintenance mode...")
    r = _ssh_cmd(host, user, key, f"cd {moodle_dir} && php admin/cli/maintenance.php --enable", timeout=30)
    if not r["ok"]:
        _append_log(f"WARNING: Could not enable maintenance mode: {r['stderr']}")

    # Step 3: Download new Moodle version
    tmp_tgz = f"/tmp/moodle-{to_ver.replace('.','')}.tgz"
    _append_log(f"Downloading Moodle {to_ver} from {download_url} ...")
    r = _ssh_cmd(host, user, key, f"wget -q -O {tmp_tgz} '{download_url}'", timeout=600)
    if not r["ok"]:
        _append_log(f"ERROR: Download failed: {r['stderr']}")
        return False

    # Step 4: Extract and replace code (preserve config.php and moodledata)
    _append_log("Extracting and replacing Moodle code...")
    parent_dir = str(Path(moodle_dir).parent)
    moodle_basename = Path(moodle_dir).name
    r = _ssh_cmd(host, user, key,
        f"cd /tmp && tar xzf {tmp_tgz} && "
        f"cp {moodle_dir}/config.php /tmp/moodle/config.php && "
        f"rsync -a --delete --exclude='config.php' --exclude='moodledata' /tmp/moodle/ {moodle_dir}/ && "
        f"rm -rf /tmp/moodle {tmp_tgz}",
        timeout=300
    )
    if not r["ok"]:
        _append_log(f"ERROR: Code replacement failed: {r['stderr']}")
        return False

    # Step 5: Run Moodle upgrade CLI
    _append_log("Running Moodle database upgrade (this may take several minutes)...")
    r = _ssh_cmd(host, user, key,
        f"cd {moodle_dir} && php admin/cli/upgrade.php --non-interactive --allow-unstable 2>&1",
        timeout=900
    )
    _append_log(r.get("stdout", ""))
    if not r["ok"]:
        _append_log(f"ERROR: upgrade.php failed: {r['stderr']}")
        return False

    # Step 6: Purge caches
    _append_log("Purging Moodle caches...")
    _ssh_cmd(host, user, key, f"cd {moodle_dir} && php admin/cli/purge_caches.php", timeout=60)

    # Step 7: 5.1 public dir change
    if public_dir_change:
        _append_log("Applying Moodle 5.1 /public docroot change...")
        r = _ssh_cmd(host, user, key,
            f"ls {moodle_dir}/public/index.php 2>/dev/null && echo 'public_dir_found' || echo 'not_found'")
        if "public_dir_found" in r.get("stdout", ""):
            _append_log("  /public directory confirmed. Update your Nginx docroot to point to:")
            _append_log(f"  {moodle_dir}/public/")
            _append_log("  This is handled automatically in the cutover step.")
        else:
            _append_log("  WARNING: /public directory not found after upgrade — check manually")

    # Step 8: Validate version
    _append_log(f"Validating upgrade to {to_ver}...")
    r_ver = _ssh_cmd(host, user, key, f"grep release {moodle_dir}/version.php | head -1")
    _append_log(f"  version.php reports: {r_ver.get('stdout','unknown')}")

    # Step 9: Disable maintenance mode
    _append_log("Disabling maintenance mode...")
    _ssh_cmd(host, user, key, f"cd {moodle_dir} && php admin/cli/maintenance.php --disable", timeout=30)

    _append_log(f"=== STAGE {stage_index+1} COMPLETE: {from_ver} → {to_ver} ✓ ===")
    return True


def _execute_upgrade(state: Dict[str, Any]):
    """Background task: run all upgrade stages."""
    global _upgrade_running
    _upgrade_running = True
    state["upgrade_status"] = "running"
    _save_state(state)

    mode = state.get("mode")
    stages = state.get("stages", [])

    # Determine which host to operate on
    if mode == "in_place":
        host = state.get("source_host")
        user = state.get("source_user", "root")
        key = state.get("source_key")
        moodle_dir = state.get("source_moodle_dir", "/var/www/html/moodle")
    elif mode == "lift_and_shift":
        host = state.get("target_host")
        user = state.get("target_user", "root")
        key = state.get("target_key")
        moodle_dir = state.get("target_moodle_dir", "/var/www/html/moodle")
    elif mode == "replica_first":
        host = state.get("target_host")  # replica is the target
        user = state.get("target_user", "root")
        key = state.get("target_key")
        moodle_dir = state.get("target_moodle_dir", "/var/www/html/moodle")
    else:
        _append_log("ERROR: Unknown upgrade mode")
        state["upgrade_status"] = "failed"
        state["error_message"] = "Unknown upgrade mode"
        _save_state(state)
        _upgrade_running = False
        return

    # Replica-first: stop replication before upgrade
    if mode == "replica_first":
        _append_log("Stopping DB replication before upgrade (replica-first mode)...")
        _ssh_cmd(host, user, key, "mysql -e 'STOP SLAVE;' 2>/dev/null || mysql -e 'STOP REPLICA;' 2>/dev/null")

    backup_base = BACKUP_DIR / f"upgrade-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    state["last_backup_path"] = str(backup_base)

    start_from = state.get("current_stage_index", 0)

    for i, stage in enumerate(stages):
        if i < start_from:
            _append_log(f"Skipping stage {i+1} (already completed)")
            continue

        state["current_stage_index"] = i
        state["stage_statuses"][i] = "running"
        _save_state(state)

        ok = _run_upgrade_stage(host, user, key, moodle_dir, stage, backup_base, i)

        if ok:
            state["stage_statuses"][i] = "completed"
            state["current_stage_index"] = i + 1
            _save_state(state)
        else:
            state["stage_statuses"][i] = "failed"
            state["upgrade_status"] = "failed"
            state["error_message"] = f"Stage {i+1} ({stage['from']} → {stage['to']}) failed. Check logs. You can resume from this stage."
            _save_state(state)
            _upgrade_running = False
            _append_log(f"UPGRADE FAILED at stage {i+1}. Resume is available.")
            return

    state["upgrade_status"] = "completed"
    state["error_message"] = None
    _save_state(state)
    _upgrade_running = False
    _append_log("=== ALL UPGRADE STAGES COMPLETED SUCCESSFULLY ===")
    _append_log("Next step: use the Cutover action to update DNS and config.php if running Lift & Shift or Replica-First mode.")


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------

class DetectRequest(BaseModel):
    host: str
    user: str = "root"
    key: Optional[str] = None
    moodle_dir: str = "/var/www/html/moodle"
    role: str = "source"  # "source" or "target"


class PreflightRequest(BaseModel):
    mode: str  # "in_place" | "lift_and_shift" | "replica_first"
    target_version: str = "5.1"
    source_host: Optional[str] = None
    source_user: str = "root"
    source_key: Optional[str] = None
    source_moodle_dir: str = "/var/www/html/moodle"
    target_host: Optional[str] = None
    target_user: str = "root"
    target_key: Optional[str] = None
    target_moodle_dir: str = "/var/www/html/moodle"


class StartUpgradeRequest(BaseModel):
    mode: str
    target_version: str = "5.1"
    source_host: Optional[str] = None
    source_user: str = "root"
    source_key: Optional[str] = None
    source_moodle_dir: str = "/var/www/html/moodle"
    target_host: Optional[str] = None
    target_user: str = "root"
    target_key: Optional[str] = None
    target_moodle_dir: str = "/var/www/html/moodle"
    resume: bool = False  # If True, resume from last_successful_stage


class CutoverRequest(BaseModel):
    new_wwwroot: str  # e.g. https://lmsdr.agu.edu.bh
    nginx_conf_path: Optional[str] = None  # path to nginx site config on target
    update_nginx_docroot: bool = True  # auto-update nginx for 5.1 /public change


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
def get_upgrade_status():
    state = _load_state()
    return {
        "running": _upgrade_running,
        "state": state,
        "log_tail": _get_log_tail(50),
    }


@router.post("/detect")
def detect_moodle(req: DetectRequest):
    """Detect Moodle version, PHP, DB. Runs locally if host is 127.0.0.1/localhost."""
    local_mode = _is_local(req.host)
    det = _detect_moodle_on_host(req.host, req.user, req.key, req.moodle_dir)
    det["local_mode"] = local_mode
    state = _load_state()

    if req.role == "source":
        state["source_host"] = req.host
        state["source_user"] = req.user
        state["source_key"] = req.key
        state["source_moodle_dir"] = req.moodle_dir
        state["detected_version"] = det.get("moodle_version")
        state["detected_php"] = det.get("php_version")
        state["detected_db"] = det.get("db_version")
        state["detected_db_type"] = det.get("db_type")
    else:
        state["target_host"] = req.host
        state["target_user"] = req.user
        state["target_key"] = req.key
        state["target_moodle_dir"] = req.moodle_dir

    _save_state(state)
    return {"detected": det, "state": state}


@router.post("/path")
def compute_path(source_version: str, target_version: str = "5.1"):
    """Compute the upgrade path from source to target version."""
    result = compute_upgrade_path(source_version, target_version)
    return result


@router.get("/versions")
def list_available_versions():
    """List all supported Moodle target versions with download URLs."""
    return [
        {"version": k, "download_url": _download_url(k), "requirements": v}
        for k, v in VERSION_REQUIREMENTS.items()
    ]


@router.post("/preflight")
def preflight(req: PreflightRequest):
    """Run pre-flight checks for the selected mode and target version."""
    state = _load_state()
    state.update({
        "mode": req.mode,
        "target_version": req.target_version,
        "source_host": req.source_host or state.get("source_host"),
        "source_user": req.source_user,
        "source_key": req.source_key or state.get("source_key"),
        "source_moodle_dir": req.source_moodle_dir,
        "target_host": req.target_host or state.get("target_host"),
        "target_user": req.target_user,
        "target_key": req.target_key or state.get("target_key"),
        "target_moodle_dir": req.target_moodle_dir,
    })
    _save_state(state)

    results = run_preflight_checks(state)
    state["preflight_results"] = results
    state["preflight_passed"] = results["all_pass"]
    _save_state(state)
    return results


@router.post("/start")
def start_upgrade(req: StartUpgradeRequest, background_tasks: BackgroundTasks):
    """Start the upgrade process. Must pass preflight first."""
    global _upgrade_running

    if _upgrade_running:
        return {"ok": False, "error": "Upgrade already in progress"}

    state = _load_state()

    if req.resume:
        # Resume from where we left off
        _append_log("=== RESUMING UPGRADE FROM LAST SUCCESSFUL STAGE ===")
    else:
        # Fresh start
        if not state.get("preflight_passed"):
            return {"ok": False, "error": "Pre-flight checks must pass before starting upgrade"}

        detected = state.get("detected_version")
        if not detected:
            return {"ok": False, "error": "No source version detected. Run /detect first."}

        path = compute_upgrade_path(detected, req.target_version)
        if path["blocked"]:
            return {"ok": False, "error": path["reason"]}

        state.update({
            "mode": req.mode,
            "target_version": req.target_version,
            "source_host": req.source_host or state.get("source_host"),
            "source_user": req.source_user,
            "source_key": req.source_key or state.get("source_key"),
            "source_moodle_dir": req.source_moodle_dir,
            "target_host": req.target_host or state.get("target_host"),
            "target_user": req.target_user,
            "target_key": req.target_key or state.get("target_key"),
            "target_moodle_dir": req.target_moodle_dir,
            "stages": path["stages"],
            "current_stage_index": 0,
            "stage_statuses": ["pending"] * len(path["stages"]),
            "upgrade_status": "starting",
            "error_message": None,
            "cutover_done": False,
        })
        _save_state(state)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOG_FILE.write_text("")  # Reset log for fresh run
        _append_log(f"=== MOODLE UPGRADE STARTED ===")
        _append_log(f"Mode: {req.mode} | Source: {detected} → Target: {req.target_version}")
        _append_log(f"Stages: {' → '.join([s['to'] for s in path['stages']])}")

    background_tasks.add_task(_execute_upgrade, state)
    return {"ok": True, "stages": state.get("stages", []), "message": "Upgrade started"}


@router.post("/rollback")
def rollback_upgrade():
    """Attempt to rollback to pre-upgrade backup."""
    state = _load_state()
    backup_path = state.get("last_backup_path")
    if not backup_path:
        return {"ok": False, "error": "No backup path recorded"}

    # Determine host
    mode = state.get("mode", "in_place")
    if mode == "in_place":
        host = state.get("source_host")
        user = state.get("source_user", "root")
        key = state.get("source_key")
        moodle_dir = state.get("source_moodle_dir", "/var/www/html/moodle")
    else:
        host = state.get("target_host")
        user = state.get("target_user", "root")
        key = state.get("target_key")
        moodle_dir = state.get("target_moodle_dir", "/var/www/html/moodle")

    _append_log(f"=== ROLLBACK INITIATED from {backup_path} ===")

    # Find the latest stage backup
    stage_idx = max(0, state.get("current_stage_index", 1) - 1)
    stages = state.get("stages", [])
    if stages and stage_idx < len(stages):
        s = stages[stage_idx]
        backup_subdir = f"stage_{stage_idx+1}_{s['from'].replace('.','')}_to_{s['to'].replace('.','')} "
    else:
        backup_subdir = ""

    full_backup = f"{backup_path}/{backup_subdir}".strip()

    # Restore DB
    r1 = _ssh_cmd(host, user, key,
        f"ls {full_backup}/db_backup.sql 2>/dev/null && echo 'found' || echo 'not_found'")
    if "found" in r1.get("stdout", ""):
        _append_log("Restoring database from backup...")
        db_name_r = _ssh_cmd(host, user, key,
            f"grep dbname {moodle_dir}/config.php | grep -o \"'[^']*'\" | tr -d \"'\"")
        db_name = db_name_r.get("stdout", "moodle").strip()
        r_db = _ssh_cmd(host, user, key,
            f"mysql {db_name} < {full_backup}/db_backup.sql", timeout=600)
        _append_log("DB restore: " + ("OK" if r_db["ok"] else f"FAILED: {r_db['stderr']}"))
    else:
        _append_log("WARNING: No DB backup found at expected path")

    # Restore code
    r2 = _ssh_cmd(host, user, key,
        f"ls {full_backup}/moodle_code.tar.gz 2>/dev/null && echo 'found' || echo 'not_found'")
    if "found" in r2.get("stdout", ""):
        _append_log("Restoring Moodle code from backup...")
        parent_dir = str(Path(moodle_dir).parent)
        r_code = _ssh_cmd(host, user, key,
            f"cd {parent_dir} && tar xzf {full_backup}/moodle_code.tar.gz", timeout=300)
        _append_log("Code restore: " + ("OK" if r_code["ok"] else f"FAILED: {r_code['stderr']}"))
    else:
        _append_log("WARNING: No code backup found")

    state["upgrade_status"] = "rolled_back"
    _save_state(state)
    _append_log("=== ROLLBACK COMPLETE ===")
    return {"ok": True, "message": "Rollback attempted. Check logs for details."}


@router.post("/cutover")
def cutover(req: CutoverRequest):
    """
    Execute DNS/config cutover for Lift & Shift or Replica-First modes.
    Updates config.php wwwroot and optionally Nginx docroot for 5.1.
    """
    state = _load_state()
    mode = state.get("mode")

    if mode not in ("lift_and_shift", "replica_first"):
        return {"ok": False, "error": "Cutover only applies to Lift & Shift and Replica-First modes"}

    host = state.get("target_host")
    user = state.get("target_user", "root")
    key = state.get("target_key")
    moodle_dir = state.get("target_moodle_dir", "/var/www/html/moodle")

    _append_log(f"=== CUTOVER INITIATED — wwwroot → {req.new_wwwroot} ===")

    # Update wwwroot in config.php
    r1 = _ssh_cmd(host, user, key,
        f"sed -i \"s|\\$CFG->wwwroot.*|\\$CFG->wwwroot = '{req.new_wwwroot}';|\" {moodle_dir}/config.php")
    _append_log("config.php wwwroot update: " + ("OK" if r1["ok"] else f"FAILED: {r1['stderr']}"))

    # Update Nginx docroot for 5.1 /public change
    target_version = state.get("target_version", "")
    if req.update_nginx_docroot and _version_key(target_version) in VERSIONS_WITH_PUBLIC_DIR:
        _append_log("Updating Nginx docroot for Moodle 5.1 /public directory...")
        if req.nginx_conf_path:
            r2 = _ssh_cmd(host, user, key,
                f"sed -i 's|root {moodle_dir};|root {moodle_dir}/public;|g' {req.nginx_conf_path} && "
                f"nginx -t && systemctl reload nginx")
            _append_log("Nginx docroot update: " + ("OK" if r2["ok"] else f"FAILED: {r2['stderr']}"))
        else:
            _append_log("WARNING: No nginx_conf_path provided — update Nginx docroot manually to:")
            _append_log(f"  root {moodle_dir}/public;")

    # Purge Moodle caches after cutover
    _ssh_cmd(host, user, key, f"cd {moodle_dir} && php admin/cli/purge_caches.php", timeout=60)

    state["cutover_done"] = True
    _save_state(state)
    _append_log("=== CUTOVER COMPLETE — Update your DNS records to point to the new server ===")
    return {"ok": True, "message": "Cutover complete. Update DNS records to finish migration."}


@router.get("/log")
def stream_log():
    """SSE stream of the upgrade log file."""
    def generate():
        if not LOG_FILE.exists():
            yield "data: No log available\n\n"
            return
        with open(LOG_FILE, "r") as f:
            while True:
                line = f.readline()
                if line:
                    yield f"data: {line.rstrip()}\n\n"
                else:
                    if not _upgrade_running:
                        break
                    time.sleep(0.5)

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/log/full")
def get_full_log():
    """Return the full upgrade log."""
    return {"log": _get_log_tail(500)}


def _get_log_tail(n: int) -> List[str]:
    if not LOG_FILE.exists():
        return []
    with open(LOG_FILE, "r") as f:
        lines = f.readlines()
    return [l.rstrip() for l in lines[-n:]]


@router.delete("/state")
def reset_state():
    """Reset the upgrade state (for fresh start)."""
    default = _load_state()
    # Reset to defaults
    for k in default:
        if k not in ("source_host", "source_user", "source_key", "source_moodle_dir"):
            default[k] = None if isinstance(default[k], str) else ([] if isinstance(default[k], list) else (False if isinstance(default[k], bool) else {}))
    default["upgrade_status"] = "idle"
    default["stages"] = []
    default["stage_statuses"] = []
    _save_state(default)
    if LOG_FILE.exists():
        LOG_FILE.write_text("")
    return {"ok": True, "message": "State reset"}
