"""
db_replication.py — All /api/db/* endpoints for MySQL GTID replication management.
"""
import asyncio
import base64
import json
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse

import state
import db_replication_db as db_db

router = APIRouter(prefix="/api/db", tags=["db_replication"])

# ── Helpers ───────────────────────────────────────────────────────────────────

def run_cmd(cmd: list, timeout: int = 15) -> dict:
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


def _ssh_cmd(host: str, user: str = None, *remote_args) -> list:
    cfg = db_db.get_raw_db_config()
    ssh_user = user or (cfg.get("replica_db_user") or "root")
    # Use the file sync SSH key for SSH connections to replica
    return [
        "ssh",
        "-i", state.SSH_KEY_PATH,
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=6",
        f"{state.AZURE_VM_USER}@{host}",
        *remote_args,
    ]


def _get_mysql_conn(host: str, port: int, user: str, password: str, db: str = ""):
    """Get a MySQL connection. Returns (conn, error_str)."""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password,
            database=db if db else None,
            connect_timeout=10,
            connection_timeout=10,
        )
        return conn, None
    except Exception as e:
        return None, str(e)


def _determine_status(io_running, sql_running, lag, last_io_error, last_sql_error) -> str:
    if io_running == "No" and sql_running == "No":
        return "stopped"
    if last_io_error or last_sql_error:
        return "error"
    if io_running != "Yes" or sql_running != "Yes":
        return "error"
    lag_val = lag or 0
    if lag_val > 300:
        return "lagging"
    if lag_val > 60:
        return "warning"
    return "healthy"


def _query_replica_status(cfg: dict) -> dict:
    """Query SHOW REPLICA STATUS from the replica MySQL."""
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"error": err, "connected": False}
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW REPLICA STATUS")
        row = cursor.fetchone()
        if not row:
            # Try old syntax
            cursor.execute("SHOW SLAVE STATUS")
            row = cursor.fetchone()
        conn.close()
        if not row:
            return {"error": "No replica status — not configured as replica", "connected": True}

        io_running = row.get("Replica_IO_Running") or row.get("Slave_IO_Running", "No")
        sql_running = row.get("Replica_SQL_Running") or row.get("Slave_SQL_Running", "No")
        lag = row.get("Seconds_Behind_Source") or row.get("Seconds_Behind_Master")
        last_io_error = row.get("Last_IO_Error", "") or ""
        last_sql_error = row.get("Last_SQL_Error", "") or ""

        return {
            "connected": True,
            "io_running": io_running,
            "sql_running": sql_running,
            "lag_seconds": lag,
            "retrieved_gtid_set": row.get("Retrieved_Gtid_Set", ""),
            "executed_gtid_set": row.get("Executed_Gtid_Set", ""),
            "last_io_error": last_io_error,
            "last_sql_error": last_sql_error,
            "source_uuid": row.get("Master_UUID", "") or row.get("Source_UUID", ""),
            "ssl_allowed": row.get("Master_SSL_Allowed", "") or row.get("Source_SSL_Allowed", ""),
            "auto_position": row.get("Auto_Position", 0),
            "heartbeats_received": row.get("Count_Received_Heartbeats", 0),
            "heartbeat_lag": row.get("Last_Heartbeat_Received"),
            "source_host": row.get("Master_Host", "") or row.get("Source_Host", ""),
            "source_port": row.get("Master_Port", 3306) or row.get("Source_Port", 3306),
            "repl_user": row.get("Master_User", "") or row.get("Source_User", ""),
        }
    except Exception as e:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return {"error": str(e), "connected": False}


# ── Config endpoints ──────────────────────────────────────────────────────────

@router.get("/config")
def get_db_config():
    cfg = db_db.get_db_config()
    return {"ok": True, "config": cfg}


@router.post("/config")
def save_db_config_endpoint(body: dict):
    try:
        # Don't overwrite passwords if they come in as "***"
        existing_raw = db_db.get_raw_db_config()
        if body.get("repl_password") == "***":
            body["repl_password"] = existing_raw.get("repl_password", "")
        if body.get("source_db_password") == "***":
            body["source_db_password"] = existing_raw.get("source_db_password", "")
        if body.get("replica_db_password") == "***":
            body["replica_db_password"] = existing_raw.get("replica_db_password", "")
        db_db.save_db_config(body)
        # Keep state.ADMIN_VM_USER in sync immediately — no restart needed
        if body.get("admin_ssh_user"):
            state.ADMIN_VM_USER = body["admin_ssh_user"]
        db_db.log_audit("save_config", details={"fields": list(body.keys())})
        return {"ok": True, "message": "Configuration saved"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/config/test-source")
def test_source_connection():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration saved"}
    conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.execute("SHOW MASTER STATUS")
        master = cursor.fetchone()
        try:
            cursor.execute("SELECT @@gtid_mode")
            gtid_mode = cursor.fetchone()[0]
        except Exception:
            # GTID not enabled yet on this server — that's OK at config stage
            gtid_mode = "OFF"
        conn.close()
        return {
            "ok": True,
            "version": version,
            "gtid_mode": gtid_mode,
            "has_binlog_position": master is not None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/config/test-replica")
def test_replica_connection():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration saved"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.execute("SELECT @@read_only")
        read_only = cursor.fetchone()[0]
        conn.close()
        return {
            "ok": True,
            "version": version,
            "read_only": bool(read_only),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Setup endpoints ───────────────────────────────────────────────────────────

SUDOERS_FILE  = "/etc/sudoers.d/moodlesync-mysql"
# !use_pty is critical: Ubuntu 24.04 sets 'use_pty' globally in sudoers.
# Without disabling it for moodlesync, sudo over SSH (no PTY) silently
# exits 0 but executes nothing — causing all remote sudo calls to appear
# to succeed while actually doing nothing.
SUDOERS_RULE  = (
    "Defaults:{user} !use_pty\n"
    "{user} ALL=(root) NOPASSWD: "
    "/bin/mkdir, /bin/cp, /bin/chown, /bin/chmod, "
    "/usr/bin/tee, /bin/systemctl, /usr/sbin/mariadbd, /usr/sbin/mysqld, "
    "/usr/bin/stat, /usr/bin/ls, "
    "/usr/bin/mysql, /usr/bin/mariadb"
    # /usr/bin/mysql + /usr/bin/mariadb required for auto-fix of @'localhost' grant
)

@router.get("/setup/check-replica-sudoers")
def check_replica_sudoers():
    """Verify moodlesync has the required NOPASSWD sudoers rule on the replica."""
    cfg = db_db.get_raw_db_config()
    replica_host = (cfg.get("replica_host") or "")
    if not replica_host:
        return {"ok": False, "error": "Replica host not configured"}

    sb = _ssh_base(replica_host)
    ssh_user = state.AZURE_VM_USER

    # 1. Test SSH connectivity
    r = _remote_run(sb, "echo ok", timeout=12)
    if not r["ok"]:
        return {"ok": False, "error": f"SSH unreachable: {r['stderr']}"}

    # 2. Check if sudoers file exists
    r = _remote_run(sb, f"test -f {SUDOERS_FILE} && echo exists || echo missing", timeout=10)
    file_exists = "exists" in r["stdout"]

    # 3. Check if sudo actually works (dry-run mkdir)
    r = _remote_run(sb, "sudo mkdir -p /tmp/moodle-dr-sudoers-test 2>&1", timeout=10)
    sudo_works = r["ok"]

    if sudo_works:
        return {
            "ok": True,
            "message": "moodlesync has NOPASSWD sudo on replica — ready to proceed",
            "sudoers_file": SUDOERS_FILE if file_exists else "(granted via another rule)",
        }
    else:
        rule = SUDOERS_RULE.format(user=ssh_user)
        return {
            "ok": False,
            "error": "moodlesync cannot run sudo on the replica without a password",
            "fix": (
                f"Run this on the replica as root:\n"
                f"echo '{rule}' > {SUDOERS_FILE} "
                f"&& chmod 440 {SUDOERS_FILE} && visudo -c"
            ),
        }


@router.post("/setup/create-repl-user")
def create_repl_user(body: dict = None):
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration saved"}
    conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
    )
    if err:
        db_db.log_audit("create_repl_user", result="error", details={"error": err})
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        # Detect MariaDB vs MySQL — syntax for CREATE USER differs
        cursor.execute("SELECT VERSION()")
        version_str = cursor.fetchone()[0]
        is_mariadb = "mariadb" in version_str.lower()

        repl_user = (cfg.get("repl_user") or "repl_user")
        repl_pass = (cfg.get("repl_password") or "")

        # MariaDB uses plain IDENTIFIED BY; MySQL 8+ uses IDENTIFIED WITH mysql_native_password BY
        if is_mariadb:
            create_sql = f"CREATE USER IF NOT EXISTS '{repl_user}'@'%' IDENTIFIED BY '{repl_pass}'"
        else:
            create_sql = f"CREATE USER IF NOT EXISTS '{repl_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{repl_pass}'"

        cursor.execute(create_sql)
        cursor.execute(f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%'")
        cursor.execute("FLUSH PRIVILEGES")
        conn.commit()
        conn.close()
        db_db.log_audit("create_repl_user", result="ok", details={"user": repl_user})
        return {"ok": True, "message": f"Replication user '{repl_user}'@'%' created and granted (MariaDB={is_mariadb})"}
    except Exception as e:
        db_db.log_audit("create_repl_user", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/setup/generate-ssl-certs")
def generate_ssl_certs():
    ssl_dir = Path("/etc/mysql/ssl")
    ssl_dir.mkdir(parents=True, exist_ok=True)
    steps = []
    try:
        # Generate CA key and cert
        r = run_cmd(["openssl", "genrsa", "-out", str(ssl_dir / "ca-key.pem"), "2048"])
        steps.append({"step": "Generate CA key", "ok": r["ok"], "output": r["stderr"]})
        r = run_cmd(["openssl", "req", "-new", "-x509", "-days", "3650",
                     "-key", str(ssl_dir / "ca-key.pem"),
                     "-out", str(ssl_dir / "ca-cert.pem"),
                     "-subj", "/CN=MySQL-CA"])
        steps.append({"step": "Generate CA cert", "ok": r["ok"]})
        # Client key and cert
        r = run_cmd(["openssl", "genrsa", "-out", str(ssl_dir / "client-key.pem"), "2048"])
        steps.append({"step": "Generate client key", "ok": r["ok"]})
        r = run_cmd(["openssl", "req", "-new",
                     "-key", str(ssl_dir / "client-key.pem"),
                     "-out", str(ssl_dir / "client-req.pem"),
                     "-subj", "/CN=MySQL-Client"])
        steps.append({"step": "Generate client CSR", "ok": r["ok"]})
        r = run_cmd(["openssl", "x509", "-req", "-days", "3650",
                     "-CA", str(ssl_dir / "ca-cert.pem"),
                     "-CAkey", str(ssl_dir / "ca-key.pem"),
                     "-CAcreateserial",
                     "-in", str(ssl_dir / "client-req.pem"),
                     "-out", str(ssl_dir / "client-cert.pem")])
        steps.append({"step": "Sign client cert", "ok": r["ok"]})
        ok = all(s["ok"] for s in steps)
        db_db.log_audit("generate_ssl_certs", result="ok" if ok else "partial")
        return {"ok": ok, "ssl_dir": str(ssl_dir), "steps": steps}
    except Exception as e:
        db_db.log_audit("generate_ssl_certs", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e), "steps": steps}


def _ssh_base(host: str) -> list:
    """Build SSH base command for moodlesync user.
    -tt forces PTY allocation — required when sudoers has 'use_pty' (Ubuntu 24.04 default).
    Without a PTY, sudo silently exits 0 but executes nothing.
    BatchMode=no is intentional: BatchMode=yes suppresses PTY and breaks use_pty sudo.
    """
    return [
        "ssh", "-i", state.SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "BatchMode=no",
        "-o", "ConnectTimeout=10",
        "-o", "PasswordAuthentication=no",
        "-tt",                              # force PTY — needed for use_pty sudoers
        f"{state.AZURE_VM_USER}@{host}",
    ]


def _remote_run(ssh_base: list, cmd: str, input_data: str = None, timeout: int = 20) -> dict:
    """Run command on remote via SSH.
    -tt forces PTY which merges stdout+stderr — capture_output=True conflicts with PTY
    and causes 'Connection closed' immediately. Use stdout=PIPE + stderr=STDOUT instead.
    stdin=DEVNULL prevents any accidental stdin interaction with the PTY.

    NOTE: subprocess.run raises ValueError if both 'input' and 'stdin' are set,
    so we must pick one or the other.
    """
    try:
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,   # PTY merges stderr into stdout anyway
            "timeout": timeout,
        }
        if input_data:
            # 'input' implies stdin=PIPE internally — do NOT also set stdin
            kwargs["input"] = input_data.encode()
        else:
            kwargs["stdin"] = subprocess.DEVNULL
        proc = subprocess.run(ssh_base + [cmd], **kwargs)
        out = proc.stdout.decode(errors='replace').strip()
        return {"ok": proc.returncode == 0, "stdout": out,
                "stderr": "", "returncode": proc.returncode}
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": "timed out", "returncode": -1}
    except Exception as e:
        return {"ok": False, "stdout": "", "stderr": str(e), "returncode": -1}


def _remote_write_file(ssh_base: list, content: str, remote_path: str,
                       use_sudo: bool = True, timeout: int = 20) -> dict:
    """Write content to a remote file without using subprocess stdin pipe.
    Encodes content as base64 and embeds it in the SSH command string.
    This avoids the -tt PTY + stdin EOF hang that breaks 'sudo tee' and 'cat >'.

    When use_sudo=True, writes to a staging file in the user's home dir first
    (no sudo needed), then uses 'sudo cp' to place it. This avoids 'sudo tee'
    in pipelines where sudo doesn't get proper PTY access inside a pipe chain.
    'sudo cp' is in the sudoers allowed list and works reliably as a standalone command.
    """
    b64 = base64.b64encode(content.encode()).decode()
    staging = "~/.mdr_staging_file"
    if use_sudo:
        # 1) decode base64 to staging in user's home (writable, no sudo)
        # 2) sudo cp to final destination (reliable, not in a pipeline)
        # 3) clean up staging
        cmd = (
            f"printf '%s' '{b64}' | base64 -d > {staging} && "
            f"sudo cp {staging} {remote_path} && "
            f"rm -f {staging}"
        )
    else:
        cmd = f"printf '%s' '{b64}' | base64 -d > {remote_path}"
    return _remote_run(ssh_base, cmd, timeout=timeout)


def _detect_db_type(host: str, port: int, user: str, password: str) -> str:
    """Return 'mariadb' or 'mysql' by connecting and reading VERSION()."""
    conn, err = _get_mysql_conn(host, port, user, password)
    if err:
        return "unknown"
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        v = (cur.fetchone()[0] or "").lower()
        conn.close()
        return "mariadb" if "mariadb" in v else "mysql"
    except Exception:
        return "unknown"


@router.get("/setup/replica-ssl-script")
def get_replica_ssl_script():
    """Return a self-contained bash script to run as root on the replica.
    This is the fallback when SSH sudo-based pushing keeps failing."""
    cfg = db_db.get_raw_db_config()
    ssl_dir = "/var/lib/mysql/ssl"
    local_ssl = "/etc/mysql/ssl"

    # Read local certs
    certs = {}
    for cert in ["ca-cert.pem", "client-cert.pem", "client-key.pem"]:
        p = Path(f"{local_ssl}/{cert}")
        if not p.exists():
            return {"ok": False, "error": f"Local cert not found: {p}. Run Generate SSL first."}
        certs[cert] = p.read_text()

    script = f"""#!/bin/bash
# ============================================================
# Moodle DR — Replica SSL Setup Script
# Run this as ROOT on the replica server: {(cfg.get("replica_host") or "")}
# Generated at: $(date)
# ============================================================
set -e

SSL_DIR="{ssl_dir}"
echo "[1/5] Creating SSL directory..."
mkdir -p "$SSL_DIR"
chown mysql:mysql "$SSL_DIR"
chmod 750 "$SSL_DIR"

echo "[2/5] Writing CA cert..."
cat > "$SSL_DIR/ca-cert.pem" << 'CERT_EOF'
{certs['ca-cert.pem']}CERT_EOF

echo "[3/5] Writing client cert..."
cat > "$SSL_DIR/client-cert.pem" << 'CERT_EOF'
{certs['client-cert.pem']}CERT_EOF

echo "[4/5] Writing client key..."
cat > "$SSL_DIR/client-key.pem" << 'CERT_EOF'
{certs['client-key.pem']}CERT_EOF

echo "[5/5] Setting ownership and permissions..."
chown mysql:mysql "$SSL_DIR/ca-cert.pem" "$SSL_DIR/client-cert.pem" "$SSL_DIR/client-key.pem"
chmod 640 "$SSL_DIR/ca-cert.pem" "$SSL_DIR/client-cert.pem" "$SSL_DIR/client-key.pem"

# Also create /var/log/mysql for binlog
mkdir -p /var/log/mysql
chown mysql:mysql /var/log/mysql

echo ""
echo "=== Verification ==="
ls -la "$SSL_DIR/"
echo ""
echo "Done! Certs written to $SSL_DIR"
echo "Now run: Configure Replica in the dashboard"
"""

    # Save script to a known path so it can be downloaded
    script_path = Path("/tmp/moodle-dr-replica-ssl-setup.sh")
    script_path.write_text(script)
    script_path.chmod(0o755)

    # Also mark ssl_remote_dir in the DB so Configure Replica uses the right path
    db_db.update_db_config_field("ssl_remote_dir", ssl_dir)

    return {
        "ok": True,
        "script": script,
        "ssl_dir": ssl_dir,
        "message": "Copy the script below and run it as root on " + (cfg.get("replica_host") or ""),
    }


@router.post("/setup/push-ssl-to-replica")
def push_ssl_to_replica():
    cfg = db_db.get_raw_db_config()
    replica_host = (cfg.get("replica_host") or "")
    if not replica_host:
        return {"ok": False, "error": "Replica host not configured"}

    local_ssl_dir  = "/etc/mysql/ssl"
    remote_staging = "/tmp/mysql-ssl"
    ssh_user       = state.AZURE_VM_USER
    ssh_tgt        = f"{ssh_user}@{replica_host}"
    sb             = _ssh_base(replica_host)
    steps          = []

    # ── 0. Validate local certs before pushing ────────────────────────────────
    # Catch the 'OpenSSH private key instead of RSA PEM' class of bugs early.
    key_path = f"{local_ssl_dir}/client-key.pem"
    ca_path  = f"{local_ssl_dir}/ca-cert.pem"
    cert_path = f"{local_ssl_dir}/client-cert.pem"
    missing = [p for p in [key_path, ca_path, cert_path] if not Path(p).exists()]
    if missing:
        return {"ok": False,
                "error": f"SSL certs not found: {missing}. Run 'Generate SSL Certs' first.",
                "steps": steps}
    # Verify client-key.pem is RSA/PKCS8 PEM, not OpenSSH format
    try:
        key_text = Path(key_path).read_text()
        if "BEGIN OPENSSH PRIVATE KEY" in key_text:
            return {
                "ok": False,
                "error": (
                    "client-key.pem is an OpenSSH private key, not an RSA/PEM key. "
                    "MariaDB cannot use this format. "
                    "Click 'Generate SSL Certs' to regenerate all certs in the correct format."
                ),
                "steps": steps,
            }
    except Exception as e:
        steps.append({"step": "Validate client-key.pem", "ok": False, "err": str(e)})
    steps.append({"step": "Validate local SSL certs (PEM format)", "ok": True, "err": ""})

    # ── 1. SSH reachability ───────────────────────────────────────────────────
    r = _remote_run(sb, "echo ok", timeout=12)
    steps.append({"step": "SSH connectivity", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"SSH failed: {r['stderr']}", "steps": steps}

    # ── 2. Check if sudoers is already working ──────────────────────────────
    # Test with sudo -n true (non-interactive). If it passes, sudoers is set up.
    # We no longer try to bootstrap sudoers here — that requires writing to
    # /etc/sudoers.d/ which itself needs root. The Check Sudoers step (run
    # manually by the admin before Push SSL) is the correct prerequisite.
    r = _remote_run(sb, "sudo -n true 2>&1; echo exit:$?", timeout=10)
    sudoers_ok = "exit:0" in r["stdout"]
    steps.append({"step": "Check sudoers on replica (sudo -n true)", "ok": sudoers_ok,
                  "err": "" if sudoers_ok else "sudo -n true failed — run Check Sudoers step first"})

    # ── 3. Decide cert destination ────────────────────────────────────────────
    # If sudoers worked → /etc/mysql/ssl (proper system path, owned by mysql)
    # Otherwise → /home/moodlesync/mysql-ssl (moodlesync owns it — no sudo needed)
    # Use /var/lib/mysql/ssl — mysql user OWNS /var/lib/mysql entirely.
    # This is the canonical location per Ubuntu 24.04 MariaDB SSL guides.
    # No chown fighting, no permission issues, no sudo needed for the dir.
    # moodlesync SSHes in, sudo mkdir the subdir, then SCP directly as moodlesync
    # since /var/lib/mysql/ssl will be chowned to mysql after creation.
    # Regardless of sudoers: always use /var/lib/mysql/ssl via sudo mkdir.
    # Fall back to ~/mysql-ssl only if sudo is completely unavailable.
    if sudoers_ok:
        remote_ssl_dir = "/var/lib/mysql/ssl"
    else:
        remote_ssl_dir = f"/home/{ssh_user}/mysql-ssl"

    # ── 4a. Create cert directory ─────────────────────────────────────────────
    if sudoers_ok:
        # /var/lib/mysql is owned by mysql — create subdir and set ownership
        mkdir_cmd = (
            f"sudo mkdir -p {remote_ssl_dir} && "
            f"sudo chown mysql:mysql {remote_ssl_dir} && "
            f"sudo chmod 750 {remote_ssl_dir}"
        )
    else:
        mkdir_cmd = f"mkdir -p {remote_ssl_dir} && chmod 700 {remote_ssl_dir}"
    r = _remote_run(sb, mkdir_cmd, timeout=10)
    steps.append({"step": f"Create {remote_ssl_dir} on replica", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"mkdir {remote_ssl_dir} failed: {r['stderr']}", "steps": steps}

    # ── 4b. Allow mysql to traverse home dir (only needed without sudo) ───────
    if not sudoers_ok:
        r = _remote_run(sb, f"chmod o+x /home/{ssh_user}", timeout=10)
        steps.append({"step": f"chmod o+x /home/{ssh_user}", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})

    # ── 4c. Create /var/log/mysql (best-effort) ───────────────────────────────
    r = _remote_run(sb,
        "sudo mkdir -p /var/log/mysql && sudo chown mysql:mysql /var/log/mysql",
        timeout=15)
    steps.append({"step": "Create /var/log/mysql on replica", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
    # Non-fatal — continue even if this fails

    # ── 5. SCP certs directly to final destination ────────────────────────────
    # /var/lib/mysql/ssl is owned by mysql:mysql (750).
    # moodlesync cannot write there directly — use sudo tee for sudoers path,
    # plain SCP for home dir path (moodlesync owns it).
    if sudoers_ok:
        # Write each cert via base64-embedded command — avoids -tt PTY stdin hang
        for cert in ["ca-cert.pem", "client-cert.pem", "client-key.pem"]:
            try:
                cert_content = Path(f"{local_ssl_dir}/{cert}").read_text()
            except Exception as e:
                return {"ok": False, "error": f"Cannot read local {cert}: {e}", "steps": steps}
            r = _remote_write_file(sb, cert_content,
                f"{remote_ssl_dir}/{cert}",
                use_sudo=True, timeout=15)
            steps.append({"step": f"Write {cert} to {remote_ssl_dir}",
                          "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
            if not r["ok"]:
                return {"ok": False,
                        "error": f"Failed to write {cert}: {r['stderr']}",
                        "steps": steps}
        # Fix ownership + perms (mysql owns the dir, set 640 on files)
        for cert in ["ca-cert.pem", "client-cert.pem", "client-key.pem"]:
            _remote_run(sb,
                f"sudo chown mysql:mysql {remote_ssl_dir}/{cert} && "
                f"sudo chmod 640 {remote_ssl_dir}/{cert}",
                timeout=10)
        steps.append({"step": f"Set cert ownership/permissions in {remote_ssl_dir}",
                      "ok": True, "err": ""})
    else:
        # Home dir — moodlesync owns it, SCP directly
        r = run_cmd([
            "scp", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            f"{local_ssl_dir}/ca-cert.pem",
            f"{local_ssl_dir}/client-cert.pem",
            f"{local_ssl_dir}/client-key.pem",
            f"{ssh_tgt}:{remote_ssl_dir}/",
        ], timeout=30)
        steps.append({"step": f"SCP certs to {remote_ssl_dir}", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
        if not r["ok"]:
            return {"ok": False, "error": f"SCP failed: {r['stderr']}", "steps": steps}
        r = _remote_run(sb, f"chmod 644 {remote_ssl_dir}/*.pem", timeout=10)
        steps.append({"step": f"chmod 644 certs", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})

    # ── 6. Verify files exist (sudo stat — works regardless of dir permissions) ─
    for cert in ["ca-cert.pem", "client-cert.pem", "client-key.pem"]:
        r = _remote_run(sb,
            f"sudo stat {remote_ssl_dir}/{cert} > /dev/null 2>&1 && echo ok || echo missing",
            timeout=10)
        exists = "ok" in r["stdout"]
        steps.append({"step": f"Verify {cert} exists on replica", "ok": exists,
                      "err": "" if exists else f"Not found: {remote_ssl_dir}/{cert}"})
        if not exists:
            return {"ok": False,
                    "error": f"{cert} missing from {remote_ssl_dir} after write",
                    "steps": steps}

    # Persist resolved path for configure_replica
    db_db.update_db_config_field("ssl_remote_dir", remote_ssl_dir)
    db_db.log_audit("push_ssl_to_replica", result="ok",
                    details={"host": replica_host, "ssl_dir": remote_ssl_dir})
    return {"ok": True, "message": f"SSL certs ready at {remote_ssl_dir} on replica",
            "ssl_dir": remote_ssl_dir, "steps": steps}


@router.post("/setup/configure-source")
def configure_source():
    cfg = db_db.get_raw_db_config()
    steps = []

    # ── 1. Detect DB flavour on source ────────────────────────────────────────
    # Dashboard runs on the source server itself, so we connect locally
    conn_s, err_s = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"), (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"), (cfg.get("source_db_password") or "")
    )
    source_is_mariadb = False
    if not err_s:
        try:
            cur = conn_s.cursor()
            cur.execute("SELECT VERSION()")
            v = (cur.fetchone()[0] or "").lower()
            source_is_mariadb = "mariadb" in v
            conn_s.close()
        except Exception:
            pass

    db_type = "mariadb" if source_is_mariadb else "mysql"
    svc_name = "mariadb" if source_is_mariadb else "mysql"
    steps.append({"step": f"Detected source DB type: {db_type}", "ok": True, "err": ""})

    # ── 2. Build flavour-correct config ──────────────────────────────────────
    if source_is_mariadb:
        gtid_lines = "# MariaDB: GTID always on; gtid_strict_mode enforces consistency\ngtid_strict_mode = ON"
        cnf_dir = "/etc/mysql/mariadb.conf.d"
    else:
        gtid_lines = "gtid_mode = ON\nenforce_gtid_consistency = ON"
        cnf_dir = "/etc/mysql/mysql.conf.d"

    # ── SSL cert validation — MUST exist before writing to cnf ───────────────
    # Writing empty or missing cert paths causes MariaDB to fail on startup.
    # Safety rule: never restart source DB with an invalid SSL config.
    # Use 'or' fallback (not .get default) — .get default only fires if key is absent,
    # but DB stores None when unconfigured, so 'or' is required to catch None values.
    ssl_ca   = cfg.get('ssl_ca_path')   or '/etc/mysql/ssl/ca-cert.pem'
    ssl_cert = cfg.get('ssl_cert_path') or '/etc/mysql/ssl/client-cert.pem'
    ssl_key  = cfg.get('ssl_key_path')  or '/etc/mysql/ssl/client-key.pem'

    ssl_paths_valid = all([
        ssl_ca   and Path(ssl_ca).is_file(),
        ssl_cert and Path(ssl_cert).is_file(),
        ssl_key  and Path(ssl_key).is_file(),
    ])

    missing_certs = []
    for label, path in [("ssl_ca", ssl_ca), ("ssl_cert", ssl_cert), ("ssl_key", ssl_key)]:
        if not path:
            missing_certs.append(f"{label}: (empty — not configured)")
        elif not Path(path).is_file():
            missing_certs.append(f"{label}: {path} (file not found)")

    if missing_certs:
        steps.append({"step": "SSL cert validation", "ok": False,
                      "err": "Missing certs: " + "; ".join(missing_certs) +
                             " — SSL lines omitted from cnf to protect production DB"})
        ssl_block = "# SSL: cert files not found — configure SSL paths and re-run Configure Source\n# ssl-ca   = /etc/mysql/ssl/ca-cert.pem\n# ssl-cert = /etc/mysql/ssl/client-cert.pem\n# ssl-key  = /etc/mysql/ssl/client-key.pem"
    else:
        steps.append({"step": "SSL cert validation", "ok": True,
                      "err": f"All 3 cert files verified: {ssl_ca}, {ssl_cert}, {ssl_key}"})
        ssl_block = f"ssl-ca                   = {ssl_ca}\nssl-cert                 = {ssl_cert}\nssl-key                  = {ssl_key}"

    mycnf_additions = f"""# === Moodle DR Source Replication Config (auto-generated) ===
# DB type: {db_type}
[mysqld]
server-id                = 1
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
{gtid_lines}
binlog_do_db             = {cfg.get('moodle_db_name') or 'moodle'}
expire_logs_days         = 7
max_binlog_size          = 100M
{ssl_block}
"""

    # ── 3. Ensure /var/log/mysql exists and is owned by mysql ─────────────────
    # MariaDB does NOT auto-create this; without it binlog startup fails.
    try:
        log_dir = Path("/var/log/mysql")
        log_dir.mkdir(parents=True, exist_ok=True)
        # chown mysql:mysql via subprocess (requires running as root)
        r = run_cmd(["chown", "mysql:mysql", str(log_dir)])
        steps.append({"step": "Create /var/log/mysql + chown mysql:mysql",
                      "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
    except Exception as e:
        steps.append({"step": "Create /var/log/mysql", "ok": False, "err": str(e)})

    # ── 4. Write cnf file ─────────────────────────────────────────────────────
    try:
        mycnf_path = Path(f"{cnf_dir}/moodle-dr-source.cnf")
        if not mycnf_path.parent.exists():
            # Fallback for MySQL layout
            alt = Path("/etc/mysql/mysql.conf.d/moodle-dr-source.cnf")
            alt.parent.mkdir(parents=True, exist_ok=True)
            mycnf_path = alt
        else:
            mycnf_path.parent.mkdir(parents=True, exist_ok=True)
        mycnf_path.write_text(mycnf_additions)
        steps.append({"step": f"Write cnf to {mycnf_path}", "ok": True, "err": ""})
    except Exception as e:
        steps.append({"step": "Write cnf", "ok": False, "err": str(e)})
        db_db.log_audit("configure_source", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e), "steps": steps}

    # ── 5. Auto-restart source DB service ────────────────────────────────────
    # SAFETY: only restart if SSL config is valid (or SSL is not configured).
    # A bad SSL cnf will crash production MariaDB — never restart blindly.
    if not ssl_paths_valid and missing_certs:
        steps.append({"step": f"Restart {svc_name} on source", "ok": False,
                      "err": "Restart skipped — SSL cert paths missing or invalid. "
                             "Fix SSL cert paths then re-run Configure Source. "
                             "Production DB untouched."})
    else:
        r = run_cmd(["systemctl", "restart", svc_name], timeout=30)
        steps.append({"step": f"Restart {svc_name} on source", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
        if r["ok"]:
            # Verify it came up
            r2 = run_cmd(["systemctl", "is-active", svc_name], timeout=10)
            steps.append({"step": f"{svc_name} service active",
                          "ok": "active" in r2["stdout"], "err": r2["stdout"]})

    db_db.log_audit("configure_source", result="ok",
                    details={"db_type": db_type, "cnf": str(mycnf_path)})
    return {
        "ok": True,
        "message": f"Source {db_type} configured at {mycnf_path} and restarted",
        "config": mycnf_additions,
        "db_type": db_type,
        "steps": steps,
    }


@router.post("/setup/configure-replica")
def configure_replica():
    cfg = db_db.get_raw_db_config()
    replica_host = (cfg.get("replica_host") or "")
    if not replica_host:
        return {"ok": False, "error": "Replica host not configured"}

    ssh_user = state.AZURE_VM_USER
    sb       = _ssh_base(replica_host)
    # ssl_remote_dir is saved by push_ssl_to_replica.
    # Canonical location on Ubuntu 24.04 MariaDB is /var/lib/mysql/ssl.
    ssl_dir_raw = (cfg.get("ssl_remote_dir") or "")
    ssl_dir = ssl_dir_raw if ssl_dir_raw else "/var/lib/mysql/ssl"
    steps    = []

    # ── 1. Detect DB flavour via SSH (MariaDB vs MySQL) ───────────────────────
    # Try connecting; fall back to SSH version string if port not open yet
    db_type = _detect_db_type(
        replica_host, (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"), (cfg.get("replica_db_password") or "")
    )
    if db_type == "unknown":
        # Read version string via SSH as fallback
        r = _remote_run(sb, "mariadbd --version 2>&1 || mysqld --version 2>&1", timeout=10)
        db_type = "mariadb" if "mariadb" in r["stdout"].lower() else "mysql"
    steps.append({"step": f"Detected DB type: {db_type}", "ok": True, "err": ""})

    # ── 2. Build DB-flavour-specific config ────────────────────────────────
    if db_type == "mariadb":
        gtid_line     = "gtid_strict_mode         = ON"
        parallel_lines = "slave_parallel_workers   = 4\nslave_parallel_mode      = optimistic"
        svc_name      = "mariadb"
        cnf_dir       = "/etc/mysql/mariadb.conf.d"
    else:
        gtid_line     = "gtid_mode                = ON\nenforce_gtid_consistency = ON"
        parallel_lines = "replica_parallel_workers = 4\nreplica_parallel_type    = LOGICAL_CLOCK"
        svc_name      = "mysql"
        cnf_dir       = "/etc/mysql/mysql.conf.d"

    cnf_path_system = f"{cnf_dir}/moodle-dr-replica.cnf"
    cnf_path_home   = f"/home/{ssh_user}/moodle-dr-replica.cnf"
    staging         = "/tmp/moodle-dr-replica.cnf"

    mycnf_content = f"""# === Moodle DR Replica Config (auto-generated) ===
# DB type: {db_type}
[mysqld]
server-id                = 2
relay-log                = /var/log/mysql/mysql-relay-bin.log
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
{gtid_line}
read_only                = ON
{parallel_lines}
ssl-ca                   = {ssl_dir}/ca-cert.pem
ssl-cert                 = {ssl_dir}/client-cert.pem
ssl-key                  = {ssl_dir}/client-key.pem
"""

    # ── 3. Pre-flight: ensure /var/log/mysql exists + is owned by mysql ──────
    r = _remote_run(sb,
        "sudo mkdir -p /var/log/mysql && sudo chown mysql:mysql /var/log/mysql",
        timeout=15)
    steps.append({"step": "Ensure /var/log/mysql exists", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
    # Non-fatal — continue even if sudo fails (may already exist)

    # ── 4. Write cnf to staging via base64-embedded command ─────────────
    # With -tt PTY, piping via subprocess stdin hangs (EOF never delivered).
    # Embed content as base64 in the command string to avoid stdin entirely.
    r = _remote_write_file(sb, mycnf_content, staging, use_sudo=True, timeout=15)
    # stderr is merged into stdout since -tt PTY fix — use stdout for error details
    write_err = r["stdout"] or r["stderr"] or "unknown error (no output)"
    steps.append({"step": f"Write cnf to {staging}", "ok": r["ok"], "err": write_err})
    if not r["ok"]:
        return {"ok": False, "error": f"Could not write staging cnf: {write_err}", "steps": steps}

    # ── 5. Install cnf to system path via sudo ────────────────────────────
    r = _remote_run(sb,
        f"sudo mkdir -p {cnf_dir} && sudo cp {staging} {cnf_path_system}",
        timeout=15)
    if r["ok"]:
        cnf_final = cnf_path_system
        steps.append({"step": f"Installed cnf to {cnf_path_system}", "ok": True, "err": ""})
    else:
        # Fall back to home dir
        r2 = _remote_run(sb, f"cp {staging} {cnf_path_home}", timeout=10)
        cnf_final = cnf_path_home
        steps.append({"step": f"sudo unavailable — cnf at {cnf_path_home}",
                      "ok": r2["ok"], "err": r["stdout"] or r["stderr"]})
        if not r2["ok"]:
            return {"ok": False, "error": r2["stderr"], "steps": steps}

    # ── 6. Restart DB service automatically ────────────────────────────────
    if cnf_final == cnf_path_system:
        r = _remote_run(sb, f"sudo systemctl restart {svc_name}", timeout=30)
        steps.append({"step": f"Restart {svc_name} on replica", "ok": r["ok"], "err": r["stdout"] or r["stderr"]})
        if r["ok"]:
            # Verify it came up
            r2 = _remote_run(sb, f"systemctl is-active {svc_name}", timeout=10)
            steps.append({"step": f"{svc_name} service status",
                          "ok": "active" in r2["stdout"],
                          "err": r2["stdout"]})

    db_db.log_audit("configure_replica", result="ok",
                    details={"host": replica_host, "cnf": cnf_final, "db_type": db_type})
    needs_manual = cnf_final != cnf_path_system
    msg = (
        f"cnf saved to {cnf_path_home}. Run manually on replica: "
        f"sudo cp {cnf_path_home} {cnf_path_system} && sudo systemctl restart {svc_name}"
        if needs_manual else
        f"{db_type} replica configured and restarted successfully"
    )
    return {"ok": True, "message": msg, "cnf_path": cnf_final,
            "db_type": db_type, "steps": steps}


@router.post("/setup/auto-configure")
async def auto_configure(background_tasks: BackgroundTasks):
    """Run all setup steps sequentially and stream results."""
    steps = [
        ("create_repl_user", "Create replication user"),
        ("generate_ssl_certs", "Generate SSL certificates"),
        ("push_ssl_to_replica", "Push SSL certs to replica"),
        ("configure_source", "Configure source MySQL"),
        ("configure_replica", "Configure replica MySQL"),
    ]
    results = []
    for func_name, label in steps:
        try:
            if func_name == "create_repl_user":
                r = create_repl_user()
            elif func_name == "generate_ssl_certs":
                r = generate_ssl_certs()
            elif func_name == "push_ssl_to_replica":
                r = push_ssl_to_replica()
            elif func_name == "configure_source":
                r = configure_source()
            elif func_name == "configure_replica":
                r = configure_replica()
            else:
                r = {"ok": False, "error": "Unknown step"}
        except Exception as e:
            r = {"ok": False, "error": str(e)}
        results.append({"step": label, "ok": r.get("ok", False), "detail": r})
    ok = all(s["ok"] for s in results)
    return {"ok": ok, "steps": results}


@router.get("/setup/status")
def setup_status():
    cfg = db_db.get_db_config()
    return {
        "configured": bool(cfg),
        "config": cfg,
        "ssl_exists": Path("/etc/mysql/ssl/ca-cert.pem").exists(),
        "mycnf_exists": Path("/etc/mysql/mysql.conf.d/moodle-dr-replication.cnf").exists(),
    }


# ── Seed endpoints ────────────────────────────────────────────────────────────

async def _run_seed_job():
    """Background task: seed replica database via mysqldump or xtrabackup."""
    cfg = db_db.get_raw_db_config()
    db_name = (cfg.get("moodle_db_name") or "moodle")
    seed_method = (cfg.get("seed_method") or "mysqldump")

    state.db_seed_job.update({
        "running": True, "phase": "starting", "progress": 0,
        "started_at": datetime.utcnow().isoformat(), "ended_at": None,
        "result": None, "error": "", "output": [],
        "dump_size_human": "", "gtid_purged": "",
    })

    try:
        # ── Pre-flight checks ──────────────────────────────────────────────────
        state.db_seed_job["phase"] = "preflight"
        state.db_seed_job["output"].append("Running pre-flight checks...")
        replica_host = (cfg.get("replica_host") or "")
        replica_user = (cfg.get("replica_db_user") or "root")
        replica_pass = (cfg.get("replica_db_password") or "")
        source_host  = (cfg.get("source_host") or "127.0.0.1")
        source_user  = (cfg.get("source_db_user") or "root")
        source_pass  = (cfg.get("source_db_password") or "")

        if not replica_host:
            raise Exception("Pre-flight failed: replica_host not configured in DB Settings")
        if not replica_user:
            raise Exception("Pre-flight failed: replica_db_user not configured")
        if not replica_pass:
            raise Exception("Pre-flight failed: replica_db_password not configured")
        if not source_pass:
            raise Exception("Pre-flight failed: source_db_password not configured")

        # Test source DB connection (using mysql.connector — always installed)
        try:
            import mysql.connector as _mc
            _sc = _mc.connect(host=source_host, port=int(cfg.get("source_port") or 3306),
                              user=source_user, password=source_pass,
                              database=db_name, connect_timeout=10)
            _sc.close()
            state.db_seed_job["output"].append(f"✓ Source DB connection OK ({source_host})")
        except Exception as e:
            raise Exception(f"Pre-flight failed: cannot connect to source DB: {e}")

        # Test replica SSH
        ssh_test = await asyncio.create_subprocess_exec(
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            f"moodlesync@{replica_host}", "echo ok",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, ssh_err = await ssh_test.communicate()
        if ssh_test.returncode != 0:
            raise Exception(f"Pre-flight failed: SSH to replica failed: {ssh_err.decode()[:200]}")
        state.db_seed_job["output"].append(f"✓ SSH to replica OK ({replica_host})")

        # Write a temp .my.cnf on replica so passwords with special chars work safely.
        # We base64-encode the content here, pass it as a plain env-safe string via
        # stdin using 'base64 -d', then write it — no shell interpolation of passwords.
        # host=127.0.0.1 forces TCP — avoids Unix socket which maps to @'localhost'
        # The sqlmoodle account was created with @'%' (covers TCP, not socket)
        mycnf_content = (
            f"[client]\nuser={replica_user}\npassword={replica_pass}\n"
            f"host=127.0.0.1\nport=3306\n"
            f"[mysql]\nuser={replica_user}\npassword={replica_pass}\n"
            f"host=127.0.0.1\nport=3306\n"
        )
        import base64 as _b64
        mycnf_b64 = _b64.b64encode(mycnf_content.encode()).decode()
        write_mycnf = await asyncio.create_subprocess_exec(
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
            f"moodlesync@{replica_host}",
            # Pass the b64 string as the remote command argument — it is pure base64
            # (A-Z a-z 0-9 + / =) so no shell quoting issues at all.
            f"echo '{mycnf_b64}' | base64 -d > ~/.moodle_dr_my.cnf && chmod 600 ~/.moodle_dr_my.cnf",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, wcnf_err = await write_mycnf.communicate()
        if write_mycnf.returncode not in (0, None):
            raise Exception(f"Pre-flight failed: could not write .my.cnf on replica: {wcnf_err.decode()[:200]}")
        state.db_seed_job["output"].append("✓ Credentials file written on replica (.moodle_dr_my.cnf)")

        # Test replica DB connection using the safe .my.cnf (TCP via 127.0.0.1)
        # .my.cnf contains host=127.0.0.1 so mysql uses TCP not Unix socket.
        # @'%' grants cover TCP; socket connections map to @'localhost'.
        replica_test = await asyncio.create_subprocess_exec(
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
            f"moodlesync@{replica_host}",
            "mysql --defaults-file=~/.moodle_dr_my.cnf -e 'SELECT 1' 2>&1; echo exit:$?",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        rt_out, _ = await replica_test.communicate()
        rt_str = rt_out.decode(errors='replace')
        if "exit:0" not in rt_str:
            # Auto-fix: if Access denied @'localhost', the user exists with @'%' but
            # not @'localhost'. Grant @'localhost' via root (sudo mysql) and retry.
            if "Access denied" in rt_str and "localhost" in rt_str:
                state.db_seed_job["output"].append(
                    "⚠ Replica DB: @'localhost' grant missing — auto-fixing via admmoodle..."
                )
                # ── Step A: update the sudoers file via admmoodle (has full sudo) ──
                # This ensures moodlesync can run mysql/mariadb via sudo going forward.
                admin_user = getattr(state, 'ADMIN_VM_USER', 'admmoodle')
                sudoers_rule = SUDOERS_RULE.format(user=state.AZURE_VM_USER)
                sudoers_b64 = __import__('base64').b64encode(sudoers_rule.encode()).decode()
                sudoers_cmd = (
                    f"echo '{sudoers_b64}' | base64 -d | sudo tee {SUDOERS_FILE} > /dev/null && "
                    f"sudo chmod 440 {SUDOERS_FILE} && sudo visudo -c -f {SUDOERS_FILE} && echo sudoers_ok"
                )
                sud_proc = await asyncio.create_subprocess_exec(
                    "ssh", "-i", state.SSH_KEY_PATH,
                    "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                    f"{admin_user}@{replica_host}", sudoers_cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                sud_out, sud_err = await sud_proc.communicate()
                if "sudoers_ok" in sud_out.decode(errors='replace'):
                    state.db_seed_job["output"].append("✓ Sudoers rule updated on replica (mysql/mariadb added)")
                else:
                    state.db_seed_job["output"].append(
                        f"⚠ Sudoers update warning: {sud_err.decode()[:100]} — continuing anyway"
                    )

                # ── Step B: create @'localhost' grant via admmoodle + sudo mariadb ──
                grant_sql = (
                    f"GRANT ALL PRIVILEGES ON *.* TO '{replica_user}'@'localhost' "
                    f"IDENTIFIED BY '{replica_pass}' WITH GRANT OPTION; "
                    f"FLUSH PRIVILEGES;"
                )
                grant_b64 = __import__('base64').b64encode(grant_sql.encode()).decode()
                grant_cmd = (
                    f"echo '{grant_b64}' | base64 -d > /tmp/.mdr_grant.sql && "
                    f"(sudo mariadb < /tmp/.mdr_grant.sql 2>/dev/null || "
                    f" sudo mysql   < /tmp/.mdr_grant.sql 2>/dev/null); "
                    f"EC=$?; rm -f /tmp/.mdr_grant.sql; exit $EC"
                )
                fix_proc = await asyncio.create_subprocess_exec(
                    "ssh", "-i", state.SSH_KEY_PATH,
                    "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                    f"{admin_user}@{replica_host}", grant_cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                _, fix_err = await fix_proc.communicate()
                if fix_proc.returncode != 0:
                    raise Exception(
                        f"Pre-flight failed: auto-fix grant failed via {admin_user}. "
                        f"Ensure the SSH key works for {admin_user}@{replica_host} "
                        f"and that {admin_user} has passwordless sudo.\n"
                        f"Detail: {fix_err.decode()[:300]}"
                    )
                state.db_seed_job["output"].append(f"✓ @'localhost' grant created on replica via {admin_user}")
            else:
                raise Exception(f"Pre-flight failed: cannot connect to replica DB: {rt_str[:300]}")
        state.db_seed_job["output"].append(f"✓ Replica DB connection OK ({replica_host})")

        # ── Check if a previous dump already landed on the replica ────────────
        # If /tmp/moodle_seed.sql exists AND is non-zero, we can skip the
        # expensive dump+transfer and jump straight to import.
        REPLICA_DUMP = "/tmp/moodle_seed.sql"
        check_dump = await asyncio.create_subprocess_exec(
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
            f"moodlesync@{replica_host}",
            f"test -s {REPLICA_DUMP} && stat --printf='%s' {REPLICA_DUMP} || echo MISSING",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        chk_out, _ = await check_dump.communicate()
        chk_str = chk_out.decode(errors='replace').strip()
        dump_exists = chk_str != "MISSING" and chk_str.isdigit() and int(chk_str) > 0

        if dump_exists:
            dump_bytes = int(chk_str)
            dump_human = f"{dump_bytes/1024/1024:.1f} MB" if dump_bytes < 1024**3 else f"{dump_bytes/1024/1024/1024:.2f} GB"
            state.db_seed_job["dump_size_human"] = dump_human
            state.db_seed_job["output"].append(
                f"✓ Found existing dump on replica ({dump_human}) — resuming from import phase"
            )
            state.db_seed_job["output"].append("✓ All pre-flight checks passed (RESUME mode)")
        else:
            state.db_seed_job["output"].append("✓ All pre-flight checks passed (FRESH seed)")

        state.db_seed_job["progress"] = 5

        if seed_method == "mysqldump":
            await _seed_mysqldump(cfg, db_name, skip_dump=dump_exists)
        else:
            await _seed_xtrabackup(cfg, db_name)
    except Exception as e:
        state.db_seed_job.update({
            "running": False, "result": "error", "error": str(e),
            "ended_at": datetime.utcnow().isoformat(), "progress": 0,
        })
        db_db.log_audit("seed_database", result="error", details={"error": str(e)})


async def _seed_mysqldump(cfg: dict, db_name: str, skip_dump: bool = False):
    source_host = (cfg.get("source_host") or "127.0.0.1")
    source_port = (cfg.get("source_port") or 3306)
    source_user = (cfg.get("source_db_user") or "root")
    source_pass = (cfg.get("source_db_password") or "")
    replica_host = (cfg.get("replica_host") or "")
    replica_user = (cfg.get("replica_db_user") or "root")
    replica_pass = (cfg.get("replica_db_password") or "")

    # Fixed remote dump path — consistent so resume detection always finds it
    REPLICA_DUMP = "/tmp/moodle_seed.sql"
    dump_path = f"/tmp/moodle_seed_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.sql"
    dump_file = Path(dump_path)

    if skip_dump:
        # ── RESUME: dump already on replica — skip straight to import ─────────
        state.db_seed_job["phase"] = "importing"
        state.db_seed_job["progress"] = 65
        state.db_seed_job["output"].append(
            f"Resuming: skipping dump+transfer, importing existing {REPLICA_DUMP} ..."
        )
    else:
        state.db_seed_job["phase"] = "dumping"
        state.db_seed_job["progress"] = 10
        state.db_seed_job["output"].append(f"Starting mysqldump of {db_name}...")

    try:
        if not skip_dump:
            # ── Detect source DB type ─────────────────────────────────────────────
            src_is_mariadb = "mariadb" in (cfg.get("source_db_version") or "").lower()
            if not src_is_mariadb:
                try:
                    import mysql.connector as _mc2
                    _c = _mc2.connect(host=source_host, port=int(source_port),
                                      user=source_user, password=source_pass, connect_timeout=5)
                    _v = _c.get_server_info().lower()
                    _c.close()
                    src_is_mariadb = "mariadb" in _v
                except Exception:
                    src_is_mariadb = True

            if src_is_mariadb:
                dump_cmd = [
                    "mysqldump",
                    f"-h{source_host}", f"-P{str(source_port)}",
                    f"-u{source_user}", f"-p{source_pass}",
                    "--single-transaction", "--master-data=2",
                    "--routines", "--triggers", "--events",
                    db_name,
                ]
            else:
                dump_cmd = [
                    "mysqldump",
                    f"-h{source_host}", f"-P{str(source_port)}",
                    f"-u{source_user}", f"-p{source_pass}",
                    "--single-transaction", "--master-data=2",
                    "--set-gtid-purged=ON", "--gtid",
                    "--routines", "--triggers", "--events",
                    db_name,
                ]

            # ── Stream mysqldump → disk (1 MB chunks — never buffer full dump in RAM) ──
            state.db_seed_job["output"].append(f"Streaming mysqldump to {dump_file} ...")
            dump_proc = await asyncio.create_subprocess_exec(
                *dump_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            import aiofiles
            async with aiofiles.open(str(dump_file), 'wb') as f:
                while True:
                    chunk = await dump_proc.stdout.read(1024 * 1024)
                    if not chunk:
                        break
                    await f.write(chunk)
                    try:
                        sz = dump_file.stat().st_size
                        state.db_seed_job["dump_size_human"] = f"{sz/1024/1024:.0f} MB"
                    except Exception:
                        pass

            _, stderr_bytes = await dump_proc.communicate()
            if dump_proc.returncode != 0:
                raise Exception(f"mysqldump failed: {stderr_bytes.decode(errors='replace')[:500]}")

            size = dump_file.stat().st_size
            size_human = f"{size/1024/1024:.1f} MB" if size < 1024**3 else f"{size/1024/1024/1024:.2f} GB"
            state.db_seed_job["dump_size_human"] = size_human
            state.db_seed_job["progress"] = 40
            state.db_seed_job["phase"] = "transferring"
            state.db_seed_job["output"].append(f"Dump complete: {size_human} — transferring to replica...")

            # ── SCP dump file to replica ──────────────────────────────────────────
            scp_cmd = [
                "scp",
                "-i", state.SSH_KEY_PATH,
                "-o", "StrictHostKeyChecking=accept-new",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=30",
                str(dump_file),
                f"moodlesync@{replica_host}:{REPLICA_DUMP}"
            ]
            scp_proc = await asyncio.create_subprocess_exec(*scp_cmd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, scp_err = await scp_proc.communicate()
            if scp_proc.returncode != 0:
                raise Exception(f"SCP failed: {scp_err.decode(errors='replace')[:300]}")

        # ── Import phase (always runs — fresh or resume) ───────────────────────
        state.db_seed_job["progress"] = 65
        state.db_seed_job["phase"] = "importing"
        state.db_seed_job["output"].append("Importing dump on replica (this may take a while)...")

        # ── Import on replica via SSH ─────────────────────────────────────────
        # IMPORTANT: we NEVER use -p<password> on the shell — passwords with @, !
        # and other special chars break every shell quoting strategy. Instead we
        # use ~/.moodle_dr_my.cnf written during pre-flight (pure base64 decode).

        # Create DB first if it doesn't exist
        create_db_cmd = [
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=30",
            f"moodlesync@{replica_host}",
            f"mysql --defaults-file=~/.moodle_dr_my.cnf -e "
            f"'CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;'"
        ]
        cdb = await asyncio.create_subprocess_exec(*create_db_cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, cdb_err = await cdb.communicate()
        if cdb.returncode != 0:
            state.db_seed_job["output"].append(f"WARN create DB: {cdb_err.decode(errors='replace')[:200]}")

        # Run import — use --defaults-file, pipe SQL in via a sub-shell
        # The pipeline runs entirely inside the SSH remote shell, so mysql reads
        # from stdin (/tmp/moodle_seed.sql). No password on the command line.
        import_cmd = [
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=30",
            f"moodlesync@{replica_host}",
            f"mysql --defaults-file=~/.moodle_dr_my.cnf {db_name} < {REPLICA_DUMP} && rm -f {REPLICA_DUMP}"
        ]
        imp_proc = await asyncio.create_subprocess_exec(*import_cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, imp_err = await imp_proc.communicate()
        if imp_proc.returncode != 0:
            raise Exception(f"Import failed: {imp_err.decode(errors='replace')[:400]}")

        state.db_seed_job["progress"] = 90
        state.db_seed_job["phase"] = "configuring"
        state.db_seed_job["output"].append("Import complete — configuring replication...")

        # ── Run CHANGE MASTER TO ──────────────────────────────────────────────
        repl_cfg = db_db.get_raw_db_config()
        repl_user_name  = (repl_cfg.get("repl_user") or "moodledbsync")
        repl_user_pass  = (repl_cfg.get("repl_password") or "")
        source_host_val = (repl_cfg.get("source_host") or "127.0.0.1")
        source_port_val = int(repl_cfg.get("source_port") or 3306)

        change_master_sql = (
            f"STOP SLAVE; RESET SLAVE ALL; "
            f"CHANGE MASTER TO "
            f"MASTER_HOST='{source_host_val}', "
            f"MASTER_PORT={source_port_val}, "
            f"MASTER_USER='{repl_user_name}', "
            f"MASTER_PASSWORD='{repl_user_pass}', "
            f"MASTER_USE_GTID=slave_pos, "
            f"MASTER_SSL=1, "
            f"MASTER_SSL_CA='/var/lib/mysql/ssl/ca-cert.pem', "
            f"MASTER_SSL_CERT='/var/lib/mysql/ssl/client-cert.pem', "
            f"MASTER_SSL_KEY='/var/lib/mysql/ssl/client-key.pem'; "
            f"START SLAVE;"
        )
        # Use --defaults-file here too — CHANGE MASTER SQL contains single quotes
        # (MASTER_PASSWORD='...') so we must NOT also have -p'...' on the CLI.
        # Write change_master_sql to a temp file on the replica, then source it.
        cm_sql_b64 = _b64.b64encode(change_master_sql.encode()).decode()
        cm_cmd = [
            "ssh", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            f"moodlesync@{replica_host}",
            (
                f"echo '{cm_sql_b64}' | base64 -d > /tmp/.mdr_cm.sql && "
                f"mysql --defaults-file=~/.moodle_dr_my.cnf < /tmp/.mdr_cm.sql; "
                f"EC=$?; rm -f /tmp/.mdr_cm.sql; exit $EC"
            )
        ]
        cm_proc = await asyncio.create_subprocess_exec(*cm_cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, cm_err = await cm_proc.communicate()
        if cm_proc.returncode != 0:
            state.db_seed_job["output"].append(f"WARN: CHANGE MASTER TO: {cm_err.decode(errors='replace')[:200]}")
        else:
            state.db_seed_job["output"].append("Replication configured and started.")

        state.db_seed_job["progress"] = 100
        state.db_seed_job["phase"] = "done"
        state.db_seed_job["running"] = False
        state.db_seed_job["result"] = "ok"
        state.db_seed_job["ended_at"] = datetime.utcnow().isoformat()
        state.db_seed_job["output"].append("✓ Seed complete! Go to DB Replication Status and verify IO/SQL threads.")
        dump_file.unlink(missing_ok=True)
        # Clean up credentials file from replica
        try:
            await asyncio.create_subprocess_exec(
                "ssh", "-i", state.SSH_KEY_PATH,
                "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                f"moodlesync@{replica_host}",
                "rm -f ~/.moodle_dr_my.cnf",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        except Exception:
            pass
        db_db.log_audit("seed_database", result="ok", details={"method": "mysqldump", "db": db_name, "size": size_human})

    except Exception as e:
        dump_file.unlink(missing_ok=True)
        # Clean up credentials file from replica on failure too
        try:
            _rh = (cfg.get("replica_host") or "")
            if _rh:
                await asyncio.create_subprocess_exec(
                    "ssh", "-i", state.SSH_KEY_PATH,
                    "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                    f"moodlesync@{_rh}",
                    "rm -f ~/.moodle_dr_my.cnf",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
        except Exception:
            pass
        state.db_seed_job.update({
            "running": False, "result": "error", "error": str(e),
            "ended_at": datetime.utcnow().isoformat(),
        })
        state.db_seed_job["output"].append(f"ERROR: {e}")
        db_db.log_audit("seed_database", result="error", details={"error": str(e)})


async def _seed_xtrabackup(cfg: dict, db_name: str):
    state.db_seed_job["output"].append("XtraBackup seed method — running xtrabackup --backup...")
    state.db_seed_job["phase"] = "xtrabackup"
    state.db_seed_job["progress"] = 20
    # Simplified placeholder — real implementation would run xtrabackup
    await asyncio.sleep(2)
    state.db_seed_job["output"].append("XtraBackup not available in this environment — use mysqldump method")
    state.db_seed_job["running"] = False
    state.db_seed_job["result"] = "error"
    state.db_seed_job["error"] = "XtraBackup method requires percona-xtrabackup installed"
    state.db_seed_job["ended_at"] = datetime.utcnow().isoformat()


@router.post("/seed/start")
async def seed_start(background_tasks: BackgroundTasks):
    if state.db_seed_job.get("running"):
        return {"ok": False, "error": "Seed job already running"}
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No DB configuration saved"}
    background_tasks.add_task(_run_seed_job)
    return {"ok": True, "message": "Seed job started"}


@router.get("/seed/status")
def seed_status():
    return state.db_seed_job


@router.post("/seed/start-replication")
def seed_start_replication():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration saved"}

    replica_host = (cfg.get("replica_host") or "")
    repl_user = (cfg.get("repl_user") or "repl_user")
    repl_pass = (cfg.get("repl_password") or "")
    source_host = (cfg.get("source_host") or "127.0.0.1")
    source_port = (cfg.get("source_port") or 3306)
    ssl_ca = (cfg.get("ssl_ca_path") or "/etc/mysql/ssl/ca-cert.pem")
    ssl_cert = (cfg.get("ssl_cert_path") or "/etc/mysql/ssl/client-cert.pem")
    ssl_key = (cfg.get("ssl_key_path") or "/etc/mysql/ssl/client-key.pem")

    conn, err = _get_mysql_conn(
        replica_host,
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        db_db.log_audit("start_replication", result="error", details={"error": err})
        return {"ok": False, "error": err}

    try:
        cursor = conn.cursor()
        # Stop any existing replication
        try:
            cursor.execute("STOP REPLICA")
        except Exception:
            try:
                cursor.execute("STOP SLAVE")
            except Exception:
                pass

        # Configure replication source
        change_cmd = f"""CHANGE REPLICATION SOURCE TO
            SOURCE_HOST='{source_host}',
            SOURCE_PORT={source_port},
            SOURCE_USER='{repl_user}',
            SOURCE_PASSWORD='{repl_pass}',
            SOURCE_SSL=1,
            SOURCE_SSL_CA='{ssl_ca}',
            SOURCE_SSL_CERT='{ssl_cert}',
            SOURCE_SSL_KEY='{ssl_key}',
            SOURCE_AUTO_POSITION=1"""

        try:
            cursor.execute(change_cmd)
        except Exception:
            # Try old syntax
            change_cmd_old = f"""CHANGE MASTER TO
                MASTER_HOST='{source_host}',
                MASTER_PORT={source_port},
                MASTER_USER='{repl_user}',
                MASTER_PASSWORD='{repl_pass}',
                MASTER_SSL=1,
                MASTER_SSL_CA='{ssl_ca}',
                MASTER_SSL_CERT='{ssl_cert}',
                MASTER_SSL_KEY='{ssl_key}',
                MASTER_AUTO_POSITION=1"""
            cursor.execute(change_cmd_old)

        # Start replica
        try:
            cursor.execute("START REPLICA")
        except Exception:
            cursor.execute("START SLAVE")

        conn.commit()
        conn.close()

        db_db.log_audit("start_replication", result="ok",
                        details={"source": source_host, "user": repl_user})
        db_db.log_event("replication_started", notes=f"GTID replication started to {source_host}")
        return {"ok": True, "message": "GTID replication started", "change_cmd": change_cmd}
    except Exception as e:
        db_db.log_audit("start_replication", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.get("/seed/verify")
def seed_verify():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    status = _query_replica_status(cfg)
    checks = {
        "replica_status_ok": status.get("connected", False) and not status.get("error"),
        "io_running": status.get("io_running") == "Yes",
        "sql_running": status.get("sql_running") == "Yes",
        "lag_acceptable": (status.get("lag_seconds") or 999) < 60,
    }
    return {"ok": all(checks.values()), "checks": checks, "status": status}


# ── Health endpoints ──────────────────────────────────────────────────────────

@router.get("/health")
def get_db_health():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No DB configuration", "status": "unconfigured"}

    status = _query_replica_status(cfg)
    if "error" in status and not status.get("connected"):
        state.db_replication_status.update({
            "status": "error", "last_checked": datetime.utcnow().isoformat(),
        })
        return {"ok": False, "error": status["error"], "status": "error"}

    io_running = status.get("io_running")
    sql_running = status.get("sql_running")
    lag = status.get("lag_seconds")
    last_io_error = status.get("last_io_error", "")
    last_sql_error = status.get("last_sql_error", "")

    status_str = _determine_status(io_running, sql_running, lag, last_io_error, last_sql_error)

    # Compute pending GTIDs count
    retrieved = status.get("retrieved_gtid_set", "") or ""
    executed = status.get("executed_gtid_set", "") or ""
    pending_count = len(retrieved) - len(executed) if retrieved and executed else 0

    snap = {
        "sampled_at": datetime.utcnow().isoformat(),
        "io_running": io_running,
        "sql_running": sql_running,
        "lag_seconds": lag,
        "retrieved_gtid_set": retrieved,
        "executed_gtid_set": executed,
        "pending_gtid_count": max(0, pending_count),
        "last_io_error": last_io_error,
        "last_sql_error": last_sql_error,
        "source_uuid": status.get("source_uuid", ""),
        "ssl_allowed": status.get("ssl_allowed", ""),
        "auto_position": status.get("auto_position", 0),
        "heartbeats_received": status.get("heartbeats_received", 0),
        "heartbeat_lag": None,
        "status": status_str,
    }
    db_db.save_health_snapshot(snap)

    state.db_replication_status.update({
        "io_running": io_running,
        "sql_running": sql_running,
        "lag_seconds": lag,
        "status": status_str,
        "last_io_error": last_io_error,
        "last_sql_error": last_sql_error,
        "last_checked": snap["sampled_at"],
        "heartbeat_lag": None,
    })

    return {"ok": True, **snap}


@router.get("/health/history")
def get_health_history(n: int = 288):
    history = db_db.get_health_history(n)
    return {"ok": True, "history": history, "count": len(history)}


@router.get("/health/gtid-lag")
def get_gtid_lag():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    status = _query_replica_status(cfg)
    retrieved = status.get("retrieved_gtid_set", "") or ""
    executed = status.get("executed_gtid_set", "") or ""
    return {
        "ok": True,
        "retrieved_gtid_set": retrieved,
        "executed_gtid_set": executed,
        "lag_seconds": status.get("lag_seconds"),
        "auto_position": status.get("auto_position"),
    }


@router.get("/health/errors")
def get_health_errors():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    status = _query_replica_status(cfg)
    return {
        "ok": True,
        "last_io_error": status.get("last_io_error", ""),
        "last_sql_error": status.get("last_sql_error", ""),
        "io_running": status.get("io_running"),
        "sql_running": status.get("sql_running"),
    }


@router.get("/health/source-status")
def get_source_status():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW MASTER STATUS")
        row = cursor.fetchone() or {}
        cursor.execute("SELECT @@gtid_mode, @@server_id, @@server_uuid")
        sr = cursor.fetchone() or {}
        conn.close()
        return {"ok": True, "master_status": row, "server_info": sr}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Replication control endpoints ─────────────────────────────────────────────

@router.post("/replication/stop")
def stop_replication():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("STOP REPLICA")
        except Exception:
            cursor.execute("STOP SLAVE")
        conn.commit()
        conn.close()
        db_db.log_audit("stop_replication", result="ok")
        db_db.log_event("replication_stopped", notes="Operator stopped replication")
        return {"ok": True, "message": "Replication stopped"}
    except Exception as e:
        db_db.log_audit("stop_replication", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/replication/start")
def start_replication():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("START REPLICA")
        except Exception:
            cursor.execute("START SLAVE")
        conn.commit()
        conn.close()
        db_db.log_audit("start_replication", result="ok")
        db_db.log_event("replication_started", notes="Operator started replication")
        return {"ok": True, "message": "Replication started"}
    except Exception as e:
        db_db.log_audit("start_replication", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/replication/reset")
def reset_replication():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("STOP REPLICA")
        except Exception:
            try:
                cursor.execute("STOP SLAVE")
            except Exception:
                pass
        try:
            cursor.execute("RESET REPLICA ALL")
        except Exception:
            cursor.execute("RESET SLAVE ALL")
        conn.commit()
        conn.close()
        db_db.log_audit("reset_replication", result="ok")
        db_db.log_event("replication_reset", notes="Operator reset replica")
        return {"ok": True, "message": "Replication reset"}
    except Exception as e:
        db_db.log_audit("reset_replication", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/replication/flush-logs")
def flush_logs():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("FLUSH BINARY LOGS")
        conn.commit()
        conn.close()
        db_db.log_audit("flush_logs", result="ok")
        return {"ok": True, "message": "Binary logs flushed on source"}
    except Exception as e:
        db_db.log_audit("flush_logs", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


# ── Recovery endpoints ────────────────────────────────────────────────────────

@router.get("/recovery/errant-gtids")
def get_errant_gtids():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}

    # Get source executed GTIDs
    src_conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": f"Source connection failed: {err}"}

    # Get replica executed GTIDs
    rep_conn, err2 = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err2:
        return {"ok": False, "error": f"Replica connection failed: {err2}"}

    try:
        c1 = src_conn.cursor()
        c1.execute("SELECT @@global.gtid_executed")
        src_gtid = c1.fetchone()[0]
        src_conn.close()

        c2 = rep_conn.cursor()
        c2.execute("SELECT @@global.gtid_executed")
        rep_gtid = c2.fetchone()[0]

        # Check for errant GTIDs using GTID_SUBTRACT
        c2.execute(f"SELECT GTID_SUBTRACT('{rep_gtid}', '{src_gtid}')")
        errant = c2.fetchone()[0]
        rep_conn.close()

        has_errant = bool(errant and errant.strip())
        return {
            "ok": True,
            "errant_gtids": errant or "",
            "has_errant": has_errant,
            "source_gtid_executed": src_gtid,
            "replica_gtid_executed": rep_gtid,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/recovery/relay-log-reset")
def relay_log_reset():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("STOP REPLICA SQL_THREAD")
        cursor.execute("RESET REPLICA")
        cursor.execute("START REPLICA SQL_THREAD")
        conn.commit()
        conn.close()
        db_db.log_audit("relay_log_reset", result="ok")
        return {"ok": True, "message": "Relay log reset completed"}
    except Exception as e:
        db_db.log_audit("relay_log_reset", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/recovery/inject-empty-txn")
def inject_empty_txn(body: dict):
    """Inject empty transaction to skip an errant GTID."""
    gtid = body.get("gtid", "")
    if not gtid:
        return {"ok": False, "error": "GTID required"}
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("STOP REPLICA")
        cursor.execute(f"SET GTID_NEXT='{gtid}'")
        cursor.execute("BEGIN")
        cursor.execute("COMMIT")
        cursor.execute("SET GTID_NEXT='AUTOMATIC'")
        cursor.execute("START REPLICA")
        conn.commit()
        conn.close()
        db_db.log_audit("inject_empty_txn", result="ok", details={"gtid": gtid})
        return {"ok": True, "message": f"Empty transaction injected for GTID {gtid}"}
    except Exception as e:
        db_db.log_audit("inject_empty_txn", result="error", details={"error": str(e), "gtid": gtid})
        return {"ok": False, "error": str(e)}


@router.post("/recovery/increase-workers")
def increase_workers(body: dict = None):
    workers = (body or {}).get("workers", 8)
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    conn, err = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
    )
    if err:
        return {"ok": False, "error": err}
    try:
        cursor = conn.cursor()
        cursor.execute("STOP REPLICA SQL_THREAD")
        cursor.execute(f"SET GLOBAL replica_parallel_workers = {workers}")
        cursor.execute("SET GLOBAL replica_parallel_type = 'LOGICAL_CLOCK'")
        cursor.execute("START REPLICA SQL_THREAD")
        conn.commit()
        conn.close()
        db_db.log_audit("increase_workers", result="ok", details={"workers": workers})
        return {"ok": True, "message": f"Parallel workers set to {workers}"}
    except Exception as e:
        db_db.log_audit("increase_workers", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


# ── Integrity endpoints ───────────────────────────────────────────────────────

MOODLE_TABLES = [
    "mdl_user", "mdl_course", "mdl_enrol", "mdl_user_enrolments",
    "mdl_grade_grades", "mdl_assign_submission", "mdl_files",
    "mdl_forum_posts", "mdl_quiz_attempts"
]


@router.post("/integrity/table-count")
def table_count_check():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    db_name = (cfg.get("moodle_db_name") or "moodle")

    src_conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
        db=db_name,
    )
    rep_conn, err2 = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
        db=db_name,
    )

    if err:
        return {"ok": False, "error": f"Source: {err}"}
    if err2:
        return {"ok": False, "error": f"Replica: {err2}"}

    results = []
    try:
        c1 = src_conn.cursor()
        c2 = rep_conn.cursor()
        for table in MOODLE_TABLES:
            try:
                c1.execute(f"SELECT COUNT(*) FROM `{table}`")
                src_count = c1.fetchone()[0]
            except Exception:
                src_count = None
            try:
                c2.execute(f"SELECT COUNT(*) FROM `{table}`")
                rep_count = c2.fetchone()[0]
            except Exception:
                rep_count = None
            delta = None
            if src_count is not None and rep_count is not None:
                delta = src_count - rep_count
            status = "ok" if delta == 0 else ("warn" if abs(delta or 0) < 100 else "drift")
            results.append({
                "table": table,
                "source": src_count,
                "replica": rep_count,
                "delta": delta,
                "status": status,
            })
        src_conn.close()
        rep_conn.close()
        drift = any(r["status"] != "ok" for r in results)
        db_db.log_audit("table_count_check", result="drift" if drift else "ok",
                        details={"tables": len(results), "drift": drift})
        return {"ok": True, "results": results, "drift_detected": drift}
    except Exception as e:
        db_db.log_audit("table_count_check", result="error", details={"error": str(e)})
        return {"ok": False, "error": str(e)}


@router.post("/integrity/checksum")
def run_checksum(body: dict = None):
    tables = (body or {}).get("tables", MOODLE_TABLES[:3])
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    db_name = (cfg.get("moodle_db_name") or "moodle")
    results = []
    src_conn, err = _get_mysql_conn(
        (cfg.get("source_host") or "127.0.0.1"),
        (cfg.get("source_port") or 3306),
        (cfg.get("source_db_user") or "root"),
        (cfg.get("source_db_password") or ""),
        db=db_name,
    )
    rep_conn, err2 = _get_mysql_conn(
        (cfg.get("replica_host") or ""),
        (cfg.get("replica_port") or 3306),
        (cfg.get("replica_db_user") or "root"),
        (cfg.get("replica_db_password") or ""),
        db=db_name,
    )
    if err or err2:
        return {"ok": False, "error": err or err2}
    try:
        c1 = src_conn.cursor()
        c2 = rep_conn.cursor()
        for table in tables:
            try:
                c1.execute(f"CHECKSUM TABLE `{table}`")
                src_row = c1.fetchone()
                src_cs = src_row[1] if src_row else None
            except Exception:
                src_cs = None
            try:
                c2.execute(f"CHECKSUM TABLE `{table}`")
                rep_row = c2.fetchone()
                rep_cs = rep_row[1] if rep_row else None
            except Exception:
                rep_cs = None
            match = src_cs == rep_cs
            results.append({
                "table": table, "source_checksum": src_cs,
                "replica_checksum": rep_cs, "match": match,
            })
        src_conn.close()
        rep_conn.close()
        drift = any(not r["match"] for r in results)
        db_db.log_audit("checksum", result="drift" if drift else "ok", details={"results": results})
        return {"ok": True, "results": results, "drift_detected": drift}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/integrity/history")
def integrity_history():
    history = db_db.get_checksum_results(20)
    return {"ok": True, "history": history}


# ── Failover endpoints ────────────────────────────────────────────────────────

@router.post("/failover/pre-check")
def failover_pre_check():
    cfg = db_db.get_raw_db_config()
    checks = []
    db_status = state.db_replication_status

    checks.append({
        "name": "Replication lag < 10s",
        "ok": (db_status.get("lag_seconds") or 999) < 10,
        "value": f"{db_status.get('lag_seconds', 'unknown')}s",
    })
    checks.append({
        "name": "IO thread running",
        "ok": db_status.get("io_running") == "Yes",
        "value": db_status.get("io_running", "unknown"),
    })
    checks.append({
        "name": "SQL thread running",
        "ok": db_status.get("sql_running") == "Yes",
        "value": db_status.get("sql_running", "unknown"),
    })
    checks.append({
        "name": "No IO errors",
        "ok": not bool(db_status.get("last_io_error")),
        "value": db_status.get("last_io_error") or "None",
    })
    checks.append({
        "name": "No SQL errors",
        "ok": not bool(db_status.get("last_sql_error")),
        "value": db_status.get("last_sql_error") or "None",
    })
    # SSH connectivity to replica
    replica_host = (cfg.get("replica_host") or "") if cfg else ""
    if replica_host:
        r = run_cmd(["ssh", "-i", state.SSH_KEY_PATH, "-o", "BatchMode=yes",
                     "-o", "ConnectTimeout=6", "-o", "StrictHostKeyChecking=accept-new",
                     f"{state.AZURE_VM_USER}@{replica_host}", "echo ok"], timeout=10)
        checks.append({"name": "SSH to replica", "ok": r["ok"], "value": "connected" if r["ok"] else r["stderr"]})
    else:
        checks.append({"name": "SSH to replica", "ok": False, "value": "not configured"})

    passed = sum(1 for c in checks if c["ok"])
    return {"ok": True, "checks": checks, "passed": passed, "total": len(checks)}


@router.post("/failover/initiate")
def failover_initiate():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}

    steps = []
    replica_host = (cfg.get("replica_host") or "")
    replica_user = (cfg.get("replica_db_user") or "root")
    replica_pass = (cfg.get("replica_db_password") or "")

    # Step 1: Stop replication on replica
    conn, err = _get_mysql_conn(replica_host, (cfg.get("replica_port") or 3306), replica_user, replica_pass)
    if err:
        return {"ok": False, "error": f"Cannot connect to replica: {err}"}

    try:
        cursor = conn.cursor()
        try:
            cursor.execute("STOP REPLICA")
        except Exception:
            cursor.execute("STOP SLAVE")
        steps.append({"step": "Stop replication", "ok": True})

        # Step 2: Disable read_only
        cursor.execute("SET GLOBAL read_only = 0")
        cursor.execute("SET GLOBAL super_read_only = 0")
        steps.append({"step": "Disable read_only", "ok": True})

        conn.commit()
        conn.close()
    except Exception as e:
        steps.append({"step": "Configure replica as primary", "ok": False, "error": str(e)})
        return {"ok": False, "steps": steps, "error": str(e)}

    db_db.log_audit("failover_initiated", result="ok",
                    details={"replica_host": replica_host, "steps": steps})
    db_db.log_event("failover", notes=f"Failover initiated to {replica_host}")
    state.db_replication_status["status"] = "failover"

    return {
        "ok": True,
        "message": "Failover initiated — replica promoted to primary",
        "steps": steps,
        "next_steps": [
            "Update Moodle config.php: $CFG->dbhost to " + replica_host,
            "Update DNS / load balancer to point to " + replica_host,
            "Verify application connectivity",
        ],
    }


@router.get("/failover/status")
def failover_status():
    return {
        "status": state.db_replication_status.get("status", "unknown"),
        "last_checked": state.db_replication_status.get("last_checked"),
        "audit_log": db_db.get_audit_log(5),
    }


@router.post("/failover/update-config")
def failover_update_config(body: dict):
    """Write new DB config to Moodle config.php via SSH."""
    new_host = body.get("new_db_host", "")
    config_path = body.get("config_path", "/var/www/html/moodle/config.php")
    if not new_host:
        return {"ok": False, "error": "new_db_host required"}
    r = run_cmd([
        "sed", "-i",
        f"s/\\$CFG->dbhost.*=.*/\\$CFG->dbhost = '{new_host}';/",
        config_path,
    ], timeout=10)
    db_db.log_audit("update_config_php", result="ok" if r["ok"] else "error",
                    details={"new_host": new_host, "path": config_path})
    return {"ok": r["ok"], "message": f"config.php updated: dbhost → {new_host}", "error": r["stderr"] if not r["ok"] else None}


# ── Percona Toolkit endpoints ─────────────────────────────────────────────────

@router.get("/pt/heartbeat-status")
def pt_heartbeat_status():
    r = run_cmd(["systemctl", "is-active", "pt-heartbeat"])
    r2 = run_cmd(["pgrep", "-f", "pt-heartbeat"])
    # Try to read heartbeat lag
    cfg = db_db.get_raw_db_config()
    lag = None
    if cfg:
        conn, err = _get_mysql_conn(
            (cfg.get("replica_host") or ""),
            (cfg.get("replica_port") or 3306),
            (cfg.get("replica_db_user") or "root"),
            (cfg.get("replica_db_password") or ""),
        )
        if not err:
            try:
                cursor = conn.cursor()
                db_name = (cfg.get("moodle_db_name") or "moodle")
                cursor.execute(f"SELECT TIMESTAMPDIFF(SECOND, ts, NOW()) FROM `{db_name}`.heartbeat ORDER BY ts DESC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    lag = row[0]
                conn.close()
            except Exception:
                pass

    return {
        "active": r["stdout"] == "active",
        "running": r2["ok"] and bool(r2["stdout"]),
        "heartbeat_lag_seconds": lag,
    }


@router.post("/pt/start-heartbeat")
def pt_start_heartbeat():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    r = run_cmd(["systemctl", "start", "pt-heartbeat"])
    if not r["ok"]:
        # Try running directly
        src_host = (cfg.get("source_host") or "127.0.0.1")
        src_user = (cfg.get("source_db_user") or "root")
        src_pass = (cfg.get("source_db_password") or "")
        db_name = (cfg.get("moodle_db_name") or "moodle")
        run_cmd([
            "pt-heartbeat", f"--user={src_user}", f"--password={src_pass}",
            f"--host={src_host}", f"--database={db_name}",
            "--update", "--daemonize", "--pid=/var/run/pt-heartbeat.pid"
        ])
    db_db.log_audit("start_pt_heartbeat", result="ok" if r["ok"] else "warn")
    return {"ok": True, "message": "pt-heartbeat start attempted"}


@router.post("/pt/stop-heartbeat")
def pt_stop_heartbeat():
    r = run_cmd(["systemctl", "stop", "pt-heartbeat"])
    if not r["ok"]:
        run_cmd(["pkill", "-f", "pt-heartbeat"])
    db_db.log_audit("stop_pt_heartbeat", result="ok")
    return {"ok": True, "message": "pt-heartbeat stopped"}


@router.post("/pt/run-checksum")
def pt_run_checksum():
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    if not shutil.which("pt-table-checksum"):
        return {"ok": False, "error": "pt-table-checksum not installed"}
    db_name = (cfg.get("moodle_db_name") or "moodle")
    src_host = (cfg.get("source_host") or "127.0.0.1")
    src_user = (cfg.get("source_db_user") or "root")
    src_pass = (cfg.get("source_db_password") or "")
    r = run_cmd([
        "pt-table-checksum",
        f"--user={src_user}", f"--password={src_pass}",
        f"--host={src_host}",
        f"--databases={db_name}",
        "--no-check-binlog-format",
        f"h={src_host},P=3306",
    ], timeout=120)
    db_db.log_audit("pt_table_checksum", result="ok" if r["ok"] else "error",
                    details={"stdout": r["stdout"][:500]})
    return {"ok": r["ok"], "output": r["stdout"], "error": r["stderr"] if not r["ok"] else None}


@router.get("/pt/checksum-results")
def pt_checksum_results():
    history = db_db.get_checksum_results(10)
    return {"ok": True, "results": history}


@router.post("/pt/run-sync")
def pt_run_sync(body: dict = None):
    """Run pt-table-sync to fix drifted tables."""
    tables = (body or {}).get("tables", [])
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration"}
    if not shutil.which("pt-table-sync"):
        return {"ok": False, "error": "pt-table-sync not installed"}
    src_host = (cfg.get("source_host") or "127.0.0.1")
    rep_host = (cfg.get("replica_host") or "")
    src_user = (cfg.get("source_db_user") or "root")
    src_pass = (cfg.get("source_db_password") or "")
    db_name = (cfg.get("moodle_db_name") or "moodle")

    cmd = [
        "pt-table-sync", "--execute",
        f"--user={src_user}", f"--password={src_pass}",
        f"h={src_host},D={db_name}",
        f"h={rep_host},D={db_name}",
    ]
    if tables:
        cmd.extend(["--tables", ",".join(tables)])

    r = run_cmd(cmd, timeout=300)
    db_db.log_audit("pt_table_sync", result="ok" if r["ok"] else "error",
                    details={"tables": tables, "stdout": r["stdout"][:500]})
    return {"ok": r["ok"], "output": r["stdout"], "error": r["stderr"] if not r["ok"] else None}
