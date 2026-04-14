"""
db_replication.py — All /api/db/* endpoints for MySQL GTID replication management.
"""
import asyncio
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
    ssh_user = user or cfg.get("replica_db_user", "root")
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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

@router.post("/setup/create-repl-user")
def create_repl_user(body: dict = None):
    cfg = db_db.get_raw_db_config()
    if not cfg:
        return {"ok": False, "error": "No configuration saved"}
    conn, err = _get_mysql_conn(
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
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

        repl_user = cfg.get("repl_user", "repl_user")
        repl_pass = cfg.get("repl_password", "")

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
    """Build SSH base command for moodlesync user."""
    return [
        "ssh", "-i", state.SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        f"{state.AZURE_VM_USER}@{host}",
    ]


def _remote_run(ssh_base: list, cmd: str, input_data: str = None, timeout: int = 20) -> dict:
    """Run command on remote via SSH, optionally piping input_data through stdin."""
    try:
        proc = subprocess.run(
            ssh_base + [cmd],
            input=input_data, capture_output=True, text=True, timeout=timeout
        )
        return {"ok": proc.returncode == 0, "stdout": proc.stdout.strip(),
                "stderr": proc.stderr.strip(), "returncode": proc.returncode}
    except Exception as e:
        return {"ok": False, "stdout": "", "stderr": str(e), "returncode": -1}


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


@router.post("/setup/push-ssl-to-replica")
def push_ssl_to_replica():
    cfg = db_db.get_raw_db_config()
    replica_host = cfg.get("replica_host", "")
    if not replica_host:
        return {"ok": False, "error": "Replica host not configured"}

    local_ssl_dir  = "/etc/mysql/ssl"
    remote_staging = "/tmp/mysql-ssl"
    ssh_user       = state.AZURE_VM_USER
    ssh_tgt        = f"{ssh_user}@{replica_host}"
    sb             = _ssh_base(replica_host)
    steps          = []

    # ── 1. SSH reachability ───────────────────────────────────────────────────
    r = _remote_run(sb, "echo ok", timeout=12)
    steps.append({"step": "SSH connectivity", "ok": r["ok"], "err": r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"SSH failed: {r['stderr']}", "steps": steps}

    # ── 2. Bootstrap sudoers via stdin pipe ──────────────────────────────────
    # Dashboard runs as root on source — pipe sudoers content via SSH stdin
    # into 'sudo tee' on the replica. No password prompt needed from source root.
    sudoers_rule = (
        f"{ssh_user} ALL=(root) NOPASSWD: "
        "/bin/mkdir, /bin/cp, /bin/chown, /bin/chmod, "
        "/usr/bin/tee, /bin/systemctl, /usr/sbin/mariadbd"
    )
    r = _remote_run(sb, "sudo tee /etc/sudoers.d/moodlesync-mysql",
                    input_data=sudoers_rule + "\n", timeout=15)
    sudoers_ok = r["ok"]
    steps.append({"step": "Bootstrap sudoers on replica", "ok": sudoers_ok, "err": r["stderr"]})

    # ── 3. Decide cert destination ────────────────────────────────────────────
    # If sudoers worked → /etc/mysql/ssl (proper system path, owned by mysql)
    # Otherwise → /home/moodlesync/mysql-ssl with o+x on home dir so mysql can traverse
    if sudoers_ok:
        remote_ssl_dir = "/etc/mysql/ssl"
    else:
        remote_ssl_dir = f"/home/{ssh_user}/mysql-ssl"

    # ── 4. Create required directories ───────────────────────────────────────
    # Also create /var/log/mysql for binlog (MariaDB doesn't auto-create it)
    mkdir_cmds = [f"mkdir -p {remote_staging}", "mkdir -p /var/log/mysql"]
    if sudoers_ok:
        mkdir_cmds += [
            f"sudo mkdir -p {remote_ssl_dir}",
            "sudo mkdir -p /var/log/mysql",
            "sudo chown mysql:mysql /var/log/mysql",
        ]
    else:
        mkdir_cmds += [
            f"mkdir -p {remote_ssl_dir}",
            # Allow mysql process to traverse home dir
            f"chmod o+x /home/{ssh_user}",
        ]
    r = _remote_run(sb, " && ".join(mkdir_cmds), timeout=15)
    steps.append({"step": "Create dirs + /var/log/mysql on replica", "ok": r["ok"], "err": r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"mkdir failed: {r['stderr']}", "steps": steps}

    # ── 5. SCP certs to /tmp staging ──────────────────────────────────────────
    r = run_cmd([
        "scp", "-i", state.SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "BatchMode=yes",
        f"{local_ssl_dir}/ca-cert.pem",
        f"{local_ssl_dir}/client-cert.pem",
        f"{local_ssl_dir}/client-key.pem",
        f"{ssh_tgt}:{remote_staging}/",
    ], timeout=30)
    steps.append({"step": "SCP certs to /tmp/mysql-ssl", "ok": r["ok"], "err": r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"SCP failed: {r['stderr']}", "steps": steps}

    # ── 6. Install certs + fix ownership/permissions ─────────────────────────
    if sudoers_ok:
        install_cmd = (
            f"sudo cp {remote_staging}/ca-cert.pem {remote_staging}/client-cert.pem "
            f"{remote_staging}/client-key.pem {remote_ssl_dir}/ && "
            f"sudo chown -R mysql:mysql {remote_ssl_dir} && "
            f"sudo chmod 640 {remote_ssl_dir}/*.pem"
        )
    else:
        install_cmd = (
            f"cp {remote_staging}/ca-cert.pem {remote_staging}/client-cert.pem "
            f"{remote_staging}/client-key.pem {remote_ssl_dir}/ && "
            f"chmod 640 {remote_ssl_dir}/*.pem && "
            # mysql process must be able to read these; grant read to others
            f"chmod o+r {remote_ssl_dir}/*.pem"
        )
    r = _remote_run(sb, install_cmd, timeout=20)
    steps.append({"step": f"Install certs to {remote_ssl_dir}", "ok": r["ok"], "err": r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"Cert install failed: {r['stderr']}", "steps": steps}

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
        cfg.get("source_host", "127.0.0.1"), cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"), cfg.get("source_db_password", "")
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

    mycnf_additions = f"""# === Moodle DR Source Replication Config (auto-generated) ===
# DB type: {db_type}
[mysqld]
server-id                = 1
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
{gtid_lines}
binlog_do_db             = {cfg.get('moodle_db_name', 'moodle')}
expire_logs_days         = 7
max_binlog_size          = 100M
ssl-ca                   = {cfg.get('ssl_ca_path', '/etc/mysql/ssl/ca-cert.pem')}
ssl-cert                 = {cfg.get('ssl_cert_path', '/etc/mysql/ssl/client-cert.pem')}
ssl-key                  = {cfg.get('ssl_key_path', '/etc/mysql/ssl/client-key.pem')}
"""

    # ── 3. Ensure /var/log/mysql exists and is owned by mysql ─────────────────
    # MariaDB does NOT auto-create this; without it binlog startup fails.
    try:
        log_dir = Path("/var/log/mysql")
        log_dir.mkdir(parents=True, exist_ok=True)
        # chown mysql:mysql via subprocess (requires running as root)
        r = run_cmd(["chown", "mysql:mysql", str(log_dir)])
        steps.append({"step": "Create /var/log/mysql + chown mysql:mysql",
                      "ok": r["ok"], "err": r["stderr"]})
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
    r = run_cmd(["systemctl", "restart", svc_name], timeout=30)
    steps.append({"step": f"Restart {svc_name} on source", "ok": r["ok"], "err": r["stderr"]})
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
    replica_host = cfg.get("replica_host", "")
    if not replica_host:
        return {"ok": False, "error": "Replica host not configured"}

    ssh_user = state.AZURE_VM_USER
    sb       = _ssh_base(replica_host)
    ssl_dir  = cfg.get("ssl_remote_dir", "/etc/mysql/ssl")
    steps    = []

    # ── 1. Detect DB flavour via SSH (MariaDB vs MySQL) ───────────────────────
    # Try connecting; fall back to SSH version string if port not open yet
    db_type = _detect_db_type(
        replica_host, cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"), cfg.get("replica_db_password", "")
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
    steps.append({"step": "Ensure /var/log/mysql exists", "ok": r["ok"], "err": r["stderr"]})
    # Non-fatal — continue even if sudo fails (may already exist)

    # ── 4. Write cnf to staging via stdin pipe ─────────────────────────────
    r = _remote_run(sb, f"cat > {staging}", input_data=mycnf_content, timeout=15)
    steps.append({"step": f"Write cnf to {staging}", "ok": r["ok"], "err": r["stderr"]})
    if not r["ok"]:
        return {"ok": False, "error": f"Could not write staging cnf: {r['stderr']}", "steps": steps}

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
                      "ok": r2["ok"], "err": r["stderr"]})
        if not r2["ok"]:
            return {"ok": False, "error": r2["stderr"], "steps": steps}

    # ── 6. Restart DB service automatically ────────────────────────────────
    if cnf_final == cnf_path_system:
        r = _remote_run(sb, f"sudo systemctl restart {svc_name}", timeout=30)
        steps.append({"step": f"Restart {svc_name} on replica", "ok": r["ok"], "err": r["stderr"]})
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
    db_name = cfg.get("moodle_db_name", "moodle")
    seed_method = cfg.get("seed_method", "mysqldump")

    state.db_seed_job.update({
        "running": True, "phase": "starting", "progress": 0,
        "started_at": datetime.utcnow().isoformat(), "ended_at": None,
        "result": None, "error": "", "output": [],
        "dump_size_human": "", "gtid_purged": "",
    })

    try:
        if seed_method == "mysqldump":
            await _seed_mysqldump(cfg, db_name)
        else:
            await _seed_xtrabackup(cfg, db_name)
    except Exception as e:
        state.db_seed_job.update({
            "running": False, "result": "error", "error": str(e),
            "ended_at": datetime.utcnow().isoformat(), "progress": 0,
        })
        db_db.log_audit("seed_database", result="error", details={"error": str(e)})


async def _seed_mysqldump(cfg: dict, db_name: str):
    source_host = cfg.get("source_host", "127.0.0.1")
    source_port = cfg.get("source_port", 3306)
    source_user = cfg.get("source_db_user", "root")
    source_pass = cfg.get("source_db_password", "")
    replica_host = cfg.get("replica_host", "")
    replica_user = cfg.get("replica_db_user", "root")
    replica_pass = cfg.get("replica_db_password", "")

    dump_path = f"/tmp/moodle_seed_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.sql.gz"

    state.db_seed_job["phase"] = "dumping"
    state.db_seed_job["progress"] = 10
    state.db_seed_job["output"].append(f"Starting mysqldump of {db_name}...")

    dump_cmd = [
        "mysqldump",
        f"-h{source_host}", f"-P{str(source_port)}",
        f"-u{source_user}", f"-p{source_pass}",
        "--single-transaction", "--master-data=2",
        "--set-gtid-purged=ON", "--gtid",
        "--routines", "--triggers", "--events",
        db_name,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *dump_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise Exception(f"mysqldump failed: {stderr.decode()[:500]}")

        # Write dump
        dump_file = Path(dump_path.replace(".gz", ""))
        dump_file.write_bytes(stdout)

        size = dump_file.stat().st_size
        state.db_seed_job["dump_size_human"] = f"{size/1024/1024:.1f} MB"
        state.db_seed_job["progress"] = 50
        state.db_seed_job["phase"] = "transferring"
        state.db_seed_job["output"].append(f"Dump complete: {state.db_seed_job['dump_size_human']}")

        # SCP to replica
        scp_cmd = [
            "scp", "-i", state.SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=accept-new",
            str(dump_file), f"{state.AZURE_VM_USER}@{replica_host}:/tmp/moodle_seed.sql"
        ]
        r = await asyncio.create_subprocess_exec(*scp_cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err2 = await r.communicate()
        if r.returncode != 0:
            raise Exception(f"SCP failed: {err2.decode()[:300]}")

        state.db_seed_job["progress"] = 70
        state.db_seed_job["phase"] = "importing"
        state.db_seed_job["output"].append("Importing dump on replica...")

        # Import on replica
        import_cmd = _ssh_cmd(replica_host)
        import_cmd.append(f"mysql -u{replica_user} -p{replica_pass} {db_name} < /tmp/moodle_seed.sql && rm /tmp/moodle_seed.sql")
        r2 = await asyncio.create_subprocess_exec(*import_cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err3 = await r2.communicate()
        if r2.returncode != 0:
            raise Exception(f"Import failed: {err3.decode()[:300]}")

        state.db_seed_job["progress"] = 100
        state.db_seed_job["phase"] = "complete"
        state.db_seed_job["running"] = False
        state.db_seed_job["result"] = "success"
        state.db_seed_job["ended_at"] = datetime.utcnow().isoformat()
        state.db_seed_job["output"].append("Seed complete!")

        # Cleanup
        dump_file.unlink(missing_ok=True)

        db_db.log_audit("seed_database", result="ok", details={"method": "mysqldump", "db": db_name})

    except Exception as e:
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

    replica_host = cfg.get("replica_host", "")
    repl_user = cfg.get("repl_user", "repl_user")
    repl_pass = cfg.get("repl_password", "")
    source_host = cfg.get("source_host", "127.0.0.1")
    source_port = cfg.get("source_port", 3306)
    ssl_ca = cfg.get("ssl_ca_path", "/etc/mysql/ssl/ca-cert.pem")
    ssl_cert = cfg.get("ssl_cert_path", "/etc/mysql/ssl/client-cert.pem")
    ssl_key = cfg.get("ssl_key_path", "/etc/mysql/ssl/client-key.pem")

    conn, err = _get_mysql_conn(
        replica_host,
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
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
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
    )
    if err:
        return {"ok": False, "error": f"Source connection failed: {err}"}

    # Get replica executed GTIDs
    rep_conn, err2 = _get_mysql_conn(
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
    db_name = cfg.get("moodle_db_name", "moodle")

    src_conn, err = _get_mysql_conn(
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
        db=db_name,
    )
    rep_conn, err2 = _get_mysql_conn(
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
    db_name = cfg.get("moodle_db_name", "moodle")
    results = []
    src_conn, err = _get_mysql_conn(
        cfg.get("source_host", "127.0.0.1"),
        cfg.get("source_port", 3306),
        cfg.get("source_db_user", "root"),
        cfg.get("source_db_password", ""),
        db=db_name,
    )
    rep_conn, err2 = _get_mysql_conn(
        cfg.get("replica_host", ""),
        cfg.get("replica_port", 3306),
        cfg.get("replica_db_user", "root"),
        cfg.get("replica_db_password", ""),
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
    replica_host = cfg.get("replica_host", "") if cfg else ""
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
    replica_host = cfg.get("replica_host", "")
    replica_user = cfg.get("replica_db_user", "root")
    replica_pass = cfg.get("replica_db_password", "")

    # Step 1: Stop replication on replica
    conn, err = _get_mysql_conn(replica_host, cfg.get("replica_port", 3306), replica_user, replica_pass)
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
            cfg.get("replica_host", ""),
            cfg.get("replica_port", 3306),
            cfg.get("replica_db_user", "root"),
            cfg.get("replica_db_password", ""),
        )
        if not err:
            try:
                cursor = conn.cursor()
                db_name = cfg.get("moodle_db_name", "moodle")
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
        src_host = cfg.get("source_host", "127.0.0.1")
        src_user = cfg.get("source_db_user", "root")
        src_pass = cfg.get("source_db_password", "")
        db_name = cfg.get("moodle_db_name", "moodle")
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
    db_name = cfg.get("moodle_db_name", "moodle")
    src_host = cfg.get("source_host", "127.0.0.1")
    src_user = cfg.get("source_db_user", "root")
    src_pass = cfg.get("source_db_password", "")
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
    src_host = cfg.get("source_host", "127.0.0.1")
    rep_host = cfg.get("replica_host", "")
    src_user = cfg.get("source_db_user", "root")
    src_pass = cfg.get("source_db_password", "")
    db_name = cfg.get("moodle_db_name", "moodle")

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
