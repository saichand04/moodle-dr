"""SQLite schema + CRUD for DB replication tracking."""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path("/home/user/workspace/moodle-dr-dashboard-v11/data/transfers.db")


async def init_db_replication():
    """Create DB replication tables if not exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS db_replication_config (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        source_host     TEXT NOT NULL DEFAULT '127.0.0.1',
        source_port     INTEGER NOT NULL DEFAULT 3306,
        replica_host    TEXT NOT NULL DEFAULT '',
        replica_port    INTEGER NOT NULL DEFAULT 3306,
        repl_user       TEXT NOT NULL DEFAULT 'repl_user',
        repl_password   TEXT NOT NULL DEFAULT '',
        source_db_user  TEXT NOT NULL DEFAULT 'root',
        source_db_password TEXT NOT NULL DEFAULT '',
        replica_db_user TEXT NOT NULL DEFAULT 'root',
        replica_db_password TEXT NOT NULL DEFAULT '',
        moodle_db_name  TEXT NOT NULL DEFAULT 'moodle',
        ssl_ca_path     TEXT NOT NULL DEFAULT '/etc/mysql/ssl/ca-cert.pem',
        ssl_cert_path   TEXT NOT NULL DEFAULT '/etc/mysql/ssl/client-cert.pem',
        ssl_key_path    TEXT NOT NULL DEFAULT '/etc/mysql/ssl/client-key.pem',
        ssl_remote_dir  TEXT NOT NULL DEFAULT '/home/moodlesync/mysql-ssl',
        seed_method     TEXT NOT NULL DEFAULT 'mysqldump',
        configured_at   TEXT,
        updated_at      TEXT
    );
    CREATE TABLE IF NOT EXISTS db_replication_health (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        sampled_at           TEXT NOT NULL,
        io_running           TEXT,
        sql_running          TEXT,
        lag_seconds          INTEGER,
        retrieved_gtid_set   TEXT,
        executed_gtid_set    TEXT,
        pending_gtid_count   INTEGER,
        last_io_error        TEXT,
        last_sql_error       TEXT,
        source_uuid          TEXT,
        repl_channel         TEXT DEFAULT '',
        ssl_allowed          TEXT,
        auto_position        INTEGER,
        heartbeats_received  INTEGER,
        heartbeat_lag        REAL,
        status               TEXT
    );
    CREATE TABLE IF NOT EXISTS db_replication_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type  TEXT NOT NULL,
        timestamp   TEXT NOT NULL,
        lag_seconds INTEGER,
        error_msg   TEXT,
        notes       TEXT
    );
    CREATE TABLE IF NOT EXISTS db_audit_log (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        action    TEXT NOT NULL,
        actor     TEXT DEFAULT 'operator',
        timestamp TEXT NOT NULL,
        details   TEXT,
        result    TEXT
    );
    """)
    # ── Schema migrations: safely add columns missing in existing DBs ──────────
    migrations = [
        "ALTER TABLE db_replication_config ADD COLUMN ssl_remote_dir TEXT NOT NULL DEFAULT '/home/moodlesync/mysql-ssl'",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except Exception:
            pass  # Column already exists — ignore
    conn.commit()
    conn.close()


def get_db_config() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM db_replication_config ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if not row:
        return {}
    d = dict(row)
    d["repl_password"] = "***"
    d["source_db_password"] = "***"
    d["replica_db_password"] = "***"
    return d


def save_db_config(cfg: dict):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    existing = conn.execute("SELECT id FROM db_replication_config LIMIT 1").fetchone()
    if existing:
        conn.execute("""UPDATE db_replication_config SET
            source_host=?, source_port=?, replica_host=?, replica_port=?,
            repl_user=?, repl_password=?, source_db_user=?, source_db_password=?,
            replica_db_user=?, replica_db_password=?, moodle_db_name=?,
            ssl_ca_path=?, ssl_cert_path=?, ssl_key_path=?, seed_method=?, updated_at=?
            WHERE id=?""",
            (cfg.get("source_host", "127.0.0.1"), cfg.get("source_port", 3306),
             cfg.get("replica_host", ""), cfg.get("replica_port", 3306),
             cfg.get("repl_user", "repl_user"), cfg.get("repl_password", ""),
             cfg.get("source_db_user", "root"), cfg.get("source_db_password", ""),
             cfg.get("replica_db_user", "root"), cfg.get("replica_db_password", ""),
             cfg.get("moodle_db_name", "moodle"), cfg.get("ssl_ca_path", ""),
             cfg.get("ssl_cert_path", ""), cfg.get("ssl_key_path", ""),
             cfg.get("seed_method", "mysqldump"), now, existing[0]))
    else:
        conn.execute("""INSERT INTO db_replication_config
            (source_host,source_port,replica_host,replica_port,repl_user,repl_password,
             source_db_user,source_db_password,replica_db_user,replica_db_password,
             moodle_db_name,ssl_ca_path,ssl_cert_path,ssl_key_path,seed_method,configured_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cfg.get("source_host", "127.0.0.1"), cfg.get("source_port", 3306),
             cfg.get("replica_host", ""), cfg.get("replica_port", 3306),
             cfg.get("repl_user", "repl_user"), cfg.get("repl_password", ""),
             cfg.get("source_db_user", "root"), cfg.get("source_db_password", ""),
             cfg.get("replica_db_user", "root"), cfg.get("replica_db_password", ""),
             cfg.get("moodle_db_name", "moodle"), cfg.get("ssl_ca_path", ""),
             cfg.get("ssl_cert_path", ""), cfg.get("ssl_key_path", ""),
             cfg.get("seed_method", "mysqldump"), now, now))
    conn.commit()
    conn.close()


def save_health_snapshot(snap: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""INSERT INTO db_replication_health
        (sampled_at,io_running,sql_running,lag_seconds,retrieved_gtid_set,
         executed_gtid_set,pending_gtid_count,last_io_error,last_sql_error,
         source_uuid,ssl_allowed,auto_position,heartbeats_received,heartbeat_lag,status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (snap.get("sampled_at"), snap.get("io_running"), snap.get("sql_running"),
         snap.get("lag_seconds"), snap.get("retrieved_gtid_set"),
         snap.get("executed_gtid_set"), snap.get("pending_gtid_count"),
         snap.get("last_io_error"), snap.get("last_sql_error"),
         snap.get("source_uuid"), snap.get("ssl_allowed"),
         snap.get("auto_position"), snap.get("heartbeats_received"),
         snap.get("heartbeat_lag"), snap.get("status")))
    conn.commit()
    # Keep only last 2880 rows (24h at 30s intervals)
    conn.execute("DELETE FROM db_replication_health WHERE id NOT IN (SELECT id FROM db_replication_health ORDER BY id DESC LIMIT 2880)")
    conn.commit()
    conn.close()


def get_health_history(n: int = 288) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM db_replication_health ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def log_event(event_type: str, lag_seconds=None, error_msg=None, notes=None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO db_replication_events (event_type,timestamp,lag_seconds,error_msg,notes) VALUES (?,?,?,?,?)",
        (event_type, datetime.utcnow().isoformat(), lag_seconds, error_msg, notes))
    conn.commit()
    conn.close()


def log_audit(action: str, details: dict = None, result: str = "ok", actor: str = "operator"):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO db_audit_log (action,actor,timestamp,details,result) VALUES (?,?,?,?,?)",
        (action, actor, datetime.utcnow().isoformat(), json.dumps(details or {}), result))
    conn.commit()
    conn.close()


def get_recent_events(n: int = 50) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM db_replication_events ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_audit_log(n: int = 50) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM db_audit_log ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_raw_db_config() -> dict:
    """Get config with actual passwords (for internal use only, never return to API)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM db_replication_config ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else {}


def update_db_config_field(field: str, value):
    """Update a single column in the db_replication_config row (best-effort)."""
    allowed = {
        "ssl_ca_path", "ssl_cert_path", "ssl_key_path", "ssl_remote_dir",
        "source_host", "replica_host", "seed_method"
    }
    if field not in allowed:
        return
    conn = sqlite3.connect(DB_PATH)
    existing = conn.execute("SELECT id FROM db_replication_config LIMIT 1").fetchone()
    if existing:
        conn.execute(f"UPDATE db_replication_config SET {field}=? WHERE id=?",
                     (value, existing[0]))
        conn.commit()
    conn.close()


def get_checksum_results(n: int = 20) -> list:
    """Get recent pt-table-checksum results from audit log."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM db_audit_log WHERE action LIKE '%checksum%' OR action LIKE '%table_count%' ORDER BY id DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Integration & Notification Tables ─────────────────────────────────────────

async def init_integrations_db():
    """Add integration/notification/token tables to existing SQLite DB."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS integration_config (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        key        TEXT UNIQUE NOT NULL,
        value      TEXT,
        updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS notification_channels (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        channel    TEXT NOT NULL,
        config     TEXT,
        enabled    INTEGER DEFAULT 1,
        updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS api_tokens (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        label        TEXT,
        token_hash   TEXT UNIQUE NOT NULL,
        created_at   TEXT NOT NULL,
        last_used_at TEXT,
        revoked      INTEGER DEFAULT 0
    );
    """)
    conn.commit()
    conn.close()


def get_integration_config(key: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT value FROM integration_config WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else None


def set_integration_config(key: str, value: str):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO integration_config (key,value,updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value, now)
    )
    conn.commit()
    conn.close()


def get_all_integration_config() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT key,value FROM integration_config").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


def get_notification_channels() -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM notification_channels ORDER BY channel").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_notification_channel(channel: str, config: dict):
    import json as _json
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    existing = conn.execute("SELECT id FROM notification_channels WHERE channel=?", (channel,)).fetchone()
    if existing:
        conn.execute("UPDATE notification_channels SET config=?,updated_at=? WHERE channel=?",
                     (_json.dumps(config), now, channel))
    else:
        conn.execute("INSERT INTO notification_channels (channel,config,enabled,updated_at) VALUES (?,?,1,?)",
                     (channel, _json.dumps(config), now))
    conn.commit()
    conn.close()


def create_api_token(label: str, raw_token: str) -> int:
    import hashlib
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO api_tokens (label,token_hash,created_at) VALUES (?,?,?)",
        (label, token_hash, now)
    )
    conn.commit()
    token_id = cur.lastrowid
    conn.close()
    return token_id


def get_api_tokens() -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id,label,created_at,last_used_at,revoked FROM api_tokens ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def revoke_api_token(token_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE api_tokens SET revoked=1 WHERE id=?", (token_id,))
    conn.commit()
    conn.close()


def verify_api_token(raw_token: str) -> bool:
    import hashlib
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT id FROM api_tokens WHERE token_hash=? AND revoked=0", (token_hash,)
    ).fetchone()
    if row:
        conn.execute("UPDATE api_tokens SET last_used_at=? WHERE token_hash=?",
                     (datetime.utcnow().isoformat(), token_hash))
        conn.commit()
    conn.close()
    return row is not None
