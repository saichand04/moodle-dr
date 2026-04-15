"""
state.py — shared mutable state across all backend modules.
No imports from other custom modules here — avoids circular dependencies.
main.py populates the config vars and trigger_rsync callback at startup.
"""

# ── Runtime config (populated from env in main.py) ────────────────────────────
AZURE_VM_IP   = ""
AZURE_VM_USER = "moodlesync"
ADMIN_VM_USER = "admmoodle"   # sudo-capable admin user on the replica
SSH_KEY_PATH  = "/root/.ssh/moodle_rsync_ed25519"
SOURCE_PATH   = "/var/www/Azure-MoodleData/"
TARGET_PATH   = "/moodledata/"

# ── Rsync job state ───────────────────────────────────────────────────────────
rsync_job: dict = {
    "running":      False,
    "progress":     0,
    "speed":        "",
    "started_at":   None,
    "last_run":     None,
    "last_result":  None,   # "success" | "error" | None
    "returncode":   None,
    "output":       [],
    "session_id":   None,   # FK → transfer_sessions.id
    "trigger":      "operator",  # "operator" | "watchdog" | "lsyncd" | "startup"
    "files_checked":      0,
    "files_transferred":  0,
    "bytes_sent":         0,
    "bytes_sent_human":   "",
}

# ── Rsync dry-run validation job state ───────────────────────────────────────
rsync_validate_job: dict = {
    "running":             False,
    "started_at":         None,
    "finished_at":        None,
    "last_result":        None,   # "in_sync" | "out_of_sync" | "error" | None
    "returncode":         None,
    "files_total":        0,      # Number of files (source)
    "files_transferred":  0,      # Files that WOULD be transferred (out-of-sync)
    "files_deleted":      0,      # Files that WOULD be deleted on target
    "bytes_would_send":   0,
    "bytes_would_send_human": "",
    "total_size_human":   "",
    "output":             [],     # last N lines of rsync --stats output
    "error":              "",
}

# ── Callback: set by main.py so watchdog can trigger rsync without import loop ─
# Signature: async trigger_rsync(trigger: str) -> bool
trigger_rsync_fn = None

# ── Watchdog singleton reference (set by main.py) ────────────────────────────
watchdog = None

# ── Setup mode ────────────────────────────────────────────────────────────────
setup_mode: str = ""          # "file_only" | "db_only" | "both" | ""
file_setup_complete: bool = False
db_setup_complete: bool = False
welcome_seen: bool = False

# ── DB replication state ──────────────────────────────────────────────────────
db_replication_status: dict = {
    "io_running": None,
    "sql_running": None,
    "lag_seconds": None,
    "status": None,
    "last_io_error": None,
    "last_sql_error": None,
    "last_checked": None,
    "heartbeat_lag": None,
}

db_seed_job: dict = {
    "running": False,
    "phase": "",
    "progress": 0,
    "started_at": None,
    "ended_at": None,
    "result": None,
    "error": "",
    "output": [],
    "dump_size_human": "",
    "gtid_purged": "",
}

# ── DB watchdog ───────────────────────────────────────────────────────────────
db_watchdog = None
