# Moodle DR — Project Context for Claude Code

> **Last updated:** 2026-05-15  
> **Session:** Version detection still broken (local mode) — needs Claude Code fix  
> **Pick up from:** Latest commit `4ab9fcc`. The version detection bug in local mode is UNRESOLVED. See "OPEN BUG" section at the top for full diagnosis and what to fix.

---

## Project Overview

**Moodle DR** is a full-stack disaster recovery and operations platform for Moodle LMS installations. It automates:
- File sync (rsync + lsyncd) from on-prem → Azure VM
- Database replication (MariaDB GTID async replication over WireGuard)
- Initial migration (bulk dump + transfer + import with smart resume)
- Live sync monitoring and DR readiness scoring
- Moodle application container management (Docker)
- **NEW: Moodle Upgrade automation (multi-mode, multi-stage)**

The app is **general-purpose** — it is NOT hardcoded to any specific Moodle version or server. All version decisions are made dynamically at runtime based on SSH detection.

---

## Infrastructure (Reference — Do Not Hardcode)

| Component | Value |
|---|---|
| Source server | On-prem Ubuntu 22.04, `192.168.1.241` |
| Source DB | MariaDB 10.6.22 |
| Source moodledata | `/var/www/Azure-MoodleData/` (~3.1 TB) |
| Target/Replica | Azure VM `20.86.145.98` |
| Target DB | MariaDB 10.11.14 |
| Target domain | `lmsdr.agu.edu.bh` |
| App install dir | `/opt/moodle-dr-v11` (on-prem) |
| Systemd service | `moodle-dr.service` |
| Uvicorn port | `8081` |
| WireGuard on-prem | `10.10.0.1` |
| WireGuard Azure | `10.10.0.2` |

---

## Tech Stack

- **Backend:** Python 3, FastAPI, uvicorn
- **Frontend:** Single-file HTML/CSS/JS (`frontend/index.html`) — glassmorphism dark UI
- **Design system:** `--bg:#07090f`, accent teal `#00d4c8`, magenta `#e040fb`, violet `#7c4dff`
- **Fonts:** Satoshi + JetBrains Mono (loaded from CDN)
- **DB on servers:** MariaDB (dynamically detected — never assume MySQL or a specific version)
- **Moodle app:** Docker container `moodle-frontend` on target, `network_mode: host`
- **Nginx:** SSL termination on 443, proxies to Apache on 8888 inside container

---

## File Structure

```
moodle-dr/
├── backend/
│   ├── main.py              # FastAPI app entry, mounts all routers
│   ├── setup.py             # Setup wizard API (/api/setup/*)
│   ├── transfers.py         # File transfer API (/api/transfer/*)
│   ├── db_replication.py    # DB replication API (/api/db/*)
│   ├── db_replication_db.py # DB helper functions
│   ├── transfer_db.py       # Transfer DB helpers
│   ├── integrations.py      # WireGuard, notifications, OpenClaw (/api/integrations/*)
│   ├── public_api.py        # Public API endpoints
│   ├── state.py             # Shared app state
│   ├── watchdog.py          # Background watchdog process
│   └── upgrade.py           # NEW: Moodle Upgrade module (/api/upgrade/*)
├── frontend/
│   └── index.html           # Single-file SPA (all CSS + JS inline)
├── context.md               # THIS FILE — always keep updated
└── README.md
```

---

## Existing Nav Groups (sidebar)

```
DR (always visible)
  - DR Readiness
  - Setup Wizard

File Sync (visible when fileSetupComplete or mode includes file)
  - File Sync Status
  - Transfers
  - Connectivity
  - Failover
  - Settings

Database Sync (visible when dbSetupComplete or mode includes db)
  - DB Replication Status
  - DB Integrity
  - DB Failover
  - Settings

Migration (visible when both file + db complete)
  - Initial Migration
  - Live Sync Status

Integrations (visible when setup complete)
  - External Access
  - WireGuard VPN
  - Notifications
  - OpenClaw API

Moodle Upgrade (NEW — always visible, independent module)
  - Upgrade Manager
```

---

## Moodle Upgrade Module — Full Design

### Why This Module Exists

The existing modules handle **DR replication** (keeping a replica in sync with the source). The Upgrade module handles **version lifecycle** — upgrading Moodle itself to a newer version. These are complementary: you can use DR sync to safely migrate data to a new server, then use the Upgrade module to walk the data up to the target version.

### Key Design Principle

**The app is version-agnostic.** It never assumes a specific Moodle version. It SSHs into the source server, reads `version.php` or `mdl_config` table, detects the current version dynamically, and computes the upgrade path at runtime.

A new client with Moodle 4.1 gets the same seamless experience as one with 4.4 or 5.0.

### Version Path Rules (from official Moodle docs)

```
Source version      → Target 5.1 path
──────────────────────────────────────────────────────
< 4.1.2             → BLOCKED: Too old, unsupported upgrade path
4.1.x               → 4.1 → 4.4 → 5.1  (4.4 waypoint mandatory)
4.2.x               → 4.2 → 4.4 → 5.1  (4.4 waypoint mandatory)
4.3.x               → 4.3 → 4.4 → 5.1  (4.4 waypoint mandatory)
4.4.x               → 4.4 → 5.1         (direct)
4.5.x               → 4.5 → 5.1         (direct)
5.0.x               → 5.0 → 5.1         (minor hop)
5.1.x               → Already current
```

**Rule:** Any Moodle version prior to 4.4 CANNOT upgrade directly to 5.x. Must pass through 4.4 first.

### Per-Version Requirements

| Target Version | Min Upgrade From | Min PHP | Min MariaDB | Min MySQL |
|---|---|---|---|---|
| 4.4 | 4.1.2+ | 8.1.0 | 10.6.7 | 8.0 |
| 4.5 | 4.1.2+ | 8.1.0 | 10.6.7 | 8.0 |
| 5.0 | 4.2.3+ | 8.2.0 | 10.11.0 | 8.4 |
| 5.1 | 4.2.3+ | 8.2.0 | 10.11.0 | 8.4 |

### Moodle 5.1 Breaking Change

Moodle 5.1 introduces a `/public` subdirectory. After upgrading to 5.1:
- Nginx docroot must be updated to point to `{moodle_dir}/public/`
- `$CFG->wwwroot` in `config.php` stays the same (domain doesn't change)
- Plugins previously above `/public` must be moved into `/public`

The app handles this automatically as a post-upgrade step.

### Download URL Pattern

```
https://download.moodle.org/stable{VER_NODOT}/moodle-latest-{VER_NODOT}.tgz
Example: stable44 → moodle-latest-44.tgz
Example: stable501 → moodle-latest-501.tgz
```

---

## Three Upgrade Modes

### Mode A — In-Place Upgrade

Upgrade the existing server directly.

```
Source server (e.g. 4.1):
  1. Pre-upgrade backup (DB dump + code tar)
  2. Enable maintenance mode
  FOR EACH STAGE (e.g. 4.1→4.4, then 4.4→5.1):
    3. Verify PHP/DB requirements for this stage
    4. Download target version tgz
    5. Replace Moodle code (preserve config.php, moodledata)
    6. php admin/cli/upgrade.php --non-interactive
    7. php admin/cli/purge_caches.php
    8. Validate version
  9. Post-upgrade: 5.1 docroot fix if applicable
  10. Disable maintenance mode
```

**Risk:** Medium — production server is touched. Rollback available from pre-upgrade backup.

### Mode B — Lift & Shift (New Server)

Migrate to a fresh server, upgrade there. Source is NEVER touched.

```
NEW SERVER PREP:
  1. Install same PHP version as source (e.g. PHP 8.0 for Moodle 4.1)
  2. Install same MariaDB version (or compatible)
  3. Install Moodle at SAME VERSION as source (not latest!)
     → This is critical: new server starts at 4.1 to match source
  4. Configure config.php on new server

DATA MIGRATION (uses existing DR sync modules):
  5. rsync moodledata from source → new server
  6. mysqldump + import DB from source → new server
  7. Verify data integrity

IN-PLACE UPGRADE ON NEW SERVER (source untouched):
  FOR EACH STAGE:
    8. Upgrade PHP/MariaDB if required for next version
    9. Download target version tgz
    10. Replace code, preserve config.php
    11. php admin/cli/upgrade.php --non-interactive
    12. Validate version

CUTOVER:
  13. Update DNS → new server IP
  14. Update wwwroot in config.php
  15. Optional: decommission old server
```

**Risk:** Zero to source. New server is expendable until DNS cutover.

### Mode C — Replica-First Upgrade

Upgrade the existing DR replica (Azure VM). Source is NEVER touched.

```
PRECONDITION: DR replication must be healthy (app checks this)

REPLICA UPGRADE (source untouched, replication paused):
  1. Stop replication on replica (STOP SLAVE)
  2. Pre-upgrade backup on replica
  3. Enable maintenance mode on replica
  FOR EACH STAGE:
    4. Upgrade PHP/MariaDB on replica if needed
    5. Download + replace Moodle code
    6. php admin/cli/upgrade.php --non-interactive
    7. Validate version
  8. Post-upgrade: 5.1 docroot fix if applicable
  9. Disable maintenance mode

CUTOVER (user-triggered):
  10. Update DNS → replica IP (new primary)
  11. Update wwwroot on replica config.php
  12. Old server decommissioned or kept as cold backup
```

**Risk:** Zero to source. Uses existing replicated data — no data migration needed.

---

## Backend API — `/api/upgrade/*`

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/upgrade/status` | Current Moodle version (detected), PHP, DB, upgrade plan, stage status, mode |
| POST | `/api/upgrade/detect` | SSH to server and detect Moodle version, PHP, DB version |
| POST | `/api/upgrade/preflight` | Run pre-upgrade checks for selected mode and target version |
| POST | `/api/upgrade/start` | Start upgrade execution (background task) |
| GET | `/api/upgrade/log` | SSE stream of live upgrade log output |
| POST | `/api/upgrade/rollback` | Restore from pre-upgrade backup |
| POST | `/api/upgrade/cutover` | Execute DNS/config cutover (Mode B and C) |
| GET | `/api/upgrade/versions` | List available Moodle versions for download |

### State File

`/opt/moodle-dr-v11/upgrade_state.json` — persists across restarts:
```json
{
  "mode": "lift_and_shift",
  "source_version": "4.1.9",
  "target_version": "5.1",
  "stages": ["4.1→4.4", "4.4→5.1"],
  "current_stage": 1,
  "stage_status": ["completed", "in_progress"],
  "last_backup_path": "/opt/moodle-dr-backups/upgrade-2026-05-12/",
  "preflight_results": {...},
  "log_path": "/opt/moodle-dr-v11/logs/upgrade.log"
}
```

### Smart Resume

If an upgrade stage fails mid-way, `upgrade_state.json` records `last_successful_stage`. The user can click "Resume" and the app picks up from the failed stage — same pattern as the existing DB migration smart resume.

---

## Frontend — `pageUpgrade()` in index.html

### Nav Entry

Added to sidebar always-visible group "Moodle Upgrade":
- Key: `upgrade`
- Label: `Upgrade Manager`
- Icon: upload/arrow-up

### Page Layout

```
┌─────────────────────────────────────────────────────────────┐
│  🔼 Moodle Upgrade Manager                                   │
│  Automated version upgrade with multi-stage path management  │
├──────────────────┬──────────────────┬───────────────────────┤
│  Detected Version│  Target Version  │  Upgrade Path          │
│  4.1.9 (SSH det.)│  [5.1 ▼]        │  4.1→4.4→5.1  (2 stg) │
└──────────────────┴──────────────────┴───────────────────────┘

MODE SELECT:
  [A] In-Place    [B] Lift & Shift    [C] Replica-First

┌─────────────────────────────────────────────────────────────┐
│  Pre-flight Checks                        [Run Checks ▶]    │
│  ✓ SSH access        ✓ Moodle dir found                     │
│  ✗ PHP 8.1 required  ✓ MariaDB 10.6.7+                      │
│  ✓ Disk space (20GB) ✓ Maintenance CLI available            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Upgrade Stages                                             │
│  ● Stage 1: 4.1 → 4.4   [████████░░] 80%  IN PROGRESS      │
│  ○ Stage 2: 4.4 → 5.1   [──────────]      WAITING          │
│                                                             │
│  [▶ Start Upgrade]   [⏸ Pause]   [↩ Rollback]              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Live Log                                          [Clear]  │
│  [10:04:22] Enabling maintenance mode...                    │
│  [10:04:23] Downloading moodle-44.tgz (87 MB)...           │
│  [10:04:51] Extracting archive...                           │
│  [10:04:58] Replacing Moodle code (preserving config.php)  │
│  [10:05:02] Running upgrade.php --non-interactive...        │
└─────────────────────────────────────────────────────────────┘
```

### Welcome Screen Card (4th card)

Added to setup wizard welcome screen grid:
- Title: "Moodle Upgrade"
- Accent color: magenta `#e040fb`
- Icon: upgrade/arrow-up
- Description: "Automate single or multi-stage Moodle version upgrades. Supports in-place, lift & shift, and replica-first modes with automatic version path calculation and rollback."
- Button: "Open Upgrade Manager"
- Action: `showPage('upgrade')`

---

## Critical Rules (Never Violate)

1. **Never hardcode Moodle versions** — always detect dynamically via SSH
2. **Never touch source server in Lift & Shift or Replica-First modes**
3. **Always backup before any upgrade stage** — DB dump + code tar
4. **Always run pre-flight checks before allowing Start Upgrade**
5. **Waypoint 4.4 is mandatory** for any source < 4.4 targeting 5.x
6. **5.1 docroot change** — Nginx must point to `/public` subdir after 5.1 upgrade
7. **PHP/MariaDB requirements change per stage** — app must upgrade these between stages if on a managed server (Mode B/C with root access)
8. **Smart resume** — upgrade_state.json tracks progress, resume from last successful stage
9. **Glassmorphism design system** — all new UI uses same CSS variables as existing pages
10. **context.md must be updated on every commit**

---

## Transfer Phase Logic (Current State)

### Phase 1 — DUMP
- `mysqldump` streamed in 1 MB chunks to local `/tmp/moodle_seed_<ts>.sql`
- Detects MariaDB vs MySQL dynamically at runtime (never assumes engine)

### Phase 2 — TRANSFER
- **Uses rsync** (not SCP) with flags: `-az --partial --partial-dir=.rsync-partial --info=progress2 --timeout=600`
- Resume-capable: interrupted transfers resume from last good byte
- gzip compression (~70-80% bandwidth saving on SQL text)
- Live progress streamed to seed job output log
- Auth: `state.AZURE_VM_USER` (moodlesync) + `state.SSH_KEY_PATH`

### Phase 3 — IMPORT
- SSH to replica, `pv | mysql` with `--init-command` for session-level optimizations
- Progress polled every 10s from `/tmp/.mdr_import_pct`

### Smart Resume
- Checks `/tmp/moodle_seed.sql` on replica before starting
- If non-zero file exists: skips dump+transfer, jumps directly to import at 65%

---

## Admin SSH Fields (Current State)

### New Config Field: `admin_ssh_key_path`

- Added to `db_replication_db.py` schema (column `admin_ssh_key_path TEXT DEFAULT ''`)
- Persists alongside `admin_ssh_user` in the same SQLite config row
- Falls back to `state.SSH_KEY_PATH` (the sync key) if left blank

### No More Hardcoded `admmoodle`

**Rule: Zero hardcoded admin user defaults anywhere in the codebase.**

| Location | Old (BAD) | New (CORRECT) |
|---|---|---|
| `_seed_mysqldump()` top | `getattr(state, 'ADMIN_VM_USER', 'admmoodle')` | `cfg.get("admin_ssh_user")` — raises Exception if empty |
| `_run_seed_job` preflight (line ~1120) | `getattr(state, 'ADMIN_VM_USER', 'admmoodle')` | `db_db.get_raw_db_config().get("admin_ssh_user")` — raises Exception if empty |
| `saveDbSettings()` JS body | `g('dbs-adminuser') \|\| 'admmoodle'` | `g('dbs-adminuser')` — no fallback |

If `admin_ssh_user` is empty at runtime, the app raises a descriptive error: _"admin_ssh_user is not configured. Set it in DB Replication Settings before seeding."_

### Frontend Fields Added

**DB Settings page** (`pageDbSettings()` → form ID prefix `dbs-`):
- `dbs-adminuser` — Admin SSH User (existed before)
- `dbs-adminkey` — Admin SSH Key Path (NEW, placeholder: `/root/.ssh/admin_key`)
- Both loaded in `initDbSettings()` from `/api/db/config`
- Both saved in `saveDbSettings()` body dict

**Setup Wizard** (`pageSrvDetails()` → form ID prefix `d-`):
- `d-adminuser` — Admin SSH User (existed before, default value removed)
- `d-adminkey` — Admin SSH Key Path (NEW)
- Both loaded in `initSrvDetails()` from `/api/setup/config`
- Both saved in `saveSrvDetails()` body dict
- `setup.py` `db_fields` list now includes `admin_ssh_user` and `admin_ssh_key_path`
- State sync: `state.ADMIN_VM_USER` updated immediately on save in both `setup.py` and `db_replication.py`

---

---

## ⚠️ OPEN BUG — Moodle Version Detection Fails in Local Mode

**Status:** UNRESOLVED as of 2026-05-15 01:20 CDT  
**File to fix:** `backend/upgrade.py` → `_detect_moodle_on_host()` and `_ssh_cmd()`  
**Latest commit:** `4ab9fcc`

### What the user sees
On the Upgrade Manager page, clicking **Detect Source Version** with host=`127.0.0.1` always returns:
> "Version detection failed: version.php not readable at /var/www/html/moodle (app runs as moodledr user)"

### What we KNOW works on the server (manually verified)
```bash
# This works — moodledr CAN read the file via sudo:
sudo -u moodledr sudo cat /var/www/html/moodle/version.php | head -5
# Returns: <?php  // This file is part of Moodle ...

# File exists and permissions:
ls -la /var/www/html/moodle/version.php
# -rw-r--r-- 1 root root 1636 Jun 17 2023 /var/www/html/moodle/version.php

# Sudoers rule IS in place:
cat /etc/sudoers.d/moodledr
# moodledr ALL=(ALL) NOPASSWD: /bin/cat, /usr/bin/find

# moodledr IS in www-data group (usermod -aG was run)
```

### Root cause hypothesis
The app runs as `moodledr` via systemd (`User=moodledr`). When `_ssh_cmd()` detects local mode (`host=127.0.0.1`), it runs:
```python
subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
```
The command it builds is:
```bash
sudo cat /var/www/html/moodle/version.php 2>/dev/null || cat /var/www/html/moodle/version.php 2>/dev/null || sudo -u www-data cat /var/www/html/moodle/version.php 2>/dev/null
```

**The problem:** `sudo` inside a systemd service without a TTY behaves differently from interactive shell. Even with `NOPASSWD`, systemd services may have `NoNewPrivileges=true`, `PrivateTmp`, or other restrictions that silently block `sudo`. All three fallbacks produce empty output and `2>/dev/null` hides the real error.

### What needs to be fixed

**Option A — Read file directly using Python (no subprocess/sudo needed):**
Since the app runs on the SAME machine as the source Moodle, use Python's `open()` to read `version.php` directly. If `moodledr` is in `www-data` group and files are `rw-r--r--` (world-readable by group), this works without any sudo at all:
```python
def _read_version_php_local(path: str) -> str:
    """Read version.php directly using Python file I/O — no subprocess needed."""
    import pathlib
    for attempt in [path + "/version.php", path]:
        try:
            p = pathlib.Path(attempt) if attempt.endswith('.php') else pathlib.Path(attempt) / 'version.php'
            if p.exists():
                return p.read_text(errors='replace')
        except (PermissionError, OSError):
            pass
    return ""
```
If `PermissionError` still occurs with Python `open()`, then the file is not group-readable and we need:

**Option B — Use `os.access()` to check readable, then escalate via a helper script:**
Install a small setuid helper or use `install.sh` to `chmod o+r` the version.php during setup.

**Option C — Simplest fix: in `install.sh`, add `chmod o+r` to moodle files during setup:**
```bash
# Add to install.sh after creating moodledr user:
if [ -f "/var/www/html/moodle/version.php" ]; then
    chmod o+r /var/www/html/moodle/version.php
fi
# Or more broadly:
find /var/www/html/moodle -name "version.php" -exec chmod o+r {} \;
```

**Option D — Fix the subprocess sudo call for systemd context:**
The issue is `sudo` needs `!use_pty` and env preservation. Try:
```bash
sudo -n -E cat /var/www/html/moodle/version.php
```
Or add `Defaults:moodledr !requiretty, !use_pty` to the sudoers file.

### Recommended fix (in order of preference)
1. **First try Python `open()` directly** in `_detect_moodle_on_host` when `_is_local(host)` is True — no subprocess at all for reading files
2. If still PermissionError, fall back to `subprocess` with `sudo -n -E cat` (non-interactive, preserve env)
3. Update `install.sh` to write correct sudoers with `!use_pty` and `!requiretty`

### Code location
- `backend/upgrade.py` lines ~257–320: `_detect_moodle_on_host()` function
- `backend/upgrade.py` lines ~197–240: `_ssh_cmd()` function — local branch
- `install.sh` lines ~84–92: sudoers rule writing

### How to test the fix
```bash
# On the server, test what the moodledr user can actually do inside systemd context:
sudo -u moodledr bash -c 'cat /var/www/html/moodle/version.php 2>&1 | head -3'
sudo -u moodledr bash -c 'sudo cat /var/www/html/moodle/version.php 2>&1 | head -3'
sudo -u moodledr python3 -c "print(open('/var/www/html/moodle/version.php').read()[:100])"
# The one that returns content is what the fix should use.
```

---

## Commit History

| Commit | Description |
|---|---|
| f4f4d55 | fix: version.php detection + welcome auto-skip guard |
| 90ca29c | feat: 4th upgrade card + pill gating on target_configured + local SSH detect |
| 1d8a306 | fix: welcome screen stops on landing, SSH/moodledata pills reflect remote state |
| 2a70356 | fix: sidebar always visible (removed fileSetupComplete/dbSetupComplete guards) |
| beb0d60 | fix: PermissionError /home/user — DATA_DIR env var, interactive port prompt |
| d368e3b | fix: install.sh self-copy when run from /opt/moodle-dr |
| c68d497 | fix: WorkingDirectory in systemd unit + port prompt |
| 5ab4485 | fix: admin SSH fields fully dynamic, context.md updated |
| 2a3832a | Moodle Upgrade module — backend upgrade.py + frontend pageUpgrade() + welcome card |
| 6e0d6f0 | Glassmorphism UI revamp |

---

## Known Issues Solved (Session 2026-05-14)

### 1. Welcome screen auto-redirect (fixed in `f4f4d55`)

**Problem:** `welcome_seen=true` stored in `/var/lib/moodle-dr/setup-state.json` caused `initApp()` to bypass the landing page on every load, even when no real config existed.  
**Fix:** Gate auto-skip on `hasConfig = appState.setupMode || appState.fileSetupComplete || appState.dbSetupComplete`. `welcome_seen=true` alone no longer bypasses the welcome screen.

### 2. Version detection failing (fixed in `f4f4d55`)

**Problem:** `moodledr` service user has no read access to `/var/www/html/moodle/version.php` (owned by `www-data`/`root`). Detection returned empty, UI showed "unknown".  
**Fix in `backend/upgrade.py` → `_detect_moodle_on_host()`:**
- Tries `sudo cat {path}/version.php` first (bypasses ownership)
- Falls back to plain `cat`
- Auto-probes `_MOODLE_COMMON_PATHS`: `/var/www/html/moodle`, `/var/www/moodle`, `/var/www/html`, `/opt/moodle`, `/srv/moodle`
- Final fallback: `find /var/www /opt /srv -name version.php -not -path '*/mod/*' -not -path '*/blocks/*'`
- Returns `resolved_dir` (actual path where version.php was found) and `version_php_found` (bool)
- Regex: single-quoted string match first (`'4.1.9+'`), then generic number fallback

**Fix in `frontend/index.html` → `upgradeDetect()`:**
- Shows `resolved_dir` in success banner when auto-discovered path differs from user-supplied
- Auto-updates `upg-src-dir` input field if path was auto-discovered
- Better error hints: distinguishes "version.php not found" from "found but can't parse"

### 3. SSH status pills gating (fixed in `90ca29c`)

**Problem:** SSH and moodledata pills showed green even when no destination server was configured.  
**Fix:** `public_api.py` `/api/public/status` now returns `target_configured` (bool) and `watchdog.connected`. Frontend gates both pills on `target_configured=false` → always grey when no destination set.

### 4. Local mode detection (fixed in `90ca29c`)

**Problem:** When app runs ON the source server (host=127.0.0.1 or localhost), `_ssh_cmd()` was still trying to SSH to localhost.  
**Fix:** `_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}` — when host is in this set, `_ssh_cmd()` runs `bash -c '{cmd}'` locally via `subprocess` instead of paramiko SSH.

### 5. Sidebar hidden (fixed in `2a70356`)

All nav groups were gated behind setup completion flags. Removed all conditional guards from `buildNav()` — all groups always visible from first load.

### 6. PermissionError /home/user (fixed in `beb0d60`)

Three backend files had hardcoded `/home/user/workspace/...` dev paths. All fixed to use `DATA_DIR` env var. Production default: `/var/lib/moodle-dr`.

### 7. install.sh issues (fixed in `c68d497`, `d368e3b`)

- Port prompt: interactive `read` replaces hardcoded 8080
- `WorkingDirectory` in systemd unit: `/opt/moodle-dr/backend` (not `/opt/moodle-dr`)
- Self-copy guard: `realpath` comparison skips `cp -r backend/` when source == dest

---

## Infrastructure — AWS Test Server

| Component | Value |
|---|---|
| AWS test server | `52.14.180.2` (Ubuntu 22.04) |
| Install dir | `/opt/moodle-dr` |
| Data dir | `/var/lib/moodle-dr` |
| Config dir | `/etc/moodle-dr` |
| Service user | `moodledr` |
| Service name | `moodle-dr` (systemd) |
| App port | Set interactively during install |
| Uvicorn target | `main:app` (WorkingDirectory=`/opt/moodle-dr/backend`) |
| PYTHONPATH | `/opt/moodle-dr/backend` |

To deploy on AWS test server after commits:
```bash
git pull && ./install.sh
```

---

## How to Resume in Claude Code

1. Read this file completely
2. Check `git log --oneline -10` to see latest commits
3. Check `backend/upgrade.py` for current backend state — especially `_detect_moodle_on_host()` and `_LOCAL_HOSTS`
4. Search `index.html` for `pageUpgrade` to find frontend state
5. Search `index.html` for `initApp` to find welcome screen auto-skip logic (`hasConfig` guard)
6. Run `grep -n "upgrade" backend/main.py` to verify router is mounted
7. The upgrade state file is at `/var/lib/moodle-dr/upgrade_state.json` on the server
8. All upgrade logs go to `/var/lib/moodle-dr/logs/upgrade.log`
9. If welcome screen still auto-redirects on server: the fix is in `f4f4d55` — `git pull && ./install.sh`
10. If sudo cat fails (moodledr not in sudoers): add `moodledr ALL=(ALL) NOPASSWD: /bin/cat` to `/etc/sudoers.d/moodledr`
