# Moodle DR — Project Context for Claude Code

> **Last updated:** 2026-05-12  
> **Session:** Moodle Upgrade Module planning + build  
> **Pick up from:** Moodle Upgrade module — backend `upgrade.py` + frontend `pageUpgrade()` built and committed. See "Current State" below.

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

## Commit History (Upgrade Module)

| Commit | Description |
|---|---|
| TBD | Initial Moodle Upgrade module — backend upgrade.py + frontend pageUpgrade() + welcome card |

---

## How to Resume in Claude Code

1. Read this file completely
2. Check `git log --oneline -10` to see latest commits
3. Check `backend/upgrade.py` for current backend state
4. Search `index.html` for `pageUpgrade` to find frontend state
5. Run `grep -n "upgrade" backend/main.py` to verify router is mounted
6. The upgrade state file is at `/opt/moodle-dr-v11/upgrade_state.json` on the production server
7. All upgrade logs go to `/opt/moodle-dr-v11/logs/upgrade.log`
