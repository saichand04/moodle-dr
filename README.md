# Moodle DR Dashboard

**Production-grade Disaster Recovery platform for Moodle LMS** — unified file replication, MySQL 8.x GTID warm-standby replication, real-time DR readiness monitoring, and one-click failover guidance.

> Built for on-prem Ubuntu → Azure VM warm standby. Manual failover only — operator decision always.

---

![Welcome Screen](docs/screenshots/01-welcome.png)

---

## Features

### Setup Wizard (Track-Based)
Single setup wizard that forks by track. Choose what you need:

| Track | What it sets up |
|-------|----------------|
| **File Sync** | rsync + lsyncd for `/moodledata/` filesystem replication |
| **Database Sync** | MySQL 8.x GTID async replication with Percona Toolkit monitoring |
| **Full DR Setup** _(Recommended)_ | Both tracks unified — file sync + DB replication + DR readiness dashboard |

### File Sync
- **lsyncd** — inotify-driven real-time file sync over SSH
- **rsync** — initial bulk transfer + incremental sync
- Partial file detection and force-resume
- Transfer history with per-session stats (bytes, files, duration, success rate)
- SSH connectivity watchdog with auto-retry and event log
- File Sync Failover checklist with gated execution

### Database Sync
- **MySQL 8.x GTID** async replication — warm standby on Azure VM
- **Percona Toolkit** integration: `pt-heartbeat` (sub-second lag), `pt-table-checksum`, `pt-table-sync`
- DB seed via **mysqldump** (default) or **XtraBackup** (no-lock, faster for large DBs)
- SSL certificate generation and exchange
- Auto-configure: replication user, `my.cnf` on source and replica, SSL push

### DB Monitoring
- Live IO/SQL thread status, GTID position, replication errors
- Sub-second heartbeat lag via pt-heartbeat (1s polling)
- Table checksum verification (pt-table-checksum)
- Check history log with per-run results
- DB Failover — pre-flight checks, failover steps preview, gated execution

### DR Readiness Dashboard
- **Readiness Score 0–100** — animated ring gauge, real-time
- **RPO estimate** — file RPO (lsyncd lag) + DB RPO (heartbeat lag) → combined
- **RTO breakdown** — step-by-step estimated recovery time (~7 minutes typical)
- DR Checklist — lsyncd, SSH, disk, MySQL threads, pt-heartbeat, lag thresholds
- DR Readiness is always the single source of truth before initiating failover

### Migration Dashboards
- **Initial Migration** — phase tracker (File Bulk Sync → DB Seed → Verify), live progress bars
- **Live Sync Status** — real-time file sync + DB replication side-by-side, 24h lag chart

### Integrations & External Access
- **Cloudflare Tunnel** — zero-config public HTTPS URL (no account needed for quick tunnels)
- **Tailscale** — mesh VPN, join your tailnet with an auth key
- **SSH Tunnel** — generates ready-to-run `ssh -L` command for local port forwarding
- **Webhook Push** — outbound POST on 7 event types (DR score change, replication error, failover, etc.)
- **Notifications** — Telegram bot, Slack webhook, Discord webhook, Email (SMTP)

### OpenClaw / AI Agent API
- `/api/public/status` — machine-readable JSON status (no auth required for read-only)
- `/api/public/stream` — Server-Sent Events stream for real-time push
- `/metrics` — Prometheus-compatible metrics endpoint
- Token-based auth for detailed data (`X-API-Token` header)
- Downloadable OpenClaw skill file — teaches your local AI agent to query and alert on DR status

---

## Screenshots

### Welcome Screen — Track Selection
![Welcome Screen](docs/screenshots/01-welcome.png)

### File Sync Dashboard
![File Sync Dashboard](docs/screenshots/02-dashboard.png)

### DB Replication Monitor
![DB Replication](docs/screenshots/03-db-replication.png)

### DR Readiness — RPO/RTO Score
![DR Readiness](docs/screenshots/04-dr-readiness.png)

### Initial Migration Tracker
![Initial Migration](docs/screenshots/05-initial-migration.png)

### External Access — Cloudflare Tunnel + Tailscale
![External Access](docs/screenshots/06-ext-access.png)

### OpenClaw API Integration
![OpenClaw API](docs/screenshots/07-openclaw.png)

### Setup Wizard — Prerequisites Check
![Prerequisites](docs/screenshots/08-prereqs.png)

---

## Architecture

```
On-Prem Ubuntu 22.04                    Azure VM (Standard_D4s_v3)
┌─────────────────────────────┐         ┌──────────────────────────┐
│  Moodle App (Docker)        │         │  Moodle App (Docker)     │
│  MySQL 8.x  (native host)   │──GTID──▶│  MySQL 8.x (native host) │
│  /var/www/Azure-MoodleData/ │──rsync─▶│  /moodledata/            │
│  lsyncd  (systemd service)  │         │                          │
│  Moodle DR Dashboard :8080  │         │                          │
└─────────────────────────────┘         └──────────────────────────┘
```

**Key architecture decisions:**
- MySQL runs **native on host** (not in Docker) — required for GTID replication
- Docker is for **Moodle app containers only**
- Failover is **always manual** — dashboard guides, never auto-promotes
- Watchdog monitors only — never auto-recovers (operator decision always)

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI (Python 3.11), uvicorn |
| Frontend | Vanilla JS SPA — single `index.html`, no build step |
| DB (local) | SQLite via aiosqlite (transfers, replication history) |
| Replication | MySQL 8.x native GTID + Percona Toolkit |
| File Sync | lsyncd + rsync over SSH |
| Containerization | Docker + docker-compose (app only, MySQL stays native) |
| Reverse Proxy | nginx |

---

## Quick Start

### Option A — Bare Metal (Recommended)

```bash
# 1. Clone the repo
git clone https://github.com/saichand04/moodle-dr-dashboard.git
cd moodle-dr-dashboard

# 2. Copy and configure environment
cp .env.example .env
nano .env    # Set TARGET_IP, SSH_KEY_PATH, credentials

# 3. Run the installer
chmod +x install.sh
sudo ./install.sh

# 4. Open the dashboard
# http://<your-server-ip>:8080
```

The installer:
- Creates a Python virtualenv at `/opt/moodle-dr/venv`
- Installs all dependencies
- Creates `/var/lib/moodle-dr/` data directory
- Registers and starts a systemd service (`moodle-dr-dashboard`)

---

### Option B — Docker Compose

```bash
git clone https://github.com/saichand04/moodle-dr-dashboard.git
cd moodle-dr-dashboard

cp .env.example .env
# Edit .env

docker compose up -d
```

> **Note:** MySQL must run natively on the host. The Docker container connects to host MySQL via `host.docker.internal`.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Dashboard listen port |
| `HOST` | `0.0.0.0` | Bind address |
| `STATE_FILE` | `/var/lib/moodle-dr/setup-state.json` | Wizard state file path |
| `TARGET_IP` | — | Azure VM IP address |
| `SYNC_USER` | `moodlesync` | SSH user for rsync/lsyncd |
| `SSH_KEY_PATH` | `/root/.ssh/moodle_rsync_ed25519` | SSH private key path |
| `SOURCE_PATH` | `/var/www/Azure-MoodleData/` | Moodle data source |
| `TARGET_PATH` | `/moodledata/` | Moodle data destination |
| `WEBHOOK_SECRET` | — | HMAC secret for outbound webhooks |
| `LOG_LEVEL` | `info` | uvicorn log level |

---

## First-Run Walkthrough

1. **Welcome Screen** — Select your DR track (File Sync / DB Sync / Full DR Setup)
2. **Server Details** — Enter Azure VM IP, SSH key path, MySQL credentials for both servers
3. **Prerequisites** — Dashboard checks all required packages. Click **Install** next to anything missing (lsyncd, Percona Toolkit, etc.)
4. **Auto-Configure** — Click **Auto-Configure Everything** to:
   - Generate SSH key and push to Azure VM
   - Write lsyncd configuration
   - Create MySQL replication user on source
   - Generate and exchange SSL certificates
   - Configure `my.cnf` on both source and replica
5. **Bulk Sync** — Start initial rsync (files) and mysqldump / XtraBackup (DB seed) in parallel
6. **Start Replication** — Configure GTID `CHANGE REPLICATION SOURCE` on replica, start replica threads
7. **Verify** — SSH, replication lag, and disk health checks gate progression
8. **Dashboard** — Live monitoring begins. Check **Live Sync Status** and **DR Readiness** regularly.

---

## MySQL GTID Replication Reference

### Source (`/etc/mysql/mysql.conf.d/mysqld.cnf`)
```ini
[mysqld]
server-id                = 1
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
gtid_mode                = ON
enforce_gtid_consistency = ON
log_slave_updates        = ON
bind-address             = 0.0.0.0
```

### Replica (`/etc/mysql/mysql.conf.d/mysqld.cnf`)
```ini
[mysqld]
server-id                = 2
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
gtid_mode                = ON
enforce_gtid_consistency = ON
log_slave_updates        = ON
read_only                = ON
relay_log                = /var/log/mysql/mysql-relay.log
```

### Replication User (run on source)
```sql
CREATE USER 'repl_user'@'%' IDENTIFIED WITH mysql_native_password BY 'strong-password';
GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

### Start Replication (run on replica after seed)
```sql
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='192.168.1.241',
  SOURCE_USER='repl_user',
  SOURCE_PASSWORD='strong-password',
  SOURCE_AUTO_POSITION=1,
  SOURCE_SSL=1;
START REPLICA;
SHOW REPLICA STATUS\G
```

---

## API Reference

### Public Status (No Auth)
```bash
curl http://your-server:8080/api/public/status
```
```json
{
  "ts": "2026-04-09T06:00:00Z",
  "file_sync": { "lsyncd_running": true, "rsync_running": false },
  "db_sync": { "status": "healthy", "lag_seconds": 0 },
  "dr_readiness": { "score": 95, "rpo_seconds": 60 }
}
```

### Prometheus Metrics
```bash
curl http://your-server:8080/metrics
```

### SSE Real-Time Stream
```bash
curl -N http://your-server:8080/api/public/stream
```

### Token Auth (for detailed data)
```bash
curl -H "X-API-Token: your-token" http://your-server:8080/api/public/status
```
Generate tokens at **Integrations → OpenClaw API → Create Token**.

---

## Project Structure

```
moodle-dr-dashboard/
├── backend/
│   ├── main.py              # FastAPI entrypoint — 86 routes
│   ├── state.py             # Shared in-memory globals
│   ├── db_replication.py    # DB sync — 30 endpoints
│   ├── db_replication_db.py # SQLite schema (4 tables)
│   ├── integrations.py      # Cloudflare, Tailscale, webhooks, OpenClaw
│   ├── public_api.py        # Public status, SSE, Prometheus
│   ├── setup.py             # Setup wizard — file/db/both tracks
│   ├── transfers.py         # Transfer history API
│   ├── transfer_db.py       # Async SQLite layer
│   └── watchdog.py          # SSH connectivity watchdog
├── frontend/
│   └── index.html           # Single-file SPA — 21 pages, ~167KB
├── docs/
│   └── screenshots/         # UI screenshots for docs
├── data/                    # Runtime state + SQLite DBs (gitignored)
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── install.sh               # Bare-metal installer + systemd service
├── requirements.txt
├── DEPLOYMENT.md            # Full production deployment guide
└── OPENCLAW.md              # AI agent / OpenClaw integration guide
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Dashboard won't start | `journalctl -u moodle-dr-dashboard -n 50` |
| Replication lag growing | DB Sync → DB Replication — check IO/SQL thread status |
| lsyncd not syncing | `systemctl status lsyncd` on source |
| SSH connection failed | Verify key is in `~moodlesync/.ssh/authorized_keys` on Azure VM |
| pt-heartbeat stopped | DB Sync → Settings → Start pt-heartbeat |
| Welcome screen loops | Edit `/var/lib/moodle-dr/setup-state.json` — set `welcome_seen: true` |

---

## License

Private — internal use only.

---

*Moodle DR Dashboard v11 — Replicate. Monitor. Recover.*
