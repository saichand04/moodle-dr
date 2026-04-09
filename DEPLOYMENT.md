# Moodle DR Dashboard v11 — Deployment Guide

Production-grade disaster recovery dashboard for Moodle LMS. Manages file replication (rsync + lsyncd) and MySQL 8.x GTID replication to an Azure VM warm standby with real-time DR readiness monitoring.

---

## Architecture Overview

```
On-Prem Ubuntu 22.04                    Azure VM (Standard_D4s_v3)
┌─────────────────────────────┐         ┌──────────────────────────┐
│  Moodle App (Docker)        │         │  Moodle App (Docker)     │
│  MySQL 8.x (native host)    │──GTID──▶│  MySQL 8.x (native host) │
│  /var/www/Azure-MoodleData/ │──rsync─▶│  /moodledata/            │
│  lsyncd (systemd service)   │         │                          │
│  Moodle DR Dashboard :8080  │         │                          │
└─────────────────────────────┘         └──────────────────────────┘
```

- **Replication**: async GTID, warm standby, **manual failover only** (operator decision always)
- **File Sync**: lsyncd → rsync over SSH (key-based, moodlesync user)
- **DB Monitoring**: Percona Toolkit (pt-heartbeat, pt-table-checksum)
- **Dashboard**: FastAPI + single-file SPA, served from source server

---

## Prerequisites

### Source Server (On-Prem)
- Ubuntu 22.04 LTS (or compatible Debian-based)
- Python 3.11+
- MySQL 8.x running natively (NOT containerized)
- rsync, openssh-client
- lsyncd (optional — dashboard can install it)
- Percona Toolkit (optional — dashboard can install it)
- ≥10 GB free disk on `/var`

### Target Server (Azure VM)
- Ubuntu 22.04 LTS
- MySQL 8.x running natively
- SSH user: `moodlesync` (passwordless sudo NOT required, rsync access only)
- Admin user: `admmoodle` (passwordless sudo, for MySQL config steps)
- `/moodledata/` mount point ready (4TB NVMe recommended)

### Network
- Source → Target: SSH port 22 open, MySQL port 3306 open (internal)
- Dashboard: port 8080 (or configure nginx reverse proxy)

---

## Method 1: Bare-Metal Install (Recommended)

### Step 1 — Clone / Extract
```bash
# Extract the package
unzip moodle-dr-dashboard-v11.zip -d /opt/moodle-dr
cd /opt/moodle-dr
```

### Step 2 — Run Install Script
```bash
chmod +x install.sh
sudo ./install.sh
```

The install script will:
- Install Python 3.11+ if needed
- Create a Python virtual environment at `/opt/moodle-dr/venv`
- Install all Python dependencies
- Create the data directory at `/var/lib/moodle-dr/`
- Set up a systemd service (`moodle-dr-dashboard.service`)
- Enable and start the service on port 8080

### Step 3 — Configure Environment
```bash
cp .env.example .env
nano .env
```

Edit the following required values:
```env
# Dashboard
PORT=8080
HOST=0.0.0.0
STATE_FILE=/var/lib/moodle-dr/setup-state.json

# File Sync (pre-fill for first run)
TARGET_IP=20.86.145.98
SYNC_USER=moodlesync
SSH_KEY_PATH=/root/.ssh/moodle_rsync_ed25519
SOURCE_PATH=/var/www/Azure-MoodleData/
TARGET_PATH=/moodledata/

# Security (generate with: openssl rand -hex 32)
WEBHOOK_SECRET=your-webhook-secret-here
```

### Step 4 — Start / Restart Service
```bash
sudo systemctl restart moodle-dr-dashboard
sudo systemctl status moodle-dr-dashboard
```

### Step 5 — Access Dashboard
Open your browser: `http://192.168.1.241:8080`

The welcome screen will appear on first run. Select your track:
- **File Sync** — rsync + lsyncd only
- **Database Sync** — MySQL GTID replication only
- **Full DR Setup** — Both (recommended for production)

---

## Method 2: Docker Compose

### Step 1 — Prepare Environment
```bash
cd /opt/moodle-dr
cp .env.example .env
# Edit .env as described above
```

### Step 2 — Build and Start
```bash
docker compose up -d
```

This starts:
- `moodle-dr-app` — FastAPI dashboard on port 8080
- `moodle-dr-nginx` — nginx reverse proxy (port 80 → 8080)

### Step 3 — Verify
```bash
docker compose ps
docker compose logs -f moodle-dr-app
```

### Data Persistence
The compose file mounts `./data/` to `/var/lib/moodle-dr/` inside the container.
All state, SQLite databases, and configuration persist across container restarts.

### Docker Notes
- MySQL runs **on the host**, not in Docker. The dashboard container connects to host MySQL via `host.docker.internal` or the host's LAN IP.
- lsyncd runs **on the host**, not in Docker. The dashboard manages it via systemctl commands.
- The dashboard Docker image is non-root (user `appuser`, uid 1000).

---

## Method 3: Cloudflare Tunnel (Remote Access)

To expose the dashboard securely without opening ports:

1. Install cloudflared on the source server:
   ```bash
   curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o cloudflared.deb
   sudo dpkg -i cloudflared.deb
   ```

2. In the dashboard, navigate to **Integrations → External Access → Cloudflare Tunnel**
3. Optionally paste a named tunnel token, or leave blank for a quick trycloudflare.com URL
4. Click **Start Tunnel** — the dashboard manages the cloudflared process

---

## MySQL GTID Replication Setup

The setup wizard (Full DR Setup path) handles most of this automatically via **Auto-Configure Everything**. For manual reference:

### Source MySQL (`/etc/mysql/mysql.conf.d/mysqld.cnf`)
```ini
[mysqld]
server-id               = 1
log_bin                 = /var/log/mysql/mysql-bin.log
binlog_format           = ROW
gtid_mode               = ON
enforce_gtid_consistency = ON
log_slave_updates       = ON
bind-address            = 0.0.0.0
```

### Replica MySQL (`/etc/mysql/mysql.conf.d/mysqld.cnf`)
```ini
[mysqld]
server-id               = 2
log_bin                 = /var/log/mysql/mysql-bin.log
binlog_format           = ROW
gtid_mode               = ON
enforce_gtid_consistency = ON
log_slave_updates       = ON
read_only               = ON
relay_log               = /var/log/mysql/mysql-relay.log
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

## pt-heartbeat Setup

Percona pt-heartbeat provides sub-second replication lag monitoring:

```bash
# Install Percona Toolkit
sudo apt-get install percona-toolkit

# Start pt-heartbeat on source (as systemd service)
# The dashboard manages this — use DB Sync → Settings → Start pt-heartbeat
# Or manually:
pt-heartbeat --user=root --password=X --create-table --daemonize \
  --database=moodle --interval=1
```

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Dashboard port |
| `HOST` | `0.0.0.0` | Bind address |
| `STATE_FILE` | `/var/lib/moodle-dr/setup-state.json` | Setup state persistence |
| `DB_PATH` | `/var/lib/moodle-dr/transfers.db` | SQLite database for transfers + replication history |
| `TARGET_IP` | `` | Azure VM IP address |
| `SYNC_USER` | `moodlesync` | SSH user for rsync |
| `SSH_KEY_PATH` | `/root/.ssh/moodle_rsync_ed25519` | SSH private key |
| `SOURCE_PATH` | `/var/www/Azure-MoodleData/` | Moodle data source path |
| `TARGET_PATH` | `/moodledata/` | Moodle data destination path |
| `WEBHOOK_SECRET` | `` | HMAC secret for outbound webhooks |
| `LOG_LEVEL` | `info` | uvicorn log level (debug/info/warning/error) |

---

## First-Run Guide

1. **Welcome Screen** — Select your DR track (File Sync / DB Sync / Full DR Setup)
2. **Server Details** — Enter Azure VM IP, SSH key path, MySQL credentials
3. **Prerequisites** — Dashboard checks rsync, lsyncd, MySQL, Percona Toolkit. Click **Install** next to any missing package.
4. **Auto-Configure** — Click **Auto-Configure Everything** to:
   - Generate SSH key (if needed) and push to Azure VM
   - Write lsyncd configuration
   - Create MySQL replication user
   - Generate and exchange SSL certs
   - Configure `my.cnf` on both servers
5. **Bulk Sync** — Kick off initial rsync (file sync) and mysqldump/XtraBackup (DB seed) in parallel
6. **Start Replication** — Configure GTID `CHANGE REPLICATION SOURCE` on replica, start replica threads
7. **Verify** — SSH connectivity, replication lag, disk health checks
8. **Dashboard** — Live monitoring begins. Check Live Sync Status and DR Readiness regularly.

---

## Production Hardening

### nginx Reverse Proxy
```nginx
server {
    listen 80;
    server_name moodle-dr.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_buffering off;  # Required for SSE streams
    }
}
```

### systemd Service (created by install.sh)
```ini
[Unit]
Description=Moodle DR Dashboard
After=network.target mysql.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/moodle-dr
EnvironmentFile=/opt/moodle-dr/.env
ExecStart=/opt/moodle-dr/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Firewall
```bash
# Allow dashboard access from your network only
ufw allow from 192.168.1.0/24 to any port 8080
ufw allow from 192.168.1.0/24 to any port 3306  # MySQL (internal only)
```

### Log Rotation
```bash
# /etc/logrotate.d/moodle-dr
/var/log/moodle-dr/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
}
```

---

## Upgrading

```bash
# Stop service
sudo systemctl stop moodle-dr-dashboard

# Backup data
cp -r /var/lib/moodle-dr/ /var/lib/moodle-dr.bak/

# Extract new version
unzip moodle-dr-dashboard-v12.zip -d /opt/moodle-dr-new
rsync -a --exclude=data/ /opt/moodle-dr-new/ /opt/moodle-dr/

# Update dependencies
source /opt/moodle-dr/venv/bin/activate
pip install -r requirements.txt

# Restart
sudo systemctl start moodle-dr-dashboard
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Dashboard won't start | Check `journalctl -u moodle-dr-dashboard -n 50` |
| Replication lag growing | Check IO/SQL thread status in DB Sync → DB Replication |
| lsyncd not syncing | Check `systemctl status lsyncd` on source server |
| SSH connection failed | Verify SSH key is in `~moodlesync/.ssh/authorized_keys` on Azure VM |
| pt-heartbeat stopped | Dashboard shows "Stopped" — restart via DB Sync → Settings → Start pt-heartbeat |
| Setup state reset accidentally | Edit `/var/lib/moodle-dr/setup-state.json` to restore values |

---

## Directory Structure

```
/opt/moodle-dr/
├── backend/
│   ├── main.py              # FastAPI entrypoint (86 routes)
│   ├── state.py             # Shared in-memory state
│   ├── db_replication.py    # DB sync endpoints (30 routes)
│   ├── db_replication_db.py # SQLite schema + CRUD (4 tables)
│   ├── integrations.py      # Cloudflare, Tailscale, webhooks, OpenClaw
│   ├── public_api.py        # /api/public/status, SSE stream, /metrics
│   ├── setup.py             # Setup wizard (file/db/both tracks)
│   ├── transfers.py         # Transfer history API
│   ├── transfer_db.py       # SQLite async layer
│   └── watchdog.py          # SSH connectivity watchdog
├── frontend/
│   └── index.html           # Single-file SPA (21 pages, ~167KB)
├── data/                    # Runtime data (state + SQLite DBs)
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── install.sh
├── requirements.txt
├── DEPLOYMENT.md            # This file
└── OPENCLAW.md              # OpenClaw integration guide
```

---

*Moodle DR Dashboard v11 — Built for production. Manual failover only — operator decision always.*
