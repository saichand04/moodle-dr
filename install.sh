#!/bin/bash
# Moodle DR Dashboard — Install Script
# Usage: sudo bash install.sh [--port PORT]
# Port is prompted interactively if not passed via --port
set -euo pipefail

# ── Fixed paths ────────────────────────────────────────────────────────────────
SERVICE_USER="moodledr"
INSTALL_DIR="/opt/moodle-dr"
DATA_DIR="/var/lib/moodle-dr"
CONFIG_DIR="/etc/moodle-dr"
SERVICE_NAME="moodle-dr"

# ── Parse args ─────────────────────────────────────────────────────────────────
APP_PORT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) APP_PORT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Moodle DR Dashboard — Install ==="
echo ""

# ── Root check (early — needed before prompt) ──────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo "Error: Run as root (sudo bash install.sh)" >&2
    exit 1
fi

# ── Interactive port prompt (if not passed via --port) ─────────────────────────
if [[ -z "$APP_PORT" ]]; then
    while true; do
        read -rp "Enter the port to run Moodle DR Dashboard on (e.g. 8080): " APP_PORT
        if [[ "$APP_PORT" =~ ^[0-9]+$ ]] && (( APP_PORT >= 1 && APP_PORT <= 65535 )); then
            break
        else
            echo "  Invalid — enter a number between 1 and 65535."
            APP_PORT=""
        fi
    done
fi

echo "  Install dir : $INSTALL_DIR"
echo "  Data dir    : $DATA_DIR"
echo "  Port        : $APP_PORT"
echo ""


# ── Detect script location (repo root) ─────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── OS check ───────────────────────────────────────────────────────────────────
if ! command -v apt-get &>/dev/null; then
    echo "Error: Only Debian/Ubuntu supported" >&2
    exit 1
fi

echo "[1/7] Installing system dependencies..."
apt-get update -qq
# Install only the CLI client — explicitly hold back any server packages so we
# never accidentally upgrade or restart a running MariaDB/MySQL server on this host.
apt-get install -y \
    python3 python3-pip python3-venv \
    rsync openssh-client lsyncd \
    curl net-tools \
    --no-install-recommends -qq

# Install mariadb-client only — guard against pulling in mariadb-server.
# mariadb-client is only needed for mysqldump / mysql CLI on the source side;
# if it would trigger a server package install or upgrade, skip it safely.
if apt-cache show mariadb-client &>/dev/null 2>&1; then
    # Check whether installing mariadb-client would drag in or upgrade mariadb-server.
    WOULD_INSTALL=$(apt-get install --dry-run mariadb-client 2>/dev/null | grep -E '^Inst mariadb-server' || true)
    if [[ -z "$WOULD_INSTALL" ]]; then
        apt-get install -y mariadb-client --no-install-recommends -qq
        echo "  Installed mariadb-client"
    else
        echo "  Skipped mariadb-client (would upgrade mariadb-server on this host — not safe)"
        echo "  Install manually if needed: apt-get install mariadb-client"
    fi
fi

# ── Service user ───────────────────────────────────────────────────────────────
echo "[2/7] Setting up service user..."
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -m -r -s /bin/false "$SERVICE_USER"
    echo "  Created user: $SERVICE_USER"
else
    echo "  User $SERVICE_USER already exists"
fi

# Add moodledr to www-data group so it can read Moodle files (version.php, config.php)
# owned by www-data without needing sudo.
if getent group www-data &>/dev/null; then
    usermod -aG www-data "$SERVICE_USER"
    echo "  Added $SERVICE_USER to www-data group"
fi

# Also grant sudoers entry for read-only ops (cat, find) so version detection
# can read files owned by root or other web-server users.
SUDOERS_FILE="/etc/sudoers.d/moodledr"
cat > "$SUDOERS_FILE" <<'SUDOEOF'
# Moodle DR — allow moodledr to read files as root for version detection
moodledr ALL=(ALL) NOPASSWD: /bin/cat, /usr/bin/find
SUDOEOF
chmod 0440 "$SUDOERS_FILE"
echo "  Sudoers rule written to $SUDOERS_FILE"

# ── Directories ────────────────────────────────────────────────────────────────
echo "[3/7] Creating directories..."
mkdir -p "$INSTALL_DIR/backend" "$INSTALL_DIR/frontend" "$DATA_DIR" "$CONFIG_DIR"

# ── Copy app files ─────────────────────────────────────────────────────────────
echo "[4/7] Copying application files..."

if [[ "$(realpath "$SCRIPT_DIR")" == "$(realpath "$INSTALL_DIR")" ]]; then
    # Script is running from inside the install directory — no copy needed.
    echo "  Source == install dir ($INSTALL_DIR) — skipping copy, files already in place."
else
    # Running from a separate clone/repo dir — copy into place.
    cp -r "$SCRIPT_DIR/backend/"  "$INSTALL_DIR/backend/"
    cp -r "$SCRIPT_DIR/frontend/" "$INSTALL_DIR/frontend/"
    cp    "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"
fi

# Clear stale bytecache (prevents import errors after file updates)
find "$INSTALL_DIR/backend" -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR"

# ── Python virtualenv ──────────────────────────────────────────────────────────
echo "[5/7] Creating Python virtualenv and installing dependencies..."
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"

# ── Default env file (safe to edit) ───────────────────────────────────────────
if [[ ! -f "$CONFIG_DIR/env" ]]; then
    cat > "$CONFIG_DIR/env" <<ENVEOF
# Moodle DR runtime environment
# Edit these values and run: systemctl restart moodle-dr

# Data directory — where SQLite databases and state files are stored
DATA_DIR=$DATA_DIR

# SSH sync user and key (used for rsync / file replication)
AZURE_VM_USER=moodlesync
SSH_KEY_PATH=/root/.ssh/moodle_rsync_ed25519

# Admin SSH user is configured via the web UI (DB Replication Settings page)
# Do NOT hardcode ADMIN_VM_USER here — set it through the app instead.

# Moodle data paths
SOURCE_PATH=/var/www/moodledata/
TARGET_PATH=/moodledata/

# Log file locations
LSYNCD_LOG=/var/log/lsyncd/lsyncd.log
LSYNCD_STATUS=/var/log/lsyncd/lsyncd-status.log
ENVEOF
    chown "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR/env"
    echo "  Created default env file at $CONFIG_DIR/env"
fi

# ── Systemd service ────────────────────────────────────────────────────────────
echo "[6/7] Installing systemd service..."

cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Moodle DR Dashboard
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=$SERVICE_USER
# Run from backend/ so all module imports (state, transfer_db, etc.) resolve
# without needing a package __init__.py
WorkingDirectory=$INSTALL_DIR/backend
ExecStart=$INSTALL_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port $APP_PORT
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=moodle-dr
Environment=PYTHONPATH=$INSTALL_DIR/backend
EnvironmentFile=-$CONFIG_DIR/env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

# ── Start / restart ────────────────────────────────────────────────────────────
echo "[7/7] Starting service..."
if systemctl is-active --quiet "$SERVICE_NAME"; then
    systemctl restart "$SERVICE_NAME"
    echo "  Service restarted"
else
    systemctl start "$SERVICE_NAME"
    echo "  Service started"
fi

# ── Wait and verify ────────────────────────────────────────────────────────────
echo ""
echo "Waiting for app to come up..."
for i in $(seq 1 15); do
    if curl -sf "http://127.0.0.1:${APP_PORT}/" -o /dev/null 2>/dev/null; then
        echo "  ✓ App is responding on port $APP_PORT"
        break
    fi
    if [[ $i -eq 15 ]]; then
        echo "  ✗ App did not respond after 15s — check logs below:"
        echo "------------------------------------------------------------"
        journalctl -u "$SERVICE_NAME" -n 30 --no-pager
        echo "------------------------------------------------------------"
        echo "  Fix any errors above, then run: systemctl start $SERVICE_NAME"
        exit 1
    fi
    sleep 1
done

# ── UFW firewall (if active) ───────────────────────────────────────────────────
if command -v ufw &>/dev/null && ufw status | grep -q "Status: active"; then
    ufw allow "$APP_PORT/tcp" comment "Moodle DR Dashboard" &>/dev/null
    echo "  UFW rule added for port $APP_PORT"
fi

# ── Done ───────────────────────────────────────────────────────────────────────
PUBLIC_IP=$(curl -sf --max-time 3 https://api.ipify.org 2>/dev/null || echo "<your-public-ip>")

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           Moodle DR Dashboard — Installed                ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Local  : http://127.0.0.1:${APP_PORT}                       ║"
echo "║  Public : http://${PUBLIC_IP}:${APP_PORT}                     ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Logs   : journalctl -u $SERVICE_NAME -f                 ║"
echo "║  Status : systemctl status $SERVICE_NAME                 ║"
echo "║  Config : $CONFIG_DIR/env                    ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "IMPORTANT: Make sure port $APP_PORT is open in your firewall / security group."
