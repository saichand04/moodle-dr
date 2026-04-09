#!/bin/bash
# Moodle DR Dashboard v11 — Install script
set -euo pipefail

SERVICE_USER="moodledr"
INSTALL_DIR="/opt/moodle-dr"
DATA_DIR="/var/lib/moodle-dr"
CONFIG_DIR="/etc/moodle-dr"
SERVICE_NAME="moodle-dr"

echo "=== Moodle DR Dashboard v11 Install ==="

# Check root
if [[ $EUID -ne 0 ]]; then
    echo "Error: Run as root" >&2
    exit 1
fi

# Detect OS
if command -v apt-get &>/dev/null; then
    PKG_MGR="apt-get"
    apt-get update -qq
    apt-get install -y python3 python3-pip python3-venv rsync openssh-client lsyncd curl
else
    echo "Unsupported OS — only Debian/Ubuntu supported"
    exit 1
fi

# Create user
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -m -r -s /bin/false "$SERVICE_USER"
    echo "Created user: $SERVICE_USER"
fi

# Create directories
mkdir -p "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR" "$CONFIG_DIR"

# Copy app files
cp -r backend frontend requirements.txt "$INSTALL_DIR/"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

# Python venv
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"

# Systemd service
cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Moodle DR Dashboard v11
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5
Environment=PYTHONPATH=$INSTALL_DIR
EnvironmentFile=-$CONFIG_DIR/env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"

echo ""
echo "=== Installation complete ==="
echo "Dashboard: http://localhost:8000"
echo "Status: systemctl status $SERVICE_NAME"
