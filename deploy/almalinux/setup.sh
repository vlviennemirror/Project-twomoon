#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/twomoon"
VENV_DIR="${INSTALL_DIR}/venv"
SERVICE_USER="twomoon"
SERVICE_NAME="twomoon-agent"

echo "=== Two Moon AlmaLinux Agent — Setup ==="

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: This script must be run as root."
    exit 1
fi

echo "[1/7] Installing system dependencies..."
dnf install -y python3.11 python3.11-pip python3.11-devel gcc

echo "[2/7] Creating service user..."
if ! id "${SERVICE_USER}" &>/dev/null; then
    useradd --system --create-home --shell /sbin/nologin "${SERVICE_USER}"
    echo "User '${SERVICE_USER}' created."
else
    echo "User '${SERVICE_USER}' already exists."
fi

echo "[3/7] Setting up project directory..."
mkdir -p "${INSTALL_DIR}"
cp -r shared_lib/ "${INSTALL_DIR}/shared_lib/"
cp -r core_node/ "${INSTALL_DIR}/core_node/"
cp almalinux_agent.py "${INSTALL_DIR}/"
cp requirements.txt "${INSTALL_DIR}/"

if [ ! -f "${INSTALL_DIR}/.env" ]; then
    cp .env.example "${INSTALL_DIR}/.env"
    echo "WARNING: .env copied from template. Edit ${INSTALL_DIR}/.env with real values."
fi

chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"

echo "[4/7] Creating Python virtual environment..."
python3.11 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
"${VENV_DIR}/bin/pip" install --no-cache-dir -r "${INSTALL_DIR}/requirements.txt"

echo "[5/7] Installing systemd service..."
cp deploy/almalinux/twomoon-agent.service /etc/systemd/system/${SERVICE_NAME}.service
systemctl daemon-reload

echo "[6/7] Configuring firewall (optional)..."
if command -v firewall-cmd &>/dev/null; then
    echo "Firewall detected. No inbound ports required for the agent."
fi

echo "[7/7] Enabling and starting service..."
systemctl enable --now "${SERVICE_NAME}"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Service status:"
systemctl status "${SERVICE_NAME}" --no-pager --lines=5
echo ""
echo "Useful commands:"
echo "  journalctl -u ${SERVICE_NAME} -f          # Live logs"
echo "  systemctl restart ${SERVICE_NAME}          # Restart agent"
echo "  systemctl stop ${SERVICE_NAME}             # Stop agent"
echo "  cat ${INSTALL_DIR}/.env                    # View config"
echo ""
echo "IMPORTANT: Edit ${INSTALL_DIR}/.env with your production values!"
