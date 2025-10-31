#!/bin/sh
set -xeuo pipefail

# Blobcache systemd user installer script
# This script installs the blobcache binary and systemd user service

BINARY_PATH="./build/out/blobcache_amd64-linux"
SERVICE_FILE="./etc/blobcache.service"

# Check if the service is currently running
WAS_RUNNING=false
if systemctl --user is-active --quiet blobcache.service 2>/dev/null; then
    WAS_RUNNING=true
    echo "Service is running, stopping it..."
    systemctl --user stop blobcache.service
fi

# Install systemd user service
mkdir -p "$HOME/.config/systemd/user"

sudo cp $BINARY_PATH "/usr/bin/blobcache"
cp $SERVICE_FILE "$HOME/.config/systemd/user/blobcache.service"

# Reload systemd daemon to pick up any service file changes
systemctl --user daemon-reload

# Restart the service if it was running before
if [ "$WAS_RUNNING" = true ]; then
    echo "Restarting service..."
    systemctl --user start blobcache.service
fi
