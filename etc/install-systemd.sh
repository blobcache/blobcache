#!/bin/sh
set -xeuo pipefail

# Blobcache systemd user installer script
# This script installs the blobcache binary and systemd user service

BINARY_PATH="./build/out/blobcache_amd64-linux"
SERVICE_FILE="./etc/blobcache.service"

# Install systemd user service
mkdir -p "$HOME/.config/systemd/user"

sudo cp $BINARY_PATH "/usr/bin/blobcache"
cp $SERVICE_FILE "$HOME/.config/systemd/user/blobcache.service"

