#!/bin/sh
set -eu

# Blobcache installer script
# Usage: curl blobcache.io/install.sh | sh

REPO="blobcache/blobcache"
PLIST_LABEL="io.blobcache.blobcache"

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
    linux)  ;;
    darwin) ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

PLATFORM="${ARCH}-${OS}"

# Get latest release tag
echo "Fetching latest release..."
LATEST_URL="https://api.github.com/repos/$REPO/releases/latest"
TAG=$(curl -sL "$LATEST_URL" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')
if [ -z "$TAG" ]; then
    echo "Failed to determine latest release."
    exit 1
fi
echo "Latest release: $TAG"

SERVICE_FILE_URL="https://raw.githubusercontent.com/$REPO/$TAG/etc/blobcache.service"
PLIST_FILE_URL="https://raw.githubusercontent.com/$REPO/$TAG/etc/io.blobcache.blobcache.plist"

DOWNLOAD_BASE="https://github.com/$REPO/releases/download/$TAG"
BLOBCACHE_URL="$DOWNLOAD_BASE/blobcache_${PLATFORM}"
GIT_REMOTE_BC_URL="$DOWNLOAD_BASE/git-remote-bc_${PLATFORM}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Download binaries
echo "Downloading blobcache for $PLATFORM..."
curl -fSL -o "$TMPDIR/blobcache" "$BLOBCACHE_URL"
chmod +x "$TMPDIR/blobcache"

echo "Downloading git-remote-bc for $PLATFORM..."
curl -fSL -o "$TMPDIR/git-remote-bc" "$GIT_REMOTE_BC_URL"
chmod +x "$TMPDIR/git-remote-bc"

# Install binaries
if [ "$OS" = "darwin" ]; then
    BIN_DIR="/usr/local/bin"
else
    BIN_DIR="/usr/bin"
fi
echo "Installing binaries to $BIN_DIR (requires sudo)..."
sudo mkdir -p "$BIN_DIR"
sudo cp "$TMPDIR/blobcache" "$BIN_DIR/blobcache"
sudo cp "$TMPDIR/git-remote-bc" "$BIN_DIR/git-remote-bc"

# Install service
if [ "$OS" = "linux" ]; then
    # Check if the service is currently running
    WAS_RUNNING=false
    if systemctl --user is-active --quiet blobcache.service 2>/dev/null; then
        WAS_RUNNING=true
        echo "Service is running, stopping it..."
        systemctl --user stop blobcache.service
    fi

    echo "Downloading systemd service file..."
    curl -fSL -o "$TMPDIR/blobcache.service" "$SERVICE_FILE_URL"

    mkdir -p "$HOME/.config/systemd/user"
    cp "$TMPDIR/blobcache.service" "$HOME/.config/systemd/user/blobcache.service"

    systemctl --user daemon-reload

    if [ "$WAS_RUNNING" = true ]; then
        echo "Restarting service..."
        systemctl --user start blobcache.service
    fi

    echo ""
    echo "To start blobcache:"
    echo "  systemctl --user enable --now blobcache.service"

elif [ "$OS" = "darwin" ]; then
    PLIST_DIR="$HOME/Library/LaunchAgents"
    PLIST_DEST="$PLIST_DIR/$PLIST_LABEL.plist"

    # Unload if already loaded
    if launchctl list "$PLIST_LABEL" >/dev/null 2>&1; then
        echo "Service is loaded, unloading it..."
        launchctl unload "$PLIST_DEST" 2>/dev/null || true
    fi

    echo "Downloading launchd plist..."
    curl -fSL -o "$TMPDIR/$PLIST_LABEL.plist" "$PLIST_FILE_URL"

    # Substitute paths in the plist
    AGENT_TMPDIR=$(getconf DARWIN_USER_TEMP_DIR)
    sed -i '' "s|__HOME__|$HOME|g" "$TMPDIR/$PLIST_LABEL.plist"
    sed -i '' "s|__TMPDIR__|$AGENT_TMPDIR|g" "$TMPDIR/$PLIST_LABEL.plist"

    mkdir -p "$PLIST_DIR"
    mkdir -p "$HOME/.local/share/blobcache"
    cp "$TMPDIR/$PLIST_LABEL.plist" "$PLIST_DEST"

    echo ""
    echo "To start blobcache:"
    echo "  launchctl load $PLIST_DEST"
fi

echo ""
echo "blobcache $TAG installed successfully."
