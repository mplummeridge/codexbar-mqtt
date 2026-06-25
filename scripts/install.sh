#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SUPPORT_DIR="$HOME/Library/Application Support/codexbar-mqtt"
BIN_DIR="$SUPPORT_DIR/bin"
LOG_DIR="$SUPPORT_DIR/logs"
CONFIG="$SUPPORT_DIR/config.json"
PASSWORD_FILE="$SUPPORT_DIR/mqtt-password"
PLIST="$HOME/Library/LaunchAgents/io.github.mplummeridge.codexbar-mqtt.plist"
LABEL="io.github.mplummeridge.codexbar-mqtt"

arch="$(uname -m)"
case "$arch" in
  arm64) release_arch="arm64" ;;
  x86_64) release_arch="amd64" ;;
  *) echo "Unsupported macOS architecture: $arch" >&2; exit 1 ;;
esac

SOURCE_BINARY="${CODEXBAR_MQTT_BINARY:-}"
if [ -z "$SOURCE_BINARY" ]; then
  for candidate in \
    "$ROOT_DIR/codexbar-mqtt" \
    "$ROOT_DIR/dist/codexbar-mqtt-darwin-$release_arch" \
    "$ROOT_DIR/bin/codexbar-mqtt"; do
    if [ -x "$candidate" ]; then
      SOURCE_BINARY="$candidate"
      break
    fi
  done
fi
if [ -z "$SOURCE_BINARY" ] || [ ! -x "$SOURCE_BINARY" ]; then
  echo "Cannot find a codexbar-mqtt binary for $arch." >&2
  echo "Run 'make release' or set CODEXBAR_MQTT_BINARY=/path/to/binary." >&2
  exit 1
fi

mkdir -p "$BIN_DIR" "$LOG_DIR" "$HOME/Library/LaunchAgents"
install -m 0755 "$SOURCE_BINARY" "$BIN_DIR/codexbar-mqtt"
xattr -d com.apple.quarantine "$BIN_DIR/codexbar-mqtt" 2>/dev/null || true
codesign --force --sign - "$BIN_DIR/codexbar-mqtt" >/dev/null 2>&1 || true

if [ ! -f "$CONFIG" ]; then
  cp "$ROOT_DIR/config.example.json" "$CONFIG"
  chmod 600 "$CONFIG"
  machine_id="${MACHINE_ID:-$(scutil --get LocalHostName 2>/dev/null || hostname -s)}"
  machine_id="$(printf '%s' "$machine_id" | tr -cs 'A-Za-z0-9._-' '-')"
  machine_id="${machine_id#-}"
  machine_id="${machine_id%-}"
  [ -n "$machine_id" ] || machine_id="mac"
  sed -i '' "s/macbook-m4/$machine_id/g" "$CONFIG"
  if [ -n "${MQTT_BROKER:-}" ]; then
    escaped="$(printf '%s' "$MQTT_BROKER" | sed 's/[&|]/\\&/g')"
    sed -i '' "s|mqtt://homeassistant.your-tailnet.ts.net:1883|$escaped|" "$CONFIG"
  fi
  if [ -n "${MQTT_USERNAME:-}" ]; then
    escaped="$(printf '%s' "$MQTT_USERNAME" | sed 's/[&|]/\\&/g')"
    sed -i '' "s|\"username\": \"codexbar\"|\"username\": \"$escaped\"|" "$CONFIG"
  fi
fi

if [ ! -f "$PASSWORD_FILE" ]; then
  : > "$PASSWORD_FILE"
fi
chmod 600 "$PASSWORD_FILE"

escape_sed() { printf '%s' "$1" | sed 's/[&|]/\\&/g'; }
sed \
  -e "s|__BINARY__|$(escape_sed "$BIN_DIR/codexbar-mqtt")|g" \
  -e "s|__CONFIG__|$(escape_sed "$CONFIG")|g" \
  -e "s|__SUPPORT_DIR__|$(escape_sed "$SUPPORT_DIR")|g" \
  -e "s|__LOG_DIR__|$(escape_sed "$LOG_DIR")|g" \
  "$ROOT_DIR/launchd/io.github.mplummeridge.codexbar-mqtt.plist.template" > "$PLIST"
chmod 600 "$PLIST"
plutil -lint "$PLIST"

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed: $BIN_DIR/codexbar-mqtt"
echo "Config:    $CONFIG"
echo "Password:  $PASSWORD_FILE"
echo "Logs:      $LOG_DIR"
echo
echo "Edit the config/password as needed, then run:"
echo "  $BIN_DIR/codexbar-mqtt doctor --config '$CONFIG'"
echo "  launchctl kickstart -k gui/$(id -u)/$LABEL"
