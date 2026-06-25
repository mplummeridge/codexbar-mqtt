#!/bin/bash
set -euo pipefail
LABEL="dev.mmv3.codexbar-mqtt"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
SUPPORT_DIR="$HOME/Library/Application Support/codexbar-mqtt"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
rm -f "$PLIST"
if [ "${1:-}" = "--purge" ]; then
  rm -rf "$SUPPORT_DIR"
  echo "Removed agent, configuration, logs and spool."
else
  rm -rf "$SUPPORT_DIR/bin"
  echo "Removed agent and binary. Configuration/spool retained in: $SUPPORT_DIR"
fi
