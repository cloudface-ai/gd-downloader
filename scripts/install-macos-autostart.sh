#!/usr/bin/env bash
# Installs a LaunchAgent so the Drive folder server starts at login and restarts if it exits.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOME="${HOME:?}"
AGENT_DIR="$HOME/Library/LaunchAgents"
PLIST_DST="$AGENT_DIR/com.gddownloader.server.plist"
mkdir -p "$AGENT_DIR"
mkdir -p "$HOME/Library/Logs"

if [[ ! -f "$ROOT/.env" ]]; then
  echo "Create $ROOT/.env with GOOGLE_API_KEY=... (see .env.example)" >&2
  exit 1
fi

sed -e "s|__ROOT__|$ROOT|g" -e "s|__HOME__|$HOME|g" \
  "$ROOT/scripts/com.gddownloader.server.plist" > "$PLIST_DST"

launchctl bootout "gui/$(id -u)/com.gddownloader.server" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_DST"

echo "Installed: $PLIST_DST"
echo "Logs: ~/Library/Logs/gddownloader-server.log"
echo "Stop: launchctl bootout gui/$(id -u)/com.gddownloader.server"
