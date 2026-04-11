#!/bin/bash
# Postino: session start - ensure GUI daemon is running, show agent status
# Fires once per session to orient the agent

PORT="${POSTINO_WEB_PORT:-3333}"
PLUGIN_ROOT="${CLAUDE_PLUGIN_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"

# Auto-start the GUI daemon if it's not already running
if ! curl -s --connect-timeout 1 --max-time 1 "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then
  nohup node "${PLUGIN_ROOT}/dist/cli.js" serve >/dev/null 2>&1 &
  # Brief wait for startup
  sleep 1
fi

# Resolve agent name (same logic as src/types.ts)
if [ -n "$POSTINO_AGENT_NAME" ]; then
  AGENT="$POSTINO_AGENT_NAME"
elif [ -n "$TERM_SESSION_ID" ]; then
  AGENT="agent-$(echo "$TERM_SESSION_ID" | rev | cut -d: -f1 | rev | cut -c1-8)"
elif [ -n "$ITERM_SESSION_ID" ]; then
  AGENT="agent-$(echo "$ITERM_SESSION_ID" | rev | cut -d: -f1 | rev | cut -c1-8)"
else
  exit 0
fi

result=$(curl -s --connect-timeout 1 --max-time 2 "http://127.0.0.1:${PORT}/api/check/${AGENT}" 2>/dev/null) || exit 0

msg=$(echo "$result" | grep -o '"unreadMessages":[0-9]*' | cut -d: -f2)
bc=$(echo "$result" | grep -o '"unseenBroadcasts":[0-9]*' | cut -d: -f2)

parts=""
[ "${msg:-0}" -gt 0 ] && parts="${msg} unread message(s)"
[ "${bc:-0}" -gt 0 ] && { [ -n "$parts" ] && parts="$parts, "; parts="${parts}${bc} new broadcast(s)"; }

# Fetch online agent count for context
agents_json=$(curl -s --connect-timeout 1 --max-time 2 "http://127.0.0.1:${PORT}/api/agents" 2>/dev/null) || agents_json="[]"
online=$(echo "$agents_json" | grep -o '"online":true' | wc -l | tr -d ' ')

cat <<POSTINO
[postino] You are "${AGENT}". ${online} other agent(s) online.
POSTINO

if [ -n "$parts" ]; then
  echo "[postino] ${parts}. Call msg_whoami now."
fi

cat <<'POSTINO'
[postino] You have postino messaging tools. Use them:
  - Call msg_whoami at the start of every session to orient yourself.
  - Call msg_broadcast when you complete a task, hit a blocker, or have a status update.
  - Call msg_read if you have unread messages, then act on them.
  - Call msg_send to request help from or delegate work to a specific agent.
  - Rename yourself with msg_rename to a meaningful name (e.g. "reviewer", "frontend-dev").
POSTINO
