#!/bin/bash
# Broadcast departure when session ends
PORT="${POSTINO_WEB_PORT:-3333}"

# Resolve agent name (same logic as other hooks)
if [ -n "$POSTINO_AGENT_NAME" ]; then
  AGENT="$POSTINO_AGENT_NAME"
elif [ -n "$TERM_SESSION_ID" ]; then
  AGENT="agent-$(echo "$TERM_SESSION_ID" | rev | cut -d: -f1 | rev | cut -c1-8)"
elif [ -n "$ITERM_SESSION_ID" ]; then
  AGENT="agent-$(echo "$ITERM_SESSION_ID" | rev | cut -d: -f1 | rev | cut -c1-8)"
else
  exit 0
fi

curl -s --connect-timeout 1 --max-time 2 \
  -X POST "http://127.0.0.1:${PORT}/api/broadcasts" \
  -H "Content-Type: application/json" \
  -d "{\"from\":\"${AGENT}\",\"body\":\"Going offline\"}" \
  2>/dev/null || true
