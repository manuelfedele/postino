#!/bin/bash
# Postino: session start - show agent identity and unread messages
# Fires once per session to orient the agent

PORT="${POSTINO_WEB_PORT:-3333}"

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

echo "[postino] Agent identity: ${AGENT}"
if [ -n "$parts" ]; then
  echo "[postino] ${parts}. Use msg_whoami to check details."
else
  echo "[postino] No unread messages or broadcasts."
fi
