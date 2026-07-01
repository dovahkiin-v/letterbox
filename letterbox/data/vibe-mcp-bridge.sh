#!/bin/sh
# Letterbox Vibe MCP bridge — one-time setup script for Mistral's Vibe CLI.
#
# WHY THIS EXISTS
# Vibe's acp.transports.spawn_stdio_transport passes only a trimmed environment
# to MCP subprocesses: HOME, PATH, SHELL, TERM, USER, LOGNAME — not the full
# parent env. The letterbox PTY-Parent sets LETTERBOX_CHANNEL, LETTERBOX_SENDER,
# and LETTERBOX_INSTANCE_ID in Vibe's own environment at launch, but those vars
# never reach the letterbox mcp child via the normal inheritance path.
#
# This script reads them back from Vibe's process environment via /proc at
# spawn time, making any channel work without touching this file. It is
# channel-agnostic by design: install it once, then any invocation of
# "letterbox vibe --channel <anything>" just works. Linux-only (/proc).
# See ADR-067.
#
# SETUP (one-time)
#   1. Copy this file somewhere on PATH or to a known absolute path, e.g.:
#        cp this-file ~/.letterbox/vibe-mcp-bridge.sh
#        chmod +x ~/.letterbox/vibe-mcp-bridge.sh
#
#   2. In ~/.vibe/config.toml, replace the letterbox [[mcp_servers]] entry with:
#        [[mcp_servers]]
#        name = "letterbox"
#        transport = "stdio"
#        command = "/home/YOU/.letterbox/vibe-mcp-bridge.sh"
#        args = []
#
#   3. Done. "letterbox vibe --channel any-name --as any-label" now works.

_ppid_env() {
    # Read a single env var from the parent process's /proc entry.
    # $PPID is Vibe's Python PID (the direct parent of this subprocess).
    tr '\0' '\n' < "/proc/$PPID/environ" 2>/dev/null \
        | grep "^${1}=" \
        | head -1 \
        | cut -d= -f2-
}

CHANNEL=$(_ppid_env LETTERBOX_CHANNEL)
SENDER=$(_ppid_env LETTERBOX_SENDER)
INSTANCE_ID="lb-$(date -u +%Y%m%dT%H%M%SZ)-$(head -c3 /dev/urandom | xxd -p)"

exec letterbox mcp \
    ${CHANNEL:+--channel "$CHANNEL"} \
    ${SENDER:+--as "$SENDER"} \
    ${INSTANCE_ID:+--instance-id "$INSTANCE_ID"}
