# Letterbox — Known Bugs

Open issues discovered during live use. File bugs here; promote to ADR when a fix decision is made.

---

## BUG-001 — Vibe MCP restart silently drops directed messages (2026-06-29)

**Symptom:** A message sent with `to="mistral"` (directed addressing) lands on disk and the
notification fires correctly, but `check_messages` and `check_latest_message` both return empty
when Mistral calls them. Intermittent — only triggers if the Vibe MCP child happens to restart
in the narrow window between the message being written and Mistral reading it.

**Root cause:** The Vibe MCP bridge script (`letterbox/data/vibe-mcp-bridge.sh`) reads
`LETTERBOX_INSTANCE_ID` from the PTY-parent's `/proc/$PPID/environ` at spawn time:

```sh
INSTANCE_ID=$(_ppid_env LETTERBOX_INSTANCE_ID)
```

The PTY-parent's `LETTERBOX_INSTANCE_ID` is fixed for its lifetime. If Vibe's ACP transport
restarts the `letterbox mcp` child for any reason (disconnect, crash, Vibe reload), the bridge
spawns a new child with the **same** instance_id. That child calls `_align_read_marker` at
startup (`mcp_server.py`), which advances the HWM to the latest message on disk — silently
consuming the directed message before the agent ever reads it.

The watcher's start watermark (ADR-024) is also set past the message at that point, so no
re-notification fires either. The message is permanently invisible to the agent.

**Why broadcast messages weren't affected in the observed case:** coincidence of timing — the
broadcast was sent after the MCP restart window had closed. Broadcasts have the same structural
exposure; they just didn't hit it.

**Evidence from the incident:**
- Message on disk: `~/.letterbox/channels/vibe/msg-20260629T180641436544-2abf88458c7c4b26a39ea4004805064c.json`
  (`sender: gemini`, `recipient: mistral`)
- Mistral's HWM advanced to that exact message ID at `18:06:45` (4 s after the message landed),
  with `instance_id: lb-20260629T180405Z-717e2a` — same instance_id as Mistral's server that
  started at `18:04:05`, consistent with a restarted child impersonating the original session.

**Proposed fix:** Generate a fresh instance_id in the bridge script instead of inheriting the
PTY-parent's:

```sh
# Replace:
INSTANCE_ID=$(_ppid_env LETTERBOX_INSTANCE_ID)

# With:
INSTANCE_ID="lb-$(date -u +%Y%m%dT%H%M%SZ)-$(head -c3 /dev/urandom | xxd -p)"
```

Each MCP child gets a unique identity. `_align_read_marker` still runs on restart (correct —
suppresses backlog flood per ADR-024), but it no longer impersonates the original PTY-parent
session, making restarts easier to diagnose and keeping the instance_id semantics honest.

**Note:** Other harness adapters (Claude, Gemini) use `--mcp-config` flags and may have a
similar latent issue if their MCP child ever restarts with the same instance_id. Worth auditing
when this gets fixed.
