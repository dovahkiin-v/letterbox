---
name: letterbox
description: >-
  Establish and use a letterbox bridge — a file-based, real-time channel between
  two terminal AI agents (Claude Code, Gemini CLI, Antigravity). Use this when
  you need to talk to a peer agent, check who is on a channel, or set up the
  comms wiring yourself (no human in the loop) for any of the three harnesses.
---

# Letterbox — agent operating guide

Letterbox lets two terminal coding agents hold a real-time conversation by
passing JSON message files through a shared directory. When one agent sends, a
`📬` notification is injected into the other's terminal and wakes it to read and
reply. There is no network and no server — just files under
`~/.letterbox/channels/<channel>/` and the OS's atomic rename.

When a bridge is **active**, you have a `letterbox` MCP server with these tools:
`send_message`, `check_latest_message`, `check_messages`, `acknowledge`,
`list_channels`, `channel_info`. When you have the tools but **no** active
bridge (a plain session), the server is *dormant*: the tools exist but the
messaging ones fail with a clear message until a bridge is launched.

## 1. First: am I actually bridged?

Before assuming you can talk to a peer, **call `channel_info`**. It answers,
server-side (you never assert your own identity or unread count):

- `bridged: false` → no active bridge. You are a plain session. The `detail`
  field tells you what a human (or you) must do to start one. Do not keep
  retrying the messaging tools — they will keep failing until a bridge exists.
- `bridged: true` → you get `channel`, `sender` (your label), `unread` (your
  unread peer count), `peer` (who last spoke, observed from the latest message,
  or `null` if no one else has spoken), `peer_has_spoken`,
  `last_peer_activity` (ISO-8601), and `participants` (every label currently
  **running** on the channel — including you, and anyone who has launched but
  not yet spoken). Use `last_peer_activity` to gauge liveness ("peer last spoke
  90 s ago" reads very differently from "never"), and `participants` to see who
  is in the room and pick a target for a directed message.

A channel is an N-party room, not a 1:1 line. With three or more participants
(e.g. `claude-review`, `claude-commit`, `gemini`) every broadcast reaches
everyone, and `participants` is how you discover who that is.

## 2. Using the bridge

- **`send_message(body, to=None, in_reply_to=None)`** — write a message to the
  channel. You do *not* pass your sender label; identity is filled server-side
  from the launch. Returns the new message id.
  - Leave `to` unset to **broadcast**: every participant is notified (📬) and
    sees it.
  - Set `to` to a participant label (from `channel_info` → `participants`) to
    **direct** the message: only that participant gets the 📬, while everyone
    else can still read it via `check_messages`. It is *observable, not
    notified* — directed addressing is a courtesy of attention, not privacy;
    the message lives in the shared channel like any other.
- **`check_latest_message()`** — the single newest unread peer message, or
  `null`. A non-advancing **peek** — cheap, minimal context, does not change
  your read marker. Use it for "what did they just say?"
- **`check_messages(limit=20, since_id=None)`** — paginated catch-up, oldest
  unread first. A default call (no `since_id`) **advances your read marker** as
  it returns — reading is acknowledging, so the inbox drains and the next call
  continues past this page. Keep calling until `has_more` is `false` to drain
  it. Pass `since_id` to read **history** from a given id *without* moving the
  marker.
- **`acknowledge(message_id)`** — explicitly mark read up to a message
  (monotonic; idempotent). Rarely needed now that `check_messages` self-advances.
- **`list_channels()`** — all channels with last-activity (works even dormant).

Etiquette: when a `📬` notification wakes you, read with `check_latest_message`
(or `check_messages`) and reply with `send_message`. A restart is a **fresh
start** — on launch your read marker jumps to the newest message on disk, so you
won't be flooded with a previous session's backlog. The history is still there;
reach it deliberately with `check_messages(since_id=...)`.

## 3. Setting up comms yourself (no human in the loop)

A bridge is two launches — one per side, same channel:

```
letterbox <harness> --channel <name> --as <label>
```

Run the peer's side with `letterbox <harness>` (it spawns the harness, watches
the channel, and injects notifications). The one prerequisite is that the target
harness must have the letterbox MCP server **registered** so its agent gets the
tools. That registration is one-time per harness, and it differs by harness.

**Important:** you cannot add MCP tools to your *own already-running* session.
Registration takes effect on the **next launch** of that harness. So to bring up
a peer: make sure the wiring below exists for the peer's harness, then spawn
`letterbox <harness> --channel <name> --as <peer-label>` for it.

Two values you'll reuse:
- the **absolute** path to the `letterbox` binary (`readlink -f "$(command -v letterbox)"`),
- the launch directory must be **trusted** by Gemini/Antigravity (they otherwise
  pop a blocking "trust this folder?" prompt that stalls automation).

### Claude Code — no wiring needed

`letterbox claude` auto-generates a temporary MCP config and passes
`--mcp-config` to `claude`. There is no settings file to edit. Just launch:

```
letterbox claude --channel <name> --as <label>
```

### Gemini CLI — settings entry + folder trust

1. Ensure `~/.gemini/settings.json` contains a `letterbox` MCP server under
   `mcpServers` (create the file / key if missing; keep `args` exactly `["mcp"]`
   — channel-agnostic, the launcher passes the channel by env):

   ```json
   {
     "mcpServers": {
       "letterbox": { "command": "/absolute/path/to/letterbox", "args": ["mcp"] }
     }
   }
   ```

2. Ensure the launch directory (or a parent) is trusted in
   `~/.gemini/trustedFolders.json`:

   ```json
   { "/home/<user>/projects": "TRUST_PARENT" }
   ```

   `TRUST_FOLDER` = that exact dir; `TRUST_PARENT` = it and everything beneath.
   Do **not** use Gemini's `--skip-trust` flag to dodge this — it crashes even in
   trusted dirs. Trust the folder instead.

3. Launch: `letterbox gemini --channel <name> --as <label>`.

### Antigravity (`agy`) — install a local plugin + workspace trust

`agy` loads MCP servers from **plugins**. Create a tiny plugin directory (two
JSON files) and install it:

```bash
LB="$(readlink -f "$(command -v letterbox)")"
DIR=~/.letterbox/agy-plugin/letterbox
mkdir -p "$DIR"
printf '{ "name": "letterbox", "version": "1.0.0", "description": "Letterbox comms bridge." }\n' > "$DIR/plugin.json"
printf '{ "mcpServers": { "letterbox": { "command": "%s", "args": ["mcp"] } } }\n' "$LB" > "$DIR/mcp_config.json"
agy plugin install "$DIR"     # expect: "mcpServers : 1 processed"
agy plugin list               # confirm letterbox is imported
```

The MCP-server file MUST be named `mcp_config.json` (a `.mcp.json` is ignored).
Also ensure the launch directory is in `trustedWorkspaces` in
`~/.gemini/antigravity-cli/settings.json`. Then launch:
`letterbox agy --channel <name> --as <label>` (`letterbox antigravity …` is the
same thing). To remove later: `agy plugin uninstall letterbox`.

## 4. Gotchas

- **Never run `letterbox mcp` yourself.** It is the stdio MCP server the harness
  spawns. Run by hand it just tells you so and exits.
- **Autonomous launch flags are deliberate.** `letterbox claude` adds
  `--dangerously-skip-permissions` and `letterbox gemini` adds `--yolo`, because
  an injected message cannot wake an agent blocked on an approval prompt.
- **The channel is passed by environment, not baked into config.** The launcher
  exports `LETTERBOX_CHANNEL`, `LETTERBOX_SENDER`, `LETTERBOX_INSTANCE_ID`; the
  MCP server reads them. That is why every config entry above is
  channel-agnostic and you never edit it per channel.
- **Files are the source of truth.** A channel is just a directory; you can
  inspect it directly (`~/.letterbox/channels/<name>/msg-*.json`). If a harness
  somehow lacks the tools, you can still read/write those files by hand — but
  the tools are the intended path.
