---
name: letterbox-setup
description: >-
  Wire up a letterbox comms bridge — the one-time, per-harness MCP registration
  and folder-trust steps needed before two terminal AI agents (Claude Code,
  Gemini CLI, Antigravity) can talk. Use this when setting up the plumbing or
  bringing up a peer; for talking on an already-live bridge, use the `letterbox`
  skill instead.
---

# Letterbox — setup & wiring guide

Letterbox lets two terminal coding agents hold a real-time conversation by
passing JSON message files through a shared directory (`~/.letterbox/channels/<channel>/`).
This skill covers the **one-time wiring** to make a harness letterbox-capable.
For using a live bridge (sending, reading, directed messages), see the separate
**`letterbox`** skill.

## The shape of a bridge

A bridge is two launches — one per side, same channel:

```
letterbox <harness> --channel <name> --as <label>
```

`letterbox <harness>` spawns the harness, watches the channel, and injects `📬`
notifications. The one prerequisite is that the target harness has the letterbox
MCP server **registered** so its agent gets the tools. Registration is one-time
per harness and differs by harness (below).

**You cannot add MCP tools to your own already-running session.** Registration
takes effect on the **next launch** of that harness. To bring up a peer: ensure
the wiring below exists for its harness, then spawn
`letterbox <harness> --channel <name> --as <peer-label>`.

Two values you'll reuse:
- the **absolute** path to the `letterbox` binary (`readlink -f "$(command -v letterbox)"`),
- the launch directory must be **trusted** by Gemini/Antigravity (they otherwise
  pop a blocking "trust this folder?" prompt that stalls automation).

## Identity: one label, one participant per channel

Each `--as <label>` must be **unique on its channel**. Launching a second
session under a label already running on that channel is refused:

```
'claude' is already running on channel 'demo' (pid 12345).
Use a different name to run a second instance, e.g.:
  letterbox claude --channel demo --as claude-2
```

This is deliberate — it makes the label the atomic unit of identity, which is
what lets directed addressing (`send_message(to="…")`) be unambiguous. For
multiple agents of the same kind in one room, give them distinct labels:
`claude-review` / `claude-commit`, `gemini-1` / `gemini-2`. The same label *on a
different channel* is fine (different conversation, no collision).

**Labels are case-sensitive, and directed addressing matches them exactly.** A
session launched `--as claude` is a *different* label from `--as Claude`. A peer
that sends `send_message(to="Claude")` does **not** reach the `claude` session;
rather than land a message no 📬 fires for, the send is **rejected at send time**
with an error naming who is live (ADR-064). Pick one spelling per participant and
use it consistently in both the `--as` flag and every `to=` argument. If two
agents disagree on a peer's casing (e.g. one launcher used `--as claude`, the
other peer addresses `to="Claude"`), relaunch so the `--as` label matches the
casing the senders actually use. Agents should always copy the target from
`channel_info` → `participants` rather than guess.

## Per-harness wiring

### Claude Code — no wiring needed

`letterbox claude` auto-generates a temporary MCP config and passes
`--mcp-config` to `claude`. No settings file to edit. Just launch:

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

## Upgrades: relaunch every session on a channel

Letterbox's guards run **inside the launcher process** — the duplicate-label
refusal and the directed-message notification filter both live in the watcher
that `letterbox <harness>` starts. A running session keeps the code it launched
with; editing the source or `pip install -U` does **not** change a live session.

**After upgrading letterbox, relaunch all active sessions on a channel.** A
mixed-version channel (some sessions launched before the upgrade) misbehaves in
predictable ways:

- a pre-upgrade session is invisible to `channel_info`'s `participants` (it never
  wrote a pid-lock), and the duplicate-label guard can't see it either — so a
  second same-label session can sneak in;
- a pre-upgrade watcher fires `📬` for **every** message, ignoring directed
  addressing — so peers get pinged for messages aimed at someone else.

These are not bugs in the current code; they are stale launchers. The cure is a
clean relaunch. If you see phantom duplicate participants or over-notification,
suspect version skew first.

## Gotchas

- **Never run `letterbox mcp` yourself.** It is the stdio MCP server the harness
  spawns. Run by hand it just tells you so and exits.
- **Autonomous launch flags are deliberate.** `letterbox claude` adds
  `--dangerously-skip-permissions` and `letterbox gemini` adds `--yolo`, because
  an injected message cannot wake an agent blocked on an approval prompt.
- **The channel is passed by environment, not baked into config.** The launcher
  exports `LETTERBOX_CHANNEL`, `LETTERBOX_SENDER`, `LETTERBOX_INSTANCE_ID`; the
  MCP server reads them. That is why every config entry above is
  channel-agnostic and you never edit it per channel.
