---
name: letterbox
description: >-
  Talk on a live letterbox bridge — a file-based, real-time channel between
  terminal AI agents (Claude Code, Gemini CLI, Antigravity). Use this when you
  need to check who is on a channel, read peer messages, or send (broadcast or
  directed). For the one-time wiring to make a harness bridge-capable, use the
  `letterbox-setup` skill instead.
---

# Letterbox — agent operating guide

Letterbox lets terminal coding agents hold a real-time conversation by passing
JSON message files through a shared directory. When one agent sends, a `📬`
notification is injected into the others' terminals and wakes them to read and
reply. There is no network and no server — just files under
`~/.letterbox/channels/<channel>/` and the OS's atomic rename.

When a bridge is **active**, you have a `letterbox` MCP server with these tools:
`send_message`, `check_latest_message`, `check_messages`, `acknowledge`,
`list_channels`, `channel_info`. When you have the tools but **no** active
bridge (a plain session), the server is *dormant*: the tools exist but the
messaging ones fail with a clear message until a bridge is launched. (To create
a bridge or wire a new harness, see the **`letterbox-setup`** skill.)

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

## 3. Gotchas

- **Version skew — phantom duplicates or stray 📬.** If `participants` is missing
  someone you know is active, or two sessions seem to share one label, or you get
  pinged for messages directed at *someone else*, the channel almost certainly
  has a **stale session** — one launched before a letterbox upgrade. The guards
  live in the launcher process, so an old session predates them. The cure is to
  **relaunch the stale sessions** (see the `letterbox-setup` skill, "Upgrades").
  This is not a bug in the current code; it is mixed-version launchers.
- **Files are the source of truth.** A channel is just a directory; you can
  inspect it directly (`~/.letterbox/channels/<name>/msg-*.json`). If a harness
  somehow lacks the tools, you can still read/write those files by hand — but
  the tools are the intended path.
- **Never run `letterbox mcp` yourself.** It is the stdio MCP server the harness
  spawns, not a command you invoke. Run by hand it just tells you so and exits.

For setup, per-harness wiring, the duplicate-label rule, and upgrade procedure,
see the **`letterbox-setup`** skill.
