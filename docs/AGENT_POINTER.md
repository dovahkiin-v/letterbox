# Letterbox — drop-in agent pointer

A short block to paste into a project's `CLAUDE.md`, `GEMINI.md`, and/or
`AGENTS.md` so an agent launched into a bridge knows how to use it. It is a
**pointer**, not a manual — the canonical guide is the `letterbox` skill
(`skills/letterbox/SKILL.md`); wiring is the `letterbox-setup` skill. Keeping
the detail in the skills and only a pointer in the per-project files avoids drift.

Copy everything between the lines:

---

## Letterbox bridge

If this session was launched with `letterbox <harness> --channel … --as …`, you
have a live peer bridge via the `letterbox` MCP tools. (If it wasn't, the tools
are dormant — `channel_info` returns `{"bridged": false}` — and you can ignore
this section.)

- **First, call `channel_info`.** Don't assume — it confirms you're bridged and
  returns `participants` (who is in the room) plus who last spoke and when.
- **`send_message(body, to=None)`** — omit `to` to **broadcast** to everyone on
  the channel; set `to="<label>"` (a name from `participants`) to **direct** it
  at one peer. Directed = only that peer is notified; others can still read it.
  `to` is **case-sensitive** and must match a *live* peer's label exactly; a
  wrong or mis-cased label (`to="Claude"` vs a peer launched `--as claude`)
  **raises an error** naming who is live rather than sending unnoticed. Copy the
  label verbatim from `participants`.
- **`check_messages()`** — when a `📬 Peer message` notification wakes you, read
  with this (it advances your read marker), then reply with `send_message`.

A channel is an **N-party room** — three or more agents can share it, so address
deliberately. If `participants` looks wrong or you get pinged for messages aimed
at someone else, a stale (pre-upgrade) session is on the channel; relaunch it.

Full usage guide: the `letterbox` skill. Setup/wiring: the `letterbox-setup` skill.

---
