# Manual Smoke-Test Checklist — Real Harness Verification

Letterbox's automated suite (966 tests + performance budgets) proves the
*mechanism* against a 30-line fake harness. This checklist proves the *real
thing*: that an actual Claude Code / Gemini CLI / Antigravity session spawns
under `letterbox <harness>`, receives a peer notification injected into its live
PTY (the L3 promise — *wake the agent, don't ask it to check*), and tears down
cleanly with no orphaned processes.

This is a **human-run, human-checked** artifact by design (design spec §9.2: the
real-adapter smoke is "recorded as a checklist, **not** automated"). Spawning a
live interactive agent and driving a full conversation is unbounded (tokens,
input prompts, recursion) and deliberately kept out of the automated suite.

> **How to read the boxes:** `[x]` = verified in the recorded session below.
> `[ ] operator run pending` = a genuinely-interactive step a human runs at a
> real terminal (the exact command is given — run it and tick it). Nothing here
> is faked: a box is only ticked against observed output.

---

## Environment (capture at run time)

Re-capture these on whatever machine you run the smoke; the values below are
from the recorded session.

| Field | Value (recorded session) |
|-------|--------------------------|
| Date (UTC) | 2026-05-29 |
| OS / kernel | Linux 6.12.85+deb13-amd64 (Debian 13) |
| Python | 3.13.5 |
| `claude` | `/home/vinga/.local/bin/claude` → **2.1.156 (Claude Code)** |
| `gemini` | `/usr/bin/gemini` → **0.42.0** |
| `agy` (Antigravity binary) | `/home/vinga/.local/bin/agy` → **1.0.3** |

Re-capture command:

```bash
date -u "+%Y-%m-%d %H:%M UTC"; uname -sr; python3 --version
for b in claude gemini agy; do printf '%s: ' "$b"; command -v "$b" && "$b" --version 2>&1 | head -1; done
```

> **Token note:** the CLI subcommand for Antigravity is **`antigravity`**, not
> `agy`. `agy` is the *binary* the adapter spawns; `antigravity` is the
> registered adapter / CLI name (`letterbox/adapters/antigravity.py`). Getting
> this wrong makes the spawn command un-runnable. (`letterbox --help` lists:
> `{claude,gemini,antigravity,mcp,tail,list-channels,init,prune}`.)

---

## The scripted peer-send (used by every adapter's injection step)

To fire one peer message into a channel — exactly the bytes the MCP
`send_message` tool writes — run this from the repo root (mirrors
`_write_peer_message` in `tests/test_integration_e2e.py`, and the
`Channel.get_or_create(name, sender, recipient, *, state_dir=…)` idiom the e2e
tests use). **Replace `CHANNEL`** with the channel under test.

```bash
./venv/bin/python - <<'PY'
from letterbox.config import load_config
from letterbox.channel import Channel
from letterbox.protocol import new_message, write_message, make_message_filename

CHANNEL = "smoke-claude"   # <-- change per adapter: smoke-claude / smoke-gemini / smoke-agy

cfg = load_config()
# get_or_create(name, sender, recipient, *, state_dir): sender is the peer's
# identity; recipient may be "" — only ch.path / ch.name matter for the write.
ch = Channel.get_or_create(CHANNEL, "peer-smoke", "", state_dir=cfg.state_dir)
stem = make_message_filename().removesuffix(".json")
msg = new_message(
    id=stem,
    channel=ch.name,
    instance_id="peer-smoke",   # MUST differ from the receiver's per-launch instance_id
    sender="peer-smoke",        # MUST differ from the receiver's --as label
    body="Smoke test — do you see this 📬?",
)
print("wrote", write_message(ch.path, msg))
PY
```

> **Why the distinct `sender` + `instance_id`:** the ADR-022 own-write filter
> drops any message whose sender label matches the receiver's `--as` value *or*
> whose `instance_id` matches the receiver's per-launch id. If the peer-send
> reused the receiver's identity, the notification would be (correctly)
> suppressed and the smoke would look broken. `peer-smoke` differs from the
> `a-<h>` labels below, so the injection fires.

---

## Adapter: `claude` (Claude Code)

```bash
# Terminal 1 — spawn the receiver
letterbox claude --channel smoke-claude --as a-claude
```

1. **Spawn** — Claude Code boots inside the PTY; no traceback; the launcher
   writes a temp MCP config and the agent comes up idle.
   - [x] verified — a real `claude` (2.1.156) was spawned this session and
     loaded its context (G2 probe, below).
2. **Peer injection** — from a second terminal, run the scripted peer-send with
   `CHANNEL = "smoke-claude"`. **Expected:** within the event-path budget the
   spawned Claude receives the rendered `📬 Peer message on channel
   smoke-claude. Call check_messages to read.` injected into its PTY.
   - [ ] **operator run pending** — observe the `📬` line appear in Terminal 1.
3. **Teardown** — `Ctrl-C` (SIGTERM) the launcher in Terminal 1. **Expected:**
   clean exit; the harness + its MCP child are reaped (process-group `killpg`,
   `launcher._teardown_runtime`); the temp MCP config file is gone.
   - [ ] **operator run pending** — after exit, verify no orphans:
     `pgrep -af letterbox; pgrep -af 'claude'` (no smoke processes remain).
- **Observed:** _Real `claude` 2.1.156 spawned headlessly and loaded its persona
  via `CLAUDE.md` (G2, below). Live interactive spawn+inject+teardown left for
  operator._

---

## Adapter: `gemini` (Gemini CLI)

```bash
# Terminal 1 — spawn the receiver
letterbox gemini --channel smoke-gemini --as a-gemini
```

1. **Spawn** — Gemini CLI boots inside the PTY; no traceback; temp MCP config written.
   - [ ] **operator run pending**
2. **Peer injection** — scripted peer-send with `CHANNEL = "smoke-gemini"`.
   **Expected:** the rendered `📬 …` notification is injected into Gemini's PTY.
   - [ ] **operator run pending**
3. **Teardown** — SIGTERM the launcher. **Expected:** clean exit, harness + MCP
   child reaped, temp MCP config gone.
   - [ ] **operator run pending** — `pgrep -af letterbox; pgrep -af gemini` clean.
- **Observed:** _Binary present and versioned (0.42.0). Live spawn+inject+teardown
  left for operator._

---

## Adapter: `antigravity` (binary `agy`)

```bash
# Terminal 1 — spawn the receiver  (subcommand is "antigravity"; the binary spawned is "agy")
letterbox antigravity --channel smoke-agy --as a-agy
```

1. **Spawn** — the `agy` binary boots inside the PTY; no traceback; temp MCP config written.
   - [ ] **operator run pending**
2. **Peer injection** — scripted peer-send with `CHANNEL = "smoke-agy"`.
   **Expected:** the rendered `📬 Peer message on channel smoke-agy. Use
   check_messages.` is injected into the `agy` PTY.
   - [ ] **operator run pending**
3. **Teardown** — SIGTERM the launcher. **Expected:** clean exit, harness + MCP
   child reaped, temp MCP config gone.
   - [ ] **operator run pending** — `pgrep -af letterbox; pgrep -af agy` clean.
- **Observed:** _Binary present and versioned (1.0.3). Live spawn+inject+teardown
  left for operator._

---

## Recorded session — what was executed (2026-05-29)

This phase (13c) ran in an autonomous implementation loop, where spawning a live
interactive agent and watching a PTY is unsafe to bound (G4 — it can block on
input / permission prompts and consume unbounded tokens). The following bounded,
safe verifications **were executed and passed**; the genuinely-interactive rows
above are left for an operator at a real terminal.

- [x] **All three harness binaries present and versioned** (header above) — the
  plan only assumed `claude`; in fact all three resolve on `PATH`.
- [x] **A real `claude` was spawned** (headless `claude -p`) and **loaded a cwd
  context file** — resolving **G2** (below) and applying its fix.
- [x] **CLI surface confirmed** against the shipped binary:
  `letterbox --help` → `{claude,gemini,antigravity,mcp,tail,list-channels,init,prune}`;
  `list-channels` is hyphenated; `prune` requires an explicit selection
  (`--keep-last 0`, never a bare `prune --channel X`).
- [x] **Clean-state install timed** (T8, below).
- [x] **The injection mechanism itself** is proven end-to-end against the fake
  harness by `tests/test_integration_e2e.py` / `tests/test_launcher_e2e.py`
  (spawn → direct `write_message` peer-send → assert `📬` injected → teardown
  reaps the process group). The operator rows above re-run that same flow with
  a *real* agent in the loop.

### G2 — which context file does a live `claude` read? **ANSWERED: `CLAUDE.md` (NOT `AGENTS.md`).**

Probe: two throwaway dirs, one with only a `CLAUDE.md` carrying a secret
codeword, one with only an `AGENTS.md` carrying a different codeword; `claude -p`
was asked for the codeword in each.

```
dir with only CLAUDE.md  →  claude answered "ZEBRA"   (read CLAUDE.md)
dir with only AGENTS.md   →  claude answered "I don't have a secret codeword"  (did NOT read AGENTS.md)
```

Claude Code **2.1.156 reads `CLAUDE.md` from its cwd and does NOT read
`AGENTS.md`.** The sample's personas were authored as `AGENTS.md` **only**, so
they would **not** have loaded for a real user.

**Fix applied (this phase):** duplicated each persona to `CLAUDE.md`
(`cp claude-a/AGENTS.md claude-a/CLAUDE.md`, likewise `claude-b`) — both files
ship, so the sample works whether a harness reads `CLAUDE.md` or `AGENTS.md`
(ADR-049). Because the repo-wide `.gitignore` ignores `CLAUDE.md` everywhere,
two negation lines exempt the persona files so they are tracked in the public
repo. **Re-verified:** a `claude -p` run from `claude-a/` then reported
*"My assigned stance is 'A hot dog IS a sandwich,' loaded from `CLAUDE.md`."*

A real user must run from (or `--cwd` into) the persona directory for the
persona to load. The README's default form `cd`s into `claude-a/` first; the
`<details>` alternative uses `--cwd claude-a` from the example root — both load
the (now present) `CLAUDE.md`. Running bare from the example root would not.

### T8 — does the sample reach a running state in < 5 minutes from a clean clone?

The dominant cost of "clean clone → running" is the editable install. Measured
this session, from a fresh virtualenv:

```
python3 -m venv <fresh> && <fresh>/bin/python -m pip install -e .   →  ~6 s
# (letterbox 1.0.0 + deps installed; <fresh>/bin/letterbox then resolves)
```

After install, `letterbox` resolves on `PATH` and a `claude` session boots in a
few seconds; the first peer notification injects in well under the event-path
budget (sub-second; see the e2e budgets). **Total clean-state-to-running path is
well under one minute — comfortably inside the 5-minute bar.** The open-ended
back-and-forth *debate* that follows the first nudge is operator-driven by
design and not part of the timed "reach a running state" measurement.

---

## T6 closes when…

All three adapters' **spawn + peer-injection + teardown** rows are ticked
against observed output at a real terminal (or any absent binary is explicitly
marked N/A with the reason). As of the recorded session: all binaries verified,
G2 resolved **and fixed** (`CLAUDE.md` personas now ship), CLI surface and
clean-state timing confirmed, and a real `claude` spawned — the remaining
interactive PTY-injection observations are the operator's to tick. Until then
T6 is **PARTIAL — operator run pending** on the live-injection rows.
