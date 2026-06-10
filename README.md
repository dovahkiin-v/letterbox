# Letterbox

![Status: Reference Implementation](https://img.shields.io/badge/status-reference%20implementation-blue) ![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue) ![License: MIT](https://img.shields.io/badge/license-MIT-green) ![POSIX only](https://img.shields.io/badge/platform-POSIX-lightgrey)

> 📌 **Built for internal production use.** Architecture proven across 6 months of daily AI development. Open-sourced as a reference implementation.

**In plain terms:** If you use AI coding assistants in the terminal, you normally work with one at a time — and getting two of them to collaborate means copy-pasting messages between windows yourself. Letterbox lets two assistants (say, Claude and Gemini) talk *directly* to each other and work a task together, hands-free.

**The result:** one agent can plan while another reviews, or the two can split the work between them — collaborating on their own while you watch, instead of relaying every message by hand.

*A small file-based comms protocol that lets two AI agents in separate terminals talk to each other in real time.*

**Letterbox** lets two terminal coding agents — Claude Code, Gemini CLI, or Antigravity — hold a real-time conversation by passing message files through a shared directory. When one agent speaks, a `📬` notification is injected into the other's terminal and wakes it to read and reply. No network, no server, no shared memory: just JSON files in a folder and the OS's atomic-rename. It's the messaging layer that was built for an internal planning loop, extracted and frozen as a standalone artifact in 2026. If you've ever wanted two CLI agents to collaborate on a task without you copy-pasting between windows, this is for you. If you're looking for a maintained, evolving project — this is a frozen reference release, not a community project.

The bridge is genuinely cross-harness: **Claude on one side, Gemini on the other**, talking through the same channel, has been verified live. The one wrinkle is setup — Claude wires itself automatically, while Gemini and Antigravity load letterbox from their own settings. The [Setup](#setup-per-harness) section walks through both.

## Why it exists

I work with two AI collaborators every day — Claude and Gemini — each living in whatever terminal harness it runs in (Claude Code, Gemini CLI, and now Antigravity CLI). Letterbox is how I get them talking to *each other* instead of through me.

That happens in two modes. Sometimes it's **manual**: we're brainstorming and I want to loop the other model into the conversation. Sometimes it's **automated** — in the planning loop, Claude drafts a plan and each plan is routed to Gemini for review as a built-in stage. Letterbox carries both the same way.

It's harness-agnostic by design — **Claude Code ↔ Gemini CLI ↔ Antigravity CLI** in any combination — and same-model pairs work just as well: two Claude tabs, or two Gemini tabs, talking over one channel.

## What it is

Each `letterbox <harness>` launch runs **two coordinated processes** inside one terminal:

```
  letterbox claude --channel demo --as alice
        │
        ├─ PTY-Parent  (the foreground letterbox process)
        │    • spawns the harness CLI as a PTY child
        │    • watches the channel directory for peer writes
        │    • injects 📬 notifications into the PTY on arrival
        │
        └─ the harness spawns:
             └─ letterbox mcp  (stdio MCP server, agent-spawned)
                  • send_message / check_messages / acknowledge
                  • check_latest_message / channel_info / list_channels

  Both sides coordinate ONLY through the filesystem:

        ~/.letterbox/channels/demo/
          msg-*.json           ← one file per message
          .read/alice.json     ← per-agent read markers
          .read/bob.json
```

There is no daemon, no IPC, no background service. The filesystem *is* the coordination medium — the PTY-Parent's watcher sees a new `msg-*.json` appear and renders a notification; the channel directory is durable, inspectable, and `cat`-able. Crash recovery is trivial because nothing valuable lives in memory.

**How the agent gets the letterbox tools differs per harness**, and it's the one thing you configure once:

- **Claude Code** takes a launch flag, so letterbox wires it *automatically* — it generates a temporary MCP config and passes `--mcp-config` to `claude`. Nothing for you to set up.
- **Gemini CLI and Antigravity** don't take that flag; they load MCP servers from their own settings file. You add a one-line, channel-agnostic `letterbox` entry there once, and the launcher hands each session its channel and identity through environment variables at launch — so you never edit settings per channel.

## Who it's for

- People running terminal coding agents who want **autonomous AI↔AI dialogue** on one machine, without babysitting copy-paste between windows.
- People who value **files as the source of truth** — auditable, greppable, no opaque protocol, no magic.

## Who it's NOT for

- Anyone wanting a **hosted or networked** chat service — letterbox is filesystem-local and never touches the network.
- Anyone wanting a **multi-user platform** — it's a point-to-point bridge between agents on one machine, not a many-user hub (see [Built for two](#built-for-two)).
- **Windows-native** users — v1 is POSIX-only (see [What we don't support](#what-we-dont-support)).
- Anyone wanting a **maintained community project** taking feature requests — this is a frozen reference artifact, not an evolving product.

## Built for two

Letterbox is a **two-way bridge at heart** — one peer talking to one peer is what it's designed and tuned for. Three or more agents *can* share a channel: directed addressing (`send_message(to="<label>")`) and the `participants` list make it workable, and same-channel broadcasts reach everyone. But a shared channel is a **broadcast bus** — every message wakes every participant. Without orchestration (turn-taking, a designated coordinator, or rules about who speaks when), an N-way room becomes a notification storm that can chew through a model's message/usage limits surprisingly fast. If you want three or more, bring your own conductor. The substrate is honest about who's in the room; the etiquette is on you.

## Install

Letterbox is installed from source (a wheel is buildable; it is not currently published to PyPI). From the repo root:

```bash
pip install -e .          # or: pip install -e ".[dev]" for the test extras
```

This puts the `letterbox` command on your `PATH`. The command must resolve by name — each agent spawns `letterbox mcp` itself — so this is load-bearing. Confirm it:

```bash
which letterbox           # note this absolute path; Gemini/Antigravity setup needs it
```

You also need the harness you're launching (`claude`, `gemini`, or `antigravity`) installed, on your `PATH`, and logged in. Letterbox launches it for you.

## Setup per harness

You only do this once per harness. Skip the harnesses you won't use.

### Claude Code — nothing to do

Letterbox wires Claude automatically: at launch it writes a temporary MCP config (mode `0600`) and passes `--mcp-config <path>` to `claude`. The letterbox tools appear in that session and nowhere else. There is no settings file to edit.

### Gemini CLI — two one-time steps

**1. Register the MCP server** in `~/.gemini/settings.json` (create the file if it doesn't exist). Use the **absolute path** to your installed `letterbox` (from `which letterbox` above), and pass only `["mcp"]` — no channel, no identity:

```json
{
  "mcpServers": {
    "letterbox": {
      "command": "/absolute/path/to/letterbox",
      "args": ["mcp"]
    }
  }
}
```

This entry is **channel-agnostic on purpose.** The launcher exports `LETTERBOX_CHANNEL`, `LETTERBOX_SENDER`, and `LETTERBOX_INSTANCE_ID` into Gemini's environment at launch, and the MCP server reads them — so the same single entry serves every channel and you never edit it again. (This mirrors how the Forge orchestrators pass a channel via an env var.)

**2. Trust the folder you launch from.** Gemini refuses to run in an untrusted directory without an interactive *"do you trust this folder?"* prompt — and a blocking TUI prompt would stall the automation. Pre-trust the launch directory (or a parent) in `~/.gemini/trustedFolders.json`:

```json
{
  "/home/you/projects": "TRUST_PARENT"
}
```

`TRUST_FOLDER` trusts exactly that directory; `TRUST_PARENT` trusts it and everything beneath, so one entry covers all your project folders. (Tip: don't reach for Gemini's `--skip-trust` flag to dodge this — it forces a workspace-system-prompt lookup that crashes even in already-trusted directories. Trust the folder instead.)

### Antigravity (`agy`)

Launch it as **`letterbox agy …`** (the long form `letterbox antigravity …` also works — `agy` is just an alias matching the binary name). Antigravity receives its per-launch channel and identity through the same environment variables as Gemini; what differs is how you register the MCP server. `agy` loads MCP servers from **plugins**, so you install letterbox as a tiny local plugin (a directory with two JSON files):

```bash
# 1. Build the plugin (one directory, two files). Use the absolute
#    `letterbox` path from `which letterbox`.
mkdir -p ~/.letterbox/agy-plugin/letterbox
cat > ~/.letterbox/agy-plugin/letterbox/plugin.json <<'JSON'
{ "name": "letterbox", "version": "1.0.0",
  "description": "Letterbox file-based AI-to-AI comms bridge." }
JSON
cat > ~/.letterbox/agy-plugin/letterbox/mcp_config.json <<'JSON'
{ "mcpServers": { "letterbox": {
    "command": "/absolute/path/to/letterbox", "args": ["mcp"] } } }
JSON

# 2. Install it (and confirm).
agy plugin install ~/.letterbox/agy-plugin/letterbox
agy plugin list
```

The `mcp_config.json` is channel-agnostic for the same reason Gemini's settings entry is — the launcher passes the channel and identity by environment at launch. Like Gemini, `agy` also gates on folder trust: it honours a `trustedWorkspaces` list in `~/.gemini/antigravity-cli/settings.json`, so add the directory you launch from there if it isn't already.

> **Status:** the PTY layer (notifications + message delivery, both directions) is **verified live**, and the plugin install above wires the tools cleanly. The full tools-in-`agy` round trip is freshly working and lightly exercised — treat Antigravity as **the newest of the three** and report anything odd.

## Quickstart

Open **two terminals** and point each at the same channel with a distinct identity. With no config file, letterbox's built-in defaults supply the shared global state directory (`~/.letterbox`).

A genuine cross-harness bridge — Claude talking to Gemini (complete the [Gemini setup](#gemini-cli--two-one-time-steps) first):

```bash
# Terminal 1
letterbox claude --channel demo --as claude

# Terminal 2
letterbox gemini --channel demo --as gemini
```

Or two of the same harness, if you'd rather keep it simple:

```bash
# Terminal 1
letterbox claude --channel demo --as alice

# Terminal 2
letterbox claude --channel demo --as bob
```

Both sessions start and sit quietly. Now nudge the agent in Terminal 1 — for example, *"Send a message to your peer."* From there, each `📬` notification wakes the other agent to read and reply: that handoff is the whole point. The `--as <label>` names make the transcript readable; underneath, message filtering uses a per-launch instance id, not the label.

A couple of honest notes:

- **You never run `letterbox mcp` yourself.** That subcommand is the stdio MCP server, spawned by the harness — it's for the agent, not for you. Run by hand in a terminal, it tells you so and exits.
- **Launch args are autonomous by design.** The Claude adapter launches with `--dangerously-skip-permissions` and the Gemini adapter with `--yolo`, because injected messages can't wake an agent that's blocked on a per-action approval prompt. If that's not a tradeoff you want, letterbox isn't the right fit — override the args in `letterbox.toml` or step away.

For a full, narrated walkthrough (two Claudes debating whether a hot dog is a sandwich), see the sample project under [`examples/two-claudes-debating/`](examples/two-claudes-debating/).

## Knowing the bridge state

Because a settings-wired harness loads letterbox on *every* session, an agent may have the letterbox tools available without an active bridge — for instance, a plain Gemini session you never launched through letterbox. Letterbox handles this calmly and gives the agent a way to check:

- **A plain session is dormant, not broken.** With no channel, the MCP server still connects (the harness shows a calm "connected"), but the messaging tools stay quiet — they fail with a clear, actionable message *only if actually called*, and never on their own. A deliberate plain session is never spammed; a genuinely misconfigured bridge surfaces the moment the agent tries to talk.
- **`channel_info` is the agent's bridge oracle.** Calling it answers, server-side: is a bridge active at all? On what channel, as whom? Who is the peer (observed from its most recent message), how many unread, and when did it last speak? An agent unsure of its situation can ask before sending — *"peer last spoke 90 s ago"* reads very differently from *"never."*

## Watching and listing channels

From any terminal, watch the raw conversation or see what channels exist:

```bash
letterbox tail --channel demo --follow   # stream messages as JSON, one per line
letterbox list-channels                  # list channels with last-activity
```

To scaffold a starting `letterbox.toml` instead of relying on defaults:

```bash
letterbox init --channel demo            # writes ./letterbox.toml (project-local)
letterbox init --global                  # writes ~/.letterbox/config.toml instead
```

## Operations

- **Reading catches you up; the inbox drains itself.** `check_messages` returns unread peer messages and advances that agent's read marker as it goes — so successive calls page through the backlog and a drained inbox stays drained, no manual bookkeeping. `check_latest_message` is a non-advancing peek for the common *"what did they just say?"*, and `acknowledge` is there for explicit, single-message control.
- **A restart is a fresh start, not a replay.** On launch, an agent's read marker is aligned to the newest message already on disk, so it sees only what arrives *after* it joined — it won't be flooded with a whole channel's history from a previous session. The history is still there and reachable on demand (`check_messages` with a `since_id` cursor); it just isn't forced on you.
- **Retention is manual.** Messages live in the channel directory until you prune them; there is no automatic deletion (surprise deletion is unacceptable in comms infrastructure). Per-agent `.read/` markers track read state — they advance markers, never touch the files, and never affect the peer's view.
- **Practical ceiling: ~10,000 unpruned messages per channel.** Beyond that, `check_messages` and `list-channels` may show noticeable latency. Prune above that point.
- **`letterbox prune` is the safe way to reclaim space.** It is **dry-run by default** — it prints what *would* happen and touches nothing. `--yes-i-am-sure` moves matched files to a reversible `cold/` subdirectory; `--delete --yes-i-am-sure` (double-gated) deletes for good. This is the only destructive command in letterbox.

```bash
letterbox prune --channel demo --keep-last 100                   # preview (dry run)
letterbox prune --channel demo --keep-last 100 --yes-i-am-sure   # move to cold/
letterbox prune --help                                           # all selection rules
```

A channel is just a folder, so `rm -rf ~/.letterbox/channels/demo` works too — letterbox locks nothing.

## Security model

The full threat model lives in [`docs/PROTOCOL.md`](docs/PROTOCOL.md). In brief:

- **The peer agent on a channel is untrusted.** Its message bodies may carry prompt-injection payloads, ANSI escapes, or shell metacharacters. Letterbox treats both sides as untrusted.
- **Notifications render only from trusted context.** The `📬` notification template substitutes variables drawn from the watcher's *own* configuration and observations (`{channel}`, `{sender}`, `{message_id}`, `{timestamp}`) — **never** from the peer's message payload. A malicious peer can write anything into its file; none of it reaches the injected notification. Message bodies are surfaced only when the agent explicitly calls `check_messages`. The same holds for `channel_info`'s peer fields: they're observed from traffic and informational, never fed into a notification.
- **No execution path.** Letterbox never `exec`s, `eval`s, or shells a message body or metadata field. Subprocesses are spawned with argv lists (never `shell=True`), and only to launch the harness configured in `letterbox.toml`.
- **Path safety.** Channel names and message ids are validated against a strict pattern before any filesystem operation — `../etc` or anything with a slash is refused.
- **Filesystem permissions.** `~/.letterbox/` and channel directories are created `0700` (user-only); the generated MCP config is `0600`.

**What letterbox does NOT defend against:** a compromised local user account (filesystem permissions are the only barrier), the consuming harness's own prompt-injection vulnerabilities, or trust boundaries introduced by cross-machine sync (NFS, syncthing). It is not an encryption-at-rest or network-trust layer — those are out of scope by design.

## Scope and anti-scope

What letterbox deliberately does *not* do is the point, not a gap:

- **No LLM calls.** Letterbox never invokes a language model, spends a token, or holds an API key. The notification template is rendered text, not a prompt.
- **No telemetry, no metrics, no analytics.** Nothing is collected, no dashboards, no usage tracking.
- **No phone-home, no auto-update, no version check.** Letterbox never contacts any server. First run is silent.
- **No network.** It is filesystem-local. Cross-machine use is your filesystem-sync's business, not letterbox's.

This anti-scope is what lets letterbox be small, inert, auditable, and durable.

## Accessibility

- **Plain text by default** (`--format=plain`) — pipe- and screen-reader-friendly; `tail` emits message JSON on stdout for `jq`. Structured/colored output is opt-in (`--format=rich`).
- **No color-only signaling.** `--color=auto|always|never` controls color independently; color is never the only way a state is conveyed.
- **stdout is data, stderr is logs** — commands pipe cleanly.
- **UTF-8 throughout.** The tool's own strings are English; message bodies are whatever language you write.
- **Calm surface.** No spinners, no telemetry banners, no upgrade nags. Quiet success, clear errors — errors cite the path, the line, or the valid options.

## What we don't support

**Letterbox v1 is POSIX-only (Linux and macOS).** The PTY spawn-and-inject layer is built on POSIX primitives; Windows support via the stdlib `pty` module is incomplete and not shipped. If you're on Windows, letterbox won't run for you in v1 — better to know now than to hit a crash.

## See also

- [`examples/two-claudes-debating/`](examples/two-claudes-debating/) — the hands-on walkthrough: two Claude Code sessions debating in real time.
- [`skills/letterbox/SKILL.md`](skills/letterbox/SKILL.md) — the agent-facing **usage** guide: how an LLM uses a live bridge (broadcast, directed messages, participants).
- [`skills/letterbox-setup/SKILL.md`](skills/letterbox-setup/SKILL.md) — the agent-facing **setup** guide: the one-time per-harness MCP wiring, the one-label-per-channel rule, and the post-upgrade relaunch procedure.
- [`docs/AGENT_POINTER.md`](docs/AGENT_POINTER.md) — a short drop-in block to paste into a project's `CLAUDE.md` / `GEMINI.md` / `AGENTS.md` so an agent knows it's on a bridge.
- The full file-format and protocol reference lives in [`docs/PROTOCOL.md`](docs/PROTOCOL.md).
- [`DECISIONS.md`](DECISIONS.md) — the architecture decision records (ADRs) behind every load-bearing choice, including the per-harness MCP wiring (ADR-054/055), dormant mode and the `channel_info` oracle (ADR-056), the submit-timing fix (ADR-057), the self-maintaining read marker (ADR-058), the per-channel duplicate-instance guard (ADR-061), and N-party directed addressing + participants (ADR-062).
- [`LICENSE`](LICENSE) — MIT.

## Status

Letterbox is a frozen v1 artifact — MIT-licensed, at [`github.com/dovahkiin-v/letterbox`](https://github.com/dovahkiin-v/letterbox). It ships complete and stands as documented; it is a personal artifact, not a product, and is not soliciting contributions. *Frozen* here means **unsupported and complete as of this version — not immutable**: the author may cut a later version at their own whim, with no promise or schedule. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for what frozen-artifact status means in practice.
