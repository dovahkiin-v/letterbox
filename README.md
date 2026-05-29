# Letterbox

*A small file-based comms protocol that lets two AI agents in separate terminals talk to each other in real time.*

**Letterbox** lets two terminal coding agents — Claude Code, Gemini CLI, or Antigravity — hold a real-time conversation by passing message files through a shared directory. When one agent speaks, a `📬` notification is injected into the other's terminal and wakes it to read and reply. No network, no server, no shared memory: just JSON files in a folder and the OS's atomic-rename. It's the messaging layer that was built for the Workshop planning loop, extracted and frozen as a standalone artifact in May 2026. If you've ever wanted two CLI agents to collaborate on a task without you copy-pasting between windows, this is for you. If you're looking for a maintained, evolving project — this is a frozen reference release, not a community project.

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
                  • send_message / check_messages / acknowledge / …

  Both sides coordinate ONLY through the filesystem:

        ~/.letterbox/channels/demo/
          msg-*.json          ← one file per message
          .read/alice.json     ← per-agent read markers
          .read/bob.json
```

There is no daemon, no IPC, no background service. The filesystem *is* the coordination medium — the PTY-Parent's watcher sees a new `msg-*.json` appear and renders a notification; the channel directory is durable, inspectable, and `cat`-able. Crash recovery is trivial because nothing valuable lives in memory.

## Who it's for

- People running terminal coding agents who want **autonomous AI↔AI dialogue** on one machine, without babysitting copy-paste between windows.
- People who value **files as the source of truth** — auditable, greppable, no opaque protocol, no magic.

## Who it's NOT for

- Anyone wanting a **hosted or networked** chat service — letterbox is filesystem-local and never touches the network.
- Anyone wanting a **multi-user platform** — it connects two terminal agents on one machine.
- **Windows-native** users — v1 is POSIX-only (see [What we don't support](#what-we-dont-support)).
- Anyone wanting a **maintained community project** taking feature requests — this is a frozen reference artifact, not an evolving product.

## Quickstart

Letterbox is installed from source (a wheel is buildable; it is not currently published to PyPI). From the repo root:

```bash
pip install -e .          # or: pip install -e ".[dev]" for the test extras
```

This puts the `letterbox` command on your `PATH`. The command must resolve by name — each agent spawns `letterbox mcp` itself, so this is load-bearing.

You also need the harness you're launching (`claude`, `gemini`, or `antigravity`) installed, on your `PATH`, and logged in. Letterbox launches it for you.

Open **two terminals** and point each at the same channel with a distinct identity. With no config file, letterbox's built-in defaults supply the harness and the shared global state directory (`~/.letterbox`):

```bash
# Terminal 1
letterbox claude --channel demo --as alice

# Terminal 2
letterbox claude --channel demo --as bob
```

Both sessions start and sit quietly. Now nudge the agent in Terminal 1 — for example, *"Send a message to your peer."* From there, each `📬` notification wakes the other agent to read and reply: that handoff is the whole point. The `--as <label>` names make the transcript readable; underneath, message filtering uses a per-launch instance id, not the label.

Watch the raw conversation from a third terminal, or see what channels exist:

```bash
letterbox tail --channel demo --follow   # stream messages as JSON, one per line
letterbox list-channels                  # list channels with last-activity
```

To scaffold a starting `letterbox.toml` instead of relying on defaults:

```bash
letterbox init --channel demo            # writes ./letterbox.toml (project-local)
letterbox init --global                  # writes ~/.letterbox/config.toml instead
```

A couple of honest notes:

- **You never run `letterbox mcp` yourself.** That subcommand is the stdio MCP server, spawned by the harness via a generated MCP config — it's for the agent, not for you.
- **Launch args are autonomous by design.** The Claude adapter launches with `--dangerously-skip-permissions` and the Gemini adapter with `--yolo`, because injected messages can't wake an agent that's blocked on a per-action approval prompt. If that's not a tradeoff you want, letterbox isn't the right fit — override the args in `letterbox.toml` or step away.

For a full, narrated walkthrough (two Claudes debating whether a hot dog is a sandwich), see the sample project under [`examples/two-claudes-debating/`](examples/two-claudes-debating/).

## Operations

- **Retention is manual.** Messages live in the channel directory until you prune them; there is no automatic deletion (surprise deletion is unacceptable in comms infrastructure). Per-agent `.read/` markers track read state — `acknowledge` clears a message from one agent's inbox without touching the file or affecting the peer.
- **Practical ceiling: ~10,000 unpruned messages per channel.** Beyond that, `check_messages` and `list-channels` may show noticeable latency. Prune above that point.
- **`letterbox prune` is the safe way to reclaim space.** It is **dry-run by default** — it prints what *would* happen and touches nothing. `--yes-i-am-sure` moves matched files to a reversible `cold/` subdirectory; `--delete --yes-i-am-sure` (double-gated) deletes for good. This is the only destructive command in letterbox.

```bash
letterbox prune --channel demo --keep-last 100             # preview (dry run)
letterbox prune --channel demo --keep-last 100 --yes-i-am-sure   # move to cold/
letterbox prune --help                                     # all selection rules
```

A channel is just a folder, so `rm -rf ~/.letterbox/channels/demo` works too — letterbox locks nothing.

## Security model

The full threat model lives in [`docs/PROTOCOL.md`](docs/PROTOCOL.md). In brief:

- **The peer agent on a channel is untrusted.** Its message bodies may carry prompt-injection payloads, ANSI escapes, or shell metacharacters. Letterbox treats both sides as untrusted.
- **Notifications render only from trusted context.** The `📬` notification template substitutes variables drawn from the watcher's *own* configuration and observations (`{channel}`, `{sender}`, `{message_id}`, `{timestamp}`) — **never** from the peer's message payload. A malicious peer can write anything into its file; none of it reaches the injected notification. Message bodies are surfaced only when the agent explicitly calls `check_messages`.
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
- The full file-format and protocol reference lives in [`docs/PROTOCOL.md`](docs/PROTOCOL.md).
- [`DECISIONS.md`](DECISIONS.md) — the architecture decision records (ADRs) behind every load-bearing choice.
- [`LICENSE`](LICENSE) — MIT.

## Status

Letterbox is a frozen v1 artifact — MIT-licensed, at [`github.com/dovahkiin-v/letterbox`](https://github.com/dovahkiin-v/letterbox), and a companion to the "Lessons from the Forge" essay. It ships complete and stands as documented; it is a personal artifact, not a product, and is not soliciting contributions. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for what frozen-artifact status means in practice.
