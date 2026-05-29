# Two Claudes Debating

A worked example: two Claude Code sessions, in two terminals, debating **"is a hot dog a sandwich?"** — by passing messages to each other through letterbox.

No network, no server, no shared memory. Just message files in a folder, and a 📬 notification that wakes the other agent when one of them speaks. Open two terminals, nudge the first Claude, and watch the argument take on a life of its own.

You should be running by the end of this page in well under five minutes.

## 1. What this is

Claude A argues a hot dog **is** a sandwich. Claude B argues it **isn't**. Each runs as a normal Claude Code session in its own terminal. When one sends a message, letterbox writes it as a file to the shared channel directory and injects a `📬` notification into the other terminal — which wakes that Claude mid-wait to read and reply. The whole conversation is just JSON files under `~/.letterbox/channels/debate-01/`; you can `cat` any of them.

## 2. Prerequisites

- **letterbox installed and on your `PATH`.** From the repo root: `pip install -e .` (or `pip install letterbox`). The `letterbox` command must resolve on `PATH` — each agent spawns `letterbox mcp` by name, so this is load-bearing, not optional.
- **Claude Code installed and on your `PATH`**, and you must be logged in (`claude` must run). Letterbox launches it for you.
- Note: the demo launches Claude with `--dangerously-skip-permissions`. This is a sandboxed demo conversation about hot dogs — it skips the per-action approval prompts so the two agents can talk freely without you babysitting each tool call. Nothing here touches anything sensitive, but it's your call.

## 3. How to run it

Open **two** terminals.

```bash
# Terminal 1 — Claude A (argues "yes, it's a sandwich")
cd examples/two-claudes-debating/claude-a
letterbox claude --channel debate-01 --as claude-a
```

```bash
# Terminal 2 — Claude B (argues "no, it isn't")
cd examples/two-claudes-debating/claude-b
letterbox claude --channel debate-01 --as claude-b
```

Both sessions will start and sit quietly. Now, in **Terminal 1**, tell Claude A to begin — for example, type:

> Start the debate — send your opening argument.

That nudge is all it takes. From there the agents drive themselves.

<details>
<summary>Alternative: run from the example root (loads this folder's <code>letterbox.toml</code>)</summary>

The commands above work with no config file at all — letterbox's built-in defaults supply the `claude` harness and the global `~/.letterbox` state directory. If you'd rather see the shipped `letterbox.toml` actually loaded, run both terminals from this example's root and point each Claude at its persona folder with `--cwd`:

```bash
# from examples/two-claudes-debating/
letterbox claude --channel debate-01 --as claude-a --cwd claude-a   # Terminal 1
letterbox claude --channel debate-01 --as claude-b --cwd claude-b   # Terminal 2
```

Run from the root, letterbox finds the project-local `./letterbox.toml`; `--cwd` puts each spawned Claude in the right persona directory.

</details>

## 4. What to expect

Claude A sends its opening argument. A moment later, **Terminal 2 shows a `📬` notification** — Claude B wakes, reads the message, acknowledges it, and fires back. Then **Terminal 1** gets its own `📬`, and so on. The two go back and forth on their own for a few rounds — hot dog as taco, the structural integrity of the bun, the tyranny of single-hinge bread — and then wind down with closing lines after four or five exchanges.

You don't drive the conversation after the first nudge. The `📬` notifications do — that's the whole point.

A note on names: the `--as claude-a` / `--as claude-b` labels are what make the transcript readable, so you can see who said what. Letterbox's message filtering underneath uses a per-launch `instance_id`, not the label — the label is for humans.

## 5. Follow along

Want to watch the raw conversation as it happens? In a **third** terminal:

```bash
letterbox tail --channel debate-01 --follow
```

That streams each message as JSON (one object per line) the moment it's written — the actual files the agents are exchanging. To see what channels exist:

```bash
letterbox list-channels
```

## 6. Clean up

The debate left a pile of message files in `~/.letterbox/channels/debate-01/`. The `letterbox prune` command is the safe, built-in way to clear them — it's a dry run by default and never deletes without an explicit confirmation.

```bash
# Preview what would be removed (dry run — nothing is touched).
# --keep-last 0 selects every message.
letterbox prune --channel debate-01 --keep-last 0

# Move the whole debate to cold storage (reversible — files go to debate-01/cold/):
letterbox prune --channel debate-01 --keep-last 0 --yes-i-am-sure

# Or delete it for good (double-gated — needs both flags):
letterbox prune --channel debate-01 --keep-last 0 --delete --yes-i-am-sure
```

Prefer the blunt instrument? The channel is just a folder:

```bash
rm -rf ~/.letterbox/channels/debate-01
```

For the full set of selection rules (`--older-than`, `--acknowledged-by-all`, …) and flags:

```bash
letterbox prune --help
```
