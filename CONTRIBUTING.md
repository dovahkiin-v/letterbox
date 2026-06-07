# Contributing to Letterbox

Letterbox is a **frozen reference artifact**, not an actively maintained
community project. This note explains what that means in practice — what's
welcome, what isn't, and why.

## What "frozen artifact" means

Letterbox v1 ships **complete, documented, and MIT-licensed**. It is "the
system as of June 2026" — a personal artifact rather than a product. There is **no commitment to
issues, pull requests, or releases beyond v1.**

**Frozen here means *unsupported and complete as of this version* — not
*immutable*.** What is frozen is the obligation *to others*, never the
author's freedom to revise their own work. The author may cut a later version
if and when they want to; there's simply no promise, schedule, or obligation
to. Think frozen like a lake, not a fossil: still and solid now, the same
water, free to thaw when the author decides the season has changed.

This isn't a brush-off. The author doesn't run a community-support cadence,
and honest framing beats implied promises of maintenance. The code stands as
documented; the [`DECISIONS.md`](DECISIONS.md) log records the *why* behind
every load-bearing choice. If you want an *evolving* letterbox, the supported
path is to fork it — that's encouraged, not begrudged (MIT makes it free and
clean). If a maintained community project is what you're after, letterbox
isn't the right fit, and that's fine.

## What's welcome

One door stays open: **keeping the frozen artifact accurate.**

- **Documentation-fix issues and PRs** — typos, broken links, and factual
  corrections to the [`README.md`](README.md),
  [`docs/PROTOCOL.md`](docs/PROTOCOL.md), or [`DECISIONS.md`](DECISIONS.md).
- Clarity fixes that make the existing v1 easier to understand.

The bar is simple: a change that makes v1 **more correct or clearer** is in
scope; a change that **adds behavior** is not (see below). Correctness keeps
the artifact trustworthy without turning it into a moving target.

## Feature requests

Feature ideas from others are **recorded, not actioned.** New directions may
be noted on the "considered, deferred" list below, but none are promised —
there's no roadmap and no scheduled release. The author may fold an
improvement into a future version on their own initiative, but that's a
choice, never a commitment, and never something an external request obligates.
If you need a feature now, fork and build it — the architecture is
designed to make that straightforward (the adapter base class and TOML
registry, for instance, accommodate a new harness by config alone).

## Considered, deferred

These directions were deliberately deferred. Some keep a forward hook in v1 so
a future need won't force a destructive migration:

- **Codex CLI adapter** — deferred until the author can test it; the adapter
  base class + TOML registry already accommodate a new harness by config.
- **IDE integrations** (VS Code, Cursor, Antigravity IDE) — each layers on top
  of letterbox unchanged; each is its own project.
- **Peer-review / planning-style state machine** — lives in the downstream
  planning loop, not in letterbox.
- **Network-mode comms** (HTTP / gRPC / message queue) — deferred *with a
  forward hook*: the reserved `address` field exists so a future scheme
  doesn't force a message-format migration.
- **Encryption at rest / signing** — deferred *with a forward hook*: the
  reserved `metadata.encryption` slot in the message JSON.
- **Web UI / GUI** — `tail` and `cat` cover inspection; a UI layers on top.
- **Cross-machine comms** — your filesystem-sync's job (NFS, syncthing), not
  letterbox's.

## Will not happen (anti-scope)

These are a different category — not "deferred," but **never, by design.** A
feature request for any of them gets a clear "no":

- **No LLM calls.** Letterbox never invokes a model, spends a token, or holds
  an API key. The notification template is rendered text, not a prompt.
- **No telemetry, metrics, or analytics.** Nothing is collected.
- **No phone-home, no auto-update, no version check.** Letterbox never
  contacts any server. First run is silent.

This anti-scope is what lets letterbox stay small, inert, auditable, and
durable. Adding any of it would change what letterbox *is*.

## How to file

Open an issue or doc-fix PR at
[`github.com/dovahkiin-v/letterbox`](https://github.com/dovahkiin-v/letterbox).
Understand that a response may be slow or absent — that's the frozen-artifact
contract, not neglect. Letterbox is MIT-licensed
([`LICENSE`](LICENSE)), so anyone is free to fork and evolve it independently.

## See also

- [`README.md`](README.md) — what letterbox is, who it's for, and quickstart.
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) — the file-format and protocol reference.
- [`DECISIONS.md`](DECISIONS.md) — the architecture decision records.
