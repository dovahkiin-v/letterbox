# Letterbox Protocol & File Format (v1, `schema_version=1`)

This is the authoritative on-disk format and protocol reference for Letterbox
`schema_version=1`. It describes the **frozen v1** format: exactly what bytes
live on disk, what every field means, and what Letterbox guarantees about
reading, writing, and recovering them. It is a **reference**, not a tutorial —
for a narrated walkthrough see the [README quickstart](../README.md#quickstart)
and the [`examples/two-claudes-debating/`](../examples/two-claudes-debating/)
sample. Every design decision below cross-links to its governing record in
[`DECISIONS.md`](../DECISIONS.md).

Letterbox's core promise is **the directory *is* the conversation**: a channel
is a plain directory of plain JSON files that a person can `cp -r`, open in any
editor, `grep`, and fully understand without the tool. This document is the
contract that keeps that promise — the one place that says, precisely, what the
bytes mean.

---

## 1. Channel directory layout

All state lives under a single **state root**, `~/.letterbox/` by default and
configurable via the `LETTERBOX_HOME` environment variable. The layout is one
level of channels, each a directory whose name *is* the channel name:

```
~/.letterbox/                          # state root (configurable via LETTERBOX_HOME)
├── config.toml                        # user-global config (a project-local letterbox.toml can override)
└── channels/
    ├── demo/                          # one channel = one directory; dir name = channel name
    │   ├── msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json
    │   ├── msg-20260527T143042892341-3cc1a2b3c4d5e6f78091021324354657.json
    │   ├── .read/                      # per-agent read state — NOT a shared archive
    │   │   ├── alice.json             # this endpoint's high-water mark
    │   │   └── bob.json               # peer's high-water mark
    │   └── cold/                       # OPTIONAL — created only by `letterbox prune` (move destination)
    ├── cartolina-review/
    └── debate-01/
```

Everything Letterbox writes is here; there is no daemon state, no database, no
network. Crash recovery is trivial because nothing valuable lives in volatile
memory (Kernel L1; [ADR-002](../DECISIONS.md)).

**There is no shared archive directory.** Earlier drafts had a
`channel/archive/` directory as the shared physical destination for an
`archive(message_id)` call — but that was a *shared mutation*: when one peer
archived a message, the file left the live directory for *both* peers. Read
state is fundamentally **per-agent** (consuming a message is a local decision,
not a global one), so v1 replaced the shared archive with per-agent
`.read/{sender_label}.json` markers. The message file stays live in the channel
directory for both peers; only each endpoint's own marker advances. Physical
removal happens *only* via `letterbox prune` (§9) or a human's `rm`
([ADR-021](../DECISIONS.md)).

**Portability (Framework P2).** A channel is plain JSON files plus plain read
markers. `cp -r ~/.letterbox/channels/<name>` backs up, shares, or migrates a
whole conversation. No lock-in, no proprietary format. The `.read/` markers and
any `cold/` subdirectory travel with it.

### 1.1 Channel naming

A channel name — and, since it becomes a `.read/{sender_label}.json` filename
component, a `sender_label` too — is validated against this regex before any
filesystem operation:

```
^[a-z0-9][a-z0-9_-]*$
```

Lowercase ASCII alphanumerics, hyphen, and underscore, with the **first
character required to be alphanumeric**. This is the shipped `_CHANNEL_NAME_RE`,
and it is intentionally **stricter** than the prose form `[a-z0-9_-]+` you might
expect: requiring an alphanumeric first character blocks a leading `-` (which a
downstream argument parser could mis-read as a flag like `-rf`) and a leading
`_` (conventionally private). The same regex guards both channel directory
names and read-state filenames — one path-safety boundary, one predicate. It is
what makes a channel name safe to interpolate into a filesystem path: no `..`,
no `/`, no shell metacharacters can pass ([ADR-028](../DECISIONS.md)).

---

## 2. Message file format

This is the core of the format. Each message is one JSON file.

### 2.1 Filename

```
msg-YYYYMMDDTHHMMSSffffff-{uuid4hex}.json
```

- `YYYYMMDDTHHMMSSffffff` — a UTC timestamp to **microsecond** precision
  (`ffffff` is the 6-digit microsecond field), with no `:`/`+`/timezone suffix.
- `{uuid4hex}` — a **full 32-character UUID4 hex** string, no dashes.

Example:

```
msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6.json
```

Every reader validates each channel-directory filename against this exact
regex before processing it:

```
^msg-[0-9]{8}T[0-9]{6}[0-9]{6}-[0-9a-f]{32}\.json$
```

This is the shipped `_MESSAGE_FILENAME_RE` ([ADR-028](../DECISIONS.md)). Note
it uses the explicit character class `[0-9]`, **not** `\d` — `\d` would match
Unicode decimal digits (Gujarati ૪, Arabic-Indic, …); `[0-9]` rejects them so
no exotic-digit filename can reach the notification renderer. A file failing
the regex is **ignored entirely**: never parsed, never substituted into a
notification, never returned by `check_messages`. A novel rejected name is
logged once per process at `WARN` and then left in place. This is the
**load-bearing defense against template-injection-via-filename**: even a file
literally named `msg-123-$(rm -rf /).json` is structurally rejected before any
processing.

**Rationale:**

- **Microsecond precision** so a filename's lexical sort equals chronological
  sort, independent of `mtime`. `mtime` is fragile — `cp -r` (without `-p`),
  syncthing, rsync, NFS clock skew, and `tar` extraction all reset or shuffle
  it. Sorting by filename is immune ([ADR-017](../DECISIONS.md)).
- **Full UUID4** (128 bits, not the earlier 4 hex chars) so collisions are
  effectively impossible (~50% chance only at ~2⁶⁴ messages). Because the
  atomic write uses `os.replace`, a filename collision would *silently
  overwrite* a message and lose it — UUID4 makes that impossible without
  relying on "writes are serialized" reasoning ([ADR-027](../DECISIONS.md)).
- **UTC-only, no timezone suffix** in the filename, for portability across
  filesystems that disallow `:` or `+` ([ADR-015](../DECISIONS.md)).

### 2.2 JSON body

```json
{
  "schema_version": 1,
  "id": "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6",
  "channel": "demo",
  "address": "file://local",
  "instance_id": "lb-20260527T143000Z-7f3a9c",
  "sender": "alice",
  "recipient": null,
  "timestamp": "2026-05-27T14:30:15.123456+00:00",
  "body": "The text of the message goes here. Supports markdown.",
  "in_reply_to": null,
  "metadata": {
    "encryption": null,
    "ext": {}
  }
}
```

The `schema_version=1` shape has **exactly 11 top-level keys**. Every one is
documented below with its type, v1 value/nullability, and meaning:

| Key | Type | v1 value / nullability | Meaning |
|---|---|---|---|
| `schema_version` | int | always `1` | Format version. Readers **check it and fail loudly** on any other value (§2.4). |
| `id` | str | the filename stem (no `.json`) | The message's identity; equals the filename minus `.json`. |
| `channel` | str | the channel name | Which channel this message belongs to. |
| `address` | str | always `"file://local"` | Transport scheme. **Reserved** — see §3. |
| `instance_id` | str | `lb-{ISO8601-no-punct}-{6-hex}` (e.g. `lb-20260527T143000Z-7f3a9c`) | Per-process mechanical identity; the watcher's own-write join key (§5). |
| `sender` | str | human-readable identity (non-empty) | Who sent it; server-resolved join key (§5). Shows in `letterbox tail`. |
| `recipient` | str \| null | usually `null` in v1 | The peer's label; informational only. |
| `timestamp` | str | ISO-8601 with explicit `+00:00` | Send time, always UTC ([ADR-015](../DECISIONS.md)). |
| `body` | str | UTF-8 text/markdown, ≤ 5 MB | The message content. |
| `in_reply_to` | str \| null | `null` for a thread head | Ancestor message id; **trusted blindly**, no integrity check ([ADR-020](../DECISIONS.md)). |
| `metadata` | object | `{"encryption": null, "ext": {}}` | Reserved/extension block — see §3. |

**Keys are always present, even when their value is `null`.** `recipient` and
`in_reply_to` are nullable *values*, but their *keys* are always emitted, and
the parser **rejects their absence**. They are not "optional keys that may be
missing" — the JSON object always carries all 11 keys. The parser also rejects
any *unknown* top-level key; the only open extension point in the whole format
is `metadata.ext` (§3). This strictness is a feature: malformed input fails
loudly with a vector error rather than silently degrading (Framework P3).

**`in_reply_to` is trusted blindly.** There is no referential-integrity check
that the target id exists — that would be expensive (a channel scan), racy
(the target may be pruning in flight), and broken cross-process. A bogus
`in_reply_to` is a downstream readability issue, not a protocol error. This is
a deliberate anti-feature ([ADR-020](../DECISIONS.md)).

### 2.3 On-disk encoding

The on-disk bytes are produced with:

- **`sort_keys=True`** — keys are alphabetically ordered at every level, so two
  equal messages serialize to byte-identical output and
  `to_json_bytes(from_json_bytes(x))` is the byte-level identity
  ([ADR-030](../DECISIONS.md)).
- **`ensure_ascii=False`** — Lithuanian, CJK, and emoji appear as themselves,
  not `\uXXXX` escapes (Vision §13.2). The on-disk file is UTF-8.
- No indentation, no trailing newline, no BOM.

### 2.4 Schema version is a stability contract

`schema_version` is `1` and stays `1` for the frozen v1 artifact. Readers parse
it and **reject any value other than `1`** with a vector error. This is the
stability contract: a future format change would bump the integer, and a
v1 reader confronted with a newer file fails loudly rather than
mis-interpreting it. The rule is shipped behavior, not a roadmap promise.

### 2.5 The 5 MB body ceiling

The maximum serialized message size is **5 MB** (`MAX_BODY_BYTES = 5 * 1024 *
1024`), [ADR-014](../DECISIONS.md). On the **write** side, an oversized payload
is rejected with a `MessageTooLarge` error *before any disk I/O* — a looping
agent that tries to write a gigabyte cannot OOM the process or leave a partial
`.tmp`. On the **read** side, a file larger than 5 MB (e.g. hand-edited)
surfaces as a `ParseError("oversized")` (§4) — the file is left in place, never
deleted.

---

## 3. Reserved / forward-compatibility fields

Three slots are reserved in v1. They have **no in-vision consumer** — that is
explicit and correct, not an omission. Each is wiring laid down now so a future
version need not rewrite an existing message corpus (the Retrofit Test, Vision
§1.2 / Framework P20):

| Field | v1 value | What a future version does with it |
|---|---|---|
| `address` | always `"file://local"` | Declares the transport scheme. A future network mode writes `"ssh://host/path"` or `"nats://channel/…"` without reshaping existing files. v1 readers accept any value but process only `file://local`. |
| `metadata.encryption` | always `null` | A future encryption layer populates cipher metadata (e.g. `{"scheme": "age", "key_id": "…"}`). v1 readers see `null` and skip. |
| `metadata.ext` | always `{}` (or caller-supplied dict) | Open extension point for consumers (e.g. a future planning layer adds `{"phase": "1a"}`). Letterbox **preserves but never interprets** it. |

Reserving these costs three JSON keys per message and zero implementation
effort, but spares any future user from rewriting their entire corpus to add a
cipher scheme or transport later. `metadata.ext` is the **only** field in the
format where unknown nested keys are permitted; every other unknown key (top
level or inside `metadata`) is rejected by the parser.

---

## 4. Write & read semantics; the `parse_error` catalogue

### 4.1 Write (atomic-rename)

Writes use **write-temp-then-rename** (Kernel L6; [ADR-014](../DECISIONS.md)
for the ceiling): the payload is written to a sibling `msg-*.json.tmp`, then
`os.replace`'d to the final `msg-*.json`. Readers therefore only ever see
complete files. The temp suffix is `.json.tmp` (note the order — **not**
`.tmp.json`), so the strict `msg-*.json` validator regex structurally excludes
in-flight files. Durability beyond visibility (an `fsync` of the data file and
then the parent directory *after* the rename) is supported at the
`write_message` primitive via an `fsync` parameter that defaults to off; v1's
agent send path uses that default, so message writes are atomic but not
fsync-durable against power loss (Vision §9.4 — the rename guarantees readers
never see a torn file; power-loss durability is deliberately not the default).
The writer is the agent's MCP `send_message`; the receiver is the peer's
watcher.

### 4.2 Read

A reader enumerates `msg-*.json` (which structurally excludes `.tmp`), sorts by
filename (= chronological order), and parses selectively. Reading one file
returns a `Message | ParseError` union — `read_message` never raises for a
payload problem; it returns a value (the one exception is `FileNotFoundError`
if the file was pruned between enumeration and read, which higher layers catch
and skip).

### 4.3 `ParseError` — the shipped shape

`ParseError` is a **return value, not an exception.** It is a frozen dataclass
with two fields — `path` and `reason` — and does **not** subclass `Exception`.
A payload error is something the agent *receives* as data, not something the
system raises and swallows ([ADR-020](../DECISIONS.md)).

The file is **never deleted** on a `ParseError` — corrupted content is
preserved in place for postmortem (Kernel L8, the Ironclad Invariant).

There are exactly two canonical `reason` strings:

| `reason` | When |
|---|---|
| `"oversized"` | The file is larger than 5 MB. Fixed limit, no detail appended. |
| `"malformed_json: <detail>"` | JSON parse failure **or** any schema-check failure. |

The `malformed_json:` prefix is a single funnel: a raw JSON syntax error, a
*missing* required key, an *unknown* top-level key, a *type* mismatch, and an
*unsupported* `schema_version` all raise `ValueError` inside the parser, which
the reader wraps under the one `malformed_json:` prefix (the `<detail>` is the
underlying vector message naming the specific field and rule).

### 4.4 The MCP-wire projection

`ParseError(path, reason)` is the **channel-layer** shape. When the
`check_messages` MCP tool encounters one, it projects it onto a different,
three-key wire envelope — **not** the full 11-key message shape:

```json
{ "id": "<message-id stem>", "parse_error": "<reason>", "body": null }
```

`id` is the message-id **stem** (the filename without `.json`), `parse_error`
carries the `reason` string verbatim, and `body` is `null`. These parse-error
envelopes are merged with the clean messages and re-sorted by `id`, so the
agent sees one correctly-ordered inbox across both shapes. The two
representations — `ParseError(path, reason)` and the `{id, parse_error, body}`
envelope — describe the same event at two layers.

---

## 5. Identity model & the combined own-write filter

### 5.1 Identity is per-launch and server-side

There are two identities per endpoint:

- **`sender`** — *human-readable* identity. Durable across restarts; shows in
  `letterbox tail` and message logs.
- **`instance_id`** — *mechanical* identity, format
  `lb-{ISO8601-no-punct}-{6-hex}` (e.g. `lb-20260527T143000Z-7f3a9c`),
  generated fresh by each `letterbox` process at startup.

The agent calls `send_message(body, in_reply_to=None)` and **does not supply
identity.** `sender` is resolved once at launch, in priority order:

1. the `--as <label>` CLI flag,
2. the `LETTERBOX_SENDER` environment variable,
3. the harness name (`claude`, `gemini`, `antigravity`) as the default.

The PTY-Parent resolves this once and passes it to the MCP-server child
(`letterbox mcp --as <label> …`) so both processes agree. `default_sender` in a
per-channel `letterbox.toml` is **deliberately not supported** — a shared
config file would otherwise collapse the identities of two endpoints in the
same project directory ([ADR-026](../DECISIONS.md)). Identity belongs to the
launch invocation, not the channel definition. This is the Join-Key Discipline
(Vision §13.3): identity is a server-side join key, never an agent-supplied API
parameter, so "a message from `alice` actually came from the alice-side
letterbox" is a property the system can trust.

### 5.2 The combined own-write filter

A message is classified as "own" (and skipped — no notification, excluded from
unread) when **either** half matches:

```
(msg.sender == self_sender)  OR  (msg.instance_id == self_instance_id)
```

This is the shipped `_is_own_write` ([ADR-022](../DECISIONS.md)). Both halves
are necessary:

- **The `sender` half** catches **cross-restart self-recognition.** A restarted
  process has a fresh `instance_id` but the same durable `sender` label, so it
  still recognizes its own historical writes — without this half, the watcher
  would flood the agent with notifications for messages it wrote before the
  restart.
- **The `instance_id` half** catches the **same-harness misconfiguration
  case.** If two endpoints both resolved `sender="claude"` (the user forgot
  `--as` on the second terminal), sender-only filtering would break; the
  always-distinct `instance_id` keeps mechanical filtering correct anyway
  ([ADR-011](../DECISIONS.md) — `instance_id` as the watcher's join key).

A defensive guard: if the value a half would compare is the empty string, that
half does **not** count as a match — a misconfigured empty identity must not
classify every message as own and starve the inbox.

### 5.3 Identity-collision detection (runtime WARN)

On the **first peer message a process observes**, if `peer.sender ==
self.sender` — both endpoints claiming the same human-readable identity — the
watcher emits a loud `WARN` to **stderr**, carrying the `[identity-collision]`
prefix and remediation text:

> Both endpoints on channel `'X'` are using sender label `'Y'`. Use `--as
> <distinct-label>` on at least one terminal (or set `LETTERBOX_SENDER` to a
> distinct value before launching). Mechanical message filtering still works
> via instance_id, but read-state files will collide and `letterbox tail`
> output will be unreadable until `--as` is fixed.

It is **non-fatal** — mechanical filtering continues via `instance_id` — and
**fires once per session** (instance-level dedupe), re-arming on each process
start so it re-warns every session until the user fixes the config. The
consequence it warns about is concrete: two endpoints sharing a `sender_label`
share one `.read/{label}.json` read-state file, so their high-water marks
collide and `letterbox tail` becomes ambiguous. The diagnostic goes to stderr
(the user's surface), never into the agent's PTY notification stream, because
the peer-controlled `sender` value is untrusted text (Vision §13.3).

---

## 6. Read-state semantics (per-agent)

"Unread" is a **local** decision (this agent consumed a message), not a global
one (the message is done for everyone). So v1 tracks read state with per-agent
marker files, not a shared archive.

### 6.1 The read-state file

`<channel>/.read/{sender_label}.json` — one file per endpoint per channel,
written with the same atomic-rename + `sort_keys=True` + `ensure_ascii=False`
discipline as messages. It has **exactly four fields**:

```json
{
  "sender_label": "alice",
  "instance_id": "lb-20260527T143000Z-7f3a9c",
  "high_water_mark": "msg-20260527T143015123456-7d8e3a1f2c4b5d6e7f8091a2b3c4d5e6",
  "updated_at": "2026-05-27T14:31:02.456789+00:00"
}
```

| Field | Meaning |
|---|---|
| `sender_label` | This endpoint's identity; also the filename stem. |
| `instance_id` | The launcher's process identity at the most recent write. |
| `high_water_mark` | The message-id **stem** (no `.json`) of the most recent acknowledged peer message. |
| `updated_at` | ISO-8601 UTC string of the last write. |

`high_water_mark` is a **stem, not a filename** (no `.json`). The empty string
`""` is the **fresh-endpoint sentinel** — it is lexically less than any real
`msg-…` stem, so a brand-new endpoint's "everything is unread" falls out of the
same `> high_water_mark` comparison with no special-casing.

### 6.2 `acknowledge(message_id)`

Replaces the old `archive` tool ([ADR-021](../DECISIONS.md)). It atomically
advances **this** endpoint's `high_water_mark` to `max(current, message_id)` by
filename order. It is idempotent (acknowledging the same or an older id is a
no-op on the mark) and **never touches message files** — the peer's view is
unaffected and every message stays live until the user prunes. Only
`.read/{self.sender_label}.json` is written. At the MCP boundary the
`message_id` is validated against the filename regex before the mark advances,
so a hallucinated id (e.g. `"zzz"`, which sorts above every real `msg-…` stem)
cannot jump the mark past the whole inbox and silently blank it
([ADR-037](../DECISIONS.md)).

### 6.3 `check_messages` query semantics

Returns peer messages whose filename is `> high_water_mark`, oldest-first,
after the §5 own-write filter ([ADR-012](../DECISIONS.md)). An optional
`since_id` argument overrides the cursor for the query **without moving the
marker** — a read-only peek. The result is capped at `limit` (default `20`,
hard maximum `100`; an out-of-range request is clamped to `[1, 100]` and a
`warning` is included). A `has_more` flag signals that more unread items exist
past the cap. Parse-error envelopes (§4.4) count against the limit because each
consumes an inbox slot the agent must attend to.

### 6.4 The six MCP tools

The MCP server exposes exactly six tools — there is no `archive` tool (it
became `acknowledge`):

| Tool | Purpose |
|---|---|
| `send_message(body, in_reply_to=None)` | Write a message; returns its `id`. Identity is server-side. |
| `check_latest_message()` | The single newest unread peer message, or `null`. Read-only; does not advance the marker. |
| `check_messages(limit=20, since_id=None)` | Paginated unread peer messages, oldest-first, with `has_more` (§6.3). |
| `acknowledge(message_id)` | Advance this endpoint's high-water mark (§6.2). |
| `list_channels()` | All channels (registered + auto-created) with last-activity. |
| `channel_info()` | This endpoint's identity on a channel + its true unread count. |

---

## 7. Retention policy

**v1 ships no automatic retention.** Channels grow until the user prunes them —
surprise auto-deletion is unacceptable in comms infrastructure, and Letterbox
honors the Ironclad Invariant: automated recovery never destroys user data
(Kernel L8). The `acknowledge` tool advances a marker; it never removes a file.

Retention is entirely user-driven via `letterbox prune` — the **only**
destructive command in Letterbox. Reference-level summary (the
[README](../README.md#operations) and sample own the how-to):

- It requires `--channel <name>` and **exactly one** selection rule (the rules
  are a required, mutually-exclusive group, so a bare `prune --channel x` is an
  error — no "prune everything by accident"):
  - `--older-than <duration>` — messages older than now − duration (e.g. `7d`,
    `2h`, `30m`, `45s`), aged by the **filename** timestamp, not `mtime`.
  - `--keep-last <N>` — all but the N newest messages (N ≥ 0).
  - `--acknowledged-by-all` — messages every known endpoint has acknowledged
    (filename `<= high_water_mark` for *every* `.read/*.json`); the
    highest-confidence rule, failing safe to none.
- It is **dry-run by default** — it prints what *would* happen and touches
  nothing.
- Without `--delete`, matched messages **move to `cold/`** (reversible).
- `--delete` upgrades the action from move to permanent deletion and requires
  `--yes-i-am-sure` (double-gated).

See [ADR-047](../DECISIONS.md) for the full action/safety matrix. Manual `rm`
or `mv` also works — files are the source of truth and Letterbox locks nothing
(Kernel L1).

**Documented practical ceiling:** ~10,000 messages per channel before
`check_messages` / `list-channels` latency becomes noticeable. Prune above that
point.

**No surprise deletion, ever.** The only paths that remove a message are
`prune --delete --yes-i-am-sure` and a human's `rm`/`mv`.

---

## 8. Recovery semantics

Letterbox is filesystem-local, so most recovery is implicit — state survives a
crash because it lives on disk. The explicit edge cases, each stated as
**shipped** behavior:

| Situation | Letterbox's response |
|---|---|
| `msg-*.json.tmp` present during a read | **Ignored** — the `msg-*.json` glob structurally excludes `.tmp`. Never read, never raced against a concurrent writer. |
| Orphaned `.tmp` from a long-past crashed write | Reaped on **startup only** (not on every access), and only when `mtime` is older than **1 hour** — anything fresher could be a live write. Logged at `INFO`. ([ADR-016](../DECISIONS.md)) |
| Malformed JSON in a message file | `ParseError` returned; **file left in place** (Kernel L8). `check_messages` surfaces the `{id, parse_error, body: null}` envelope (§4.4). |
| Oversized message file (> 5 MB) | `ParseError("oversized")`; file left in place; same wire envelope. |
| Future-dated timestamp (clock skew) | **Accepted as-is.** Letterbox trusts the writer's clock; it does not enforce wall-clock sanity. |
| Watcher crash mid-poll | On restart the watcher rescans and re-notifies only **unacknowledged** peer messages (gated by `high_water_mark`) — never a blast. No message is lost; the filesystem holds the truth. |
| Channel directory deleted mid-run | Watcher logs a `[channel-missing]` `WARN` and lazily **re-creates** the directory (mode `0700`) on the next polling tick; it does not crash. The prior conversation is gone (user intent), and Letterbox respects that. |
| `.read/{label}.json` missing on startup | Treated as a **fresh endpoint**: `high_water_mark = ""`, full peer backlog returned. The file is created on first `acknowledge`. |
| `.read/{label}.json` corrupted / unparseable | Renamed to `.read/{label}.json.broken.<ts>` (a fixed-width `YYYYMMDDTHHMMSSffffffZ` suffix — colon-free, microsecond-precise, sort-monotonic), a fresh file is created, and a `WARN` is logged. The **original is preserved** (renamed, not deleted — Kernel L8). |
| Restart with a different `sender_label` | Treated as a new endpoint identity: it uses/creates the new label's read-state file; the old label's state is preserved. The user explicitly changed identity and Letterbox respects that. |
| Permissions denied on write | `send_message` returns an error to the agent — not silently swallowed, and no retry. |

> **There is no `channel/archive/` directory.** An earlier draft documented a
> "same filename in `channel/` and `channel/archive/`" collision case; that
> directory does not exist in v1. The shared archive became per-agent `.read/`
> markers ([ADR-021](../DECISIONS.md)), so the collision case it described
> cannot arise.

**The Ironclad Invariant in practice.** Letterbox never deletes user data
automatically. The only deletion paths are user-initiated (`prune --delete
--yes-i-am-sure`, manual `rm`, or `cp`-and-`rm` migration). Cascade test: if
every Letterbox instance worldwide ran startup recovery at once, the worst
outcome is some orphaned `.tmp` files getting cleaned up — no user message is
ever lost.

---

## 9. Restart as fresh start (no backlog flood)

On startup — after a crash, an intentional restart, or a first launch on an
existing channel — the watcher does **not** auto-surface historical messages.
It initializes a **start watermark** (the newest existing filename, or the
current UTC time if the channel is empty) and fires notifications **only for
messages arriving strictly after** that point ([ADR-024](../DECISIONS.md)).

Historical messages still exist and are reachable on demand —
`check_messages`, `check_latest_message`, or `letterbox tail` all surface them
deliberately when the user asks.

The rationale: a restart usually means "fresh start," not "replay everything."
Auto-flooding the agent with backlog notifications would bomb its context
window, force it to respond to stale state, and take the user out of the loop.
The watcher's job is Kernel L3 — *wake the agent for **new** arrivals* — not
historical replay. A peer that writes *after* this endpoint starts watching
produces a normal live notification, exactly as if both had been watching the
whole time.

---

## 10. Design decision index

Every load-bearing decision in this document is recorded as an ADR in
[`DECISIONS.md`](../DECISIONS.md):

| Decision | ADR |
|---|---|
| Files are the source of truth (no daemon, no network) | [ADR-002](../DECISIONS.md) |
| `instance_id` as the watcher's join key | [ADR-011](../DECISIONS.md) |
| `check_messages` pagination (`limit=20`, max `100`, `since_id`) | [ADR-012](../DECISIONS.md) |
| 5 MB body ceiling | [ADR-014](../DECISIONS.md) |
| UTC timestamps everywhere; local time forbidden | [ADR-015](../DECISIONS.md) |
| `.tmp` ignored on read, reaped only on startup (`mtime` > 1h) | [ADR-016](../DECISIONS.md) |
| Microsecond precision; sort by filename, not `mtime` | [ADR-017](../DECISIONS.md) |
| `in_reply_to` trusted blindly; no integrity check | [ADR-020](../DECISIONS.md) |
| Per-agent read state replaces the shared archive | [ADR-021](../DECISIONS.md) |
| Combined own-write filter (`sender` OR `instance_id`) | [ADR-022](../DECISIONS.md) |
| Watcher does not auto-surface backlog; restart = fresh start | [ADR-024](../DECISIONS.md) |
| Identity is per-launch (CLI/env), not per-channel config | [ADR-026](../DECISIONS.md) |
| Full UUID4 filenames for collision safety | [ADR-027](../DECISIONS.md) |
| Strict filename validation; malformed names ignored | [ADR-028](../DECISIONS.md) |
| Sorted-key JSON for stable round-trip equality | [ADR-030](../DECISIONS.md) |
| `letterbox prune`: dry-run default, four-row action/safety matrix | [ADR-047](../DECISIONS.md) |

For the complete record — including decisions outside this document's scope —
see [`DECISIONS.md`](../DECISIONS.md). For the threat model and security
posture in prose, see the [README security model](../README.md#security-model).
Licensed MIT ([LICENSE](../LICENSE)).
