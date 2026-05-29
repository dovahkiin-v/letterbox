#!/usr/bin/env python3
"""Fake harness CLI — a stand-in for `claude`/`gemini`/`agy` in tests.

Reads stdin in a loop, appending every chunk to the file given by
`--echo-to`. Optionally parses `--mcp-config <path>` and spawns the
configured stdio MCP server as a child process (mirroring how a real
harness would launch its MCP server child).

Designed to be invoked the same way as a real CLI: an argv list passed
to `pty.fork`/`os.execvp` by an adapter, or to `subprocess.Popen` by a
meta-test. Stdlib only — must run without any `letterbox` install on
the path (the adapter spawn contract is "executable on disk", not
"importable Python module").

Usage:
    python tests/fake_harness.py --echo-to <path> [--mcp-config <path>]
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
from pathlib import Path


def _parse_mcp_config(config_path: Path) -> tuple[str, list[str]]:
    """Read an MCP config file and return (command, args) for the first entry.

    Accepts two shapes:
      1. Claude-Code-compatible: {"mcpServers": {"<name>": {"command": "...", "args": [...]}}}
      2. Ergonomic flat shape:   {"command": "...", "args": [...]}

    Phase 5c will emit shape (1); shape (2) is supported so ad-hoc tests
    can hand-write a tiny config without the `mcpServers` envelope.
    """
    with config_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if "mcpServers" in data:
        servers = data["mcpServers"]
        if not isinstance(servers, dict) or not servers:
            raise ValueError(f"mcpServers in {config_path} is empty or wrong type")
        first_name = next(iter(servers))
        entry = servers[first_name]
    elif "command" in data:
        entry = data
    else:
        raise ValueError(
            f"{config_path} has neither 'mcpServers' nor a top-level 'command'"
        )

    command = entry.get("command")
    if not isinstance(command, str) or not command:
        raise ValueError(f"MCP entry in {config_path} missing 'command' string")
    args = entry.get("args", [])
    if not isinstance(args, list):
        raise ValueError(f"MCP entry in {config_path} has non-list 'args'")
    return command, [str(a) for a in args]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fake harness for letterbox tests.")
    parser.add_argument(
        "--echo-to",
        required=True,
        type=Path,
        help="File to append all stdin bytes to (created if missing).",
    )
    parser.add_argument(
        "--mcp-config",
        type=Path,
        default=None,
        help="Optional MCP config file; spawns the configured child via subprocess.",
    )
    args = parser.parse_args(argv)

    mcp_child: subprocess.Popen[bytes] | None = None
    if args.mcp_config is not None:
        command, mcp_args = _parse_mcp_config(args.mcp_config)
        mcp_child = subprocess.Popen(
            [command, *mcp_args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Log to stderr so tests can verify the child was spawned without
        # polluting stdout (which is reserved for harness-style data flow).
        print(f"fake_harness: spawned MCP child pid={mcp_child.pid}", file=sys.stderr)

    # Open echo file in binary append mode so multiple write() calls
    # accumulate the same way a real terminal log would.
    args.echo_to.parent.mkdir(parents=True, exist_ok=True)
    with args.echo_to.open("ab") as echo_fh:
        terminated = False

        def _on_sigterm(_signum: int, _frame: object) -> None:
            nonlocal terminated
            terminated = True

        signal.signal(signal.SIGTERM, _on_sigterm)

        # read1 gives us "whatever is available right now" without blocking
        # waiting for a full buffer — important when stdin is a PTY and the
        # other side sends short bursts (one notification at a time).
        stdin_buf = sys.stdin.buffer
        while not terminated:
            chunk = stdin_buf.read1(4096)
            if not chunk:
                break  # EOF
            echo_fh.write(chunk)
            echo_fh.flush()

    # Reap the MCP child cleanly. Real harnesses own their MCP child's
    # lifecycle; we mirror that contract here. communicate() is used over
    # wait() to drain the pipe buffers so ResourceWarning is never raised
    # when this script runs under a parent with filterwarnings=error.
    if mcp_child is not None:
        if mcp_child.poll() is None:
            mcp_child.terminate()
            try:
                mcp_child.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                mcp_child.kill()
                mcp_child.communicate(timeout=1.0)
        else:
            mcp_child.communicate(timeout=1.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
