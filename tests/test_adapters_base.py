"""Tests for ``letterbox.adapters.base`` — Phases 5b/5d.

Eight test classes covering the Adapter ABC class shape, ``register_adapter``
validation, ``get_adapter`` lookup, the async ``spawn``/``inject``/
``teardown`` lifecycle composing 5a's ``pty_common`` primitives, and (5d) the
end-to-end lifecycle-hook ordering/argument contract. Real PTYs, real
subprocesses, real fds — no mocks on the production path.

Async lifecycle tests carry ``@pytest.mark.asyncio`` (the project's
``asyncio_mode = "strict"`` — no auto-marking) and await the methods
directly. Every spawn is wrapped in ``try/finally await adapter.teardown``
per G6: ``filterwarnings = ["error"]`` promotes a leaked fd's
``ResourceWarning`` to a test failure.

Teardown timeout discipline: ``fake_harness`` installs a SIGTERM handler
that sets a flag, but its blocking ``read1`` is auto-retried after the
handler (PEP 475), so the flag is never re-checked until SIGKILL. Thus
``close_pty_handle`` waits the full timeout before SIGKILL. Cleanup
teardowns pass ``timeout=_FAST_TEARDOWN`` (1.0s) to keep the suite fast;
the default 5.0s would be paid on every spawn test otherwise.

Byte-exact line-terminator tests set the slave end raw (``tty.setraw``)
so input line discipline doesn't translate ``\\r`` → ``\\n``, and assert
on what the child RECEIVED on stdin (the echo file) — NOT on what echoes
back on the master fd, where OPOST output-processing can mangle ``\\r``.
"""
from __future__ import annotations

import inspect
import os
import sys
import time
import tty
from pathlib import Path

import pytest

from letterbox.adapters import base
from letterbox.adapters.base import (
    Adapter,
    AdapterAlreadyRegistered,
    AdapterConfigurationError,
    get_adapter,
    register_adapter,
)
from letterbox.adapters.pty_common import PTYHandle
from tests.conftest import FakeHarness
from tests.helpers import wait_for

# fake_harness can't interrupt its blocking read on SIGTERM (PEP 475), so
# close_pty_handle pays the full timeout then SIGKILLs. Keep cleanup snappy.
_FAST_TEARDOWN = 1.0

# A `python -c` child that writes its positional argv (sys.argv[2:]) to the
# file named by sys.argv[1], one per line, then exits. Lets a test inspect
# the exact argv the adapter assembled, without fake_harness cooperation.
_ARGV_DUMP = (
    "import sys; "
    "open(sys.argv[1], 'w', encoding='utf-8').write(chr(10).join(sys.argv[2:]))"
)

# Verbatim copy of base.py's tier-header (lines 1-10). The
# test_tier_header_preserved_verbatim lock fails if the body fill-in ever
# disturbs the §13.5 import-discipline record.
_EXPECTED_TIER_HEADER = [
    '"""Adapter ABC, registry, injection contract (CR enforcement in base class).',
    "",
    "Tier: 2",
    "May import from: stdlib; ``letterbox.adapters.pty_common`` (sibling-package-mate, the one",
    "    controlled cross-Tier-2 case within ``adapters/`` named by the index).",
    "Must NOT import from: concrete adapters (``letterbox.adapters.claude``, ``gemini``,",
    "    ``antigravity``) or any Tier 4 module — bulkhead §13.5.",
    "",
    "Filled in: Phase 5b/5d per PHASE_INDEX.",
    '"""',
]


# ──────────────────────────────────────────────────────────────────────
# Local helpers
# ──────────────────────────────────────────────────────────────────────


def _minimal_env() -> dict[str, str]:
    """Just enough env for a python child to find its own interpreter."""
    return {"PATH": os.environ["PATH"]}


def _dummy_handle() -> PTYHandle:
    """A handle for tests whose code path never touches the fds (type guards)."""
    return PTYHandle(pid=-1, master_fd=-1, slave_fd=-1, process=None)  # type: ignore[arg-type]


def _make_cls(clsname: str = "FakeAdapter", **overrides: object) -> type[Adapter]:
    """Build a concrete Adapter subclass with valid defaults + overrides.

    Used by the registry tests; the defaults all pass validation so each
    override isolates a single rejection branch.
    """
    attrs: dict[str, object] = {
        "name": "fake",
        "command": "cmd",
        "default_args": [],
        "notification_template": "test {channel}",
        "line_terminator": b"\r",
    }
    attrs.update(overrides)
    return type(clsname, (Adapter,), attrs)


def _fake_adapter_cls(
    fake_harness: FakeHarness, *, name: str = "fake", line_terminator: bytes = b"\r"
) -> type[Adapter]:
    """Build an Adapter subclass that spawns the bundled fake_harness."""
    return type(
        "_FakeAdapter",
        (Adapter,),
        {
            "name": name,
            "command": sys.executable,
            "default_args": [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ],
            "notification_template": "test {channel}",
            "line_terminator": line_terminator,
        },
    )


def _recording_adapter_cls(fake_harness: FakeHarness) -> type[Adapter]:
    """Build an Adapter subclass that records every lifecycle-hook call.

    Each of the four sync hooks appends ``(hook_name, received_arg)`` to the
    instance's ``calls`` list while preserving the default identity/no-op
    semantics (``pre_spawn``/``pre_inject`` still return their input
    unchanged). The base class supplies all async behaviour; the subclass
    only *observes*. One shared ordered ``calls`` list is the only structure
    that can prove cross-method ordering (5d K3) — 5b's per-hook tests each
    prove one hook fires, none proves the relative sequence.
    """

    class _RecordingAdapter(Adapter):
        name = "recording"
        command = sys.executable
        default_args = [
            str(fake_harness.script_path),
            "--echo-to",
            str(fake_harness.echo_file),
        ]
        notification_template = "t"

        def __init__(self) -> None:
            super().__init__()
            self.calls: list[tuple[str, object]] = []

        def pre_spawn(self, args: list[str]) -> list[str]:
            self.calls.append(("pre_spawn", args))
            return args

        def post_spawn(self, handle: PTYHandle) -> None:
            self.calls.append(("post_spawn", handle))

        def pre_inject(self, message: str) -> str:
            self.calls.append(("pre_inject", message))
            return message

        def pre_teardown(self, handle: PTYHandle) -> None:
            self.calls.append(("pre_teardown", handle))

    return _RecordingAdapter


@pytest.fixture
def reset_registry(monkeypatch: pytest.MonkeyPatch) -> dict[str, type[Adapter]]:
    """Replace the module-level ``_REGISTRY`` with a fresh empty dict.

    Mandatory for any test that mutates the registry: unlike 5a's pid-keyed
    ``_closed_handles``, adapter NAMES collide across tests registering
    "fake", so without isolation a second registration raises
    ``AdapterAlreadyRegistered``. Mirrors 2c's ``reset_warn_dedupe``.
    """
    fresh: dict[str, type[Adapter]] = {}
    monkeypatch.setattr(base, "_REGISTRY", fresh)
    return fresh


# ──────────────────────────────────────────────────────────────────────
# TestAdapterClassShape
# ──────────────────────────────────────────────────────────────────────


class TestAdapterClassShape:
    def test_direct_instantiation_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="abstract base class"):
            Adapter()

        # A minimal subclass passes the __new__ guard cleanly.
        class Sub(Adapter):
            name = "x"
            command = "y"
            default_args: list[str] = []
            notification_template = "t"

        assert isinstance(Sub(), Sub)

    def test_adapter_has_required_class_attrs(self) -> None:
        assert Adapter.name == ""
        assert Adapter.command == ""
        assert Adapter.default_args == []
        assert Adapter.notification_template == ""
        assert Adapter.line_terminator == b"\r"

    def test_class_attrs_are_classvar_annotated(self) -> None:
        ann = inspect.get_annotations(Adapter)
        assert ann["name"] == "ClassVar[str]"
        assert ann["command"] == "ClassVar[str]"
        assert ann["default_args"] == "ClassVar[list[str]]"
        assert ann["notification_template"] == "ClassVar[str]"
        assert ann["line_terminator"] == "ClassVar[bytes]"

    def test_default_lifecycle_hooks_are_identity_noops(self) -> None:
        # Use a subclass — Adapter() itself is rejected by the __new__ guard.
        class Sub(Adapter):
            name = "x"
            command = "y"
            default_args: list[str] = []
            notification_template = "t"

        sub = Sub()
        handle = _dummy_handle()
        assert sub.pre_spawn(["a", "b"]) == ["a", "b"]
        assert sub.pre_inject("hi") == "hi"
        assert sub.post_spawn(handle) is None
        assert sub.pre_teardown(handle) is None

    def test_line_terminator_default_is_cr_bytes(self) -> None:
        assert Adapter.line_terminator == b"\r"

    def test_adapter_async_methods_are_coroutines(self) -> None:
        assert inspect.iscoroutinefunction(Adapter.spawn)
        assert inspect.iscoroutinefunction(Adapter.inject)
        assert inspect.iscoroutinefunction(Adapter.teardown)


# ──────────────────────────────────────────────────────────────────────
# TestRegisterAdapter
# ──────────────────────────────────────────────────────────────────────


class TestRegisterAdapter:
    def test_register_decorator_returns_class_unchanged(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        cls = _make_cls(name="fake")
        returned = register_adapter(cls)
        assert returned is cls  # no wrapping (G3)

    def test_register_stores_in_registry_by_name(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        cls = _make_cls(name="fake")
        register_adapter(cls)
        assert base._REGISTRY["fake"] is cls

    def test_register_rejects_empty_name(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError, match="name"):
            register_adapter(_make_cls(name=""))

    def test_register_rejects_empty_command(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError, match="command"):
            register_adapter(_make_cls(command=""))

    def test_register_rejects_empty_notification_template(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError, match="notification_template"):
            register_adapter(_make_cls(notification_template=""))

    def test_register_rejects_empty_line_terminator(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError, match="line_terminator"):
            register_adapter(_make_cls(line_terminator=b""))

    def test_register_rejects_non_bool_mcp_config_via_flag(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(
            AdapterConfigurationError, match="mcp_config_via_flag"
        ):
            register_adapter(_make_cls(mcp_config_via_flag="yes"))

    def test_register_rejects_negative_terminator_delay(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(
            AdapterConfigurationError, match="terminator_delay"
        ):
            register_adapter(_make_cls(terminator_delay=-0.5))

    def test_register_rejects_non_number_terminator_delay(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        # bool is rejected too (True/False are ints but nonsensical as a delay).
        with pytest.raises(
            AdapterConfigurationError, match="terminator_delay"
        ):
            register_adapter(_make_cls(terminator_delay=True))

    def test_register_accepts_empty_default_args(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        # The Antigravity adapter (6c) ships default_args=[] (Vision §5.3).
        cls = _make_cls(name="empty_args", default_args=[])
        register_adapter(cls)
        assert base._REGISTRY["empty_args"] is cls

    def test_register_rejects_non_list_default_args(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError) as exc:
            register_adapter(_make_cls(default_args="--yolo"))
        msg = exc.value.args[0]
        assert "default_args" in msg
        assert "list[str]" in msg

    def test_register_rejects_non_str_default_args_element(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(AdapterConfigurationError) as exc:
            register_adapter(_make_cls(default_args=["--ok", 123]))
        msg = exc.value.args[0]
        assert "default_args" in msg
        assert "list[str]" in msg

    def test_register_rejects_duplicate_name(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        register_adapter(_make_cls("AdapterOne", name="dup"))
        with pytest.raises(AdapterAlreadyRegistered) as exc:
            register_adapter(_make_cls("AdapterTwo", name="dup", command="other"))
        msg = exc.value.args[0]
        assert "AdapterOne" in msg
        assert "AdapterTwo" in msg


# ──────────────────────────────────────────────────────────────────────
# TestGetAdapter
# ──────────────────────────────────────────────────────────────────────


class TestGetAdapter:
    def test_returns_instance_of_registered_class(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        cls = _make_cls(name="fake")
        register_adapter(cls)
        assert isinstance(get_adapter("fake"), cls)

    def test_returns_fresh_instance_each_call(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        cls = _make_cls(name="fake")
        register_adapter(cls)
        first = get_adapter("fake")
        second = get_adapter("fake")
        assert first is not second
        assert type(first) is type(second) is cls

    def test_raises_keyerror_with_vector_message_on_unknown_name(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        register_adapter(_make_cls(name="fake"))
        with pytest.raises(KeyError) as exc:
            get_adapter("nonexistent")
        msg = exc.value.args[0]
        assert "nonexistent" in msg
        assert "fake" in msg

    def test_keyerror_lists_registered_names_alphabetically(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        register_adapter(_make_cls("Z", name="zeta"))
        register_adapter(_make_cls("A", name="alpha"))
        register_adapter(_make_cls("M", name="mu"))
        with pytest.raises(KeyError) as exc:
            get_adapter("missing")
        msg = exc.value.args[0]
        assert "['alpha', 'mu', 'zeta']" in msg

    def test_rejects_non_str_name(
        self, reset_registry: dict[str, type[Adapter]]
    ) -> None:
        with pytest.raises(TypeError, match="must be str"):
            get_adapter(123)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestSpawn
# ──────────────────────────────────────────────────────────────────────


class TestSpawn:
    @pytest.mark.asyncio
    async def test_spawn_returns_pty_handle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            assert isinstance(handle, PTYHandle)
            assert handle.pid > 0
            assert handle.process.poll() is None
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)

    @pytest.mark.asyncio
    async def test_spawn_uses_class_command_and_default_args(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            # fake_harness opens --echo-to in append mode at startup → the
            # file's existence proves the argv (command + default_args) was
            # assembled correctly and executed.
            await wait_for(lambda: fake_harness.echo_file.exists(), timeout=5.0)
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert fake_harness.echo_file.exists()

    @pytest.mark.asyncio
    async def test_spawn_appends_extra_args_after_default_args(
        self, tmp_path: Path
    ) -> None:
        out = tmp_path / "argv.txt"
        cls = type(
            "_ArgvAdapter",
            (Adapter,),
            {
                "name": "argv",
                "command": sys.executable,
                "default_args": ["-c", _ARGV_DUMP, str(out), "DEFAULT"],
                "notification_template": "t",
                "line_terminator": b"\r",
            },
        )
        adapter = cls()
        handle = await adapter.spawn(["EXTRA"], tmp_path, _minimal_env())
        try:
            await wait_for(lambda: out.exists() and out.read_text(), timeout=5.0)
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert out.read_text(encoding="utf-8").splitlines() == ["DEFAULT", "EXTRA"]

    @pytest.mark.asyncio
    async def test_spawn_calls_pre_spawn_hook(self, tmp_path: Path) -> None:
        out = tmp_path / "argv.txt"

        class _PreSpawnAdapter(Adapter):
            name = "ps"
            command = sys.executable
            default_args = ["-c", _ARGV_DUMP, str(out), "DEFAULT"]
            notification_template = "t"

            def pre_spawn(self, args: list[str]) -> list[str]:
                return [*args, "SENTINEL"]

        adapter = _PreSpawnAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await wait_for(lambda: out.exists() and out.read_text(), timeout=5.0)
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert out.read_text(encoding="utf-8").splitlines() == ["DEFAULT", "SENTINEL"]

    @pytest.mark.asyncio
    async def test_spawn_calls_post_spawn_hook(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        seen: list[int] = []

        class _PostSpawnAdapter(Adapter):
            name = "post"
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]
            notification_template = "t"

            def post_spawn(self, handle: PTYHandle) -> None:
                seen.append(handle.pid)

        adapter = _PostSpawnAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            assert seen == [handle.pid]
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)


# ──────────────────────────────────────────────────────────────────────
# TestInject
# ──────────────────────────────────────────────────────────────────────


class TestInject:
    @pytest.mark.asyncio
    async def test_inject_writes_message_plus_cr_to_master_fd(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "hello")
            # Cooked-mode PTY converts the trailing \r to \n on input (5a G9);
            # assert the substring for portability.
            await wait_for(
                lambda: b"hello" in fake_harness.read_echo(), timeout=5.0
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert b"hello" in fake_harness.read_echo()

    @pytest.mark.asyncio
    async def test_inject_encodes_utf_8(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        message = "📬 Pranešimas: ąčęėįšųūž 中文"
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, message)
            want = message.encode("utf-8")
            await wait_for(lambda: want in fake_harness.read_echo(), timeout=5.0)
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert message.encode("utf-8") in fake_harness.read_echo()

    @pytest.mark.asyncio
    async def test_inject_appends_line_terminator_unconditionally(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()  # default b"\r"
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            # Raw mode drops input line discipline so \r reaches the child
            # unchanged; assert on what the child received (the echo file).
            tty.setraw(handle.slave_fd)
            await adapter.inject(handle, "x")
            await wait_for(
                lambda: fake_harness.read_echo() == b"x\r", timeout=5.0
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert fake_harness.read_echo() == b"x\r"

    @pytest.mark.asyncio
    async def test_inject_uses_custom_line_terminator_when_overridden(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness, line_terminator=b"\n")()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            tty.setraw(handle.slave_fd)
            await adapter.inject(handle, "x")
            await wait_for(
                lambda: fake_harness.read_echo() == b"x\n", timeout=5.0
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert fake_harness.read_echo() == b"x\n"

    @pytest.mark.asyncio
    async def test_inject_combined_write_when_no_delay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ADR-057: terminator_delay == 0 → text + terminator in ONE os.write.
        writes: list[bytes] = []
        monkeypatch.setattr(
            "letterbox.adapters.base.inject_to_pty",
            lambda _fd, payload: writes.append(payload),
        )
        adapter = _make_cls(terminator_delay=0.0)()
        await adapter.inject(_dummy_handle(), "hi")
        assert writes == [b"hi\r"]

    @pytest.mark.asyncio
    async def test_inject_delayed_terminator_is_separate_write(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ADR-057: terminator_delay > 0 (Gemini, Antigravity) emits the text and
        # the terminator as TWO writes with a gap between them, so the terminator
        # clears the harness's fast-return window and registers as a submit — the
        # fix for "message lands in the input box but never submits". The delay
        # is awaited between the writes.
        writes: list[bytes] = []
        monkeypatch.setattr(
            "letterbox.adapters.base.inject_to_pty",
            lambda _fd, payload: writes.append(payload),
        )
        slept: list[float] = []

        async def _fake_sleep(secs: float) -> None:
            slept.append(secs)

        monkeypatch.setattr("letterbox.adapters.base.asyncio.sleep", _fake_sleep)
        adapter = _make_cls(terminator_delay=0.05)()
        await adapter.inject(_dummy_handle(), "hi")
        assert writes == [b"hi", b"\r"]
        assert slept == [0.05]

    @pytest.mark.asyncio
    async def test_inject_calls_pre_inject_to_transform_message(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        class _UpperAdapter(Adapter):
            name = "upper"
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]
            notification_template = "t"

            def pre_inject(self, message: str) -> str:
                return message.upper()

        adapter = _UpperAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "hello")
            await wait_for(
                lambda: b"HELLO" in fake_harness.read_echo(), timeout=5.0
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert b"HELLO" in fake_harness.read_echo()

    @pytest.mark.asyncio
    async def test_inject_pre_inject_transforms_string_not_bytes(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        seen_types: list[str] = []

        class _TypeRecordingAdapter(Adapter):
            name = "typerec"
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]
            notification_template = "t"

            def pre_inject(self, message: str) -> str:
                seen_types.append(type(message).__name__)
                return message

        adapter = _TypeRecordingAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "x")
            # pre_inject runs synchronously inside inject, before the encode.
            assert seen_types == ["str"]
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)

    @pytest.mark.asyncio
    async def test_inject_rejects_non_str_message(self) -> None:
        # The type guard fires before any fd is touched — a dummy handle is
        # sufficient and avoids spawning a subprocess.
        class _Min(Adapter):
            pass

        adapter = _Min()
        with pytest.raises(TypeError, match="message must be str"):
            await adapter.inject(_dummy_handle(), b"raw bytes")  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# TestTeardown
# ──────────────────────────────────────────────────────────────────────


class TestTeardown:
    @pytest.mark.asyncio
    async def test_teardown_terminates_child_via_close_pty_handle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert handle.process.returncode is not None

    @pytest.mark.asyncio
    async def test_teardown_calls_pre_teardown_hook(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        seen: list[int] = []

        class _PreTeardownAdapter(Adapter):
            name = "pretd"
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]
            notification_template = "t"

            def pre_teardown(self, handle: PTYHandle) -> None:
                seen.append(handle.pid)

        adapter = _PreTeardownAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert seen == [handle.pid]

    @pytest.mark.asyncio
    async def test_teardown_is_idempotent_via_close_pty_handle_idempotence(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        calls: list[int] = []

        class _CountingAdapter(Adapter):
            name = "count"
            command = sys.executable
            default_args = [
                str(fake_harness.script_path),
                "--echo-to",
                str(fake_harness.echo_file),
            ]
            notification_template = "t"

            def pre_teardown(self, handle: PTYHandle) -> None:
                calls.append(handle.pid)

        adapter = _CountingAdapter()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        # Second call: close_pty_handle short-circuits (pid in _closed_handles)
        # so no second signal is sent, but the pre_teardown hook still runs —
        # the idempotence guard lives below the hook (plan §9 / scout note).
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert calls == [handle.pid, handle.pid]
        assert handle.process.returncode is not None

    @pytest.mark.asyncio
    async def test_teardown_with_custom_timeout(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        # Let the child install its SIGTERM handler + reach its read loop, so
        # this exercises the timeout path (not the spawn→teardown race that
        # would let the default SIGTERM action win immediately).
        await wait_for(lambda: fake_harness.echo_file.exists(), timeout=5.0)
        start = time.monotonic()
        await adapter.teardown(handle, timeout=1.0)
        elapsed = time.monotonic() - start
        assert handle.process.returncode is not None
        # 1.0s timeout would resolve well under 3s; the 5.0s default would not
        # — so this also proves the custom timeout was honored.
        assert elapsed < 3.0, f"elapsed={elapsed!r}"


# ──────────────────────────────────────────────────────────────────────
# TestPublicSurface
# ──────────────────────────────────────────────────────────────────────


class TestPublicSurface:
    def test_public_exports(self) -> None:
        assert set(base.__all__) == {
            "Adapter",
            "AdapterAlreadyRegistered",
            "AdapterConfigurationError",
            "get_adapter",
            "register_adapter",
        }

    def test_private_state_not_exported(self) -> None:
        assert hasattr(base, "_REGISTRY")
        assert hasattr(base, "_LOGGER")
        assert hasattr(base, "_DEFAULT_TEARDOWN_TIMEOUT_SECONDS")
        assert hasattr(base, "_config_error")
        for name in (
            "_REGISTRY",
            "_LOGGER",
            "_DEFAULT_TEARDOWN_TIMEOUT_SECONDS",
            "_config_error",
        ):
            assert name not in base.__all__

    def test_tier_header_preserved_verbatim(self) -> None:
        source_lines = inspect.getsource(base).splitlines()
        assert source_lines[:10] == _EXPECTED_TIER_HEADER


# ──────────────────────────────────────────────────────────────────────
# TestHookOrdering — Phase 5d
# ──────────────────────────────────────────────────────────────────────


class TestHookOrdering:
    """End-to-end lifecycle-hook ordering/argument contract (Phase 5d).

    5b proved each hook *fires*; these prove the four hooks fire in the right
    relative order across one ``spawn``→``inject``→``teardown`` session, each
    receiving the right argument, with a zero-override adapter surviving the
    full lifecycle on default no-ops. The contract is documented in the
    ``Adapter`` class docstring and ADR-034.
    """

    @pytest.mark.asyncio
    async def test_full_lifecycle_hook_call_order(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # The canonical 5d test: one subclass overriding all four hooks into a
        # single shared ordered list across a whole session.
        adapter = _recording_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "hi")
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert [name for name, _ in adapter.calls] == [
            "pre_spawn",
            "post_spawn",
            "pre_inject",
            "pre_teardown",
        ]

    @pytest.mark.asyncio
    async def test_pre_spawn_receives_full_assembled_argv(
        self, tmp_path: Path
    ) -> None:
        # G8: the arg pre_spawn receives is the COMPLETE argv (command at
        # index 0), not just extra_args — the one args-correctness lens 5b
        # never asserts. Uses the _ARGV_DUMP child (not fake_harness, whose
        # argparse would reject the unknown "EXTRA" positional); only the
        # *received* arg matters, captured before spawn_pty even runs.
        out = tmp_path / "argv.txt"
        captured: list[list[str]] = []

        class _ArgvRecordingAdapter(Adapter):
            name = "argvrec"
            command = sys.executable
            default_args = ["-c", _ARGV_DUMP, str(out), "DEFAULT"]
            notification_template = "t"

            def pre_spawn(self, args: list[str]) -> list[str]:
                captured.append(args)
                return args

        adapter = _ArgvRecordingAdapter()
        extra_args = ["EXTRA"]
        handle = await adapter.spawn(extra_args, tmp_path, _minimal_env())
        try:
            assert captured == [
                [adapter.command, *adapter.default_args, *extra_args]
            ]
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)

    @pytest.mark.asyncio
    async def test_post_spawn_receives_the_returned_handle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _recording_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            received = [
                arg for name, arg in adapter.calls if name == "post_spawn"
            ]
            assert len(received) == 1
            # Identity, not liveness — a fast child may already be dead by the
            # time post_spawn runs, so assert object identity + pid (G6).
            assert received[0] is handle
            assert handle.pid > 0
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)

    @pytest.mark.asyncio
    async def test_pre_inject_receives_the_injected_message(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # Non-ASCII sample for good measure; pre_inject sees the str before
        # the UTF-8 encode in inject() (received-arg lens, distinct from 5b's
        # transform/type lenses).
        message = "📬 ąčę"
        adapter = _recording_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, message)
            received = [
                arg for name, arg in adapter.calls if name == "pre_inject"
            ]
            assert received == [message]
            assert isinstance(received[0], str)
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)

    @pytest.mark.asyncio
    async def test_pre_teardown_receives_the_spawned_handle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        adapter = _recording_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        # teardown is the thing under test (it reaps the child), so no
        # try/finally — mirror 5b's test_teardown_calls_pre_teardown_hook.
        await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        received = [arg for name, arg in adapter.calls if name == "pre_teardown"]
        assert len(received) == 1
        assert received[0] is handle

    @pytest.mark.asyncio
    async def test_bare_adapter_completes_full_lifecycle_with_default_hooks(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # Zero hook overrides — the default no-ops must carry a full session
        # (PHASE_INDEX "default no-op doesn't break anything", at the
        # full-lifecycle level 5b only checks in isolation).
        adapter = _fake_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "hello")
            await wait_for(
                lambda: b"hello" in fake_harness.read_echo(), timeout=5.0
            )
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        assert b"hello" in fake_harness.read_echo()
        assert handle.process.returncode is not None

    @pytest.mark.asyncio
    async def test_hooks_fire_exactly_once_per_lifecycle(
        self, fake_harness: FakeHarness, tmp_path: Path
    ) -> None:
        # Guards against an accidental double-invocation regression in a
        # future base.py refactor.
        adapter = _recording_adapter_cls(fake_harness)()
        handle = await adapter.spawn([], tmp_path, _minimal_env())
        try:
            await adapter.inject(handle, "hi")
        finally:
            await adapter.teardown(handle, timeout=_FAST_TEARDOWN)
        names = [name for name, _ in adapter.calls]
        assert len(names) == 4
        assert len(set(names)) == 4  # no duplicates
