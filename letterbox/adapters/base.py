"""Adapter ABC, registry, injection contract (CR enforcement in base class).

Tier: 2
May import from: stdlib; ``letterbox.adapters.pty_common`` (sibling-package-mate, the one
    controlled cross-Tier-2 case within ``adapters/`` named by the index).
Must NOT import from: concrete adapters (``letterbox.adapters.claude``, ``gemini``,
    ``antigravity``) or any Tier 4 module — bulkhead §13.5.

Filled in: Phase 5b/5d per PHASE_INDEX.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC
from pathlib import Path
from typing import ClassVar

from letterbox.adapters.pty_common import (
    PTYHandle,
    close_pty_handle,
    inject_to_pty,
    spawn_pty,
)

__all__ = [
    "Adapter",
    "AdapterAlreadyRegistered",
    "AdapterConfigurationError",
    "get_adapter",
    "register_adapter",
]

# ── Module-private state ──────────────────────────────────────────────
# Adapter-name → class. Mirrors 5a's ``_closed_handles`` / 2c's
# ``_WARNED_BAD_NAMES`` module-level-state precedent: a plain dict, no
# lock (GIL-atomic ``__setitem__``/``__getitem__``), populated once per
# process when concrete adapter modules are imported (decorator side
# effect), read-only thereafter. Tests isolate via a ``reset_registry``
# fixture (``monkeypatch.setattr(base, "_REGISTRY", {})``) because, unlike
# pid keys, adapter names CAN collide across tests registering "fake".
_REGISTRY: dict[str, type["Adapter"]] = {}

_LOGGER = logging.getLogger("letterbox.adapters.base")

# Default teardown grace period (seconds) before SIGKILL escalation. The
# literal mirrors 5a's ``pty_common._DEFAULT_TEARDOWN_TIMEOUT_SECONDS``
# (= 5.0); restated rather than imported (it is module-private to 5a).
_DEFAULT_TEARDOWN_TIMEOUT_SECONDS: float = 5.0


class AdapterConfigurationError(Exception):
    """Raised when a subclass has empty/invalid required class attributes.

    Raised at registration time (``register_adapter``) so a misconfigured
    concrete adapter fails at import, before any launcher tries to spawn
    it (Framework P3 — errors are vectors, surfaced early).
    """


class AdapterAlreadyRegistered(Exception):
    """Raised when ``register_adapter`` is called twice for the same name."""


def _config_error(
    adapter_cls: type["Adapter"], field: str, expected: str, actual: object
) -> str:
    """Build a vector error string for an invalid adapter class attribute.

    Args:
        adapter_cls: The concrete adapter class being registered.
        field: Name of the offending class attribute (e.g. ``"command"``).
        expected: Human description of the required shape (e.g.
            ``"a non-empty str"``); included verbatim so the message reads
            ``X.field must be <expected>, got <actual>``.
        actual: The offending value, rendered with ``repr`` so the type is
            visible (``''`` vs ``b''`` vs ``[]``).

    Returns:
        The formatted error message.
    """
    return (
        f"{adapter_cls.__name__}.{field} must be {expected}, got {actual!r}"
    )


class Adapter(ABC):
    """Base class for harness adapters (Vision §5.1).

    Direct instantiation is rejected (K1 — ``__new__`` raises ``TypeError``
    when ``cls is Adapter``). Concrete adapters declare class attrs only —
    the base supplies behaviour, so a v1 adapter is a five-line class with
    no method overrides::

        @register_adapter
        class ClaudeAdapter(Adapter):
            name = "claude"
            command = "claude"
            default_args = ["--dangerously-skip-permissions"]
            notification_template = "📬 Peer message on channel {channel}..."
            # line_terminator left at default b"\\r"

    The three async methods (``spawn``/``inject``/``teardown``) are concrete
    (NOT ``@abstractmethod``) — they compose 5a's ``pty_common`` primitives
    via ``asyncio.to_thread``. Subclasses customise via class attrs and the
    four lifecycle hooks (``pre_spawn``/``post_spawn``/``pre_inject``/
    ``pre_teardown``), all default no-ops (Vision §5.4).

    Carriage-return enforcement (ADR-018 / L3 "Wake the Agent") is
    structural: ``inject`` appends ``line_terminator`` (default ``b"\\r"``)
    to every payload. Subclasses can change the terminator for a future
    harness only by overriding the ``line_terminator`` class attribute —
    they cannot opt out, because the append lives in the base method.

    POSIX-only by inheritance: the wrapped 5a primitives use ``os.openpty``
    (Vision §12).

    Lifecycle hook call order (locked end-to-end by Phase 5d; see ADR-034).
    Across one session the four hooks fire in exactly this sequence — each
    ``pre_*`` hook *before* the primitive it wraps, ``post_spawn`` *after*::

        spawn:    pre_spawn(argv)     → spawn_pty(...)       → post_spawn(handle)
        inject:   pre_inject(message) → encode + terminator  → inject_to_pty(...)
        teardown: pre_teardown(handle)                       → close_pty_handle(...)

    ``pre_spawn`` receives the complete assembled argv
    ``[command, *default_args, *extra_args]``; ``post_spawn`` and
    ``pre_teardown`` receive the same :class:`PTYHandle` ``spawn`` returns;
    ``pre_inject`` receives the exact ``str`` passed to :meth:`inject`. A
    refactor that reorders these (e.g. moves ``post_spawn`` before the child
    exists) silently breaks every adapter that trusts the contract.
    """

    name: ClassVar[str] = ""
    command: ClassVar[str] = ""
    default_args: ClassVar[list[str]] = []
    notification_template: ClassVar[str] = ""
    # ADR-018: the wire terminator that wakes the agent. Subclasses override
    # the class attr (NOT the inject method) for a future harness needing a
    # different terminator. Bytes, not str, because 5a's inject_to_pty takes
    # bytes — appending an already-encoded literal is one op (G9).
    line_terminator: ClassVar[bytes] = b"\r"

    def __new__(cls, *args: object, **kwargs: object) -> "Adapter":
        if cls is Adapter:
            raise TypeError(
                "Adapter is an abstract base class; subclass it "
                "(see ClaudeAdapter)."
            )
        return super().__new__(cls)

    async def spawn(
        self,
        extra_args: list[str],
        cwd: Path,
        env: dict[str, str],
        *,
        start_new_session: bool = True,
    ) -> PTYHandle:
        """Spawn the configured harness CLI attached to a fresh PTY.

        Composes the argv as ``[command, *default_args, *extra_args]``,
        passes it through :meth:`pre_spawn` (identity by default), spawns
        via 5a's ``spawn_pty`` on a worker thread, then calls
        :meth:`post_spawn` with the resulting handle.

        Args:
            extra_args: Per-launch arguments appended after ``default_args``
                (e.g. the ``--mcp-config <path>`` the launcher generates).
            cwd: Working directory for the spawned harness.
            env: Fully-specified environment dict (NOT merged with
                ``os.environ`` — 5a K6; the launcher decides what to forward).
            start_new_session: Place the child in its own session/process
                group so teardown can ``killpg`` the whole tree. Default True
                (production value); tests may pass False.

        Returns:
            The :class:`PTYHandle` for the spawned process.

        Raises:
            TypeError: If the assembled argv is not ``list[str]`` (from 5a).
            ValueError: If the assembled argv is empty (from 5a).
            OSError: If ``openpty`` or the underlying ``Popen`` fails.
        """
        args = self.pre_spawn([self.command, *self.default_args, *extra_args])
        handle = await asyncio.to_thread(
            spawn_pty, args, cwd, env, start_new_session=start_new_session
        )
        self.post_spawn(handle)
        return handle

    async def inject(self, handle: PTYHandle, message: str) -> None:
        """Write ``message`` plus the line terminator to the PTY master fd.

        Passes ``message`` through :meth:`pre_inject` (identity by default),
        encodes the result as UTF-8 (Vision §11.1), appends
        ``line_terminator`` (ADR-018 — the agent never wakes without it),
        then writes via 5a's ``inject_to_pty`` on a worker thread.

        Args:
            handle: The handle returned by :meth:`spawn`.
            message: The already-rendered notification text. Rendering and
                whitelist validation happen upstream at 4a's
                ``render_notification``; this layer is byte-faithful
                transport, not a substitution site.

        Raises:
            TypeError: If ``message`` is not ``str`` (defence-in-depth at
                the inject boundary — a buggy caller might pass bytes).
            OSError: If the slave end has closed (``EIO``) or the master fd
                is invalid (``EBADF``) — propagated uncaught per Vision §12
                (the launcher's injection loop, 8b, surfaces it).
        """
        if not isinstance(message, str):
            raise TypeError(
                f"Adapter.inject: message must be str, "
                f"got {type(message).__name__}"
            )
        transformed = self.pre_inject(message)
        payload = transformed.encode("utf-8") + self.line_terminator
        await asyncio.to_thread(inject_to_pty, handle.master_fd, payload)

    async def teardown(
        self, handle: PTYHandle, *, timeout: float = _DEFAULT_TEARDOWN_TIMEOUT_SECONDS
    ) -> None:
        """Terminate the spawned process tree and close its fds.

        Calls :meth:`pre_teardown` (no-op by default) then delegates to 5a's
        ``close_pty_handle``, which is idempotent (a second teardown of the
        same handle short-circuits inside ``close_pty_handle``; the
        ``pre_teardown`` hook still runs on every call — the idempotence
        guard lives below the hook).

        Args:
            handle: The handle returned by :meth:`spawn`.
            timeout: Seconds to wait for SIGTERM before escalating to
                SIGKILL. Default 5.0 (matches 5a).
        """
        self.pre_teardown(handle)
        await asyncio.to_thread(close_pty_handle, handle, timeout)

    # ── Lifecycle hooks — default no-ops; override per-adapter (Vision §5.4).
    def pre_spawn(self, args: list[str]) -> list[str]:
        """Transform the assembled argv before spawning. Returns it unchanged.

        The return value is the COMPLETE argv (including ``command``), not
        just ``extra_args`` — a subclass override could prepend a wrapper
        command or rearrange. Default is the identity transform.
        """
        return args

    def post_spawn(self, handle: PTYHandle) -> None:
        """Side-effect hook after a successful spawn. No-op by default."""

    def pre_inject(self, message: str) -> str:
        """Transform the notification text before encoding. Returns it unchanged.

        Operates on the ``str`` (encoding to UTF-8 happens in :meth:`inject`
        after this hook). A future adapter might escape characters its
        prompt-input loop misreads. Default is the identity transform.
        """
        return message

    def pre_teardown(self, handle: PTYHandle) -> None:
        """Side-effect hook before teardown. No-op by default."""


def register_adapter(adapter_cls: type[Adapter]) -> type[Adapter]:
    """Validate and register a concrete adapter class by its ``name``.

    Used as a decorator on concrete adapter classes (6a-c)::

        @register_adapter
        class ClaudeAdapter(Adapter):
            ...

    Validates the required class attributes at decoration time (so a
    misconfigured adapter fails at import, not at launch) and returns the
    class UNCHANGED (no wrapping) so ``isinstance(get_adapter(name), cls)``
    and type-checkers both work.

    Args:
        adapter_cls: The concrete :class:`Adapter` subclass to register.

    Returns:
        ``adapter_cls`` unchanged.

    Raises:
        AdapterConfigurationError: If ``name``/``command``/
            ``notification_template`` are not non-empty strings,
            ``default_args`` is not ``list[str]`` (may be empty), or
            ``line_terminator`` is not non-empty bytes.
        AdapterAlreadyRegistered: If ``name`` is already in the registry.
    """
    name = adapter_cls.name
    if not isinstance(name, str) or not name:
        raise AdapterConfigurationError(
            _config_error(adapter_cls, "name", "a non-empty str", name)
        )
    if not isinstance(adapter_cls.command, str) or not adapter_cls.command:
        raise AdapterConfigurationError(
            _config_error(adapter_cls, "command", "a non-empty str", adapter_cls.command)
        )
    default_args = adapter_cls.default_args
    if not isinstance(default_args, list) or not all(
        isinstance(arg, str) for arg in default_args
    ):
        raise AdapterConfigurationError(
            _config_error(adapter_cls, "default_args", "a list[str]", default_args)
        )
    template = adapter_cls.notification_template
    if not isinstance(template, str) or not template:
        raise AdapterConfigurationError(
            _config_error(
                adapter_cls, "notification_template", "a non-empty str", template
            )
        )
    terminator = adapter_cls.line_terminator
    if not isinstance(terminator, bytes) or not terminator:
        raise AdapterConfigurationError(
            _config_error(adapter_cls, "line_terminator", "non-empty bytes", terminator)
        )

    if name in _REGISTRY:
        existing = _REGISTRY[name]
        raise AdapterAlreadyRegistered(
            f"Adapter name {name!r} is already registered to "
            f"{existing.__name__}; cannot also register {adapter_cls.__name__}."
        )
    _REGISTRY[name] = adapter_cls
    return adapter_cls


def get_adapter(name: str) -> Adapter:
    """Look up a registered adapter class by name and return a fresh instance.

    Args:
        name: The adapter name (the registry key — the same lowercase
            harness string the launcher resolves from the CLI, e.g.
            ``"claude"``).

    Returns:
        A newly constructed instance of the registered class. Each call
        returns a DISTINCT instance (adapters are stateless — class attrs
        only — so per-call construction is microsecond-cheap; K5).

    Raises:
        TypeError: If ``name`` is not a ``str``.
        KeyError: If no adapter is registered under ``name``. The message
            names the unknown name and lists the registered names
            (alphabetical) per Framework P3.
    """
    if not isinstance(name, str):
        raise TypeError(
            f"get_adapter: name must be str, got {type(name).__name__}"
        )
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown adapter: {name!r}. Registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]()
