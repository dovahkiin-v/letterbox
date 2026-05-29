"""Adapter package — concrete harness adapters live here.

Adapter discovery uses the registry in ``letterbox.adapters.base`` (Phase 5b), not
package-level imports. Importing ``letterbox.adapters`` (the package) has **no side
effect** and pulls in no concrete adapter modules. Registration happens only when
:func:`load_builtin_adapters` is *called* (Phase 8a), which performs the concrete
imports inside its body — preserving the no-side-effect-on-import property the
adapter modules rely on (ADR-039).
"""
from __future__ import annotations

__all__ = ["load_builtin_adapters"]


def load_builtin_adapters() -> None:
    """Import the built-in concrete adapters for their registration side-effect.

    Each concrete adapter module (``claude``, ``gemini``, ``antigravity``)
    registers itself with the ``letterbox.adapters.base`` registry via the
    ``@register_adapter`` decorator at import time. This function performs those
    imports *inside its body* — never at module level — so that importing the
    ``letterbox.adapters`` package stays free of side effects (ADR-039). The
    launcher calls it once during setup so ``get_adapter("claude" | "gemini" |
    "antigravity")`` resolves.

    Idempotent: the import cache makes repeated calls a no-op (each
    ``@register_adapter`` runs once per process, on the first import of its
    module). Tests that reset the registry must register their own adapter — see
    the Phase 8a Gotchas: a reset followed by ``load_builtin_adapters()`` will NOT
    re-register, because the modules are already cached.

    Returns:
        None.
    """
    # Imported for the @register_adapter side-effect only; the names are
    # intentionally unused after import.
    from letterbox.adapters import antigravity, claude, gemini  # noqa: F401
