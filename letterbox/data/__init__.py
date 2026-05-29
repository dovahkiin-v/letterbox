"""Packaged data resources for letterbox.

This package marker exists so ``importlib.resources.files("letterbox.data")``
resolves to a real Traversable under setuptools' ``packages.find`` discovery.
Without the marker, the directory is excluded from the wheel and the bundled
``sample_letterbox.toml`` becomes unreadable post-install.

Tier: N/A (packaging marker, not a logic module).
"""
