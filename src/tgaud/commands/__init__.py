"""Report commands of the `tgaud` CLI. Each module here registers itself with tgaud.cli.register when imported;
`tgaud.cli` imports every module of this package. Filled by the cli task (overlap, unique, chats-per-user,
user, campaign, link create, probe, forget-user, ...; design.md 10).
"""

from __future__ import annotations

import importlib
import pkgutil


def load_all() -> list[str]:
    """Import every command module of this package; returns their names."""
    names = []
    for info in pkgutil.iter_modules(__path__, __name__ + "."):
        importlib.import_module(info.name)
        names.append(info.name)
    return names
