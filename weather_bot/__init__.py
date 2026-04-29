"""Weather bot platform package."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .bootstrap import WeatherApplication

__all__ = ["WeatherApplication", "get_application"]


def get_application():
    """Lazily import the bootstrap layer for lightweight subcommands."""

    from .bootstrap import get_application as _get_application

    return _get_application()


def __getattr__(name: str) -> Any:
    if name == "WeatherApplication":
        from .bootstrap import WeatherApplication as _WeatherApplication

        return _WeatherApplication
    if name == "get_application":
        return get_application
    try:
        module = importlib.import_module(f"{__name__}.{name}")
    except ModuleNotFoundError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    globals()[name] = module
    return module
