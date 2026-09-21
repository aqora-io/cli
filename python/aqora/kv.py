"""A key-value store held in one JSON document on your aqora storage."""

from __future__ import annotations

import atexit
import warnings
import weakref
from typing import Any

from ._aqora import _KV
from ._provider.client import DEFAULT_SYNC_TIMEOUT, _run_sync
from .store import Store

__all__ = ["KV"]

KV_ATEXIT_TIMEOUT_SEC = 10.0

_open: weakref.WeakSet[KV] = weakref.WeakSet()


class KV(_KV):
    """A dictionary of JSON values stored as one object at ``path``.

    ``set`` and ``delete`` apply at once and are written in the background,
    retried on errors and rebased on whatever someone else wrote to the same
    document in the meantime. ``get`` and ``list`` see your own pending writes
    immediately and fetch the document again once the local copy is older
    than ``stale_after`` seconds.

    Pending writes are flushed on ``close``, on leaving a ``with`` block, and
    (best effort) at interpreter exit; call ``flush`` to wait for them and
    surface errors explicitly.
    """

    def __init__(
        self,
        path: str,
        store: Store | None = None,
        *,
        stale_after: float = 5.0,
    ) -> None:
        super().__init__()
        _open.add(self)

    def get(self, key: str, default: Any = None) -> Any:
        """The value under ``key``, or ``default`` when missing or ``null``."""
        value = _run_sync(lambda: self.get_async(key))
        return default if value is None else value

    def list(self) -> list[str]:
        """All keys, sorted."""
        return _run_sync(lambda: self.list_async())

    def flush(self, *, timeout: float | None = DEFAULT_SYNC_TIMEOUT) -> None:
        """Wait until every pending write is stored, raising if that fails."""
        _run_sync(lambda: self.flush_async(), timeout=timeout)

    def close(self, *, timeout: float | None = DEFAULT_SYNC_TIMEOUT) -> None:
        """Flush, then stop accepting writes."""
        _run_sync(lambda: self.close_async(), timeout=timeout)
        _open.discard(self)

    def __enter__(self) -> KV:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.close()
        except Exception as error:
            if exc_type is None:
                raise
            warnings.warn(f"aqora.KV: closing failed: {error}", RuntimeWarning, stacklevel=2)

    async def __aenter__(self) -> KV:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.close_async()
            _open.discard(self)
        except Exception as error:
            if exc_type is None:
                raise
            warnings.warn(f"aqora.KV: closing failed: {error}", RuntimeWarning, stacklevel=2)


def _flush_open_kvs() -> None:
    for kv in list(_open):
        try:
            kv.flush(timeout=KV_ATEXIT_TIMEOUT_SEC)
        except Exception as error:
            warnings.warn(
                f"aqora.KV: pending writes were not flushed at exit: {error}",
                RuntimeWarning,
                stacklevel=1,
            )


atexit.register(_flush_open_kvs)
