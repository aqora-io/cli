"""`aqora.KV`: the Python layer over the native store, exercised offline."""

from __future__ import annotations

import pytest

import aqora
from aqora import Client, ClientError
from aqora.kv import KV, _open
from aqora.store import Store


def offline_store() -> Store:
    """A store whose credential mint fails fast: nothing listens on port 1."""
    return Store(Client("http://127.0.0.1:1", allow_insecure_host=True))


def test_kv_is_exported():
    assert aqora.KV is KV


def test_constructor_validation():
    with pytest.raises(ValueError):
        KV("", offline_store())
    with pytest.raises(ValueError):
        KV("/absolute.json", offline_store())
    with pytest.raises(ValueError):
        KV("item.json", offline_store(), stale_after=-1)
    with pytest.raises(ValueError):
        KV("a/../b.json", offline_store())
    kv = KV("path/to/item.json", offline_store())
    assert isinstance(kv, KV)
    assert kv in _open


def test_set_takes_json_values_and_rejects_the_rest():
    kv = KV("item.json", offline_store())
    kv.set("a", {"nested": [1, 2.5, "s", None, True]})
    kv.delete("a")
    with pytest.raises(TypeError):
        kv.set("b", object())


def test_reads_surface_transport_errors():
    kv = KV("item.json", offline_store())
    with pytest.raises(ClientError):
        kv.get("a")
    with pytest.raises(ClientError):
        kv.list()


@pytest.mark.asyncio
async def test_async_reads_surface_transport_errors():
    kv = KV("item.json", offline_store())
    with pytest.raises(ClientError):
        await kv.get_async("a")


def test_flush_can_be_bounded_while_writes_are_retried():
    # The flusher keeps retrying with backoff in the background; giving up on
    # a batch is covered by the Rust tests, here only the bounded wait is.
    kv = KV("item.json", offline_store())
    kv.set("a", 1)
    with pytest.raises(TimeoutError):
        kv.flush(timeout=0.5)


def test_context_manager_closes(monkeypatch):
    closed: list[float | None] = []
    monkeypatch.setattr(KV, "close", lambda self, *, timeout=None: closed.append(timeout))
    with KV("item.json", offline_store()) as kv:
        assert isinstance(kv, KV)
    assert closed == [None]


def test_context_manager_does_not_mask_the_body_exception(monkeypatch):
    def failing_close(self, *, timeout=None):
        raise RuntimeError("close failed")

    monkeypatch.setattr(KV, "close", failing_close)
    with pytest.raises(ValueError, match="body"):
        with pytest.warns(RuntimeWarning, match="closing failed"):
            with KV("item.json", offline_store()):
                raise ValueError("body")
    with pytest.raises(RuntimeError, match="close failed"):
        with KV("item.json", offline_store()):
            pass


def test_a_failed_close_keeps_the_kv_registered_for_the_exit_flush():
    kv = KV("item.json", offline_store())
    kv.set("a", 1)
    with pytest.raises(TimeoutError):
        kv.close(timeout=0.5)
    assert kv in _open
