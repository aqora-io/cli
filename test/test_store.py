"""`aqora.Store` and `aqora.StoreCredentials`, exercised without a server."""

from __future__ import annotations

import datetime as dt

import pytest

import aqora
from aqora import Client
from aqora.store import Store, StoreCredentials

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def creds(**overrides) -> StoreCredentials:
    fields = dict(
        access_key_id="AQS1key1",
        secret_access_key="se cret'",
        expires_at=T0 + dt.timedelta(hours=1),
        endpoint="http://100.123.32.73:3001",
        bucket="alice",
        region="aqora",
    )
    return StoreCredentials(**{**fields, **overrides})


def offline_client() -> Client:
    return Client("http://127.0.0.1:1", allow_insecure_host=True)


@pytest.fixture
def store(monkeypatch) -> Store:
    """A store whose credentials come from a counter rather than the server."""
    mints: list[bool] = []

    async def credentials_async(self, *, force=False):
        mints.append(force)
        return creds(access_key_id=f"AQS1key{len(mints)}")

    monkeypatch.setattr(Store, "credentials_async", credentials_async)
    store = Store(offline_client(), refresh_margin=60)
    store.mints = mints
    return store


def test_store_is_exported():
    assert aqora.Store is Store
    assert aqora.StoreCredentials is StoreCredentials


def test_credential_helpers():
    c = creds(expires_at=T0)
    assert c.host == "100.123.32.73:3001"
    assert not c.use_ssl
    assert c.url("a/b") == "http://100.123.32.73:3001/alice/a/b"
    assert c.url("a b") == "http://100.123.32.73:3001/alice/a%20b"
    assert c.remaining(T0 - dt.timedelta(seconds=30)) == 30
    assert c.expires_at == T0
    assert c.expires_at.tzinfo is not None
    secure = creds(endpoint="https://s3.aqora.io")
    assert secure.host == "s3.aqora.io"
    assert secure.use_ssl


def test_constructor_validation():
    with pytest.raises(ValueError):
        Store(offline_client(), url="https://example.com")
    with pytest.raises(ValueError):
        Store(offline_client(), duration=10)
    store = Store(offline_client(), duration=120, refresh_margin=30)
    assert store.duration == 120
    assert store.refresh_margin == 30
    assert isinstance(store.client, Client)
    assert isinstance(Store(url="https://example.com"), Store)


def test_sync_credentials_bridge_the_native_coroutine(store):
    first = store.credentials()
    assert isinstance(first, StoreCredentials)
    assert first.access_key_id == "AQS1key1"
    assert store.credentials(force=True).access_key_id == "AQS1key2"
    assert store.mints == [False, True]


@pytest.mark.asyncio
async def test_sync_call_works_under_a_running_loop(store):
    assert store.credentials().access_key_id == "AQS1key1"
    assert (await store.credentials_async()).access_key_id == "AQS1key2"


def test_obstore_credential_shape(store):
    cred = store._obstore_credential()
    assert cred["access_key_id"] == "AQS1key1"
    assert cred["token"] is None
    assert cred["expires_at"] == T0 + dt.timedelta(hours=1)


def test_botocore_metadata_shape(store):
    metadata = store._botocore_metadata()
    assert metadata["access_key"] == "AQS1key1"
    assert metadata["token"] is None
    assert metadata["expiry_time"] == (T0 + dt.timedelta(hours=1)).isoformat()
    assert store._botocore_metadata(force=True)["access_key"] == "AQS1key2"


@pytest.mark.asyncio
async def test_botocore_metadata_async_uses_the_running_loop(store):
    metadata = await store._botocore_metadata_async()
    assert metadata["secret_key"] == "se cret'"


def test_duckdb_sql_matches_the_cli_output():
    assert creds().duckdb_sql("aqora") == (
        "CREATE OR REPLACE SECRET aqora (\n"
        "    TYPE s3,\n"
        "    KEY_ID 'AQS1key1',\n"
        "    SECRET 'se cret''',\n"
        "    ENDPOINT '100.123.32.73:3001',\n"
        "    REGION 'aqora',\n"
        "    URL_STYLE 'path',\n"
        "    USE_SSL false,\n"
        "    SCOPE 's3://alice/'\n"
        ");"
    )


def test_duckdb_executes_the_secret(store):
    executed: list[str] = []

    class Con:
        def execute(self, sql: str) -> None:
            executed.append(sql)

    store.duckdb(Con(), name="mine")
    assert executed and executed[0].startswith("CREATE OR REPLACE SECRET mine (")
    with pytest.raises(ValueError):
        store.duckdb(Con(), name="my-secret")


def test_missing_optional_dependency_names_the_extra(store, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("obstore"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match=r"aqora\[obstore\]"):
        store.obstore()


def test_obstore_provider_aligns_its_refresh_threshold_with_the_cache(monkeypatch):
    import sys
    import types

    captured: dict = {}

    class S3Store:
        def __init__(self, bucket, **kwargs):
            captured.update(kwargs, bucket=bucket)

    fake = types.ModuleType("obstore.store")
    fake.S3Store = S3Store
    monkeypatch.setitem(sys.modules, "obstore", types.ModuleType("obstore"))
    monkeypatch.setitem(sys.modules, "obstore.store", fake)

    async def credentials_async(self, *, force=False):
        return creds()

    monkeypatch.setattr(Store, "credentials_async", credentials_async)
    Store(offline_client(), refresh_margin=90).obstore()
    provider = captured["credential_provider"]
    assert provider.refresh_threshold == dt.timedelta(seconds=90)
    assert provider()["expires_at"] == T0 + dt.timedelta(hours=1)
    assert captured["bucket"] == "alice"
    assert captured["virtual_hosted_style_request"] is False
