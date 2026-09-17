"""`aqora.Store` and `aqora.StoreCredentials`, exercised against a fake client."""

from __future__ import annotations

import datetime as dt

import pytest

import aqora
from aqora import store as store_module
from aqora.store import Store, StoreCredentials

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


class FakeClient:
    """Mints credentials that last an hour from the injected clock."""

    def __init__(self, *, authenticated: bool = True) -> None:
        self.authenticated = authenticated
        self.authenticate_calls = 0
        self.mints: list[int | None] = []

    async def authenticate(self) -> None:
        self.authenticate_calls += 1
        self.authenticated = True

    async def store_credentials(self, duration=None):
        self.mints.append(duration)
        lifetime = duration if duration is not None else 3600
        expires_at = store_module._utcnow() + dt.timedelta(seconds=lifetime)
        return {
            "access_key_id": f"AQS1key{len(self.mints)}",
            "secret_access_key": "se cret'",
            "expires_at": expires_at.isoformat(),
            "endpoint": "http://100.123.32.73:3001",
            "bucket": "alice",
            "region": "aqora",
        }


@pytest.fixture
def clock(monkeypatch):
    now = {"value": T0}
    monkeypatch.setattr(store_module, "_utcnow", lambda: now["value"])
    return now


def test_store_is_exported():
    assert aqora.Store is Store
    assert aqora.StoreCredentials is StoreCredentials


def test_from_dict_parses_offsets_and_z():
    base = {
        "access_key_id": "k",
        "secret_access_key": "s",
        "endpoint": "https://s3.aqora.io",
        "bucket": "alice",
        "region": "aqora",
    }
    offset = StoreCredentials.from_dict({**base, "expires_at": "2026-01-02T03:04:05+00:00"})
    zulu = StoreCredentials.from_dict({**base, "expires_at": "2026-01-02T03:04:05Z"})
    assert offset.expires_at == zulu.expires_at == T0
    assert offset.expires_at.tzinfo is not None


def test_credential_helpers():
    creds = StoreCredentials("k", "s", T0, "http://100.123.32.73:3001", "alice", "aqora")
    assert creds.host == "100.123.32.73:3001"
    assert not creds.use_ssl
    assert creds.url("a/b") == "http://100.123.32.73:3001/alice/a/b"
    assert creds.remaining(T0 - dt.timedelta(seconds=30)) == 30
    secure = StoreCredentials("k", "s", T0, "https://s3.aqora.io", "alice", "aqora")
    assert secure.host == "s3.aqora.io"
    assert secure.use_ssl


def test_client_and_url_are_exclusive():
    with pytest.raises(ValueError):
        Store(FakeClient(), url="https://example.com")
    with pytest.raises(ValueError):
        Store(FakeClient(), duration=10)


def test_sync_call_outside_a_loop_authenticates_once(clock):
    client = FakeClient(authenticated=False)
    store = Store(client, duration=120)
    creds = store.credentials()
    assert isinstance(creds, StoreCredentials)
    assert creds.access_key_id == "AQS1key1"
    assert client.authenticate_calls == 1
    assert client.mints == [120]
    # A second call inside the lifetime is served from the cache.
    assert store.credentials() is creds
    assert client.mints == [120]


def test_already_authenticated_client_is_not_authenticated_again(clock):
    client = FakeClient(authenticated=True)
    Store(client).credentials()
    assert client.authenticate_calls == 0
    assert client.mints == [None]


@pytest.mark.asyncio
async def test_sync_call_works_under_a_running_loop(clock):
    store = Store(FakeClient())
    creds = store.credentials()
    assert isinstance(creds, StoreCredentials)
    assert await store.credentials_async() is creds


def test_cache_re_mints_inside_the_refresh_margin_and_on_force(clock):
    client = FakeClient()
    store = Store(client, refresh_margin=60)
    first = store.credentials()
    clock["value"] = T0 + dt.timedelta(seconds=3600 - 61)
    assert store.credentials() is first
    clock["value"] = T0 + dt.timedelta(seconds=3600 - 59)
    second = store.credentials()
    assert second is not first
    assert len(client.mints) == 2
    assert store.credentials(force=True) is not second
    assert len(client.mints) == 3


def test_obstore_credential_shape(clock):
    store = Store(FakeClient(), refresh_margin=60)
    cred = store._obstore_credential()
    assert cred["access_key_id"] == "AQS1key1"
    assert cred["token"] is None
    assert cred["expires_at"] == T0 + dt.timedelta(seconds=3600)


def test_botocore_metadata_shape(clock):
    store = Store(FakeClient())
    metadata = store._botocore_metadata()
    assert metadata["access_key"] == "AQS1key1"
    assert metadata["token"] is None
    assert metadata["expiry_time"] == (T0 + dt.timedelta(hours=1)).isoformat()
    assert store._botocore_metadata(force=True)["access_key"] == "AQS1key2"


@pytest.mark.asyncio
async def test_botocore_metadata_async_uses_the_running_loop(clock):
    store = Store(FakeClient())
    metadata = await store._botocore_metadata_async()
    assert metadata["secret_key"] == "se cret'"


def test_duckdb_sql_matches_the_cli_output():
    creds = StoreCredentials("k", "se cret'", T0, "http://100.123.32.73:3001", "alice", "aqora")
    assert Store._duckdb_sql(creds, "aqora") == (
        "CREATE OR REPLACE SECRET aqora (\n"
        "    TYPE s3,\n"
        "    KEY_ID 'k',\n"
        "    SECRET 'se cret''',\n"
        "    ENDPOINT '100.123.32.73:3001',\n"
        "    REGION 'aqora',\n"
        "    URL_STYLE 'path',\n"
        "    USE_SSL false,\n"
        "    SCOPE 's3://alice/'\n"
        ");"
    )


def test_duckdb_executes_the_secret(clock):
    executed: list[str] = []

    class Con:
        def execute(self, sql: str) -> None:
            executed.append(sql)

    Store(FakeClient()).duckdb(Con(), name="mine")
    assert executed and executed[0].startswith("CREATE OR REPLACE SECRET mine (")
    with pytest.raises(ValueError):
        Store(FakeClient()).duckdb(Con(), name="my-secret")


def test_missing_optional_dependency_names_the_extra(clock, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("obstore"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match=r"aqora\[obstore\]"):
        Store(FakeClient()).obstore()


def test_obstore_provider_aligns_its_refresh_threshold_with_the_cache(clock, monkeypatch):
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
    Store(FakeClient(), refresh_margin=90).obstore()
    provider = captured["credential_provider"]
    assert provider.refresh_threshold == dt.timedelta(seconds=90)
    assert provider()["expires_at"] == T0 + dt.timedelta(hours=1)
    assert captured["bucket"] == "alice"
    assert captured["virtual_hosted_style_request"] is False
