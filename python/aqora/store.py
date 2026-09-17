"""Your aqora S3 storage: credentials minted on demand and re-minted before they expire."""

from __future__ import annotations

import datetime as dt
import functools
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping

from ._aqora import Client
from ._provider.client import _run_sync

DEFAULT_REFRESH_MARGIN_SEC = 60.0


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


@dataclass(frozen=True)
class StoreCredentials:
    """SigV4 credentials for your bucket on the aqora object store.

    Use path-style addressing: objects live at ``{endpoint}/{bucket}/{key}``.
    """

    access_key_id: str
    secret_access_key: str
    expires_at: dt.datetime
    endpoint: str
    bucket: str
    region: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StoreCredentials:
        raw = data["expires_at"]
        if isinstance(raw, dt.datetime):
            expires_at = raw
        else:
            expires_at = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=dt.timezone.utc)
        return cls(
            access_key_id=data["access_key_id"],
            secret_access_key=data["secret_access_key"],
            expires_at=expires_at.astimezone(dt.timezone.utc),
            endpoint=data["endpoint"],
            bucket=data["bucket"],
            region=data["region"],
        )

    @property
    def use_ssl(self) -> bool:
        return not self.endpoint.startswith("http://")

    @property
    def host(self) -> str:
        """``host[:port]`` of the endpoint, the form DuckDB's ``ENDPOINT`` takes."""
        return urllib.parse.urlsplit(self.endpoint).netloc

    def url(self, key: str = "") -> str:
        return f"{self.endpoint}/{self.bucket}/{key}"

    def remaining(self, now: dt.datetime | None = None) -> float:
        """Seconds until the credentials expire; negative once they have."""
        return (self.expires_at - (now or _utcnow())).total_seconds()


class Store:
    """Your aqora S3 bucket.

    Credentials are minted from the client's own login, grant or runner key,
    cached, and minted again once fewer than ``refresh_margin`` seconds remain,
    so every client returned here keeps working for as long as the notebook or
    script runs. Inside a workspace runner no login is needed; elsewhere the
    client falls back to ``aqora login``.

    Writes need the ``write:storage`` scope on viewer and API-key sessions.
    """

    def __init__(
        self,
        client: Client | None = None,
        *,
        duration: int | None = None,
        refresh_margin: float = DEFAULT_REFRESH_MARGIN_SEC,
        url: str | None = None,
        allow_insecure_host: bool | None = None,
    ) -> None:
        if client is not None and (url is not None or allow_insecure_host is not None):
            raise ValueError(
                "`url` and `allow_insecure_host` cannot be combined with an explicit `client`"
            )
        if duration is not None and duration < 60:
            raise ValueError("`duration` must be at least 60 seconds")
        self._client = client or Client(url, allow_insecure_host=allow_insecure_host)
        self._duration = duration
        self._refresh_margin = float(refresh_margin)
        self._cached: StoreCredentials | None = None
        self._lock = threading.Lock()

    def credentials(self, *, force: bool = False) -> StoreCredentials:
        """Current credentials, minting new ones when needed.

        Safe under a running event loop too (for example inside marimo),
        though it blocks that loop while minting; ``credentials_async``
        awaits instead.
        """
        return self._credentials_sync(force=force)

    async def credentials_async(self, *, force: bool = False) -> StoreCredentials:
        """``credentials`` for code already running on an event loop."""
        return await self._credentials_async(force=force)

    def _fresh(self, force: bool) -> StoreCredentials | None:
        if force:
            return None
        cached = self._cached
        if cached is not None and cached.remaining() > self._refresh_margin:
            return cached
        return None

    async def _credentials_async(self, *, force: bool = False) -> StoreCredentials:
        cached = self._fresh(force)
        if cached is not None:
            return cached
        if not self._client.authenticated:
            await self._client.authenticate()
        creds = StoreCredentials.from_dict(
            await self._client.store_credentials(self._duration)
        )
        self._cached = creds
        return creds

    def _credentials_sync(self, *, force: bool = False) -> StoreCredentials:
        # Runs on the shared background loop, so this is safe from a plain
        # script, from marimo's kernel thread, and from the worker threads
        # obstore and botocore call providers on. Never call it from a
        # coroutine running on that background loop itself. The lock makes
        # threads that miss the cache together mint once, not once each.
        with self._lock:
            cached = self._fresh(force)
            if cached is not None:
                return cached
            return _run_sync(lambda: self._credentials_async(force=force))

    # --- obstore -----------------------------------------------------------

    def obstore(self, *, prefix: str | None = None, **config: Any) -> Any:
        """An ``obstore.store.S3Store`` for your bucket that re-mints on expiry."""
        try:
            from obstore.store import S3Store
        except ImportError as exc:
            raise ImportError(
                "Store.obstore() requires obstore. Install `aqora[obstore]` to use it."
            ) from exc
        creds = self._credentials_sync()
        # obstore refreshes a credential once fewer than `refresh_threshold`
        # seconds remain (300 by default), read off the provider callable; it
        # is aligned with the cache's margin so both agree on when to re-mint.
        provider = functools.partial(self._obstore_credential)
        provider.refresh_threshold = dt.timedelta(seconds=self._refresh_margin)
        return S3Store(
            creds.bucket,
            prefix=prefix,
            credential_provider=provider,
            endpoint=creds.endpoint,
            region=creds.region,
            virtual_hosted_style_request=False,
            client_options={"allow_http": not creds.use_ssl},
            **config,
        )

    def _obstore_credential(self) -> dict[str, Any]:
        creds = self._credentials_sync()
        return {
            "access_key_id": creds.access_key_id,
            "secret_access_key": creds.secret_access_key,
            "token": None,
            "expires_at": creds.expires_at,
        }

    # --- boto3 / botocore --------------------------------------------------

    def boto3(self, **client_kwargs: Any) -> Any:
        """A boto3 S3 client for your bucket that re-mints on expiry.

        botocore asks for fresh credentials when under fifteen minutes
        remain; the store's cache decides whether that is a new mint.
        """
        try:
            import boto3
            import botocore.session
            from botocore.config import Config
            from botocore.credentials import CredentialProvider, RefreshableCredentials
        except ImportError as exc:
            raise ImportError(
                "Store.boto3() requires boto3. Install `aqora[boto3]` to use it."
            ) from exc
        store = self

        class _Provider(CredentialProvider):
            METHOD = "aqora-store"
            CANONICAL_NAME = "aqora-store"

            def load(self):
                return RefreshableCredentials.create_from_metadata(
                    metadata=store._botocore_metadata(),
                    refresh_using=store._botocore_metadata,
                    method=self.METHOD,
                )

        session = botocore.session.get_session()
        session.get_component("credential_provider").insert_before("env", _Provider())
        creds = self._credentials_sync()
        return boto3.Session(botocore_session=session).client(
            "s3",
            endpoint_url=creds.endpoint,
            region_name=creds.region,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
            **client_kwargs,
        )

    def _botocore_metadata(self, *, force: bool = False) -> dict[str, Any]:
        return self._as_botocore_metadata(self._credentials_sync(force=force))

    async def _botocore_metadata_async(self, *, force: bool = False) -> dict[str, Any]:
        return self._as_botocore_metadata(await self._credentials_async(force=force))

    @staticmethod
    def _as_botocore_metadata(creds: StoreCredentials) -> dict[str, Any]:
        return {
            "access_key": creds.access_key_id,
            "secret_key": creds.secret_access_key,
            "token": None,
            "expiry_time": creds.expires_at.isoformat(),
        }

    # --- s3fs --------------------------------------------------------------

    def s3fs(self, **kwargs: Any) -> Any:
        """An ``s3fs.S3FileSystem`` for paths like ``{bucket}/data.parquet``."""
        try:
            import s3fs
            from aiobotocore.credentials import AioRefreshableCredentials
            from aiobotocore.session import AioSession
            from botocore.credentials import CredentialProvider
        except ImportError as exc:
            raise ImportError(
                "Store.s3fs() requires s3fs. Install `aqora[s3fs]` to use it."
            ) from exc
        store = self

        # aiobotocore providers subclass botocore's and make `load` async.
        class _Provider(CredentialProvider):
            METHOD = "aqora-store"
            CANONICAL_NAME = "aqora-store"

            async def load(self):
                return AioRefreshableCredentials.create_from_metadata(
                    metadata=await store._botocore_metadata_async(),
                    refresh_using=store._botocore_metadata_async,
                    method=self.METHOD,
                )

        session = AioSession()
        session.get_component("credential_provider").insert_before("env", _Provider())
        creds = self._credentials_sync()
        return s3fs.S3FileSystem(
            session=session,
            client_kwargs={"endpoint_url": creds.endpoint, "region_name": creds.region},
            config_kwargs={"s3": {"addressing_style": "path"}, "signature_version": "s3v4"},
            use_ssl=creds.use_ssl,
            skip_instance_cache=True,
            **kwargs,
        )

    # --- duckdb ------------------------------------------------------------

    def duckdb(self, con: Any, *, name: str = "aqora") -> None:
        """Register an S3 secret for your bucket on a DuckDB connection.

        DuckDB secrets are static: call this again once the credentials
        expire, or pass a longer ``duration`` to the store.
        """
        if not name.isidentifier():
            raise ValueError(f"`name` must be a plain identifier, got {name!r}")
        con.execute(self._duckdb_sql(self._credentials_sync(), name))

    @staticmethod
    def _duckdb_sql(creds: StoreCredentials, name: str) -> str:
        def quote(value: str) -> str:
            return value.replace("'", "''")

        return (
            f"CREATE OR REPLACE SECRET {name} (\n"
            f"    TYPE s3,\n"
            f"    KEY_ID '{quote(creds.access_key_id)}',\n"
            f"    SECRET '{quote(creds.secret_access_key)}',\n"
            f"    ENDPOINT '{quote(creds.host)}',\n"
            f"    REGION '{quote(creds.region)}',\n"
            f"    URL_STYLE 'path',\n"
            f"    USE_SSL {'true' if creds.use_ssl else 'false'},\n"
            f"    SCOPE 's3://{quote(creds.bucket)}/'\n"
            f");"
        )
