"""Your aqora S3 storage: credentials minted on demand and re-minted before they expire."""

from __future__ import annotations

import datetime as dt
import functools
from typing import Any

from ._aqora import StoreCredentials, _Store
from ._provider.client import _run_sync

__all__ = ["Store", "StoreCredentials"]


class Store(_Store):
    """Your aqora S3 bucket.

    Credentials are minted from the client's own login, grant or runner key,
    cached, and minted again once fewer than ``refresh_margin`` seconds remain,
    so every client returned here keeps working for as long as the notebook or
    script runs. Inside a workspace runner no login is needed; elsewhere the
    client falls back to ``aqora login``.

    Writes need the ``write:storage`` scope on viewer and API-key sessions.
    """

    def credentials(self, *, force: bool = False) -> StoreCredentials:
        """Current credentials, minting new ones when needed.

        Safe under a running event loop too (for example inside marimo),
        though it blocks that loop while minting; ``credentials_async``
        awaits instead.
        """
        return _run_sync(lambda: self.credentials_async(force=force))

    # --- obstore -----------------------------------------------------------

    def obstore(self, *, prefix: str | None = None, **config: Any) -> Any:
        """An ``obstore.store.S3Store`` for your bucket that re-mints on expiry."""
        try:
            from obstore.store import S3Store
        except ImportError as exc:
            raise ImportError(
                "Store.obstore() requires obstore. Install `aqora[obstore]` to use it."
            ) from exc
        creds = self.credentials()
        # obstore refreshes a credential once fewer than `refresh_threshold`
        # seconds remain (300 by default), read off the provider callable; it
        # is aligned with the cache's margin so both agree on when to re-mint.
        provider = functools.partial(self._obstore_credential)
        provider.refresh_threshold = dt.timedelta(seconds=self.refresh_margin)
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
        creds = self.credentials()
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
        creds = self.credentials()
        return boto3.Session(botocore_session=session).client(
            "s3",
            endpoint_url=creds.endpoint,
            region_name=creds.region,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
            **client_kwargs,
        )

    def _botocore_metadata(self, *, force: bool = False) -> dict[str, Any]:
        return self._as_botocore_metadata(self.credentials(force=force))

    async def _botocore_metadata_async(self, *, force: bool = False) -> dict[str, Any]:
        return self._as_botocore_metadata(await self.credentials_async(force=force))

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
        creds = self.credentials()
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
        con.execute(self.credentials().duckdb_sql(name))
