# pyright: reportMissingTypeStubs=false

import os
import shutil
import subprocess
import threading
import time
from typing_extensions import Any, override

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.rest.auth import AUTH_MANAGER, AuthManager
from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.typedef import Properties


DEFAULT_AQORA_URL = "https://aqora.io"
DEFAULT_TOKEN_TTL_SEC = 5 * 60.0
DEFAULT_FETCH_TIMEOUT_SEC = 30.0


def _resolve_aqora_url(explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    return os.environ.get("AQORA_URL", DEFAULT_AQORA_URL)


def _split_path(path: str) -> tuple[str, str]:
    parts = path.strip("/").split("/")
    if len(parts) != 2 or not all(parts):
        raise ValueError(
            f"Expected `<owner>/<slug>` data catalog identifier, got: {path!r}"
        )
    return parts[0], parts[1]


class AqoraAuthManager(AuthManager):
    """Auth manager that obtains aqora access tokens via `aqora auth token`.

    The CLI handles credential storage and refresh internally; this manager
    simply re-invokes it after `ttl_seconds` to pick up rotated tokens.
    """

    def __init__(
        self,
        aqora_url: str | None = None,
        ttl_seconds: float = DEFAULT_TOKEN_TTL_SEC,
        timeout_seconds: float = DEFAULT_FETCH_TIMEOUT_SEC,
        executable: str | None = None,
    ) -> None:
        self._aqora_url = aqora_url
        self._ttl_seconds = float(ttl_seconds)
        self._timeout_seconds = float(timeout_seconds)
        self._executable = executable or shutil.which("aqora") or "aqora"
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _fetch_token(self) -> str:
        cmd: list[str] = [self._executable]
        if self._aqora_url is not None:
            cmd.extend(["--url", self._aqora_url])
        cmd.extend(["auth", "token"])
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=self._timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"Could not run {self._executable!r}; install the aqora CLI to use AqoraCatalog"
            ) from exc
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            raise RuntimeError(
                f"`aqora auth token` failed: {detail}" if detail else "`aqora auth token` failed"
            ) from exc

        token = result.stdout.strip()
        if not token:
            raise RuntimeError("`aqora auth token` returned an empty token")
        return token

    @override
    def auth_header(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at:
                self._token = self._fetch_token()
                self._expires_at = time.monotonic() + self._ttl_seconds
            return f"Bearer {self._token}"


def _build_aqora_fs(properties: Properties) -> Any:
    from .fsspec import AqoraFileSystem

    return AqoraFileSystem(aqora_url=properties.get("aqora_url"))


def _build_aqora_iceberg_fs(properties: Properties) -> Any:
    from .iceberg_fs import AqoraIcebergFileSystem

    raw_auth = properties.get(AUTH_MANAGER)
    auth_manager = raw_auth if isinstance(raw_auth, AqoraAuthManager) else None
    return AqoraIcebergFileSystem(
        aqora_url=properties.get("aqora_url"),
        auth_manager=auth_manager,
    )


class AqoraFsspecFileIO(FsspecFileIO):
    """`FsspecFileIO` that recognises the `aqora://` and `aqora-iceberg://` schemes.

    PyIceberg's stock `FsspecFileIO` keeps its own `SCHEME_TO_FS` dict and never
    consults fsspec's global registry, so registering the schemes via
    `fsspec.register_implementation` alone is not enough — we need to inject our
    builders into this instance's dict.
    """

    def __init__(self, properties: Properties) -> None:
        super().__init__(properties)
        self._scheme_to_fs["aqora"] = _build_aqora_fs
        self._scheme_to_fs["aqora-iceberg"] = _build_aqora_iceberg_fs


class AqoraCatalog(RestCatalog):
    """Iceberg REST catalog for an aqora data catalog identified by `<owner>/<slug>`.

    Authenticates via `AqoraAuthManager`, which delegates to the `aqora auth token`
    CLI subcommand for token retrieval and refresh. Reads manifest and data files
    through `AqoraFsspecFileIO`, which routes `aqora-iceberg://` (manifest metadata)
    and `aqora://` (parquet partitions) through the matching fsspec implementations.
    """

    def __init__(
        self,
        path: str,
        *,
        name: str | None = None,
        aqora_url: str | None = None,
        **properties: Any,
    ) -> None:
        owner, slug = _split_path(path)
        url = _resolve_aqora_url(aqora_url).rstrip("/")
        prefix = f"{owner}/{slug}"
        merged: dict[str, Any] = {
            "uri": f"{url}/data-catalog",
            "prefix": prefix,
            "py-io-impl": "aqora_cli.iceberg.AqoraFsspecFileIO",
            "aqora_url": url,
            "auth": {
                "type": "custom",
                "impl": "aqora_cli.iceberg.AqoraAuthManager",
                "custom": {"aqora_url": url},
            },
        }
        merged.update(properties)
        super().__init__(name or f"aqora:{prefix}", **merged)

    @override
    def url(self, endpoint: str, prefixed: bool = True, **kwargs: Any) -> str:
        # The aqora data-catalog REST API is per-catalog: every endpoint —
        # including `/config` — lives under `/v1/{prefix}/...`, so always
        # apply the prefix when building URLs.
        return super().url(endpoint, prefixed=True, **kwargs)


__all__ = ["AqoraCatalog", "AqoraAuthManager", "AqoraFsspecFileIO"]
