# pyright: reportMissingTypeStubs=false

from typing_extensions import Any, override

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.http import HTTPFileSystem, get_client

from .iceberg import AqoraAuthManager, _resolve_aqora_url


REDIRECT_STATUSES = {301, 302, 303, 307, 308}


class AqoraIcebergFileSystem(AsyncFileSystem):
    """fsspec impl for `aqora-iceberg://{owner}/{slug}/{dataset_id}/{filename}`.

    The data-catalog REST service emits these URIs as `manifest_list` /
    `manifest_path` inside Iceberg metadata. We translate them to
    `{aqora_url}/data-catalog/v1/{owner}/{slug}/iceberg-meta/{dataset_id}/{filename}`,
    send a Bearer-authenticated request, and *manually* follow the redirect so
    we can drop the Authorization header before hitting the S3 presigned URL.
    Forwarding it triggers `SignatureDoesNotMatch` on S3/MinIO.
    """

    protocol = "aqora-iceberg"

    def __init__(
        self,
        *args: Any,
        aqora_url: str | None = None,
        auth_manager: AqoraAuthManager | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._aqora_url = _resolve_aqora_url(aqora_url).rstrip("/")
        self._auth = auth_manager or AqoraAuthManager(self._aqora_url)
        self._http = HTTPFileSystem(asynchronous=self.asynchronous, loop=self._loop)

    def _to_rest_url(self, path: str) -> str:
        parts = path.lstrip("/").split("/")
        if len(parts) != 4:
            raise ValueError(
                f"Expected aqora-iceberg path `<owner>/<slug>/<dataset-id>/<filename>`, got: {path!r}"
            )
        owner, slug, dataset_id, filename = parts
        return f"{self._aqora_url}/data-catalog/v1/{owner}/{slug}/iceberg-meta/{dataset_id}/{filename}"

    @staticmethod
    def _range_header(start: int | None, end: int | None) -> str | None:
        if start is None and end is None:
            return None
        lo = start or 0
        hi = "" if end is None else str(end - 1)
        return f"bytes={lo}-{hi}"

    async def _resolve_signed_url(self, url: str) -> str:
        """Hit our REST endpoint with Bearer, return the Location of the 302."""
        session = await get_client(**self._http.client_kwargs)
        async with session.get(
            url,
            headers={"Authorization": self._auth.auth_header()},
            allow_redirects=False,
        ) as resp:
            if resp.status in REDIRECT_STATUSES:
                location = resp.headers.get("Location")
                if not location:
                    raise IOError(f"redirect from {url} missing Location header")
                return location
            # Server didn't redirect — surface the body as-is.
            resp.raise_for_status()
            raise IOError(
                f"expected redirect from {url}, got HTTP {resp.status}"
            )

    @override
    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        signed_url = await self._resolve_signed_url(self._to_rest_url(path))

        s3_headers: dict[str, str] = {}
        range_header = self._range_header(start, end)
        if range_header:
            s3_headers["Range"] = range_header

        session = await get_client(**self._http.client_kwargs)
        async with session.get(signed_url, headers=s3_headers) as resp:
            resp.raise_for_status()
            return await resp.read()

    @override
    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        signed_url = await self._resolve_signed_url(self._to_rest_url(path))

        # Use a ranged GET rather than HEAD: the presigned URL is signed for
        # GetObject, so HEAD comes back as 403 SignatureDoesNotMatch on
        # MinIO/S3. `bytes=0-0` returns 206 with `Content-Range: bytes 0-0/SIZE`,
        # from which we parse the total size without downloading the body.
        session = await get_client(**self._http.client_kwargs)
        async with session.get(signed_url, headers={"Range": "bytes=0-0"}) as resp:
            resp.raise_for_status()
            if resp.status == 206:
                content_range = resp.headers.get("Content-Range", "")
                # Format: "bytes 0-0/12345"
                if "/" in content_range:
                    total = content_range.rsplit("/", 1)[1].strip()
                    if total != "*":
                        return {"name": path, "size": int(total), "type": "file"}
            # Server ignored the Range header (200) or returned a wildcard total —
            # fall back to Content-Length, which in that case covers the full body.
            size = int(resp.headers.get("Content-Length") or 0)
            return {"name": path, "size": size, "type": "file"}


__all__ = ["AqoraIcebergFileSystem"]
