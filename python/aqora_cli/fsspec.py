# pyright: reportMissingTypeStubs=false

from asyncio import AbstractEventLoop
from dataclasses import dataclass, replace as dc_replace

from fsspec.asyn import AsyncFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK as FSSPEC_DEFAULT_CALLBACK
from typing_extensions import Any, Unpack, TypedDict, override

from ._aqora_cli import Client


@dataclass(frozen=True)
class AqoraSemver:
    major: int
    minor: int
    patch: int

    @override
    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}.{self.patch}"

    @classmethod
    def from_str(cls, semver: str):
        if semver.startswith("v"):
            semver = semver[1:]
        major, minor, patch = semver.split(".", 3)
        return cls(major=int(major), minor=int(minor), patch=int(patch))


@dataclass(frozen=True)
class AqoraPath:
    owner: str
    slug: str
    version: AqoraSemver
    partition_num: int | None

    def with_partition_num(self, partition_num: int):
        return dc_replace(self, partition_num=partition_num)

    @property
    def rest(self) -> list[str]:
        if self.partition_num is not None:
            return [str(self.partition_num)]
        return []

    @override
    def __str__(self) -> str:
        return "/".join([self.owner, self.slug, str(self.version), *self.rest])

    @classmethod
    def from_str(cls, path: str):
        path_elems = path.split("/")
        if len(path_elems) < 3:
            raise ValueError(
                "Malformed `aqora://<owner>/<dataset>/<version>` dataset uri"
            )

        owner, slug, version, *rest = path_elems
        if rest:
            partition_num = int(rest[0])
        else:
            partition_num = None
        return cls(
            owner=owner,
            slug=slug,
            version=AqoraSemver.from_str(version),
            partition_num=partition_num,
        )


class _Init(TypedDict):
    pass


class _Info(TypedDict):
    pass


class _Ls(TypedDict):
    pass


class _Cat(TypedDict):
    pass


class AqoraFileSystem(AsyncFileSystem):
    _client: Client
    _need_auth: bool

    def __init__(
        self,
        *args: Any,
        asynchronous: bool = False,
        loop: AbstractEventLoop | None = None,
        batch_size: int | None = None,
        aqora_url: str | None = None,
        aqora_auth: bool = True,
        **kwargs: Unpack[_Init],
    ):
        super().__init__(
            *args,
            asynchronous=asynchronous,
            loop=loop,
            batch_size=batch_size,
            **kwargs,
        )
        self._client = Client(aqora_url)
        self._need_auth = aqora_auth

    async def _authenticate(self):
        if self._need_auth and not self._client.authenticated:
            await self._client.authenticate()

    @override
    async def _info(self, path: str, **kwargs: Unpack[_Info]):
        await self._authenticate()

        # print(f"aqora_info: {path=} {kwargs=}")
        try:
            target = AqoraPath.from_str(path)
        except ValueError as error:
            raise FileNotFoundError from error

        if target.partition_num is not None:
            response = await self._client.send(
                """
                query GetDatasetId($owner: String!, $localSlug: String!, $major: Int!, $minor: Int!, $patch: Int!, $partitionNum: Int!) {
                    datasetBySlug(owner: $owner, localSlug: $localSlug) {
                        id
                        version(major: $major, minor: $minor, patch: $patch) {
                            id
                            fileByPartitionNum(partitionNum: $partitionNum) {
                                id
                                size
                            }
                        }
                    }
                }
                """,
                owner=target.owner,
                localSlug=target.slug,
                major=target.version.major,
                minor=target.version.minor,
                patch=target.version.patch,
                partitionNum=target.partition_num,
            )
            dataset = response["datasetBySlug"]
            if not dataset:
                raise FileNotFoundError
            dataset_version = dataset["version"]
            if not dataset_version:
                raise FileNotFoundError
            file = dataset_version["fileByPartitionNum"]
            if not file:
                raise FileNotFoundError
            return {
                "name": str(target),
                "size": file["size"],
                "type": "file",
                "dataset_id": dataset["id"],
                "dataset_version_id": dataset_version["id"],
                "dataset_version_file_id": file["id"],
            }
        else:
            response = await self._client.send(
                """
                query GetDatasetId($owner: String!, $localSlug: String!, $major: Int!, $minor: Int!, $patch: Int!) {
                    datasetBySlug(owner: $owner, localSlug: $localSlug) {
                        id
                        version(major: $major, minor: $minor, patch: $patch) {
                            id
                        }
                    }
                }
                """,
                owner=target.owner,
                localSlug=target.slug,
                major=target.version.major,
                minor=target.version.minor,
                patch=target.version.patch,
            )
            dataset = response["datasetBySlug"]
            if not dataset:
                raise FileNotFoundError
            dataset_version = dataset["version"]
            if not dataset_version:
                raise FileNotFoundError
            return {
                "name": str(target),
                "size": 0,
                "type": "directory",
                "dataset_id": dataset["id"],
                "dataset_version_id": dataset_version["id"],
            }

    @override
    async def _ls(self, path: str, detail: bool = True, **kwargs: Unpack[_Ls]):
        await self._authenticate()

        # print(f"aqora_ls: {path=} {detail=} {kwargs=}")
        target = AqoraPath.from_str(path)
        if target.partition_num is not None:
            raise IOError

        response = await self._client.send(
            """
            query ListDatasetFiles($owner: String!, $localSlug: String!, $major: Int!, $minor: Int!, $patch: Int!) {
                datasetBySlug(owner: $owner, localSlug: $localSlug) {
                    version(major: $major, minor: $minor, patch: $patch) {
                        files {
                            nodes {
                                partitionNum
                                url
                                size
                            }
                        }
                    }
                }
            }
            """,
            owner=target.owner,
            localSlug=target.slug,
            major=target.version.major,
            minor=target.version.minor,
            patch=target.version.patch,
        )
        dataset = response["datasetBySlug"]
        if not dataset:
            raise FileNotFoundError
        dataset_version = dataset["version"]
        if not dataset_version:
            raise FileNotFoundError
        files = dataset_version["files"]["nodes"]

        if detail:
            return [
                {
                    "name": str(target.with_partition_num(file["partitionNum"])),
                    "size": file["size"],
                    "type": "file",
                    "url": file["url"],
                }
                for file in files
            ]
        else:
            return [
                str(target.with_partition_num(file["partitionNum"])) for file in files
            ]

    @override
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detail: bool = True,
        **kwargs: Unpack[_Ls],
    ):
        await self._authenticate()
        listing = await self._ls(path, detail, **kwargs)
        if detail:
            return {entry["name"]: entry for entry in listing}  # pyright: ignore[reportArgumentType]
        return listing

    @override
    async def _isfile(self, path: str) -> bool:
        # print(f"aqora_isfile: {path=}")
        target = AqoraPath.from_str(path)
        return target.partition_num is not None

    @override
    async def _isdir(self, path: str) -> bool:
        # print(f"aqora_isdir: {path=}")
        target = AqoraPath.from_str(path)
        return target.partition_num is None

    @override
    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Unpack[_Cat],
    ) -> bytes:
        await self._authenticate()

        # print(f"aqora_cat_file: {path=} {start=} {end=} {kwargs=}")
        target = AqoraPath.from_str(path)
        if target.partition_num is None:
            raise IOError

        response = await self._client.send(
            """
            query ListDatasetFiles($owner: String!, $localSlug: String!, $major: Int!, $minor: Int!, $patch: Int!, $partitionNum: Int!) {
                datasetBySlug(owner: $owner, localSlug: $localSlug) {
                    version(major: $major, minor: $minor, patch: $patch) {
                        fileByPartitionNum(partitionNum: $partitionNum) {
                            url
                        }
                    }
                }
            }
            """,
            owner=target.owner,
            localSlug=target.slug,
            major=target.version.major,
            minor=target.version.minor,
            patch=target.version.patch,
            partitionNum=target.partition_num,
        )
        dataset = response["datasetBySlug"]
        if not dataset:
            raise FileNotFoundError
        dataset_version = dataset["version"]
        if not dataset_version:
            raise FileNotFoundError
        file = dataset_version["fileByPartitionNum"]
        if not file:
            raise FileNotFoundError
        return await self._client.s3_get(
            file["url"],
            range=(start, end),
        )

    @override
    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        raise NotImplementedError

    @override
    async def _mv_file(self, path1, path2):
        raise NotImplementedError

    @override
    async def _copy(
        self,
        path1,
        path2,
        recursive=False,
        on_error=None,
        maxdepth=None,
        batch_size=None,
        **kwargs,
    ):
        raise NotImplementedError

    @override
    async def _pipe(self, path, value=None, batch_size=None, **kwargs):
        raise NotImplementedError

    @override
    async def _put(
        self,
        lpath,
        rpath,
        recursive=False,
        callback=FSSPEC_DEFAULT_CALLBACK,
        batch_size=None,
        maxdepth=None,
        **kwargs,
    ):
        raise NotImplementedError


__all__ = ["AqoraFileSystem"]
