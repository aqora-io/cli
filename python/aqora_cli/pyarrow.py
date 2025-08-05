# pyright: reportUnknownVariableType=false, reportMissingTypeStubs=false

from asyncio import AbstractEventLoop
from pyarrow._fs import PyFileSystem
from pyarrow.dataset import dataset as pyarrow_dataset, Dataset
from pyarrow.fs import FSSpecHandler


from aqora_cli.fsspec import AqoraFileSystem


def dataset(
    slug: str,
    version: str,
    *,
    aqora_url: str | None = None,
    aqora_auth: bool = True,
    loop: AbstractEventLoop | None = None,
) -> Dataset:
    return pyarrow_dataset(
        f"{slug}/{version}",
        filesystem=PyFileSystem(
            FSSpecHandler(
                AqoraFileSystem(
                    asynchronous=False,
                    loop=loop,
                    aqora_url=aqora_url,
                    aqora_auth=aqora_auth,
                )
            )  # pyright: ignore[reportAbstractUsage]
        ),
        format="parquet",
    )


__all__ = ["dataset"]
