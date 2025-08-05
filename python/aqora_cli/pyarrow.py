# pyright: reportUnknownVariableType=false, reportMissingTypeStubs=false

from pyarrow.dataset import dataset as pyarrow_dataset, Dataset

from aqora_cli.fsspec import AqoraFileSystem


def dataset(
    slug: str, version: str, *, aqora_url: str | None = None, aqora_auth: bool = True
) -> Dataset:
    from pyarrow._fs import PyFileSystem
    from pyarrow.fs import FSSpecHandler

    return pyarrow_dataset(
        f"{slug}/{version}",
        filesystem=PyFileSystem(
            FSSpecHandler(AqoraFileSystem(aqora_url=aqora_url, aqora_auth=aqora_auth))  # pyright: ignore[reportAbstractUsage]
        ),
        format="parquet",
    )
