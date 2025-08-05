import pytest

testdata = [
    ("aqora://alice/test/v1.0.0", (100_000, 8)),
    ("aqora://alice/test2/v1.0.0", (10_000_000, 8)),
    ("aqora://alice/test2/v1.1.0", (10_000_000, 8)),
]


@pytest.mark.parametrize("url,shape", testdata)
def test_polars(url: str, shape: tuple[int, int]):
    from pyarrow._fs import PyFileSystem
    from pyarrow.fs import FSSpecHandler
    import pyarrow.dataset as ds
    import fsspec as fs
    import polars as pl

    handle = fs.open(url)
    assert isinstance(handle, fs.core.OpenFile)
    alice_test = ds.dataset(
        handle.path, filesystem=PyFileSystem(FSSpecHandler(handle.fs)), format="parquet"
    )
    df = pl.scan_pyarrow_dataset(alice_test).collect()
    assert df.shape == shape


@pytest.mark.parametrize("url,_shape", testdata)
def test_polars_select_head(url: str, _shape: tuple[int, int]):
    from pyarrow._fs import PyFileSystem
    from pyarrow.fs import FSSpecHandler
    import pyarrow.dataset as ds
    import fsspec as fs
    import polars as pl

    handle = fs.open(url)
    assert isinstance(handle, fs.core.OpenFile)
    alice_test = ds.dataset(
        handle.path, filesystem=PyFileSystem(FSSpecHandler(handle.fs)), format="parquet"
    )
    df = pl.scan_pyarrow_dataset(alice_test).select(pl.col("email")).head(100).collect()
    assert df.shape == (100, 1)


@pytest.mark.parametrize("url,shape", testdata)
def test_pandas(url: str, shape: tuple[int, int]):
    import pandas as pd

    df = pd.read_parquet(url)
    assert df.shape == shape
