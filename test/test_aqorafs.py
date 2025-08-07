import pytest

from aqora_cli.pyarrow import dataset

testdata = [
    ("alice/test", "v1.0.0", (100_000, 8)),
    ("alice/test2", "v1.0.0", (10_000_000, 8)),
    ("alice/test2", "v1.1.0", (10_000_000, 8)),
    ("alice/test2", "v1.2.0", (10_000_000, 8)),
]


@pytest.mark.parametrize("slug,version,shape", testdata)
def test_polars(slug: str, version: str, shape: tuple[int, int]):
    import polars as pl

    df = pl.scan_pyarrow_dataset(dataset(slug, version)).collect()
    assert df.shape == shape


@pytest.mark.parametrize("slug,version,_shape", testdata)
def test_polars_select_head(slug: str, version: str, _shape: tuple[int, int]):
    import polars as pl

    df = (
        pl.scan_pyarrow_dataset(dataset(slug, version))
        .select(pl.col("email"))
        .head(100)
        .collect()
    )
    assert df.shape == (100, 1)


@pytest.mark.parametrize("slug,version,shape", testdata)
def test_pandas(slug: str, version: str, shape: tuple[int, int]):
    import pandas as pd

    df = pd.read_parquet(f"aqora://{slug}/{version}")
    assert df.shape == shape
