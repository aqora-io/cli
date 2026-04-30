import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    print(os.environ["AQORA_URL"])
    print(os.environ["AQORA_ALLOW_INSECURE_HOST"])
    return


@app.cell
def _():
    from aqora_cli.iceberg import AqoraCatalog

    return (AqoraCatalog,)


@app.cell
def _(AqoraCatalog):
    catalog = AqoraCatalog("julian/mixed-a")
    return (catalog,)


@app.cell
def _(catalog):
    # `Table.to_polars()` returns a polars LazyFrame whose plan embeds
    # `aqora://...` parquet URIs. Polars resolves those through its own
    # object_store, not via fsspec, so the lazy path can't find our custom
    # scheme. Use `scan().to_polars()` to materialize eagerly through
    # PyIceberg's FsspecFileIO (which dispatches `aqora://` correctly).
    data = catalog.load_table("default.bitcoin").scan().to_polars()
    return (data,)


@app.cell
def _(data, mo):
    _df = mo.sql(
        f"""
        select * from data
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
