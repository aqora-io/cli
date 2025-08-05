# pyright: reportAny=false
import asyncio
import pytest

from aqora_cli import Client, ClientError


@pytest.mark.asyncio
async def test_unauthenticated():
    c = Client()
    assert not c.authenticated

    def is_not_authorized(error: ClientError) -> bool:
        if not error.graphql_errors:
            return False
        exts = error.graphql_errors[0].get("extensions")
        if not exts:
            return False
        return exts.get("code") == "NOT_AUTHORIZED"

    with pytest.raises(ClientError, check=is_not_authorized):
        await c.send("""
        {
            viewer {
                id
            }
        }
        """)


@pytest.mark.asyncio
async def test_authenticated():
    c = Client()
    await c.authenticate()
    assert c.authenticated
    data = await c.send("""
    {
        viewer {
            id
            username
        }
    }
    """)
    assert data["viewer"]["id"]
    assert data["viewer"]["username"]


@pytest.mark.asyncio
async def test_s3_get():
    c = Client()
    await c.authenticate()

    dataset = await c.send("""
    {
        datasetBySlug(owner: "alice", localSlug: "test2") {
            latestVersion {
                files {
                    nodes {
                        url
                    }
                }
            }
        }
    }
    """)
    urls = [
        file["url"]
        for file in dataset["datasetBySlug"]["latestVersion"]["files"]["nodes"]
    ]
    files = await asyncio.gather(*(c.s3_get(url, range=(0, 0)) for url in urls))
    assert files
    assert all(files)
