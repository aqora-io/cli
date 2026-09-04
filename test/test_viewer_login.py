"""`aqora.viewer_login` (real module, faked marimo and client)."""

from __future__ import annotations

import sys
import types

import pytest

import aqora
from aqora.auth import viewer_login

AUTHORIZE_URL = (
    "https://aqora.io/oauth2/authorize"
    "?client_id=workspace-1&state=st4te&redirect_uri=https%3A%2F%2Faqora.io%2Foauth2%2Fsub%2Fkey"
)


class FakeOutput:
    def __init__(self) -> None:
        self.appended: list[object] = []
        self.cleared = 0

    def append(self, value: object) -> None:
        self.appended.append(value)

    def clear(self) -> None:
        self.cleared += 1


class FakeHtml:
    def __init__(self, text: str) -> None:
        self.text = text


@pytest.fixture
def marimo():
    module = types.ModuleType("marimo")
    module.output = FakeOutput()
    module.Html = FakeHtml
    saved = sys.modules.get("marimo")
    sys.modules["marimo"] = module
    try:
        yield module
    finally:
        if saved is None:
            del sys.modules["marimo"]
        else:
            sys.modules["marimo"] = saved


VIEWER_CLIENT = object()


class FakeAuthorization:
    def __init__(self, url: str) -> None:
        self.url = url
        self.client_id = "workspace-1"
        self.timeouts: list[float | None] = []

    async def wait(self, *, timeout: float | None = None) -> object:
        self.timeouts.append(timeout)
        return VIEWER_CLIENT


class FakeClient:
    def __init__(self, url: str = AUTHORIZE_URL) -> None:
        self.authorization = FakeAuthorization(url)
        self.scopes: list[object] = []

    async def authorize_viewer(self, scope=None) -> FakeAuthorization:
        self.scopes.append(scope)
        return self.authorization


def only_html(marimo: types.ModuleType) -> str:
    assert len(marimo.output.appended) == 1
    html = marimo.output.appended[0]
    assert isinstance(html, FakeHtml)
    return html.text


def test_viewer_login_is_exported():
    assert aqora.viewer_login is viewer_login


@pytest.mark.asyncio
async def test_renders_a_link_that_opens_a_new_tab(marimo):
    await viewer_login(client=FakeClient())

    html = only_html(marimo)
    assert 'target="_blank"' in html
    assert 'rel="noopener noreferrer"' in html
    assert "Sign in with aqora" in html


@pytest.mark.asyncio
async def test_escapes_the_url_and_the_label(marimo):
    await viewer_login(client=FakeClient(), label='Sign in <b>"now"</b>')

    html = only_html(marimo)
    assert AUTHORIZE_URL not in html
    assert AUTHORIZE_URL.replace("&", "&amp;") in html
    assert "Sign in &lt;b&gt;&quot;now&quot;&lt;/b&gt;" in html


@pytest.mark.asyncio
async def test_returns_the_viewer_client_and_clears_the_link(marimo):
    client = FakeClient()

    viewer = await viewer_login(client=client)

    assert viewer is VIEWER_CLIENT
    assert marimo.output.cleared == 1


@pytest.mark.asyncio
async def test_forwards_the_scope_and_the_timeout(marimo):
    client = FakeClient()

    await viewer_login(["viewer", "submissions"], client=client, timeout=1.5)

    assert client.scopes == [["viewer", "submissions"]]
    assert client.authorization.timeouts == [1.5]


@pytest.mark.asyncio
async def test_defaults_to_a_ten_minute_timeout(marimo):
    client = FakeClient()

    await viewer_login(client=client)

    assert client.scopes == [None]
    assert client.authorization.timeouts == [600.0]
