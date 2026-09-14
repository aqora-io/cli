"""`aqora.viewer_login` (real module, faked marimo and client)."""

from __future__ import annotations

import dataclasses
import sys
import types

import pytest

import aqora
from aqora.auth import _covers, viewer_login

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


class FakeRequest:
    def __init__(self, query_params: dict[str, object]) -> None:
        self.query_params = query_params


class FakeAppMeta:
    def __init__(self) -> None:
        self.request: FakeRequest | None = None


@dataclasses.dataclass
class FakeContext:
    """marimo's per-session runtime context.

    Like the real ``KernelRuntimeContext`` it is a dataclass: unhashable, and
    equal to every other context, so only identity can tell two sessions apart.
    """


def _no_context():
    # marimo raises ContextNotInitializedError outside a session
    raise RuntimeError("context not initialized")


def start_session() -> FakeContext:
    """Put a fresh marimo session behind ``get_context``."""
    context = FakeContext()
    sys.modules["marimo._runtime.context"].get_context = lambda: context
    return context


@pytest.fixture
def marimo():
    module = types.ModuleType("marimo")
    module.output = FakeOutput()
    module.Html = FakeHtml
    meta = FakeAppMeta()
    module.app_meta = lambda: meta
    context = types.ModuleType("marimo._runtime.context")
    context.get_context = _no_context  # no session until a test starts one
    saved = {name: sys.modules.get(name) for name in ("marimo", context.__name__)}
    sys.modules["marimo"] = module
    sys.modules[context.__name__] = context
    try:
        yield module
    finally:
        for name, previous in saved.items():
            if previous is None:
                del sys.modules[name]
            else:
                sys.modules[name] = previous


class FakeViewer:
    def __init__(self, granted_scopes: list[str]) -> None:
        self.granted_scopes = granted_scopes


VIEWER_CLIENT = FakeViewer(["default"])


class FakeAuthorization:
    def __init__(self, url: str, viewer: FakeViewer) -> None:
        self.url = url
        self.client_id = "workspace-1"
        self.viewer = viewer
        self.timeouts: list[float | None] = []

    async def wait(self, *, timeout: float | None = None) -> FakeViewer:
        self.timeouts.append(timeout)
        return self.viewer


class FakeClient:
    def __init__(
        self, url: str = AUTHORIZE_URL, *, viewer: FakeViewer = VIEWER_CLIENT
    ) -> None:
        self.authorization = FakeAuthorization(url, viewer)
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


@pytest.mark.parametrize(
    ("granted", "requested", "covered"),
    [
        (["default"], None, True),
        (["default", "read:workspace"], ["default"], True),
        (["default"], ["default", "read:workspace"], False),
        (["read:workspace"], None, False),
    ],
)
def test_covers_requires_every_requested_scope(granted, requested, covered):
    assert _covers(granted, requested) is covered


@pytest.mark.asyncio
async def test_reuses_the_grant_for_the_rest_of_the_session(marimo):
    client = FakeClient()
    start_session()

    first = await viewer_login(client=client)
    second = await viewer_login(client=client)

    assert second is first
    assert client.scopes == [None]
    assert len(marimo.output.appended) == 1
    assert marimo.output.cleared == 1


@pytest.mark.asyncio
async def test_reuses_the_grant_on_a_rerun_without_a_session_param(marimo):
    client = FakeClient()
    start_session()
    marimo.app_meta().request = FakeRequest(
        {"session_id": ["s_dwbe3f"], "file": ["app.py"]}
    )

    first = await viewer_login(client=client)
    # a re-run still has a request, but session_id only ever rides on the request
    # that opened the session
    marimo.app_meta().request = FakeRequest({"file": ["app.py"]})
    second = await viewer_login(client=client)

    assert second is first
    assert client.scopes == [None]
    assert len(marimo.output.appended) == 1


@pytest.mark.asyncio
async def test_asks_again_for_a_scope_the_grant_does_not_cover(marimo):
    client = FakeClient()
    start_session()

    await viewer_login(client=client)
    await viewer_login(["default", "read:workspace"], client=client)

    assert client.scopes == [None, ["default", "read:workspace"]]
    assert len(marimo.output.appended) == 2


@pytest.mark.asyncio
async def test_does_not_share_a_grant_between_sessions(marimo):
    client = FakeClient()
    other_client = FakeClient(viewer=FakeViewer(["default"]))

    start_session()
    viewer = await viewer_login(client=client)
    start_session()
    other_viewer = await viewer_login(client=other_client)

    assert other_viewer is not viewer
    assert other_client.scopes == [None]
    assert len(marimo.output.appended) == 2


@pytest.mark.asyncio
async def test_does_not_cache_without_a_session_context(marimo):
    client = FakeClient()

    with pytest.raises(RuntimeError):
        sys.modules["marimo._runtime.context"].get_context()

    await viewer_login(client=client)
    await viewer_login(client=client)

    assert client.scopes == [None, None]
    assert len(marimo.output.appended) == 2
