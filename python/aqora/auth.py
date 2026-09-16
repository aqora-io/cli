import html
import warnings
from typing import Any, Sequence

from ._aqora import Client

# The grant a viewer made is remembered on marimo's runtime context, under this
# attribute, so that re-running a cell does not ask the same person again. It is
# stored on the context rather than in a module of our own because marimo installs
# a context per session: the grant then lives and dies with the session and cannot
# be reached from another one, even if marimo ever ran several sessions in one
# process. Anything process-wide would hand one viewer's token to another.
_GRANT_ATTR = "_aqora_viewer_grant"


def _covers(granted: Sequence[str] | None, requested: Sequence[str] | None) -> bool:
    return set(requested or ["default"]) <= set(granted or [])


def _session_context() -> Any:
    """marimo's runtime state for this session, or ``None`` if out of reach.

    ``get_context`` is a marimo internal, so the import and the call are both
    guarded; no context simply means no caching.
    """
    try:
        from marimo._runtime.context import get_context

        return get_context()
    except Exception:
        return None


async def viewer_login(
    scope: Sequence[str] | None = None,
    *,
    client: Client | None = None,
    timeout: float = 600.0,
    label: str = "Sign in with aqora",
) -> Client:
    """Ask the person viewing this notebook to grant it access to their aqora account.

    Renders a sign-in link in the current cell output, waits until they approve,
    and returns a client authenticated as *them*. The grant is reused for the rest
    of their session, so re-running a cell does not ask again; a fresh visit does,
    as does a grant that ended because they revoked the app or left it unused for
    longer than aqora allows.
    The default ``Client()`` inside a workspace runner acts as the workspace owner;
    use the returned client for anything done on the viewer's behalf.

    Outside a workspace runner, in a notebook on your own machine, there is no
    viewer to ask: the client returned acts as whoever ran ``aqora login``, with
    that account's full access rather than ``scope``. That is meant for
    development; the consent flow only runs on aqora.
    """
    import marimo as mo  # imported lazily; aqora does not depend on marimo

    session = _session_context()
    if session is not None:
        cached, granted = getattr(session, _GRANT_ATTR, (None, None))
        if cached is not None and _covers(granted, scope):
            if await cached.viewer_grant_active():
                return cached
            # revoked, or idle past aqora's limit: they have to consent again

    client = client or Client()
    auth = await client.authorize_viewer(scope)
    if auth is None:
        return await _local_login(client)
    mo.output.append(
        mo.Html(
            f'<a href="{html.escape(auth.url, quote=True)}"'
            ' target="_blank" rel="noopener noreferrer">'
            f"{html.escape(label)}</a>"
        )
    )
    viewer = await auth.wait(timeout=timeout)
    mo.output.clear()
    if session is not None:
        try:
            setattr(session, _GRANT_ATTR, (viewer, viewer.granted_scopes))
        except Exception:
            pass  # a context that refuses attributes just means no caching
    return viewer


async def _local_login(client: Client) -> Client:
    """The developer's own ``aqora login``, standing in for a viewer."""
    # `aqora/__init__` imports this module before it defines ClientError
    from . import ClientError

    if not client.authenticated:
        await client.authenticate()
    try:
        data = await client.send("query { viewer { username } }")
    except ClientError as error:
        codes = {
            (graphql_error.get("extensions") or {}).get("code")
            for graphql_error in error.graphql_errors or []
        }
        if "NOT_AUTHORIZED" not in codes:
            raise
        raise ClientError(
            "viewer_login: not inside an aqora workspace runner and no aqora"
            " credentials found; run `aqora login` first"
        ) from error
    warnings.warn(
        "viewer_login: not inside an aqora workspace runner; acting as"
        f" {data['viewer']['username']} from `aqora login` with that account's"
        " full access",
        stacklevel=3,
    )
    return client
