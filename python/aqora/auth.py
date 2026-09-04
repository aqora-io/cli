import html
from typing import Sequence

from ._aqora import Client


async def viewer_login(
    scope: Sequence[str] | None = None,
    *,
    client: Client | None = None,
    timeout: float = 600.0,
    label: str = "Sign in with aqora",
) -> Client:
    """Ask the person viewing this notebook to grant it access to their aqora account.

    Renders a sign-in link in the current cell output, waits until they approve,
    and returns a client authenticated as *them*. The default ``Client()`` inside a
    workspace runner acts as the workspace owner; use the returned client for
    anything done on the viewer's behalf. Only works inside an aqora workspace runner.
    """
    import marimo as mo  # imported lazily; aqora does not depend on marimo

    client = client or Client()
    auth = await client.authorize_viewer(scope)
    mo.output.append(
        mo.Html(
            f'<a href="{html.escape(auth.url, quote=True)}"'
            ' target="_blank" rel="noopener noreferrer">'
            f"{html.escape(label)}</a>"
        )
    )
    viewer = await auth.wait(timeout=timeout)
    mo.output.clear()
    return viewer
