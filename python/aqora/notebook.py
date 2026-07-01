from __future__ import annotations

import asyncio
import importlib.util
import site
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Coroutine

from ._aqora import Client

def _parse_workspace_slug(workspace: str) -> tuple[str, str]:
    owner, slash, slug = workspace.strip().partition("/")
    owner = owner.lstrip("@")
    if not slash or not owner or not slug or "/" in slug:
        raise ValueError(
            "Malformed workspace slug. Expected format: 'owner/slug' (e.g. 'julian/my-workspace')"
        )
    return owner, slug


def _notebook_cache_dir() -> Path:
    package_dir = Path(__file__).resolve().parent
    candidates = [
        package_dir / "_workspace_notebooks",
        Path(site.getusersitepackages()) / "aqora" / "_workspace_notebooks",
    ]
    for site_package in site.getsitepackages():
        candidates.append(Path(site_package) / "aqora" / "_workspace_notebooks")

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe = candidate / ".aqora_write_probe"
            with probe.open("wb"):
                pass
            probe.unlink()
            return candidate
        except OSError:
            continue

    raise PermissionError("Could not find a writable site-packages cache directory")


def _module_name(owner: str, slug: str, filename: str, version: str | None = None) -> str:
    safe_owner = "".join(c if c.isalnum() or c == "_" else "_" for c in owner)
    safe_slug = "".join(c if c.isalnum() or c == "_" else "_" for c in slug)
    safe_file = "".join(c if c.isalnum() or c == "_" else "_" for c in Path(filename).stem)
    name = f"aqora_workspace_{safe_owner}_{safe_slug}_{safe_file}"
    if version is not None:
        safe_version = "".join(c if c.isalnum() or c == "_" else "_" for c in version)
        name = f"{name}_v{safe_version}"
    return name


def _path_part(value: str) -> str:
    safe_value = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in value)
    return safe_value or "_"


async def _notebook_async(
    workspace: str,
    *,
    filename: str | None = None,
    version: str | None = None,
    aqora_url: str | None = None,
    aqora_auth: bool = True,
    force_download: bool = False,
) -> ModuleType:
    owner, slug = _parse_workspace_slug(workspace)
    dest_dir = _notebook_cache_dir() / _path_part(owner) / _path_part(slug)
    if version is not None:
        dest_dir = dest_dir / _path_part(version)

    if filename and not force_download:
        notebook_path = dest_dir / filename
        if notebook_path.exists():
            return _load_module(owner, slug, filename, notebook_path, version)

    client = Client(aqora_url)
    if aqora_auth and not client.authenticated:
        await client.authenticate()
    actual_filename = await client._download_workspace_notebook(
        owner, slug, str(dest_dir), filename, version, force_download
    )
    notebook_path = dest_dir / actual_filename
    return _load_module(owner, slug, actual_filename, notebook_path, version)


def _load_module(
    owner: str, slug: str, filename: str, path: Path, version: str | None = None
) -> ModuleType:
    module_name = _module_name(owner, slug, filename, version)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec for '{path}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load(
    workspace: str,
    *,
    filename: str | None = None,
    version: str | None = None,
    aqora_url: str | None = None,
    aqora_auth: bool = True,
    force_download: bool = False,
) -> ModuleType | Coroutine[Any, Any, ModuleType]:
    coro = _notebook_async(
        workspace,
        filename=filename,
        version=version,
        aqora_url=aqora_url,
        aqora_auth=aqora_auth,
        force_download=force_download,
    )
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return coro
