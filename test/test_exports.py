"""Top-level `aqora` exports (real import, no faked modules)."""

from __future__ import annotations


def test_provider_job_exported_from_top_level():
    import aqora
    from aqora._provider.jobs import ProviderJob
    from aqora._provider.results import ProviderResult

    assert aqora.ProviderJob is ProviderJob
    assert aqora.ProviderResult is ProviderResult
    assert callable(aqora.ProviderJob.from_id)
