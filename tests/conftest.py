"""Offline tests never launch a real transfer executable."""

import pytest
from tdr_fakes import install_tdr


@pytest.fixture(autouse=True)
def tdr_calls(monkeypatch):
    return install_tdr(monkeypatch)
