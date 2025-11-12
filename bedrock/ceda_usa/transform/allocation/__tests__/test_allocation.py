from __future__ import annotations

import logging

from bedrock.ceda_usa.transform.allocation.constants import EmissionsSource
from bedrock.ceda_usa.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY

logger = logging.getLogger(__name__)


def test_all_sources_allocated() -> None:
    assert set(ALLOCATED_EMISSIONS_REGISTRY.keys()) == set(EmissionsSource)
