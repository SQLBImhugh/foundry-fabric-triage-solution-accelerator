from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from triage.monitoring.models import (
    CoverageGap,
    InventoryBatch,
    InventoryDomain,
    InventoryGeneration,
    InventoryWorkspace,
    MonitoringWorkDraft,
    RegistryVersion,
    ScopeDefinition,
    ScopePreviewRequest,
    ScopeSelector,
)

TENANT = "10000000-0000-4000-8000-000000000001"
EPOCH = "20000000-0000-4000-8000-000000000002"
GENERATION = "30000000-0000-4000-8000-000000000003"
WORKSPACE = "40000000-0000-4000-8000-000000000004"
DOMAIN = "50000000-0000-4000-8000-000000000005"
REQUEST = "60000000-0000-4000-8000-000000000006"
OTHER = "70000000-0000-4000-8000-000000000007"
NOW = datetime(2026, 9, 15, tzinfo=UTC)
CONTEXT = {"tenant_id": TENANT, "epoch": EPOCH}


def inventory(*, complete=True):
    return InventoryGeneration(
        **CONTEXT, generation_id=GENERATION,
        selector=ScopeSelector(tenant_id=TENANT, kind="tenant"),
        adapter="fixture", authority="fixture",
        completeness="complete" if complete else "partial",
        started_at=NOW, completed_at=NOW if complete else None,
        gaps=() if complete else (CoverageGap(code="partial", detail="A page is pending."),),
    )


def workspace(**changes):
    return InventoryWorkspace(**{
        **CONTEXT, "generation_id": GENERATION, "workspace_id": WORKSPACE,
        "name": "Synthetic workspace", "domain_id": DOMAIN, "observed_at": NOW,
        **changes,
    })


def domain(**changes):
    return InventoryDomain(**{
        **CONTEXT, "generation_id": GENERATION, "domain_id": DOMAIN,
        "name": "Synthetic domain", "observed_at": NOW, **changes,
    })


def test_named_containers_are_not_workload_items():
    batch = InventoryBatch(
        request_id=REQUEST, expected=RegistryVersion(**CONTEXT, revision=0),
        generation=inventory(), items=(), workspaces=(workspace(),), domains=(domain(),),
    )
    assert not batch.items
    assert batch.generation.discovered_count == 0
    assert batch.workspaces[0].name == "Synthetic workspace"
    assert batch.domains[0].name == "Synthetic domain"
    assert InventoryBatch.model_validate_json(batch.model_dump_json()) == batch


@pytest.mark.parametrize("changes", [
    {"tenant_id": OTHER}, {"epoch": OTHER}, {"generation_id": OTHER},
])
def test_container_metadata_must_match_scan_context(changes):
    with pytest.raises(ValidationError):
        InventoryBatch(
            request_id=REQUEST, expected=RegistryVersion(**CONTEXT, revision=0),
            generation=inventory(), items=(), workspaces=(workspace(**changes),),
        )


def test_partial_inventory_cannot_delete_containers():
    with pytest.raises(ValidationError, match="container deletion"):
        InventoryBatch(
            request_id=REQUEST, expected=RegistryVersion(**CONTEXT, revision=0),
            generation=inventory(complete=False), items=(),
            domains=(domain(state="deleted"),),
        )


def test_duplicate_container_ids_and_self_parent_are_invalid():
    with pytest.raises(ValidationError, match="own parent"):
        domain(parent_domain_id=DOMAIN)
    with pytest.raises(ValidationError, match="unique"):
        InventoryBatch(
            request_id=REQUEST, expected=RegistryVersion(**CONTEXT, revision=0),
            generation=inventory(), items=(), workspaces=(workspace(), workspace()),
        )


def test_explicit_discovery_can_be_queued_before_scope_activation():
    work = MonitoringWorkDraft(
        **CONTEXT, work_id=REQUEST, kind="inventory", policy_revision=0,
        due_at=NOW, created_at=NOW, reason="Discover a selected workspace.",
        discovery_selector=ScopeSelector(
            tenant_id=TENANT, kind="workspace", workspace_id=WORKSPACE,
        ),
    )
    assert work.scope_id is None
    assert work.discovery_selector.workspace_id == WORKSPACE


def test_discovery_selector_cannot_cross_tenants():
    with pytest.raises(ValidationError, match="tenant"):
        MonitoringWorkDraft(
            **CONTEXT, work_id=REQUEST, kind="inventory", policy_revision=0,
            due_at=NOW, created_at=NOW, reason="Invalid discovery.",
            discovery_selector=ScopeSelector(tenant_id=OTHER, kind="tenant"),
        )


def test_preview_retains_server_assigned_author():
    request = ScopePreviewRequest(
        expected=RegistryVersion(**CONTEXT, revision=0), idempotency_id=REQUEST,
        scope=ScopeDefinition(**CONTEXT, scope_id=OTHER, name="Synthetic scope"),
        requested_by=WORKSPACE,
    )
    assert request.requested_by == WORKSPACE
    assert ScopePreviewRequest.model_validate_json(request.model_dump_json()) == request
