# BI triage

This context describes failure monitoring, investigation and permitted
remediation for Power BI semantic models and scheduled Fabric pipelines.

## Language

**Monitored target**:
A semantic model or pipeline identified within a tenant and monitoring epoch.
Its display name is not its identity.
_Avoid_: Report name as identity, resource label

**Admission**:
The current permission to observe or act on a monitored target.
Discovery and readable service metadata do not establish admission.
_Avoid_: Discovery approval, implicit enrollment

**Source execution**:
The original refresh or pipeline job being investigated.
It is distinct from a new execution submitted as remediation.
_Avoid_: Rerun ID for the original failure

**Incident**:
The ongoing investigation of a failure signature on one monitored target.
Multiple occurrences can belong to the same incident without renewing its
remediation budget.
_Avoid_: One incident per alert

**Occurrence**:
Another observed instance of a failure associated with an incident.
_Avoid_: New remediation opportunity

**Original receipt**:
The immutable record of an accepted operation and its original result.
A later observation does not replace that record or prove that an uncertain
operation failed.
_Avoid_: Latest state as receipt, successful acknowledgement as final outcome

**Desired source membership**:
The set of event sources currently intended for monitoring.
Removing desired membership stops intake without proving physical removal.
_Avoid_: Remote topology

**Retained source ownership**:
Responsibility for an app-owned event source until original evidence proves
its exact physical removal.
_Avoid_: Ownership inferred from a matching name

**Source-removal supersession**:
Restoration of desired membership for a retained physical source whose removal
was never submitted, supported by original complete presence evidence.
It is not delivery verification or permission to remediate a workload.
_Avoid_: Resetting removal history, recreating the source

**Resolved by user**:
A human tracking closure attached to the incident evidence that was reviewed.
It is not a verified repair and renews no operational authority or budget.
_Avoid_: Verified remediation
