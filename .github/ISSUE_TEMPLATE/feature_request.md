---
name: Feature request
about: A failure mode to handle, or a capability to add
labels: enhancement
---

## The failure this addresses

What breaks in Power BI or Fabric, and how a person currently finds out.
For a UI request, identify the read-only Fabric cockpit or the separate
Azure-hosted command center and describe the operator workflow.

## Who is affected

The role, and what they do about it today.

## Proposed behaviour

What should the agent do? Be specific about whether it should **act** or only
**report** -- the split matters more than the detection.
For a human workflow, distinguish an approval, an investigation request and a
tracking-only resolution. Resolving an incident in the UI is not verified
remediation and must not reset its budget.

## Automation tier

- [ ] Tier 1 -- transient and idempotent; safe to remediate unattended
- [ ] Tier 2 -- deterministic fix, but needs human approval first
- [ ] Tier 3 -- never automate; escalate with evidence

These describe the proposed behavior, not permission to bypass an existing
guard. Fabric pipeline reruns currently require explicit approval even when the
failure is transient and the target has been reviewed for replay safety.

## Blast radius

What else is affected if this action runs? This becomes the impact line on the
approval card, and an approval that hides the consequence is a rubber stamp.

## Evidence the controller should require

What must be deterministically true before this is permitted? Preconditions go
in the dispatcher, not in prompt wording -- a model can be argued out of a
precondition; a controller cannot.

## Is there an API?

Which Power BI, Fabric or Graph endpoint, and does it accept app-only service
principal auth? Several plausible remediations have no API at all, and saying so
is more useful than assuming one exists.
Name the component that calls it and the permission it needs. Grant service
permissions to the component that acts, not to the reasoning agent; human
command-center access is controlled by Entra app-role claims.
