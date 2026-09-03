---
name: Feature request
about: A failure mode to handle, or a capability to add
labels: enhancement
---

## The failure this addresses

What breaks in Power BI or Fabric, and how a person currently finds out.

## Who is affected

The role, and what they do about it today.

## Proposed behaviour

What should the agent do? Be specific about whether it should **act** or only
**report** -- the split matters more than the detection.

## Automation tier

- [ ] Tier 1 -- transient and idempotent; safe to remediate unattended
- [ ] Tier 2 -- deterministic fix, but needs human approval first
- [ ] Tier 3 -- never automate; escalate with evidence

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
