/**
 * DAX over the BI triage state model.
 *
 * The controller writes to a Fabric SQL Database; Fabric mirrors that to the
 * SQL analytics endpoint; a Direct Lake semantic model sits over the endpoint.
 * This file is the only place that knows the table and column names, so a
 * schema change lands here rather than in the components.
 *
 * Two things shape every query below:
 *
 * 1. **Timestamps are strings.** The controller persists ISO-8601 text
 *    (`NVARCHAR(40)`), not `DATETIME2`, so these are ordered lexically. That is
 *    correct for ISO-8601 with a fixed offset, and it is why there is no date
 *    axis anywhere in this dashboard — a real date hierarchy needs a typed
 *    column, and inventing one in DAX would only hide that.
 * 2. **Absence is a number.** Every count is wrapped in `COALESCE(..., 0)`.
 *    A monitoring tile that renders blank when a table is empty is
 *    indistinguishable from one that failed to load, and telling those two
 *    apart is this dashboard's entire job.
 */

/** Headline counts. One row, one column per metric. */
export const HEADLINE = `
EVALUATE
ROW(
    "incidents", COALESCE(COUNTROWS(triage_incidents), 0),
    "open", COALESCE(CALCULATE(COUNTROWS(triage_incidents), triage_incidents[status] = "open"), 0),
    "investigating", COALESCE(CALCULATE(COUNTROWS(triage_incidents), triage_incidents[status] = "investigating"), 0),
    "resolved", COALESCE(CALCULATE(COUNTROWS(triage_incidents), triage_incidents[status] = "resolved"), 0),
    "approvalsPending", COALESCE(CALCULATE(COUNTROWS(triage_approvals), ISBLANK(triage_approvals[decision]) || triage_approvals[decision] = ""), 0),
    "retriesPending", COALESCE(CALCULATE(COUNTROWS(triage_deferred_retries), triage_deferred_retries[status] = "pending"), 0),
    "claimsHeld", COALESCE(CALCULATE(COUNTROWS(triage_claims), triage_claims[expires_at] > UTCNOW()), 0),
    "leasesHeld", COALESCE(CALCULATE(COUNTROWS(triage_sweep_leases), triage_sweep_leases[expires_at] > UTCNOW()), 0),
    "processed", COALESCE(COUNTROWS(triage_processed_messages), 0),
    "suspectProbes", COALESCE(CALCULATE(COUNTROWS(triage_semantic_health), triage_semantic_health[suspect_count] > 0), 0)
)
`;

/** Incident mix by status — the shape of what the loop is doing. */
export const BY_STATUS = `
EVALUATE
SUMMARIZECOLUMNS(
    triage_incidents[status],
    "incidents", COALESCE(COUNTROWS(triage_incidents), 0)
)
ORDER BY [incidents] DESC
`;

/**
 * Most recent incidents. Lexical ordering is valid for ISO-8601 text.
 *
 * The ORDER BY repeats the TOPN sort deliberately: TOPN picks the 25 newest but
 * makes no promise about the order it hands them back, so without this the
 * "most recent" table was only coincidentally in order.
 */
export const RECENT_INCIDENTS = `
EVALUATE
TOPN(
    25,
    SELECTCOLUMNS(
        triage_incidents,
        "Incident", triage_incidents[incident_id],
        "Signature", triage_incidents[signature],
        "Status", triage_incidents[status],
        "Updated", triage_incidents[updated_at]
    ),
    [Updated], DESC
)
ORDER BY [Updated] DESC
`;

/**
 * Claims and leases: the two primitives that stop a second invocation acting on
 * work already in flight.
 *
 * Both are released by expiry, not by deletion — a claim is taken by a
 * conditional UPDATE that overwrites an expired row, so expired rows sit in the
 * table indefinitely. Counting rows therefore counts history, not what is held.
 * Every query here filters on expires_at > UTCNOW(), which is what makes "held"
 * true. expires_at is DATETIME2(3) written in UTC, so this compares like to like.
 */
export const CONCURRENCY = `
EVALUATE
UNION(
    SELECTCOLUMNS(
        FILTER(triage_claims, triage_claims[expires_at] > UTCNOW()),
        "Kind", "claim",
        "Key", triage_claims[claim_key],
        "Owner", triage_claims[owner],
        "Expires", triage_claims[expires_at]
    ),
    SELECTCOLUMNS(
        FILTER(triage_sweep_leases, triage_sweep_leases[expires_at] > UTCNOW()),
        "Kind", "lease",
        "Key", triage_sweep_leases[lease_name],
        "Owner", triage_sweep_leases[owner],
        "Expires", triage_sweep_leases[expires_at]
    )
)
ORDER BY [Expires] DESC
`;

/**
 * Approvals. Undecided rows are the ones that matter, so they sort first.
 *
 * Ordering by decided_at alone put them last — it is blank until somebody
 * answers — so once 25 requests had been settled every pending one fell off the
 * end of the TOPN and the panel showed only closed work.
 *
 * The trailing ORDER BY is not redundant. TOPN's arguments decide *which* rows
 * survive the cut, not the order they come back in; without an explicit ORDER BY
 * the row order is undefined and the pending rows can still render anywhere.
 */
export const APPROVALS = `
EVALUATE
TOPN(
    25,
    SELECTCOLUMNS(
        triage_approvals,
        "Request", triage_approvals[request_id],
        "Decision", triage_approvals[decision],
        "Responder", triage_approvals[responder],
        "Decided", triage_approvals[decided_at]
    ),
    IF(ISBLANK([Decision]) || [Decision] = "", 1, 0), DESC,
    [Decided], DESC
)
ORDER BY IF(ISBLANK([Decision]) || [Decision] = "", 1, 0) DESC, [Decided] DESC
`;

/** Postponed work. A row that is due and still pending has not been drained. */
export const RETRIES = `
EVALUATE
SELECTCOLUMNS(
    triage_deferred_retries,
    "Signature", triage_deferred_retries[signature],
    "Status", triage_deferred_retries[status],
    "Due", triage_deferred_retries[due_at],
    "Attempts", triage_deferred_retries[attempts]
)
`;

/**
 * Messages the inbox filter refused.
 *
 * The filter is a security control — an agent that acts on every message it
 * receives is steerable by anyone who can email it — and this is the evidence
 * that it fired. A count cannot distinguish a filter correctly rejecting noise
 * from one silently rejecting the real alerts; the reason column can.
 */
export const INBOX_AUDIT = `
EVALUATE
TOPN(
    25,
    SELECTCOLUMNS(
        triage_inbox_audit,
        "Sender", triage_inbox_audit[sender],
        "Subject", triage_inbox_audit[subject],
        "Reason", triage_inbox_audit[reason],
        "Ignored", triage_inbox_audit[ignored_at]
    ),
    [Ignored], DESC
)
`;

/**
 * Silent-failure baselines. `suspect_count` above zero means a probe saw
 * something it has not yet confirmed — the suspect-then-confirm rule mid-flight,
 * not a finding.
 */
export const PROBES = `
EVALUATE
SELECTCOLUMNS(
    triage_semantic_health,
    "Probe", triage_semantic_health[probe_name],
    "Report", triage_semantic_health[report_name],
    "Watermark", triage_semantic_health[last_max_date],
    "Rows", triage_semantic_health[last_row_count],
    "Suspect", triage_semantic_health[suspect_count]
)
`;
