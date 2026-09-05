import { useMemo } from "react";
import {
    PageShell,
    DashboardGrid,
    Tile,
    StatStrip,
    Stat,
    ChartCard,
    DataTableCard,
} from "@/components/dashboard";
import { useSemanticModelQuery } from "@/hooks/use-semantic-model-query";
import { toChartData } from "@/lib/to-chart-data";
import { toTable } from "@/lib/to-table";
import * as Q from "@/queries/triage";

const CONNECTION = "triageState";

/**
 * Read-only monitoring surface over the triage controller's durable state.
 *
 * Deliberately not a demo prop. There are no trigger buttons, no reset, no
 * scripted scenarios — nothing here can change the system it is watching. It
 * shows what the controller has actually recorded, and every panel maps to a
 * guarantee the controller makes:
 *
 *   incidents   — every terminal outcome is persisted, including refusals
 *   approvals   — silence is never consent; undecided rows are visible
 *   retries     — postponed work that nothing has drained yet
 *   probes      — silent-failure baselines, suspect before confirmed
 *   concurrency — the claims and leases that stop a second invocation acting
 *
 * The concurrency panel is the one an operator should look at first during an
 * incident: a stale claim is how duplicate remediation starts.
 */
export function CockpitPage() {
    const headline = useSemanticModelQuery({ connection: CONNECTION, query: Q.HEADLINE });
    const byStatus = useSemanticModelQuery({ connection: CONNECTION, query: Q.BY_STATUS });
    const incidents = useSemanticModelQuery({ connection: CONNECTION, query: Q.RECENT_INCIDENTS });
    const concurrency = useSemanticModelQuery({ connection: CONNECTION, query: Q.CONCURRENCY });
    const approvals = useSemanticModelQuery({ connection: CONNECTION, query: Q.APPROVALS });
    const retries = useSemanticModelQuery({ connection: CONNECTION, query: Q.RETRIES });
    const probes = useSemanticModelQuery({ connection: CONNECTION, query: Q.PROBES });
    const inboxAudit = useSemanticModelQuery({ connection: CONNECTION, query: Q.INBOX_AUDIT });

    const kpis = useMemo(
        // Columns are emitted under their short name — DAX `[incidents]` becomes
        // `incidents` — so no explicit mapping is needed here.
        () => toChartData(headline.data),
        [headline.data],
    );

    const statusRows = useMemo(
        // `triage_incidents[status]` -> `status`, `[incidents]` -> `incidents`.
        () => toChartData(byStatus.data),
        [byStatus.data],
    );

    const statusSpec = useMemo(
        () => ({
            // Graphein discriminates on `type`, not Vega-Lite's `mark`. The
            // headless preview harness catches this; the app would just render
            // an empty card.
            type: "bar" as const,
            data: statusRows,
            encoding: {
                // Vertical bars: the category on x, the measure on y. These are
                // the *output* keys from toChartData — DAX emits every column
                // under its bracketed leaf name, so `triage_incidents[status]`
                // arrives as `status`.
                x: { field: "status", type: "nominal" as const },
                y: {
                    field: "incidents",
                    type: "quantitative" as const,
                    title: "Incidents",
                    format: ",d",
                },
            },
        }),
        [statusRows],
    );

    const incidentsSpec = useMemo(
        () =>
            toTable(incidents.data, {
                columns: [
                    { field: "Incident", title: "Incident" },
                    { field: "Signature", title: "Signature" },
                    { field: "Status", title: "Status" },
                    { field: "Updated", title: "Updated (UTC)" },
                ],
            }),
        [incidents.data],
    );

    const concurrencySpec = useMemo(
        () =>
            toTable(concurrency.data, {
                columns: [
                    { field: "Kind", title: "Kind" },
                    { field: "Key", title: "Key" },
                    { field: "Owner", title: "Held by" },
                    { field: "Expires", title: "Expires (UTC)" },
                ],
            }),
        [concurrency.data],
    );

    const approvalsSpec = useMemo(
        () =>
            toTable(approvals.data, {
                columns: [
                    { field: "Request", title: "Request" },
                    { field: "Decision", title: "Decision" },
                    { field: "Responder", title: "Responder" },
                    { field: "Decided", title: "Decided (UTC)" },
                ],
            }),
        [approvals.data],
    );

    const retriesSpec = useMemo(
        () =>
            toTable(retries.data, {
                columns: [
                    { field: "Signature", title: "Signature" },
                    { field: "Status", title: "Status" },
                    { field: "Due", title: "Due (UTC)" },
                    { field: "Attempts", title: "Attempts" },
                ],
            }),
        [retries.data],
    );

    const probesSpec = useMemo(
        () =>
            toTable(probes.data, {
                columns: [
                    { field: "Probe", title: "Probe" },
                    { field: "Report", title: "Report" },
                    { field: "Watermark", title: "Watermark" },
                    { field: "Rows", title: "Rows" },
                    { field: "Suspect", title: "Suspect" },
                ],
            }),
        [probes.data],
    );

    const inboxAuditSpec = useMemo(
        () =>
            toTable(inboxAudit.data, {
                columns: [
                    { field: "Sender", title: "Sender" },
                    { field: "Subject", title: "Subject" },
                    { field: "Reason", title: "Why it was ignored" },
                    { field: "Ignored", title: "At (UTC)" },
                ],
            }),
        [inboxAudit.data],
    );

    return (
        <PageShell
            eyebrow="BI Triage"
            title="Controller state"
            subtitle="Read-only view of what the triage loop has recorded"
        >
            <StatStrip>
                <Stat
                    label="Open incidents"
                    data={kpis}
                    valueKey="open"
                    accent="chart-1"
                    loading={headline.isLoading}
                />
                <Stat
                    label="Awaiting a human"
                    data={kpis}
                    valueKey="approvalsPending"
                    accent="chart-3"
                    secondary="approvals with no decision"
                    loading={headline.isLoading}
                />
                <Stat
                    label="Retries pending"
                    data={kpis}
                    valueKey="retriesPending"
                    accent="chart-5"
                    secondary="postponed, not yet drained"
                    loading={headline.isLoading}
                />
                <Stat
                    label="Claims held"
                    data={kpis}
                    valueKey="claimsHeld"
                    accent="chart-2"
                    secondary="one holder per alert"
                    loading={headline.isLoading}
                />
                <Stat
                    label="Suspect probes"
                    data={kpis}
                    valueKey="suspectProbes"
                    accent="chart-4"
                    secondary="seen once, not confirmed"
                    loading={headline.isLoading}
                />
            </StatStrip>

            <DashboardGrid>
                {/* The hero carries the densest panel, not the prettiest one.
                    A `hero` is 8 columns by 2 rows: a table fills that height
                    with rows, where a two-category bar chart just leaves a
                    large empty rectangle under its bars. The 4-column gap
                    beside it takes the two `md` tiles. */}
                <Tile size="hero">
                    <DataTableCard
                        title="Recent incidents"
                        subtitle="Newest first"
                        className="h-full"
                        height={560}
                        spec={incidentsSpec}
                        loading={incidents.isLoading}
                        error={incidents.error}
                    />
                </Tile>

                <Tile size="md">
                    <DataTableCard
                        title="Claims and leases"
                        subtitle="What is holding work right now"
                        spec={concurrencySpec}
                        height={220}
                        loading={concurrency.isLoading}
                        error={concurrency.error}
                    />
                </Tile>

                <Tile size="md">
                    <DataTableCard
                        title="Deferred retries"
                        subtitle="Postponed work, and whether anything drained it"
                        spec={retriesSpec}
                        height={220}
                        loading={retries.isLoading}
                        error={retries.error}
                    />
                </Tile>

                <Tile size="lg">
                    <ChartCard
                        title="Incidents by status"
                        subtitle="Every terminal outcome is persisted, refusals included"
                        accent="chart-1"
                        spec={statusSpec}
                        loading={byStatus.isLoading}
                        error={byStatus.error}
                    />
                </Tile>

                <Tile size="lg">
                    <DataTableCard
                        title="Approvals"
                        subtitle="A blank decision is not a yes"
                        spec={approvalsSpec}
                        height={280}
                        loading={approvals.isLoading}
                        error={approvals.error}
                    />
                </Tile>

                <Tile size="lg">
                    <DataTableCard
                        title="Semantic health baselines"
                        subtitle="Silent-failure detector state"
                        spec={probesSpec}
                        height={280}
                        loading={probes.isLoading}
                        error={probes.error}
                    />
                </Tile>

                <Tile size="lg">
                    <DataTableCard
                        title="Ignored mail"
                        subtitle="The inbox filter is a security control — this is it firing"
                        spec={inboxAuditSpec}
                        height={280}
                        loading={inboxAudit.isLoading}
                        error={inboxAudit.error}
                    />
                </Tile>
            </DashboardGrid>
        </PageShell>
    );
}


