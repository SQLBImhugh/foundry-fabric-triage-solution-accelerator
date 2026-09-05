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
                    error={headline.error}
                />
                <Stat
                    label="Awaiting a human"
                    data={kpis}
                    valueKey="approvalsPending"
                    accent="chart-3"
                    secondary="approvals with no decision"
                    loading={headline.isLoading}
                    error={headline.error}
                />
                <Stat
                    label="Retries pending"
                    data={kpis}
                    valueKey="retriesPending"
                    accent="chart-5"
                    secondary="postponed, not yet drained"
                    loading={headline.isLoading}
                    error={headline.error}
                />
                <Stat
                    label="Claims held"
                    data={kpis}
                    valueKey="claimsHeld"
                    accent="chart-2"
                    secondary="one holder per alert"
                    loading={headline.isLoading}
                    error={headline.error}
                />
                <Stat
                    label="Suspect probes"
                    data={kpis}
                    valueKey="suspectProbes"
                    accent="chart-4"
                    secondary="seen once, not confirmed"
                    loading={headline.isLoading}
                    error={headline.error}
                />
            </StatStrip>

            <DashboardGrid>
                <Tile size="hero">
                    <ChartCard
                        title="Incidents by status"
                        subtitle="Every terminal outcome is persisted, refusals included"
                        variant="feature"
                        accent="chart-1"
                        className="h-full"
                        spec={statusSpec}
                        loading={byStatus.isLoading}
                        error={byStatus.error}
                    />
                </Tile>

                <Tile size="md">
                    <DataTableCard
                        title="Claims and leases"
                        subtitle="What is holding work right now"
                        spec={concurrencySpec}
                        height={300}
                        loading={concurrency.isLoading}
                        error={concurrency.error}
                    />
                </Tile>

                <Tile size="md">
                    <DataTableCard
                        title="Deferred retries"
                        subtitle="Postponed work, and whether anything drained it"
                        spec={retriesSpec}
                        height={300}
                        loading={retries.isLoading}
                        error={retries.error}
                    />
                </Tile>

                <Tile size="full">
                    <DataTableCard
                        title="Recent incidents"
                        subtitle="Newest first"
                        spec={incidentsSpec}
                        loading={incidents.isLoading}
                        error={incidents.error}
                    />
                </Tile>

                <Tile size="lg">
                    <DataTableCard
                        title="Approvals"
                        subtitle="A blank decision is not a yes"
                        spec={approvalsSpec}
                        height={300}
                        loading={approvals.isLoading}
                        error={approvals.error}
                    />
                </Tile>

                <Tile size="lg">
                    <DataTableCard
                        title="Semantic health baselines"
                        subtitle="Silent-failure detector state"
                        spec={probesSpec}
                        height={300}
                        loading={probes.isLoading}
                        error={probes.error}
                    />
                </Tile>
            </DashboardGrid>
        </PageShell>
    );
}

