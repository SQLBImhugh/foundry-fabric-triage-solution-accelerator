"""Reading a semantic model's health without waiting for it to fail.

The rest of this system is alert-driven: Power BI fails, sends mail, the agent
triages. The failures that hurt most send no mail at all. A refresh reports
success while the source never landed, a table loads a tenth of its rows, a
column disappears upstream -- the report looks normal and is wrong, which is
exactly the case an analyst describes as "the report looked fine".

Finding those means asking the model questions rather than waiting to be told.
Three facts answer most of it: how far the data reaches, how much of it there
is, and whether the shape still matches.

Deliberately separate from ``PowerBIClient``. That client exists to *change*
things -- trigger a refresh, rebind a gateway -- and is on the remediation
allowlist. This one only reads, and keeping them apart means a read path can
never accidentally acquire a write.

The model never supplies DAX. Queries are generated from stored probe
configuration, so a prompt injection in an alert email cannot turn the
detector into an arbitrary query engine against the finance model.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger("triage.semantic_health")

_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
_API = "https://api.powerbi.com/v1.0/myorg"

#: Power BI rejects executeQueries beyond this per minute, per user, across all
#: datasets. Budgeted rather than discovered: a detector that trips throttling
#: becomes the outage it was watching for.
EXECUTE_QUERIES_PER_MINUTE = 120


@dataclass
class ProbeResult:
    """One measurement of one semantic model.

    ``ok=False`` means the detector could not see, which is not the same as
    the model being unhealthy. Conflating those two produces alerts about a
    broken detector dressed up as alerts about broken data.
    """

    ok: bool
    max_date: str = ""
    row_count: int | None = None
    control_totals: dict[str, float] = field(default_factory=dict)
    error: str = ""
    #: Set when the failure is the detector's own, not the model's: a missing
    #: permission, a disabled tenant setting, an unsupported dataset.
    detector_fault: bool = False
    query: str = ""


def _permission_hint(status: int, detail: str) -> str:
    """Say what the caller has to go and change.

    Power BI's refusals do not describe their own cause, and the ones that stop
    a detector look nothing like what is actually wrong:

    * ``401 PowerBINotAuthorizedException`` arrives with an empty parameter bag
      and no message at all. Against a Fabric workspace the usual cause is a
      **Direct Lake** model: it reaches OneLake as the *caller*, and Microsoft
      does not support app-only callers for that, so a service principal is
      refused no matter what permission it holds. Measured against a real
      medallion workspace, where every table was Direct Lake and the identity
      was already workspace Contributor. The fix is to point the model's OneLake
      connection at a fixed identity instead of SSO -- not to grant more access,
      which is the natural but wrong reading of "not authorized".
    * ``404 PowerBIEntityNotFound`` is what insufficient workspace permission
      looks like. The dataset exists; the caller cannot see it. Reading it as
      "wrong id" sends people to check GUIDs that were right all along.

    Cost most of an afternoon to work out once. Writing it into the error means
    nobody pays that twice.
    """
    if status == 401 and "NotAuthorized" in detail:
        return (
            "\nHint: if this is a Direct Lake model, app-only callers are not "
            "supported while it reaches OneLake by SSO -- set the model's "
            "OneLake connection to a fixed identity. Otherwise check that "
            "'Semantic Model Execute Queries REST API' is enabled and that the "
            "identity is inside any security group those tenant settings are "
            "scoped to. Granting more workspace permission will not fix it."
        )
    if status == 404 and "EntityNotFound" in detail:
        return (
            "\nHint: this is what missing workspace permission looks like, not "
            "a wrong id. executeQueries needs Build, which for an app-only "
            "caller means workspace Contributor -- Power BI will not grant "
            "Build on a single dataset to a service principal."
        )
    return ""



@dataclass
class SchemaReading:
    """The model's shape at one moment, or why it could not be read.

    Separate from ``ProbeResult`` because the two answer different questions
    and fail independently: a model can be perfectly readable while its schema
    query is refused, and reporting that as missing data would be wrong.
    """

    ok: bool
    entries: tuple[str, ...] = ()
    error: str = ""
    detector_fault: bool = False
    query: str = ""


#: Visible columns and measures, as one query.
#:
#: Deliberately not XMLA. The scope for this called for ADOMD or TOM, which
#: means a .NET dependency and, in practice, a Windows host -- the controller
#: runs in a Linux container. ``INFO.VIEW`` returns the same names over the
#: read-only endpoint the detector already holds, so schema drift costs one
#: extra query rather than a second access path with its own permissions.
#:
#: Hidden objects are excluded on purpose: a key column being hidden is a
#: modelling decision, not a change anyone reports on.
SCHEMA_DAX = """EVALUATE
UNION(
    SELECTCOLUMNS(FILTER(INFO.VIEW.COLUMNS(), NOT [IsHidden]), "Kind", "column", "Name", [Table] & "[" & [Name] & "]"),
    SELECTCOLUMNS(FILTER(INFO.VIEW.MEASURES(), NOT [IsHidden]), "Kind", "measure", "Name", [Name])
)
ORDER BY [Kind], [Name]"""


def build_schema_dax() -> str:
    """The schema probe. Takes no configuration, so nothing to escape."""
    return SCHEMA_DAX


def schema_entries(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    """Normalise the schema query's rows into comparable identifiers."""
    out: list[str] = []
    for row in rows:
        kind = str(row.get("[Kind]") or row.get("Kind") or "").strip()
        name = str(row.get("[Name]") or row.get("Name") or "").strip()
        if name:
            out.append(f"{kind}:{name}" if kind else name)
    return tuple(sorted(set(out)))


def build_probe_dax(
    *,
    table: str,
    date_column: str = "",
    date_table: str = "",
    row_count_table: str = "",
    control_measures: tuple[str, ...] = (),
) -> str:
    """Generate the scalar probe for one model.

    One ``ROW(...)`` returning a handful of values, never detail rows. The
    limits allow 100,000 rows per query; fetching them would make the detector
    a load source on the capacity it is monitoring.

    ``date_table`` handles the star schema, which is most of them. A fact table
    usually holds an integer date *key* and no date column at all, so there is
    nothing on it to take a MAX of. Measuring the date dimension instead is
    worse than useless: a calendar dimension is populated years ahead, so
    ``MAX('dim_date'[date])`` returned 2030-12-31 on a real model whose data
    stopped in 2024. The probe would have reported that model fresh forever --
    a detector that silently never fires, which is the failure mode this whole
    component exists to prevent.

    With ``date_table`` set, the watermark is taken across the fact and through
    the relationship, which is the date the data actually reaches.

    Escaping matters here even though the model cannot reach this function:
    configuration is edited by people, and a stray quote in a table name should
    produce a broken query rather than a differently-scoped one.
    """
    if not table.strip():
        raise ValueError("A probe needs a table to measure.")
    if date_table and not date_column:
        raise ValueError("date_table needs date_column: which column holds the date?")

    parts: list[str] = []
    safe_table = table.replace("'", "''")
    if date_column:
        safe_column = date_column.replace("]", "]]")
        if date_table:
            safe_date_table = date_table.replace("'", "''")
            watermark = (
                f"MAXX('{safe_table}', RELATED('{safe_date_table}'[{safe_column}]))"
            )
        else:
            watermark = f"MAX('{safe_table}'[{safe_column}])"
        parts.append(f"\"MaxDate\", FORMAT({watermark}, \"yyyy-mm-dd\")")
    counted = (row_count_table or table).replace("'", "''")
    parts.append(f"\"RowCount\", COUNTROWS('{counted}')")
    for measure in control_measures:
        safe_measure = measure.replace("]", "]]")
        parts.append(f"\"{safe_measure}\", [{safe_measure}]")

    return "EVALUATE\nROW(\n    " + ",\n    ".join(parts) + "\n)"


class SemanticModelHealthClient(Protocol):
    async def run_probe(
        self,
        workspace_id: str,
        dataset_id: str,
        *,
        table: str,
        date_column: str = "",
        date_table: str = "",
        row_count_table: str = "",
        control_measures: tuple[str, ...] = (),
    ) -> ProbeResult: ...

    async def read_schema(self, workspace_id: str, dataset_id: str) -> SchemaReading: ...


@dataclass
class MockSemanticHealthClient:
    """Scripted readings. Deterministic, and records what was asked."""

    max_date: str = "2026-09-01"
    row_count: int | None = 10_000
    control_totals: dict[str, float] = field(default_factory=dict)
    ok: bool = True
    error: str = ""
    detector_fault: bool = False
    latency_ms: int = 0
    schema: tuple[str, ...] = ()
    schema_ok: bool = True
    schema_error: str = ""
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def run_probe(
        self,
        workspace_id: str,
        dataset_id: str,
        *,
        table: str,
        date_column: str = "",
        date_table: str = "",
        row_count_table: str = "",
        control_measures: tuple[str, ...] = (),
    ) -> ProbeResult:
        query = build_probe_dax(
            table=table,
            date_column=date_column,
            date_table=date_table,
            row_count_table=row_count_table,
            control_measures=control_measures,
        )
        self.calls.append(
            {"workspace_id": workspace_id, "dataset_id": dataset_id, "query": query}
        )
        if self.latency_ms:
            await asyncio.sleep(self.latency_ms / 1000)
        if not self.ok:
            return ProbeResult(
                ok=False, error=self.error, detector_fault=self.detector_fault, query=query
            )
        return ProbeResult(
            ok=True,
            max_date=self.max_date,
            row_count=self.row_count,
            control_totals=dict(self.control_totals),
            query=query,
        )

    async def read_schema(self, workspace_id: str, dataset_id: str) -> SchemaReading:
        query = build_schema_dax()
        self.calls.append(
            {"workspace_id": workspace_id, "dataset_id": dataset_id, "query": query}
        )
        if not self.schema_ok:
            return SchemaReading(
                ok=False, error=self.schema_error, detector_fault=True, query=query
            )
        return SchemaReading(ok=True, entries=tuple(sorted(self.schema)), query=query)


class LiveSemanticHealthClient:
    """Reads a semantic model through the Power BI REST executeQueries endpoint.

    Requires, and fails clearly without: the *Dataset Execute Queries REST API*
    tenant setting, *Allow service principals to use Power BI APIs*, and the
    caller holding workspace access with dataset read and build permission.

    Not supported by Microsoft for app-only callers: datasets with row-level
    security or single sign-on. That is a documented platform limit rather than
    a bug here, so it is reported as a detector fault and never as evidence the
    data is wrong.
    """

    def __init__(
        self,
        *,
        tenant_id: str = "",
        client_id: str = "",
        client_secret: str = "",
        timeout_seconds: int = 60,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout = timeout_seconds
        self._token: str = ""
        self._token_expires_at: float = 0.0

    async def _get_token(self) -> str:
        import httpx

        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        if not self._client_secret:
            # Same posture as the refresh client: the hosted controller
            # authenticates as its own agent identity. Human credentials are
            # excluded on purpose -- an unattended detector that can quietly
            # read as a person is worse than one that cannot start.
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential(
                exclude_cli_credential=True,
                exclude_developer_cli_credential=True,
                exclude_interactive_browser_credential=True,
                exclude_shared_token_cache_credential=True,
                exclude_visual_studio_code_credential=True,
                managed_identity_client_id=self._client_id or None,
            )
            token = await asyncio.to_thread(credential.get_token, _SCOPE)
            self._token = token.token
            try:
                self._token_expires_at = float(token.expires_on)
            except (AttributeError, TypeError, ValueError):
                self._token_expires_at = 0.0
            return self._token

        url = f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": _SCOPE,
                },
            )
            resp.raise_for_status()
            payload = resp.json()

        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    async def _execute(
        self, workspace_id: str, dataset_id: str, query: str
    ) -> tuple[list[dict[str, Any]] | None, str, bool]:
        """Run one DAX query. Returns (rows, error, detector_fault).

        Shared by the data probe and the schema probe so the two cannot drift
        apart on error handling. Which failures count as the detector's fault
        rather than the data's is the classification this whole component rests
        on, and writing it twice is how the two copies stop agreeing.
        """
        import httpx

        if not (workspace_id or "").strip() or not (dataset_id or "").strip():
            # The same failure that once let a 404 be read as a conclusion:
            # empty ids produce a plausible-looking error the model rationalises.
            raise ValueError(
                "Cannot probe a semantic model without a workspace id and dataset id."
            )

        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "queries": [{"query": query}],
                    "serializerSettings": {"includeNulls": True},
                },
            )

        if resp.status_code >= 400:
            detail = resp.text[:400]
            # 401/403 and the RLS/SSO rejection are the detector's problem, not
            # the data's. Reporting them as data findings would tell somebody
            # their numbers are wrong because of a missing permission.
            detector_fault = resp.status_code in (401, 403, 404, 429) or "RLS" in detail
            logger.warning(
                "Semantic probe failed: HTTP %s %s", resp.status_code, detail[:200]
            )
            hint = _permission_hint(resp.status_code, detail)
            return None, f"HTTP {resp.status_code}: {detail}{hint}", detector_fault

        try:
            return resp.json()["results"][0]["tables"][0]["rows"], "", False
        except (KeyError, IndexError, ValueError) as exc:
            return None, f"Unreadable probe response ({type(exc).__name__})", True

    async def read_schema(self, workspace_id: str, dataset_id: str) -> SchemaReading:
        query = build_schema_dax()
        rows, error, fault = await self._execute(workspace_id, dataset_id, query)
        if rows is None:
            return SchemaReading(ok=False, error=error, detector_fault=fault, query=query)
        entries = schema_entries(rows)
        if not entries:
            # An empty schema is never real. Treated as blindness rather than as
            # "every column was deleted", which would be a spectacular and
            # entirely wrong finding.
            return SchemaReading(
                ok=False,
                error="Schema query returned nothing.",
                detector_fault=True,
                query=query,
            )
        return SchemaReading(ok=True, entries=entries, query=query)

    async def run_probe(
        self,
        workspace_id: str,
        dataset_id: str,
        *,
        table: str,
        date_column: str = "",
        date_table: str = "",
        row_count_table: str = "",
        control_measures: tuple[str, ...] = (),
    ) -> ProbeResult:
        query = build_probe_dax(
            table=table,
            date_column=date_column,
            date_table=date_table,
            row_count_table=row_count_table,
            control_measures=control_measures,
        )
        rows, error, fault = await self._execute(workspace_id, dataset_id, query)
        if rows is None:
            return ProbeResult(ok=False, error=error, detector_fault=fault, query=query)

        if not rows:
            return ProbeResult(
                ok=False, error="Probe returned no rows.", detector_fault=True, query=query
            )

        row = rows[0]
        return ProbeResult(
            ok=True,
            max_date=str(row.get("[MaxDate]") or row.get("MaxDate") or ""),
            row_count=_as_int(row.get("[RowCount]", row.get("RowCount"))),
            control_totals={
                str(k).strip("[]"): float(v)
                for k, v in row.items()
                if str(k).strip("[]") in control_measures and _is_number(v)
            },
            query=query,
        )


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
