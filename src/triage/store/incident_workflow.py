"""Append-only incident collaboration, separate from automated incident state.

A closure names the exact persisted incident payload, not just its identifier.
Any subsequent evidence change invalidates that closure. The tracking version
counts manual resolutions; adding a note or a question does not advance it.

The SQL backend needs SELECT on incidents and SELECT/INSERT on its own table.
Schema installation is an operator operation. There is no runtime DDL, local
fallback, mutation retry, or write to an incident, approval, claim or budget.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from triage.models import Incident
from triage.redaction import redact_text
from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable, quote_identifier
from triage.store.command_center import _digest, _identifier, _sql_time, _timestamp
from triage.store.incidents import InMemoryIncidentStore

logger = logging.getLogger("triage.store.incident_workflow")
DEFAULT_ACTIVITY_TABLE = "triage_incident_activity"
Hash = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
ActivityKind = Literal["note", "resolution", "question", "answer"]
ActivityStatus = Literal["recorded", "pending", "completed", "failed"]
ObserverMode = Literal["records", "model"]
IncidentFilter = Literal[
    "all", "open", "needs_investigation", "needs_review", "resolved",
    "resolved_by_user", "investigating", "wont_fix",
]


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def incident_identifier(value: str) -> str:
    return _identifier(value, 200)


def source_revision(value: Incident | str) -> str:
    """Hash NVARCHAR payload bytes, matching SQL HASHBYTES('SHA2_256', payload).

    Pass the original database payload in live mode. Re-serializing an older
    payload can introduce defaults or change whitespace and produce a different
    revision. Even a serialization-only change conservatively invalidates a
    closure; it never hides a new occurrence.
    """
    payload = value.model_dump_json() if isinstance(value, Incident) else value
    return hashlib.sha256(payload.encode("utf-16-le")).hexdigest()


class WorkflowConflict(ValueError):
    """A stale or conflicting request was not applied."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class IncidentActivity(_Model):
    """Mutation receipt and thread entry.

    Notes, resolutions and questions use their submitted idempotency UUID as
    ``id``. Answers have a separate ID and name the question in
    ``correlation_id``; notes and resolutions have no correlation ID.
    """

    id: str
    incident_id: str
    kind: ActivityKind
    body: str
    created_at: str
    user_id: str
    user_name: str
    status: ActivityStatus = "recorded"
    correlation_id: str | None = None
    mode: ObserverMode | None = None

    @field_validator("id", "correlation_id")
    @classmethod
    def uuid_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    _incident_id = field_validator("incident_id")(incident_identifier)
    _created_at = field_validator("created_at")(_timestamp)

    @field_validator("user_id")
    @classmethod
    def actor_id(cls, value: str) -> str:
        return _identifier(value, 200)


class IncidentTracking(_Model):
    status: Literal["open", "resolved", "resolved_by_user"] = "open"
    version: int = Field(default=0, ge=0)
    source_revision: Hash
    resolved_at: str | None = None
    resolved_by: str | None = None
    resolution_note: str | None = None


class IncidentSource(_Model):
    incident: Incident
    revision: Hash


class IncidentState(_Model):
    source: IncidentSource
    tracking: IncidentTracking
    activity: list[IncidentActivity]


class IncidentProjection(_Model):
    source: IncidentSource
    tracking: IncidentTracking


class IncidentQuery(_Model):
    limit: int = Field(default=25, ge=1, le=100, strict=True)
    offset: int = Field(default=0, ge=0, le=2_147_483_647, strict=True)
    query: str = Field(default="", max_length=200)
    status: IncidentFilter = "all"
    workload: Literal["all", "powerbi", "fabric_pipeline"] = "all"

    @field_validator("query")
    @classmethod
    def trim_query(cls, value: str) -> str:
        return value.strip()


class IncidentPage(_Model):
    items: list[IncidentProjection]
    total: int
    offset: int
    limit: int


class QuestionReservation(_Model):
    question: IncidentActivity
    acquired: bool


class _Entry(IncidentActivity):
    request_hash: Hash
    tracking_version: int | None = Field(default=None, ge=1)
    source_revision: Hash | None = None
    observer_run_id: str | None = None

    @field_validator("observer_run_id")
    @classmethod
    def run_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @model_validator(mode="after")
    def consistent_record(self) -> _Entry:
        if self.kind == "resolution":
            if self.tracking_version is None or self.source_revision is None:
                raise ValueError("A resolution must name its tracking version and source revision")
        elif self.tracking_version is not None or self.source_revision is not None:
            raise ValueError("Only resolutions can advance tracking state")
        if self.kind in {"note", "resolution"}:
            if self.status != "recorded" or self.correlation_id is not None or self.mode is not None:
                raise ValueError("Notes and resolutions must be immutable recorded activity")
        elif self.kind == "question":
            if self.status != "pending" or self.correlation_id != self.id or self.mode is not None:
                raise ValueError("A stored question is an immutable pending reservation")
        elif (
            self.status not in {"completed", "failed"} or self.correlation_id is None
            or (self.status == "completed" and self.mode is None)
        ):
            raise ValueError("An answer must identify its question and completion state")
        if self.kind != "answer" and self.observer_run_id is not None:
            raise ValueError("Only answers can name an observer run")
        return self


def _source(payload: str, expected_id: str | None = None) -> IncidentSource:
    incident = Incident.model_validate_json(payload)
    incident_identifier(incident.id)
    if expected_id is not None and incident.id != expected_id:
        raise RuntimeError("Incident payload does not match its database identifier")
    return IncidentSource(incident=incident, revision=source_revision(payload))


def _activity(entry: _Entry) -> IncidentActivity:
    return IncidentActivity.model_validate({
        name: getattr(entry, name) for name in IncidentActivity.model_fields
    })


def _tracking(source: IncidentSource, entries: Sequence[_Entry]) -> IncidentTracking:
    latest = max(
        (entry for entry in entries if entry.kind == "resolution"),
        key=lambda entry: entry.tracking_version or 0, default=None,
    )
    version = 0
    if latest is not None:
        if latest.tracking_version is None:
            raise RuntimeError("Stored resolution has no tracking version")
        version = latest.tracking_version
    result = IncidentTracking(
        source_revision=source.revision, version=version,
    )
    if source.incident.status == "resolved":
        result.status = "resolved"
        result.resolved_at = source.incident.last_seen_at
        result.resolved_by = source.incident.agent_name or "TriageAgent"
    elif latest is not None and latest.source_revision == source.revision:
        result.status = "resolved_by_user"
        result.resolved_at = latest.created_at
        result.resolved_by = latest.user_name or latest.user_id
        result.resolution_note = latest.body
    return result


def _state(source: IncidentSource, entries: Sequence[_Entry]) -> IncidentState:
    answers = {entry.correlation_id: entry for entry in entries if entry.kind == "answer"}
    activity = []
    for entry in sorted(entries, key=lambda row: (row.created_at, row.kind == "answer", row.id)):
        item = _activity(entry)
        if item.kind == "question" and item.id in answers:
            item.status = answers[item.id].status
            item.mode = answers[item.id].mode
        activity.append(item)
    return IncidentState(source=source, tracking=_tracking(source, entries), activity=activity)


def _text(value: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"Text must contain 1 to {maximum} characters and cannot be blank")
    return value.strip()


def _request(
    incident_id: str, kind: Literal["note", "resolution", "question"], body: str, *,
    idempotency_key: str, user_id: str, user_name: str,
    expected_version: int | None = None, revision: str | None = None,
) -> _Entry:
    incident_id = incident_identifier(incident_id)
    key = str(UUID(idempotency_key))
    user_id = _identifier(user_id, 200)
    maximum = 2000 if kind == "question" else 4000
    body = _text(body, maximum)
    if kind == "resolution" and (
        type(expected_version) is not int or not 0 <= expected_version < 9_007_199_254_740_991
    ):
        raise ValueError("Expected tracking version must be a nonnegative safe integer")
    fingerprint = _digest({
        "incident_id": incident_id, "kind": kind, "body": body, "user_id": user_id,
        "expected_version": expected_version, "source_revision": revision,
    })
    return _Entry(
        id=key, incident_id=incident_id, kind=kind,
        # Hash the original request first: different secrets must not become
        # equivalent idempotent requests after both redact to the same marker.
        body=redact_text(body)[:maximum], created_at=_utcnow(),
        user_id=user_id, user_name=redact_text(user_name)[:200],
        status="pending" if kind == "question" else "recorded",
        correlation_id=key if kind == "question" else None,
        request_hash=fingerprint,
        tracking_version=expected_version + 1 if expected_version is not None else None,
        source_revision=revision,
    )


def _same_request(prior: _Entry, entry: _Entry) -> None:
    if prior.request_hash != entry.request_hash:
        raise WorkflowConflict("Idempotency key conflicts with a different request.")


def _check_resolution(entry: _Entry, state: IncidentState) -> None:
    if (
        entry.source_revision != state.source.revision
        or entry.tracking_version != state.tracking.version + 1
    ):
        raise WorkflowConflict("Incident evidence or tracking version changed. Reload before resolving.")
    if state.tracking.status != "open":
        raise WorkflowConflict("This incident is already closed for the current evidence.")


def projected_status(projection: IncidentProjection) -> str:
    incident = projection.source.incident
    if projection.tracking.status != "open":
        return projection.tracking.status
    return "needs_review" if incident.requires_investigation and incident.status == "open" else incident.status


def _matches(row: IncidentProjection, query: IncidentQuery) -> bool:
    incident = row.source.incident
    state = projected_status(row)
    closed = state in {"resolved", "resolved_by_user"}
    if query.status == "open" and closed:
        return False
    if query.status == "resolved" and not closed:
        return False
    if query.status in {"needs_review", "needs_investigation"}:
        if closed or not incident.requires_investigation:
            return False
    elif query.status not in {"all", "open", "resolved"} and query.status != state:
        return False
    workload = "fabric_pipeline" if incident.source.startswith("fabric_pipeline") else "powerbi"
    if query.workload != "all" and query.workload != workload:
        return False
    return not query.query or any(query.query.lower() in value.lower() for value in (
        incident.id, incident.report_name, incident.original_error,
        incident.diagnosed_root_cause, incident.action_applied,
    ))


class IncidentWorkflowStore(ABC):
    is_durable: bool

    @abstractmethod
    def source(self, incident_id: str) -> IncidentSource: ...

    @abstractmethod
    def _entries(self, incident_id: str) -> list[_Entry]: ...

    @abstractmethod
    def _entry(self, key: str) -> _Entry | None: ...

    @abstractmethod
    def _append(self, entry: _Entry) -> tuple[_Entry, bool]: ...

    @abstractmethod
    def page(self, query: IncidentQuery) -> IncidentPage: ...

    @abstractmethod
    def projections(self, incident_ids: Sequence[str]) -> dict[str, IncidentProjection]: ...

    @abstractmethod
    def counts(self) -> dict[str, int]: ...

    def state(self, incident_id: str) -> IncidentState:
        source = self.source(incident_id)
        return _state(source, self._entries(source.incident.id))

    def add_note(
        self, incident_id: str, body: str, *,
        idempotency_key: str, user_id: str, user_name: str,
    ) -> IncidentActivity:
        entry, _ = self._append(_request(
            incident_id, "note", body, idempotency_key=idempotency_key,
            user_id=user_id, user_name=user_name,
        ))
        return _activity(entry)

    def resolve(
        self, incident_id: str, reason: str, *, expected_version: int, source_revision: str,
        idempotency_key: str, user_id: str, user_name: str,
    ) -> IncidentActivity:
        entry, _ = self._append(_request(
            incident_id, "resolution", reason, expected_version=expected_version,
            revision=source_revision, idempotency_key=idempotency_key,
            user_id=user_id, user_name=user_name,
        ))
        return _activity(entry)

    def reserve_question(
        self, incident_id: str, question: str, *,
        idempotency_key: str, user_id: str, user_name: str,
    ) -> QuestionReservation:
        entry, acquired = self._append(_request(
            incident_id, "question", question, idempotency_key=idempotency_key,
            user_id=user_id, user_name=user_name,
        ))
        return QuestionReservation(question=_activity(entry), acquired=acquired)

    def finish_question(
        self, question_id: str, answer: str, *, mode: ObserverMode | None = None,
        failed: bool = False, observer_run_id: str | None = None,
    ) -> IncidentActivity:
        question = self._entry(str(UUID(question_id)))
        if question is None or question.kind != "question":
            raise ValueError("Cannot finish an unknown question")
        if not failed and mode not in {"records", "model"}:
            raise ValueError("A completed answer must identify its observer mode")
        if not isinstance(answer, str) or not answer.strip():
            raise ValueError("An answer or explicit failure explanation is required")
        status = "failed" if failed else "completed"
        entry = _Entry(
            id=str(uuid5(UUID(question.id), "incident-observer-answer")),
            incident_id=question.incident_id, kind="answer",
            # Padding must not consume the whole limit and leave a completed
            # answer whose persisted body contains only whitespace.
            body=redact_text(answer.strip())[:4000], created_at=_utcnow(),
            user_id="incident-observer", user_name="Incident observer",
            status=status, correlation_id=question.id, mode=mode,
            observer_run_id=observer_run_id,
            request_hash=_digest({
                "question_id": question.id, "answer": answer, "status": status,
                "mode": mode, "observer_run_id": observer_run_id,
            }),
        )
        stored, _ = self._append(entry)
        return _activity(stored)


class InMemoryIncidentWorkflowStore(IncidentWorkflowStore):
    """Offline reference implementation; explicitly not durable."""

    is_durable = False

    def __init__(self, incidents: InMemoryIncidentStore) -> None:
        self._incidents = incidents
        self._items: dict[str, _Entry] = {}
        self._lock = threading.RLock()

    def source(self, incident_id: str) -> IncidentSource:
        incident_id = incident_identifier(incident_id)
        incident = self._incidents.get(incident_id)
        if incident is None:
            raise KeyError(incident_id)
        return _source(incident.model_dump_json(), incident_id)

    def _entries(self, incident_id: str) -> list[_Entry]:
        with self._lock:
            return [entry.model_copy(deep=True) for entry in self._items.values()
                    if entry.incident_id == incident_id]

    def _entry(self, key: str) -> _Entry | None:
        with self._lock:
            prior = self._items.get(str(UUID(key)))
            return prior.model_copy(deep=True) if prior else None

    def state(self, incident_id: str) -> IncidentState:
        with self._lock, self._incidents._lock:
            return super().state(incident_id)

    def _append(self, entry: _Entry) -> tuple[_Entry, bool]:
        # Share the offline incident lock so an occurrence cannot slip between
        # checking its source revision and appending a closure.
        with self._lock, self._incidents._lock:
            self.source(entry.incident_id)
            prior = self._entry(entry.id)
            if prior is not None:
                _same_request(prior, entry)
                return prior, False
            if entry.kind == "resolution":
                _check_resolution(entry, self.state(entry.incident_id))
            self._items[entry.id] = entry.model_copy(deep=True)
            return entry.model_copy(deep=True), True

    def projections(self, incident_ids: Sequence[str]) -> dict[str, IncidentProjection]:
        ids = {incident_identifier(value) for value in incident_ids}
        with self._lock, self._incidents._lock:
            return {
                incident.id: IncidentProjection(
                    source=source,
                    tracking=_tracking(source, self._entries(incident.id)),
                )
                for incident in self._incidents.list_all() if incident.id in ids
                for source in [_source(incident.model_dump_json(), incident.id)]
            }

    def _all_projections(self) -> list[IncidentProjection]:
        return list(self.projections([row.id for row in self._incidents.list_all()]).values())

    def page(self, query: IncidentQuery) -> IncidentPage:
        with self._lock, self._incidents._lock:
            rows = [row for row in self._all_projections() if _matches(row, query)]
        rows.sort(
            key=lambda row: (row.source.incident.last_seen_at, row.source.incident.id), reverse=True,
        )
        return IncidentPage(
            items=rows[query.offset:query.offset + query.limit], total=len(rows),
            limit=query.limit, offset=query.offset,
        )

    def counts(self) -> dict[str, int]:
        with self._lock, self._incidents._lock:
            rows = self._all_projections()
        return {
            "needs_investigation": sum(
                row.source.incident.requires_investigation and row.tracking.status == "open"
                for row in rows
            ),
            "resolved": sum(row.tracking.status != "open" for row in rows),
            "resolved_by_user": sum(row.tracking.status == "resolved_by_user" for row in rows),
        }


def _revision_sql(alias: str) -> str:
    return f"LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {alias}.payload), 2))"


class AzureSqlIncidentWorkflowStore(IncidentWorkflowStore):
    """Read-through, fail-closed SQL state with single-statement append/CAS."""

    is_durable = True

    def __init__(
        self, db: AzureSqlDatabase, *, activity_table: str = DEFAULT_ACTIVITY_TABLE,
        incident_table: str = "triage_incidents",
    ) -> None:
        self._db = db
        self._table, self._incidents = map(quote_identifier, (activity_table, incident_table))
        if self._table.casefold() == self._incidents.casefold():
            raise ValueError("Incident activity and core incident tables must be distinct")

    def _query(self, sql: str, *params: Any) -> list[tuple]:
        try:
            return self._db.query(sql, *params)
        except Exception as exc:
            logger.error("Incident collaboration read failed (%s); no cached state used", type(exc).__name__)
            raise SqlUnavailable("Incident collaboration could not be read.") from exc

    def source(self, incident_id: str) -> IncidentSource:
        incident_id = incident_identifier(incident_id)
        rows = self._query(
            f"SELECT payload FROM {self._incidents} "
            "WHERE incident_id COLLATE Latin1_General_100_BIN2 = ?", incident_id,
        )
        if not rows:
            raise KeyError(incident_id)
        return _source(rows[0][0], incident_id)

    def _entry(self, key: str) -> _Entry | None:
        rows = self._query(f"SELECT payload FROM {self._table} WHERE activity_id = ?", str(UUID(key)))
        return _Entry.model_validate_json(rows[0][0]) if rows else None

    def _entries(self, incident_id: str) -> list[_Entry]:
        rows = self._query(
            f"SELECT payload FROM {self._table} WHERE incident_id = ? "
            "ORDER BY created_at, activity_id", incident_identifier(incident_id),
        )
        return [_Entry.model_validate_json(row[0]) for row in rows]

    def _append(self, entry: _Entry) -> tuple[_Entry, bool]:
        prior = self._entry(entry.id)
        if prior is not None:
            _same_request(prior, entry)
            return prior, False
        self.source(entry.incident_id)
        guards = [
            f"EXISTS (SELECT 1 FROM {self._incidents} AS i WITH (HOLDLOCK) "
            "WHERE i.incident_id COLLATE Latin1_General_100_BIN2 = ?)",
        ]
        guard_params: list[Any] = [entry.incident_id]
        if entry.kind == "resolution":
            _check_resolution(entry, self.state(entry.incident_id))
            if entry.tracking_version is None:
                raise ValueError("A resolution requires a tracking version")
            guards = [
                f"EXISTS (SELECT 1 FROM {self._incidents} AS i WITH (HOLDLOCK) "
                "WHERE i.incident_id COLLATE Latin1_General_100_BIN2 = ? "
                f"AND {_revision_sql('i')} = ? AND JSON_VALUE(i.payload, '$.status') <> 'resolved')",
                f"COALESCE((SELECT MAX(tracking_version) FROM {self._table} WITH (UPDLOCK, HOLDLOCK) "
                "WHERE incident_id = ?), 0) = ?",
            ]
            guard_params = [
                entry.incident_id, entry.source_revision,
                entry.incident_id, entry.tracking_version - 1,
            ]
        elif entry.kind == "answer":
            guards.append(
                f"EXISTS (SELECT 1 FROM {self._table} WHERE activity_id = ? "
                "AND incident_id = ? AND kind = 'question')",
            )
            guard_params.extend((entry.correlation_id, entry.incident_id))
        try:
            affected = self._db.execute(
                f"INSERT INTO {self._table} "
                "(activity_id, incident_id, kind, created_at, request_hash, tracking_version, "
                "source_revision, correlation_id, payload) "
                f"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE {' AND '.join(guards)}",
                entry.id, entry.incident_id, entry.kind, _sql_time(entry.created_at),
                entry.request_hash, entry.tracking_version, entry.source_revision,
                entry.correlation_id, entry.model_dump_json(), *guard_params,
            )
        except self._db.integrity_error() as exc:
            prior = self._entry(entry.id)
            if prior is not None:
                _same_request(prior, entry)
                return prior, False
            if entry.kind == "resolution":
                _check_resolution(entry, self.state(entry.incident_id))
            logger.error("Incident activity constraint rejected an insert (%s)", type(exc).__name__)
            raise SqlUnavailable("Incident activity insert was not confirmed.") from exc
        except Exception as exc:
            # The server may have committed before the connection failed.
            # Never replay here, especially when reserving a model call.
            logger.error("Incident activity write unconfirmed (%s); not retried", type(exc).__name__)
            raise SqlUnavailable("Incident activity write was not confirmed; reload before retrying.") from exc
        if affected == 1:
            return entry, True
        prior = self._entry(entry.id)
        if prior is not None:
            _same_request(prior, entry)
            return prior, False
        if affected == 0:
            self.source(entry.incident_id)
            raise WorkflowConflict("Incident evidence or tracking state changed. Reload before continuing.")
        raise SqlUnavailable("Incident activity insert returned no reliable affected-row count.")

    def _projection_from(self) -> str:
        # The core table inherits its database collation; the journal uses
        # binary identifiers. Make the cross-table comparison explicit.
        incident_id = "i.incident_id COLLATE Latin1_General_100_BIN2"
        return (
            f"FROM {self._incidents} AS i LEFT JOIN {self._table} AS r "
            f"ON r.incident_id = {incident_id} AND r.tracking_version = "
            f"(SELECT MAX(v.tracking_version) FROM {self._table} AS v WHERE v.incident_id = {incident_id})"
        )

    @staticmethod
    def _status_sql() -> str:
        status = "JSON_VALUE(i.payload, '$.status')"
        return (
            f"CASE WHEN {status} = 'resolved' THEN 'resolved' "
            f"WHEN r.source_revision = {_revision_sql('i')} THEN 'resolved_by_user' "
            "WHEN JSON_VALUE(i.payload, '$.requires_investigation') = 'true' "
            f"AND {status} = 'open' THEN 'needs_review' ELSE {status} END"
        )

    def _where(self, query: IncidentQuery) -> tuple[str, list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        status = self._status_sql()
        if query.status == "resolved":
            conditions.append(f"({status}) IN ('resolved', 'resolved_by_user')")
        elif query.status == "open":
            conditions.append(f"({status}) NOT IN ('resolved', 'resolved_by_user')")
        elif query.status in {"needs_review", "needs_investigation"}:
            conditions.extend((
                "JSON_VALUE(i.payload, '$.requires_investigation') = 'true'",
                f"({status}) NOT IN ('resolved', 'resolved_by_user')",
            ))
        elif query.status != "all":
            conditions.append(f"({status}) = ?")
            params.append(query.status)
        if query.workload != "all":
            conditions.append(
                "(CASE WHEN JSON_VALUE(i.payload, '$.source') LIKE 'fabric_pipeline%' "
                "THEN 'fabric_pipeline' ELSE 'powerbi' END) = ?",
            )
            params.append(query.workload)
        if query.query:
            # LIKE metacharacters are literal user text, not extra query syntax.
            term = query.query.lower()
            for char in ("\\", "%", "_", "["):
                term = term.replace(char, "\\" + char)
            # OPENJSON retains long scalar strings; JSON_VALUE silently returns
            # NULL above 4000 characters, including redaction truncation markers.
            conditions.append(
                "(LOWER(i.incident_id) COLLATE Latin1_General_100_BIN2 LIKE ? ESCAPE '\\' OR EXISTS ("
                "SELECT 1 FROM OPENJSON(i.payload) AS evidence "
                "WHERE evidence.[key] IN ('report_name', 'original_error', "
                "'diagnosed_root_cause', 'action_applied') "
                "AND LOWER(evidence.[value]) COLLATE Latin1_General_100_BIN2 LIKE ? ESCAPE '\\'))",
            )
            params.extend(["%" + term + "%"] * 2)
        return (" WHERE " + " AND ".join(conditions) if conditions else ""), params

    @staticmethod
    def _projection(payload: str, resolution: str | None) -> IncidentProjection:
        source = _source(payload)
        entries = [_Entry.model_validate_json(resolution)] if resolution else []
        return IncidentProjection(source=source, tracking=_tracking(source, entries))

    def page(self, query: IncidentQuery) -> IncidentPage:
        where, params = self._where(query)
        count = self._query(f"SELECT COUNT_BIG(*) {self._projection_from()}{where}", *params)
        rows = self._query(
            f"SELECT i.payload, r.payload {self._projection_from()}{where} "
            "ORDER BY JSON_VALUE(i.payload, '$.last_seen_at') DESC, "
            "i.incident_id COLLATE Latin1_General_100_BIN2 DESC "
            f"OFFSET {query.offset} ROWS FETCH NEXT {query.limit} ROWS ONLY", *params,
        )
        if len(count) != 1:
            raise SqlUnavailable("Incident count did not return one row")
        return IncidentPage(
            items=[self._projection(*row) for row in rows], total=int(count[0][0]),
            offset=query.offset, limit=query.limit,
        )

    def projections(self, incident_ids: Sequence[str]) -> dict[str, IncidentProjection]:
        ids = list(dict.fromkeys(incident_identifier(value) for value in incident_ids))
        if not ids:
            return {}
        if len(ids) > 1000:
            raise ValueError("At most 1000 incident projections may be requested at once")
        rows = self._query(
            f"SELECT i.payload, r.payload {self._projection_from()} "
            "WHERE i.incident_id COLLATE Latin1_General_100_BIN2 "
            f"IN ({', '.join('?' for _ in ids)})", *ids,
        )
        projections = [self._projection(*row) for row in rows]
        return {row.source.incident.id: row for row in projections}

    def counts(self) -> dict[str, int]:
        status = self._status_sql()
        rows = self._query(
            "SELECT "
            "COALESCE(SUM(CASE WHEN JSON_VALUE(i.payload, '$.requires_investigation') = 'true' "
            f"AND ({status}) NOT IN ('resolved', 'resolved_by_user') THEN 1 ELSE 0 END), 0), "
            f"COALESCE(SUM(CASE WHEN ({status}) IN ('resolved', 'resolved_by_user') THEN 1 ELSE 0 END), 0), "
            f"COALESCE(SUM(CASE WHEN ({status}) = 'resolved_by_user' THEN 1 ELSE 0 END), 0) "
            f"{self._projection_from()}",
        )
        if len(rows) != 1:
            raise SqlUnavailable("Incident counts did not return one row")
        return dict(zip(("needs_investigation", "resolved", "resolved_by_user"), map(int, rows[0]), strict=True))


def schema_statements(activity_table: str = DEFAULT_ACTIVITY_TABLE) -> list[str]:
    """Operator-only DDL. Runtime grants: SELECT, INSERT on this table only."""
    table = quote_identifier(activity_table)
    return [
        f"""
        IF OBJECT_ID('dbo.{activity_table}', 'U') IS NULL
        CREATE TABLE {table} (
            activity_id      NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
            incident_id      NVARCHAR(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
            kind             NVARCHAR(16) NOT NULL CHECK (kind IN ('note', 'resolution', 'question', 'answer')),
            created_at       DATETIME2(6) NOT NULL,
            request_hash     CHAR(64) NOT NULL,
            tracking_version BIGINT NULL,
            source_revision  CHAR(64) NULL,
            correlation_id   NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NULL,
            payload          NVARCHAR(MAX) NOT NULL,
            CHECK ((kind = 'resolution' AND tracking_version IS NOT NULL
                    AND tracking_version > 0 AND source_revision IS NOT NULL)
                OR (kind <> 'resolution' AND tracking_version IS NULL AND source_revision IS NULL)),
            INDEX ix_incident_activity_history (incident_id, created_at, activity_id)
        )""",
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_incident_activity_version'
            AND object_id = OBJECT_ID('dbo.{activity_table}', 'U'))
        CREATE UNIQUE INDEX ix_incident_activity_version ON {table} (incident_id, tracking_version)
            WHERE tracking_version IS NOT NULL""",
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_incident_activity_answer'
            AND object_id = OBJECT_ID('dbo.{activity_table}', 'U'))
        CREATE UNIQUE INDEX ix_incident_activity_answer ON {table} (correlation_id)
            WHERE kind = 'answer'""",
    ]
