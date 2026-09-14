"""Run history and at-most-once command dispatch.

Events contain bounded operational metadata, not model messages or thinking.
SQL reads go to the database on every call. An unavailable history store raises;
an unavailable command store must never authorize work from a local cache.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from triage.models import TriageResult
from triage.redaction import redact_text
from triage.store.fabric_sql import SqlUnavailable, quote_identifier

RunState = Literal["running", "completed", "failed"]
CommandState = Literal["queued", "running", "completed", "failed", "interrupted"]
CommandKind = Literal["powerbi_triage", "pipeline_sweep"]
CommandFinalState = Literal["completed", "failed"]
_Hash = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_MODEL_CONTENT = frozenset({
    "prompt", "prompts", "rawprompt", "systemprompt", "userprompt",
    "completion", "completions", "rawcompletion", "messages", "messagehistory",
    "inputmessages", "outputmessages", "thinking", "thoughts", "chainofthought",
    "reasoningcontent",
})
_SECRET_FIELDS = frozenset({
    "password", "pwd", "secret", "clientsecret", "apikey", "key", "token",
    "accesstoken", "refreshtoken", "accesskey", "accountkey", "authorization",
    "connectionstring", "sas", "sastoken", "credential", "credentials",
    "privatekey", "clientassertion", "sharedaccesskey", "sharedaccesssignature",
    "xapikey", "subscriptionkey", "ocpapimsubscriptionkey", "authtoken", "bearertoken",
})
_EXPIRED_SUMMARY = "Worker lease expired; execution was not replayed."


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _timestamp(value: str | datetime) -> str:
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("Command-center timestamps must include a UTC offset")
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def _sql_time(value: str | datetime) -> datetime:
    return datetime.fromisoformat(_timestamp(value)).replace(tzinfo=None)


def _from_sql_time(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    # DATETIME2 values are UTC by schema convention but the driver returns them
    # without tzinfo. Only this database boundary may supply that missing offset.
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return _timestamp(moment)


def _uuid(value: str) -> str:
    return str(UUID(value))


def _identifier(value: str, maximum: int, *, empty: bool = False) -> str:
    if empty and value == "":
        return value
    if not isinstance(value, str) or not 1 <= len(value) <= maximum:
        raise ValueError(f"Identifier must contain 1 to {maximum} characters")
    if not _ID_PATTERN.fullmatch(value) or redact_text(value) != value:
        raise ValueError("Identifier contains unsafe characters or credential material")
    return value


def _text(value: str, maximum: int = 4000) -> str:
    return redact_text(value)[:maximum]


def _field_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _redact_json(value: JsonValue) -> JsonValue:
    if isinstance(value, str):
        return _text(value)
    if isinstance(value, list):
        return [_redact_json(item) for item in value]
    if isinstance(value, dict):
        clean: dict[str, JsonValue] = {}
        for key, item in value.items():
            name = _text(key)
            if name in clean:
                raise ValueError("Redaction would collapse distinct evidence keys")
            normalized = _field_key(key)
            if normalized in _MODEL_CONTENT:
                clean[name] = "[OMITTED:model_content]"
            elif normalized in _SECRET_FIELDS:
                # A bare password in an arguments dictionary has no textual
                # "password=" prefix for the shared regex redactor to match.
                clean[name] = "[REDACTED:credential]"
            else:
                clean[name] = _redact_json(item)
        return clean
    return value


def _digest(value: dict[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class RunRecord(_Record):
    id: str
    request_id: str = Field(max_length=512)
    incident_id: str = Field(default="", max_length=200)
    signature: str = Field(default="", max_length=200)
    target: str = ""
    workload: str = Field(default="powerbi", min_length=1, max_length=50)
    agent_name: str = "TriageAgent"
    state: RunState = "running"
    outcome: str = ""
    summary: str = ""
    started_at: str = Field(default_factory=_utcnow)
    finished_at: str | None = None
    result: TriageResult | None = None

    _validate_id = field_validator("id")(_uuid)
    _validate_start = field_validator("started_at")(_timestamp)

    @field_validator("finished_at")
    @classmethod
    def _validate_finish(cls, value: str | None) -> str | None:
        return _timestamp(value) if value is not None else None

    @property
    def duration_ms(self) -> int:
        return self.result.wall_clock_ms if self.result is not None else 0

    @property
    def tool_calls(self) -> int:
        return self.result.tool_calls if self.result is not None else 0

    @property
    def tokens_used(self) -> int:
        return self.result.tokens_used if self.result is not None else 0

    @property
    def write_actions(self) -> int:
        return self.result.write_actions if self.result is not None else 0


class RunEvent(_Record):
    id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    sequence: int = Field(ge=0, le=2**63 - 1, strict=True)
    timestamp: str = Field(default_factory=_utcnow)
    kind: str = Field(min_length=1, max_length=64)
    label: str
    status: str = ""
    detail: str = ""
    tool_name: str = ""

    _validate_ids = field_validator("id", "run_id")(_uuid)
    _validate_timestamp = field_validator("timestamp")(_timestamp)

    @field_validator("kind")
    @classmethod
    def _metadata_only(cls, value: str) -> str:
        _identifier(value, 64)
        normalized = _field_key(value)
        if any(part in normalized for part in (
            "prompt", "completion", "thinking", "thought", "reasoning", "messages",
        )):
            raise ValueError("Run events cannot contain model prompts, completions, or thinking")
        return value


class CommandRecord(_Record):
    id: str
    kind: CommandKind
    target_id: str
    subject: str = ""
    body: str = ""
    actor_id: str
    actor_name: str = ""
    created_at: str = Field(default_factory=_utcnow)
    state: CommandState = "queued"
    started_at: str | None = None
    finished_at: str | None = None
    worker_id: str = ""
    lease_expires_at: str | None = None
    run_id: str = ""
    summary: str = ""
    request_hash: str = Field(default="", pattern=r"^(?:[a-f0-9]{64})?$")
    reconciled_by: str = ""
    reconciled_at: str | None = None
    reconciliation_reason: str = ""

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, 100)

    @field_validator("actor_id", "target_id")
    @classmethod
    def _validate_routing_id(cls, value: str) -> str:
        return _identifier(value, 200)

    @field_validator("worker_id", "reconciled_by")
    @classmethod
    def _validate_optional_identity(cls, value: str) -> str:
        return _identifier(value, 200, empty=True)

    @field_validator("run_id")
    @classmethod
    def _validate_run_id(cls, value: str) -> str:
        return _uuid(value) if value else ""

    _validate_created = field_validator("created_at")(_timestamp)

    @field_validator("started_at", "finished_at", "lease_expires_at", "reconciled_at")
    @classmethod
    def _validate_time(cls, value: str | None) -> str | None:
        return _timestamp(value) if value is not None else None


def _command_hash(command: CommandRecord) -> str:
    return _digest(command.model_dump(mode="json", include={
        "id", "kind", "target_id", "subject", "body", "actor_id", "actor_name",
    }))


def _new_command(command: CommandRecord) -> CommandRecord:
    command = CommandRecord.model_validate(command.model_dump(mode="json"))
    if (
        command.state != "queued" or command.started_at is not None
        or command.finished_at is not None or command.worker_id
        or command.lease_expires_at is not None or command.run_id or command.summary
        or command.reconciled_by or command.reconciled_at is not None
        or command.reconciliation_reason
    ):
        raise ValueError("A new command must be queued and have no execution state")
    # Never trust a caller-supplied hash. Replays must provide the original
    # request, not the redacted receipt returned by enqueue().
    return command.model_copy(update={
        "request_hash": _command_hash(command),
        "subject": _text(command.subject),
        "body": _text(command.body),
        "actor_name": _text(command.actor_name, 200),
    }, deep=True)


def _reconciled_command(
    prior: CommandRecord, actor_id: str, reason: str,
) -> CommandRecord:
    actor_id = _identifier(actor_id, 200)
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("Reconciliation requires a nonempty reason")
    if prior.state != "interrupted":
        raise ValueError("Only an interrupted command can be reconciled")
    reason = redact_text(reason.strip())
    if len(reason) > 4000:
        raise ValueError("Reconciliation reason must not exceed 4000 characters after redaction")
    separator = "\n\n" if prior.summary else ""
    # The SQL summary column is bounded. Never remove the original summary;
    # the separate reason field retains what cannot fit in the appended preview.
    summary = f"{prior.summary}{separator}Reconciled: {reason}"[:4000]
    return prior.model_copy(update={
        "state": "failed", "summary": summary,
        "reconciled_by": actor_id, "reconciled_at": _timestamp(_utcnow()),
        "reconciliation_reason": reason,
    }, deep=True)


def _new_run(record: RunRecord) -> tuple[RunRecord, str]:
    record = RunRecord.model_validate(record.model_dump(mode="json"))
    if record.state != "running" or record.result is not None or record.finished_at is not None:
        raise ValueError("A new run must be running with no terminal result")
    if record.outcome:
        raise ValueError("A new run cannot already have a terminal outcome")
    for value in (record.request_id, record.incident_id, record.signature, record.workload):
        if redact_text(value) != value:
            raise ValueError("Run identifiers cannot contain credential material")
    return record.model_copy(update={
        "target": _text(record.target),
        "agent_name": _text(record.agent_name, 200),
        "summary": _text(record.summary),
    }, deep=True), _digest(record.model_dump(mode="json"))


def _event(event: RunEvent) -> RunEvent:
    event = RunEvent.model_validate(event.model_dump(mode="json"))
    return event.model_copy(update={
        "label": _text(event.label, 200),
        "status": _text(event.status, 100),
        "detail": _text(event.detail),
        "tool_name": _text(event.tool_name, 200),
    }, deep=True)


def _finished_run(
    prior: RunRecord, result: TriageResult, incident_id: str,
) -> RunRecord:
    clean = TriageResult.model_validate(_redact_json(result.model_dump(mode="json")))
    if len(incident_id) > 200 or _text(incident_id) != incident_id:
        raise ValueError("Invalid incident identifier")
    if prior.state != "running":
        if prior.result != clean or (incident_id and prior.incident_id != incident_id):
            raise ValueError("Run already has a different terminal result")
        return prior
    return RunRecord.model_validate(prior.model_dump(mode="json") | {
        "state": "failed" if clean.outcome == "agent_crashed" else "completed",
        "outcome": clean.outcome,
        "summary": clean.summary,
        "signature": prior.signature or clean.signature,
        "incident_id": incident_id or prior.incident_id,
        "finished_at": _timestamp(clean.finished_at),
        "result": clean.model_dump(mode="json"),
    })


def _page(limit: int, offset: int = 0) -> tuple[int, int]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    if type(offset) is not int or not 0 <= offset <= 2**31 - 1:
        raise ValueError("offset must be a nonnegative 32-bit integer")
    return limit, offset


def _lease_seconds(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise ValueError("lease_seconds must be a positive 32-bit integer")
    return value


def _final_state(value: CommandFinalState) -> CommandFinalState:
    if value not in ("completed", "failed"):
        raise ValueError("A worker may finish a command only as completed or failed")
    return value


class CommandCenterStore(Protocol):
    @property
    def is_durable(self) -> bool: ...
    def start_run(self, record: RunRecord) -> None: ...
    def append_event(self, event: RunEvent) -> None: ...
    def finish_run(
        self, run_id: str, result: TriageResult, incident_id: str = "",
    ) -> RunRecord: ...
    def get_run(self, run_id: str) -> RunRecord | None: ...
    def list_runs(
        self, limit: int = 50, offset: int = 0, signature: str = "",
    ) -> list[RunRecord]: ...
    def count_runs(self, signature: str = "") -> int: ...
    def events(self, run_id: str) -> list[RunEvent]: ...
    def enqueue(self, command: CommandRecord) -> CommandRecord: ...
    def commands(self, limit: int = 50) -> list[CommandRecord]: ...
    def queued_commands(self, limit: int = 100) -> list[CommandRecord]: ...
    def eligible_commands(self, limit: int = 100) -> list[CommandRecord]: ...
    def active_commands(self, limit: int = 100) -> list[CommandRecord]: ...
    def target_blocked(self, target_id: str) -> bool: ...
    def get_command(self, id: str) -> CommandRecord | None: ...
    def claim_command(
        self, id: str, worker_id: str, lease_seconds: int = 660,
    ) -> CommandRecord | None: ...
    def finish_command(
        self, id: str, worker_id: str, state: CommandFinalState,
        summary: str, run_id: str = "",
    ) -> CommandRecord: ...
    def interrupt_command(
        self, id: str, worker_id: str, summary: str, run_id: str = "",
    ) -> CommandRecord: ...
    def reconcile_command(self, id: str, actor_id: str, reason: str) -> CommandRecord: ...
    def expire_commands(self, now: str | datetime | None = None) -> int: ...


class _Snapshot(_Record):
    version: Literal[1]
    runs: dict[str, RunRecord]
    run_hashes: dict[str, _Hash]
    events: dict[str, RunEvent]
    commands: dict[str, CommandRecord]

    @model_validator(mode="after")
    def _consistent_keys(self) -> _Snapshot:
        if self.runs.keys() != self.run_hashes.keys():
            raise ValueError("Run history is missing its original start fingerprints")
        for collection in (self.runs, self.events, self.commands):
            if any(key != record.id for key, record in collection.items()):
                raise ValueError("Stored record ID does not match its key")
        if any(event.run_id not in self.runs for event in self.events.values()):
            raise ValueError("Stored event references an unknown run")
        if any(not command.request_hash for command in self.commands.values()):
            raise ValueError("Stored command is missing its request fingerprint")
        return self


class InMemoryCommandCenterStore:
    """Thread-safe reference store. Copies never expose the internal records."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data = _Snapshot(version=1, runs={}, run_hashes={}, events={}, commands={})

    @property
    def is_durable(self) -> bool:
        return False

    def _persist(self, snapshot: _Snapshot) -> None:
        pass

    def _commit(self, **updates: Any) -> None:
        snapshot = self._data.model_copy(update=updates)
        # Persist before publishing in memory. A failed file write must not
        # leave a success-shaped claim or history entry in this instance.
        self._persist(snapshot)
        self._data = snapshot

    def start_run(self, record: RunRecord) -> None:
        clean, fingerprint = _new_run(record)
        with self._lock:
            if clean.id in self._data.runs:
                if self._data.run_hashes[clean.id] != fingerprint:
                    raise ValueError("Run ID conflicts with a different start record")
                return
            self._commit(
                runs=self._data.runs | {clean.id: clean},
                run_hashes=self._data.run_hashes | {clean.id: fingerprint},
            )

    def append_event(self, event: RunEvent) -> None:
        clean = _event(event)
        with self._lock:
            prior = self._data.events.get(clean.id)
            if prior is not None:
                if prior != clean:
                    raise ValueError("Event ID conflicts with different metadata")
                return
            if clean.run_id not in self._data.runs:
                raise ValueError("Cannot append an event to an unknown run")
            self._commit(events=self._data.events | {clean.id: clean})

    def finish_run(
        self, run_id: str, result: TriageResult, incident_id: str = "",
    ) -> RunRecord:
        with self._lock:
            prior = self._data.runs.get(_uuid(run_id))
            if prior is None:
                raise ValueError("Cannot finish an unknown run")
            finished = _finished_run(prior, result, incident_id)
            if prior.state == "running":
                self._commit(runs=self._data.runs | {prior.id: finished})
            return finished.model_copy(deep=True)

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._lock:
            record = self._data.runs.get(_uuid(run_id))
            return record.model_copy(deep=True) if record is not None else None

    def list_runs(
        self, limit: int = 50, offset: int = 0, signature: str = "",
    ) -> list[RunRecord]:
        limit, offset = _page(limit, offset)
        with self._lock:
            rows = sorted(
                (r for r in self._data.runs.values() if not signature or r.signature == signature),
                key=lambda r: (r.started_at, r.id), reverse=True,
            )
            return [row.model_copy(deep=True) for row in rows[offset:offset + limit]]

    def count_runs(self, signature: str = "") -> int:
        with self._lock:
            return sum(not signature or r.signature == signature for r in self._data.runs.values())

    def events(self, run_id: str) -> list[RunEvent]:
        run_id = _uuid(run_id)
        with self._lock:
            rows = sorted(
                (e for e in self._data.events.values() if e.run_id == run_id),
                key=lambda e: (e.sequence, e.timestamp, e.id),
            )
            return [row.model_copy(deep=True) for row in rows]

    def enqueue(self, command: CommandRecord) -> CommandRecord:
        clean = _new_command(command)
        with self._lock:
            prior = self._data.commands.get(clean.id)
            if prior is not None:
                if prior.request_hash != clean.request_hash:
                    raise ValueError("Command ID conflicts with a different request hash")
                return prior.model_copy(deep=True)
            self._commit(commands=self._data.commands | {clean.id: clean})
            return clean.model_copy(deep=True)

    def commands(self, limit: int = 50) -> list[CommandRecord]:
        limit, _ = _page(limit)
        with self._lock:
            rows = sorted(
                self._data.commands.values(), key=lambda c: (c.created_at, c.id), reverse=True,
            )
            return [row.model_copy(deep=True) for row in rows[:limit]]

    def queued_commands(self, limit: int = 100) -> list[CommandRecord]:
        """Filter before limiting so terminal history cannot hide pending work."""
        limit, _ = _page(limit)
        with self._lock:
            rows = sorted(
                (c for c in self._data.commands.values() if c.state == "queued"),
                key=lambda c: (c.created_at, c.id),
            )
            return [row.model_copy(deep=True) for row in rows[:limit]]

    def eligible_commands(self, limit: int = 100) -> list[CommandRecord]:
        """Filter target barriers before the limit; callers still claim and recheck."""
        limit, _ = _page(limit)
        with self._lock:
            blocked = {
                c.target_id for c in self._data.commands.values()
                if c.state in ("running", "interrupted")
            }
            rows = sorted(
                (c for c in self._data.commands.values()
                 if c.state == "queued" and c.target_id not in blocked),
                key=lambda c: (c.created_at, c.id),
            )
            return [row.model_copy(deep=True) for row in rows[:limit]]

    def active_commands(self, limit: int = 100) -> list[CommandRecord]:
        """Keep unfinished work discoverable independently of terminal history."""
        limit, _ = _page(limit)
        with self._lock:
            rows = sorted(
                (c for c in self._data.commands.values()
                 if c.state in ("queued", "running", "interrupted")),
                key=lambda c: (c.created_at, c.id),
            )
            return [row.model_copy(deep=True) for row in rows[:limit]]

    def target_blocked(self, target_id: str) -> bool:
        """Read target uncertainty while the caller holds its shared target claim."""
        target_id = _identifier(target_id, 200)
        with self._lock:
            return any(
                command.target_id == target_id and command.state in ("running", "interrupted")
                for command in self._data.commands.values()
            )

    def get_command(self, id: str) -> CommandRecord | None:
        with self._lock:
            record = self._data.commands.get(_identifier(id, 100))
            return record.model_copy(deep=True) if record is not None else None

    def claim_command(
        self, id: str, worker_id: str, lease_seconds: int = 660,
    ) -> CommandRecord | None:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        lease_seconds = _lease_seconds(lease_seconds)
        with self._lock:
            prior = self._data.commands.get(id)
            if prior is None or prior.state != "queued":
                return None
            now = _timestamp(_utcnow())
            lease = datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)
            claimed = prior.model_copy(update={
                "state": "running", "worker_id": worker_id,
                "started_at": now, "lease_expires_at": _timestamp(lease),
            })
            self._commit(commands=self._data.commands | {id: claimed})
            return claimed.model_copy(deep=True)

    def finish_command(
        self, id: str, worker_id: str, state: CommandFinalState,
        summary: str, run_id: str = "",
    ) -> CommandRecord:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        state = _final_state(state)
        run_id = _uuid(run_id) if run_id else ""
        with self._lock:
            now = _timestamp(_utcnow())
            prior = self._data.commands.get(id)
            if (
                prior is None or prior.state != "running" or prior.worker_id != worker_id
                or prior.lease_expires_at is None or prior.lease_expires_at <= now
            ):
                raise ValueError("Cannot finish command: wrong owner, state, or expired lease")
            finished = prior.model_copy(update={
                "state": state, "summary": _text(summary), "run_id": run_id, "finished_at": now,
            })
            self._commit(commands=self._data.commands | {id: finished})
            return finished.model_copy(deep=True)

    def interrupt_command(
        self, id: str, worker_id: str, summary: str, run_id: str = "",
    ) -> CommandRecord:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        run_id = _uuid(run_id) if run_id else ""
        with self._lock:
            prior = self._data.commands.get(id)
            if (
                prior is None or prior.worker_id != worker_id
                or prior.state not in ("running", "interrupted")
            ):
                raise ValueError("Cannot interrupt command: wrong owner or state")
            if prior.state == "interrupted":
                return prior.model_copy(deep=True)
            interrupted = prior.model_copy(update={
                "state": "interrupted", "summary": _text(summary),
                "run_id": run_id or prior.run_id, "finished_at": _timestamp(_utcnow()),
            })
            self._commit(commands=self._data.commands | {id: interrupted})
            return interrupted.model_copy(deep=True)

    def reconcile_command(self, id: str, actor_id: str, reason: str) -> CommandRecord:
        id = _identifier(id, 100)
        with self._lock:
            prior = self._data.commands.get(id)
            if prior is None:
                raise ValueError("Only an interrupted command can be reconciled")
            reconciled = _reconciled_command(prior, actor_id, reason)
            self._commit(commands=self._data.commands | {id: reconciled})
            return reconciled.model_copy(deep=True)

    def expire_commands(self, now: str | datetime | None = None) -> int:
        now = _timestamp(now if now is not None else _utcnow())
        with self._lock:
            expired = {
                key: command.model_copy(update={
                    "state": "interrupted", "finished_at": now, "summary": _EXPIRED_SUMMARY,
                })
                for key, command in self._data.commands.items()
                if command.state == "running"
                and (command.lease_expires_at is None or command.lease_expires_at <= now)
            }
            if expired:
                self._commit(commands=self._data.commands | expired)
            return len(expired)


class JsonFileCommandCenterStore(InMemoryCommandCenterStore):
    """Offline restart persistence for one store instance in one process.

    Writes are atomic and threads using this instance share its lock. Separate
    instances/processes must not share a file; use SQL for a shared command queue.
    is_durable remains False so this file cannot authorize live distributed work.
    """

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        if self.path.exists():
            self._data = _Snapshot.model_validate_json(self.path.read_text(encoding="utf-8"))

    def _persist(self, snapshot: _Snapshot) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.path.parent,
                prefix=f".{self.path.name}.", suffix=".tmp", delete=False,
            ) as output:
                temp_path = Path(output.name)
                json.dump(snapshot.model_dump(mode="json"), output, ensure_ascii=True)
                output.flush()
                os.fsync(output.fileno())
            temp_path.replace(self.path)
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)


class _Database(Protocol):
    @property
    def is_available(self) -> bool: ...
    def execute(self, sql: str, *params: Any) -> int: ...
    def query(self, sql: str, *params: Any) -> list[tuple]: ...
    def integrity_error(self) -> type[Exception]: ...


class FabricSqlCommandCenterStore:
    """Read-through history and conditional command transitions, without fallback.

    Deployment installs the schema. Runtime identities need data privileges,
    never DDL privileges or access to the shared approval procedure definition.
    """

    def __init__(
        self, db: _Database, run_table: str = "triage_agent_runs",
        event_table: str = "triage_agent_events", command_table: str = "triage_agent_commands",
    ) -> None:
        self._db = db
        self._runs = quote_identifier(run_table)
        self._events = quote_identifier(event_table)
        self._commands = quote_identifier(command_table)
        if len({self._runs.casefold(), self._events.casefold(), self._commands.casefold()}) != 3:
            raise ValueError("Command-center tables must have distinct names")

    @property
    def is_durable(self) -> bool:
        return self._db.is_available

    def _ready(self) -> None:
        if not self._db.is_available:
            detail = getattr(self._db, "connection_error", "")
            message = "Command-center SQL history and command arbitration are unavailable"
            raise SqlUnavailable(f"{message}: {detail}" if detail else message)

    @staticmethod
    def _one_row(affected: int) -> None:
        if affected != 1:
            raise RuntimeError("Command-center write did not affect exactly one row")

    def start_run(self, record: RunRecord) -> None:
        clean, fingerprint = _new_run(record)
        self._ready()
        try:
            affected = self._db.execute(
                f"INSERT INTO {self._runs} "
                "(run_id, request_id, incident_id, signature, started_at, state, "
                "outcome, finished_at, start_hash, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                clean.id, clean.request_id, clean.incident_id, clean.signature,
                _sql_time(clean.started_at), clean.state, clean.outcome, None,
                fingerprint, clean.model_dump_json(),
            )
        except self._db.integrity_error():
            rows = self._db.query(
                f"SELECT start_hash FROM {self._runs} WHERE run_id = ?", clean.id,
            )
            if not rows:
                raise
            if rows[0][0] != fingerprint:
                raise ValueError("Run ID conflicts with a different start record") from None
            return
        self._one_row(affected)

    def append_event(self, event: RunEvent) -> None:
        clean = _event(event)
        self._ready()
        if self.get_run(clean.run_id) is None:
            raise ValueError("Cannot append an event to an unknown run")
        try:
            affected = self._db.execute(
                f"INSERT INTO {self._events} "
                "(event_id, run_id, sequence, occurred_at, kind, payload) VALUES (?, ?, ?, ?, ?, ?)",
                clean.id, clean.run_id, clean.sequence, _sql_time(clean.timestamp),
                clean.kind, clean.model_dump_json(),
            )
        except self._db.integrity_error():
            rows = self._db.query(
                f"SELECT payload FROM {self._events} WHERE event_id = ?", clean.id,
            )
            if not rows:
                raise
            if RunEvent.model_validate_json(rows[0][0]) != clean:
                raise ValueError("Event ID conflicts with different metadata") from None
            return
        self._one_row(affected)

    def finish_run(
        self, run_id: str, result: TriageResult, incident_id: str = "",
    ) -> RunRecord:
        prior = self.get_run(run_id)
        if prior is None:
            raise ValueError("Cannot finish an unknown run")
        finished = _finished_run(prior, result, incident_id)
        if prior.state != "running":
            return finished
        affected = self._db.execute(
            f"UPDATE {self._runs} SET incident_id = ?, signature = ?, state = ?, "
            "outcome = ?, finished_at = ?, payload = ? WHERE run_id = ? AND state = 'running'",
            finished.incident_id, finished.signature, finished.state, finished.outcome,
            _sql_time(finished.finished_at), finished.model_dump_json(), prior.id,
        )
        if affected == 0:
            current = self.get_run(prior.id)
            if current is None or current.state == "running":
                raise RuntimeError("Run completion was not recorded")
            return _finished_run(current, result, incident_id)
        self._one_row(affected)
        return finished

    def get_run(self, run_id: str) -> RunRecord | None:
        self._ready()
        rows = self._db.query(
            f"SELECT payload FROM {self._runs} WHERE run_id = ?", _uuid(run_id),
        )
        return RunRecord.model_validate_json(rows[0][0]) if rows else None

    def list_runs(
        self, limit: int = 50, offset: int = 0, signature: str = "",
    ) -> list[RunRecord]:
        limit, offset = _page(limit, offset)
        self._ready()
        where, params = (" WHERE signature = ?", (signature,)) if signature else ("", ())
        rows = self._db.query(
            f"SELECT payload FROM {self._runs}{where} ORDER BY started_at DESC, run_id DESC "
            f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY", *params,
        )
        return [RunRecord.model_validate_json(row[0]) for row in rows]

    def count_runs(self, signature: str = "") -> int:
        self._ready()
        where, params = (" WHERE signature = ?", (signature,)) if signature else ("", ())
        rows = self._db.query(f"SELECT COUNT_BIG(*) FROM {self._runs}{where}", *params)
        if len(rows) != 1:
            raise RuntimeError("SQL run count did not return one row")
        return int(rows[0][0])

    def events(self, run_id: str) -> list[RunEvent]:
        self._ready()
        rows = self._db.query(
            f"SELECT payload FROM {self._events} WHERE run_id = ? "
            "ORDER BY sequence, occurred_at, event_id", _uuid(run_id),
        )
        return [RunEvent.model_validate_json(row[0]) for row in rows]

    def enqueue(self, command: CommandRecord) -> CommandRecord:
        clean = _new_command(command)
        self._ready()
        try:
            affected = self._db.execute(
                f"INSERT INTO {self._commands} "
                "(command_id, kind, target_id, created_at, state, started_at, finished_at, "
                "worker_id, lease_expires_at, run_id, summary, request_hash, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                clean.id, clean.kind, clean.target_id, _sql_time(clean.created_at),
                clean.state, None, None, "", None, "", "", clean.request_hash, clean.model_dump_json(),
            )
        except self._db.integrity_error():
            prior = self.get_command(clean.id)
            if prior is None:
                raise
            if prior.request_hash != clean.request_hash:
                raise ValueError("Command ID conflicts with a different request hash") from None
            return prior
        self._one_row(affected)
        return clean

    @staticmethod
    def _command_columns() -> str:
        return (
            "payload, state, started_at, finished_at, worker_id, "
            "lease_expires_at, run_id, summary, request_hash"
        )

    @staticmethod
    def _command_row(row: tuple) -> CommandRecord:
        payload, state, started, finished, worker, lease, run, summary, fingerprint = row
        # Payload contains the immutable, redacted request. These columns are
        # authoritative after CAS updates; reading payload.state would requeue
        # work that is already running or interrupted.
        raw = json.loads(payload)
        return CommandRecord.model_validate(raw | {
            "state": state, "started_at": _from_sql_time(started),
            "finished_at": _from_sql_time(finished), "worker_id": worker,
            "lease_expires_at": _from_sql_time(lease), "run_id": run,
            "summary": summary, "request_hash": fingerprint,
        })

    def commands(self, limit: int = 50) -> list[CommandRecord]:
        limit, _ = _page(limit)
        self._ready()
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} "
            f"ORDER BY created_at DESC, command_id DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
        )
        return [self._command_row(row) for row in rows]

    def queued_commands(self, limit: int = 100) -> list[CommandRecord]:
        limit, _ = _page(limit)
        self._ready()
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} WHERE state = 'queued' "
            f"ORDER BY created_at ASC, command_id ASC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
        )
        return [self._command_row(row) for row in rows]

    def eligible_commands(self, limit: int = 100) -> list[CommandRecord]:
        limit, _ = _page(limit)
        self._ready()
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} AS candidate "
            "WHERE candidate.state = 'queued' AND NOT EXISTS ("
            f"SELECT 1 FROM {self._commands} AS blocker "
            "WHERE blocker.target_id = candidate.target_id "
            "AND blocker.state IN ('running', 'interrupted')) "
            "ORDER BY candidate.created_at ASC, candidate.command_id ASC "
            f"OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
        )
        return [self._command_row(row) for row in rows]

    def active_commands(self, limit: int = 100) -> list[CommandRecord]:
        limit, _ = _page(limit)
        self._ready()
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} "
            "WHERE state IN ('queued', 'running', 'interrupted') "
            f"ORDER BY created_at ASC, command_id ASC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
        )
        return [self._command_row(row) for row in rows]

    def target_blocked(self, target_id: str) -> bool:
        target_id = _identifier(target_id, 200)
        self._ready()
        rows = self._db.query(
            f"SELECT command_id FROM {self._commands} "
            "WHERE target_id = ? AND state IN ('running', 'interrupted') "
            "ORDER BY command_id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
            target_id,
        )
        return bool(rows)

    def get_command(self, id: str) -> CommandRecord | None:
        self._ready()
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} WHERE command_id = ?",
            _identifier(id, 100),
        )
        return self._command_row(rows[0]) if rows else None

    def claim_command(
        self, id: str, worker_id: str, lease_seconds: int = 660,
    ) -> CommandRecord | None:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        lease_seconds = _lease_seconds(lease_seconds)
        self._ready()
        affected = self._db.execute(
            f"UPDATE {self._commands} SET state = 'running', worker_id = ?, "
            "started_at = SYSUTCDATETIME(), "
            "lease_expires_at = DATEADD(second, ?, SYSUTCDATETIME()) "
            "WHERE command_id = ? AND state = 'queued'",
            worker_id, lease_seconds, id,
        )
        if affected == 0:
            return None
        self._one_row(affected)
        rows = self._db.query(
            f"SELECT {self._command_columns()} FROM {self._commands} "
            "WHERE command_id = ? AND state = 'running' AND worker_id = ? "
            "AND lease_expires_at > SYSUTCDATETIME()",
            id, worker_id,
        )
        if len(rows) != 1:
            raise RuntimeError("Claimed command no longer has a live lease for this worker")
        return self._command_row(rows[0])

    def finish_command(
        self, id: str, worker_id: str, state: CommandFinalState,
        summary: str, run_id: str = "",
    ) -> CommandRecord:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        state = _final_state(state)
        run_id = _uuid(run_id) if run_id else ""
        self._ready()
        affected = self._db.execute(
            f"UPDATE {self._commands} SET state = ?, finished_at = SYSUTCDATETIME(), "
            "summary = ?, run_id = ? WHERE command_id = ? AND state = 'running' "
            "AND worker_id = ? AND lease_expires_at > SYSUTCDATETIME()",
            state, _text(summary), run_id, id, worker_id,
        )
        if affected == 0:
            raise ValueError("Cannot finish command: wrong owner, state, or expired lease")
        self._one_row(affected)
        finished = self.get_command(id)
        if finished is None or finished.state != state or finished.worker_id != worker_id:
            raise RuntimeError("Command completion could not be read back")
        return finished

    def interrupt_command(
        self, id: str, worker_id: str, summary: str, run_id: str = "",
    ) -> CommandRecord:
        id = _identifier(id, 100)
        worker_id = _identifier(worker_id, 200)
        run_id = _uuid(run_id) if run_id else ""
        self._ready()
        affected = self._db.execute(
            f"UPDATE {self._commands} SET state = 'interrupted', finished_at = SYSUTCDATETIME(), "
            "summary = ?, run_id = CASE WHEN ? = '' THEN run_id ELSE ? END "
            "WHERE command_id = ? AND state = 'running' AND worker_id = ?",
            _text(summary), run_id, run_id, id, worker_id,
        )
        if affected != 0:
            self._one_row(affected)
        interrupted = self.get_command(id)
        if (
            interrupted is not None and interrupted.state == "interrupted"
            and interrupted.worker_id == worker_id
        ):
            return interrupted
        if affected == 0:
            raise ValueError("Cannot interrupt command: wrong owner or state")
        raise RuntimeError("Command interruption could not be read back")

    def reconcile_command(self, id: str, actor_id: str, reason: str) -> CommandRecord:
        prior = self.get_command(id)
        if prior is None:
            raise ValueError("Only an interrupted command can be reconciled")
        reconciled = _reconciled_command(prior, actor_id, reason)
        # Interrupted records cannot be edited by workers. The state predicate
        # makes competing operator reconciliations single-assignment as well.
        affected = self._db.execute(
            f"UPDATE {self._commands} SET state = 'failed', summary = ?, payload = ? "
            "WHERE command_id = ? AND state = 'interrupted'",
            reconciled.summary, reconciled.model_dump_json(), prior.id,
        )
        if affected == 0:
            raise ValueError("Only an interrupted command can be reconciled")
        self._one_row(affected)
        return reconciled

    def expire_commands(self, now: str | datetime | None = None) -> int:
        self._ready()
        time_sql = "SYSUTCDATETIME()" if now is None else "?"
        params = (
            (_EXPIRED_SUMMARY,)
            if now is None else (_sql_time(now), _EXPIRED_SUMMARY, _sql_time(now))
        )
        affected = self._db.execute(
            f"UPDATE {self._commands} SET state = 'interrupted', finished_at = {time_sql}, "
            "summary = ? WHERE state = 'running' "
            f"AND (lease_expires_at IS NULL OR lease_expires_at <= {time_sql})",
            *params,
        )
        if affected < 0:
            raise RuntimeError("Command expiration did not return an affected-row count")
        return affected


def schema_statements(
    run_table: str = "triage_agent_runs", event_table: str = "triage_agent_events",
    command_table: str = "triage_agent_commands",
) -> list[str]:
    """Idempotent CREATE TABLE statements; integration owns schema installation."""
    runs, events, commands = map(quote_identifier, (run_table, event_table, command_table))
    if len({runs.casefold(), events.casefold(), commands.casefold()}) != 3:
        raise ValueError("Command-center tables must have distinct names")
    return [
        f"""
        IF OBJECT_ID('dbo.{run_table}', 'U') IS NULL
        CREATE TABLE {runs} (
            run_id       NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
            request_id   NVARCHAR(512) NOT NULL,
            incident_id  NVARCHAR(200) NOT NULL,
            signature    NVARCHAR(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
            started_at   DATETIME2(6)  NOT NULL,
            state        NVARCHAR(20) NOT NULL CHECK (state IN ('running', 'completed', 'failed')),
            outcome      NVARCHAR(50) NOT NULL,
            finished_at  DATETIME2(6)  NULL,
            start_hash   CHAR(64)      NOT NULL,
            payload      NVARCHAR(MAX) NOT NULL,
            INDEX ix_run_history (started_at DESC, run_id DESC),
            INDEX ix_run_signature (signature, started_at DESC, run_id DESC)
        )""",
        f"""
        IF OBJECT_ID('dbo.{event_table}', 'U') IS NULL
        CREATE TABLE {events} (
            event_id     NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
            run_id       NVARCHAR(36) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES {runs}(run_id),
            sequence     BIGINT       NOT NULL CHECK (sequence >= 0),
            occurred_at  DATETIME2(6)  NOT NULL,
            kind         NVARCHAR(64) NOT NULL,
            payload      NVARCHAR(MAX) NOT NULL,
            INDEX ix_run_events (run_id, sequence, occurred_at, event_id)
        )""",
        f"""
        IF OBJECT_ID('dbo.{command_table}', 'U') IS NULL
        CREATE TABLE {commands} (
            command_id       NVARCHAR(100) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
            kind             NVARCHAR(40) NOT NULL CHECK (kind IN ('powerbi_triage', 'pipeline_sweep')),
            target_id        NVARCHAR(200) NOT NULL,
            created_at       DATETIME2(6)  NOT NULL,
            state            NVARCHAR(20) NOT NULL CHECK
                (state IN ('queued', 'running', 'completed', 'failed', 'interrupted')),
            started_at       DATETIME2(6)  NULL,
            finished_at      DATETIME2(6)  NULL,
            worker_id        NVARCHAR(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
            lease_expires_at DATETIME2(6)  NULL,
            run_id           NVARCHAR(36) NOT NULL,
            summary          NVARCHAR(4000) NOT NULL,
            request_hash     CHAR(64)      NOT NULL,
            payload          NVARCHAR(MAX) NOT NULL,
            INDEX ix_command_history (created_at DESC, command_id DESC),
            INDEX ix_command_expiry (state, lease_expires_at)
        )""",
    ]
