"""Azure SQL Database plumbing shared by every durable store.

Application state is relational: incidents have occurrences, approvals belong
to actions and deferred retries belong to signatures. One database holds these
records and the monitoring registry so their receipts, leases and finalization
can share a transaction. It is independent of Fabric capacity and either UI.

Two properties govern the store boundary:

* **A conditional ``UPDATE`` is atomic on its own.** The claim and lease stores
  previously did read-then-write guarded by an ETag, which is three round trips
  and a race the code had to reason about explicitly. ``UPDATE ... WHERE
  expires_at < SYSUTCDATETIME()`` is one statement, and ``rowcount`` says
  whether this caller won. A primary-key ``INSERT`` that raises
  ``IntegrityError`` is the same compare-and-set the old code got from
  ``ResourceExistsError``.
* **Runtime authentication uses Entra tokens only.** Deployment enables
  Entra-only authentication on the logical server and private connectivity.
  This adapter has no SQL-login, password or developer-identity fallback.

Why ``mssql-python`` and not ``pyodbc``
--------------------------------------
The controller runs as a Foundry hosted agent: a managed Linux image built with
``dependency_resolution: remote_build``, which installs ``src/requirements.txt``
with pip and nothing else. ``pyodbc`` needs the ``msodbcsql18`` system driver,
which is an apt package and cannot be pip-installed, so it cannot work there.
``mssql-python`` ships the driver inside the wheel as a normal dependency
(``mssql-python-odbc``) and publishes a cp313 manylinux build matching the
container runtime. The driver was verified on the previous SQL-backed deployment;
the Azure SQL target and every deployed identity require their own live proof.

Reconnection is deliberate, not incidental
------------------------------------------
An earlier version of the storage layer opened its client once in ``__init__``
and, if that failed, degraded to in-memory for the life of the process. That is
a silent data-loss bug: tenant policy disabled public network access on the
storage account minutes after it was created, the container started while the
account was unreachable, and it then reported healthy triage outcomes while
persisting none of them. Restoring connectivity changed nothing, because
nothing ever tried again. Three invocations were lost that way before a forced
redeploy fixed it.

So availability here is re-evaluated on use, with a cooldown so a genuinely
dead database does not turn every write into a connection attempt.
"""

from __future__ import annotations

import logging
import re
import struct
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from triage.redaction import redact_text

logger = logging.getLogger("triage.store.sql")

#: SQL_COPT_SS_ACCESS_TOKEN. Pre-login connection attribute carrying an Entra
#: token, which is how a driver authenticates without a password.
SQL_COPT_SS_ACCESS_TOKEN = 1256

#: The Azure SQL token audience.
SQL_SCOPE = "https://database.windows.net/.default"

#: How long to wait before retrying a database that failed to open. Long enough
#: that a hard outage is not hammered, short enough that a run a minute later
#: picks the database back up without a redeploy.
RECONNECT_COOLDOWN_SECONDS = 30.0

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def quote_identifier(name: str) -> str:
    """Validate and bracket a table name that came from configuration.

    Table names are operator-supplied settings, and they are interpolated into
    DDL and DML because SQL has no parameter placeholder for an identifier.
    Anything that is not a plain identifier is rejected outright rather than
    escaped, because the set of legitimate values here is small and known.
    """
    if not _IDENTIFIER.match(name or ""):
        raise ValueError(
            f"Refusing to use {name!r} as a SQL table name. Use letters, "
            "digits and underscores, starting with a letter or underscore."
        )
    return f"[dbo].[{name}]"


def _token_struct(token: str) -> bytes:
    """Pack a token the way the ODBC access-token attribute expects it."""
    raw = token.encode("utf-16-le")
    return struct.pack("<I", len(raw)) + raw


class AzureSqlDatabase:
    """Connections to an Azure SQL Database, shared by the durable stores.

    One handle, but **one connection per thread**. The driver's connections are
    not thread-safe, and serialising every statement behind a single lock was
    tried first: under eight concurrent callers it produced an
    ``OperationalError`` followed by ``InterfaceError`` on every subsequent
    use, because the connection was already being torn down when the next
    thread reached it. Thread-local connections cost one login per thread --
    threads here are few and long-lived -- and remove the failure mode
    entirely.

    A dead connection is replaced on the next call. That is deliberately *not*
    treated the same as an unreachable database: the first is routine and
    should be retried immediately, the second deserves a back-off. Conflating
    them meant one broken connection silently disabled every store for the
    cooldown period, which the live verification caught.
    """

    def __init__(
        self,
        *,
        server: str,
        database: str,
        credential: Any = None,
        cooldown_seconds: float = RECONNECT_COOLDOWN_SECONDS,
        tables: dict[str, str] | None = None,
    ) -> None:
        self._server = server
        self._database = database
        self._credential = credential
        self._cooldown = cooldown_seconds
        self._tables = tables
        self._schema_ready = False
        self._local = threading.local()
        self._state_lock = threading.Lock()
        # Fresh hosted sandboxes can have under 30 seconds of monotonic uptime.
        # Zero is a valid failure time, not a never-failed sentinel.
        self._last_failure_at: float | None = None
        self._last_error = ""
        self._last_error_detail = ""

    def ensure_schema_once(self) -> bool:
        """Deployment-only schema creation; runtime stores must never call it."""
        if self._schema_ready:
            return True
        if not ensure_schema(self, self._tables):
            return False
        self._schema_ready = True
        return True

    # --- connection --------------------------------------------------------

    @property
    def target(self) -> str:
        return f"{self._database} on {self._server}"

    @property
    def connection_error(self) -> str:
        """Keep a sanitized cause available when hosted console logs are unavailable."""
        with self._state_lock:
            return f"{self._last_error}: {self._last_error_detail}" if self._last_error else ""

    def _connect(self) -> Any:
        import mssql_python

        credential = self._credential
        if credential is None:
            from azure.identity import DefaultAzureCredential

            # A failed service identity must not silently become a developer.
            # Operator commands inject an explicitly selected credential.
            credential = DefaultAzureCredential(
                exclude_environment_credential=True,
                exclude_cli_credential=True,
                exclude_developer_cli_credential=True,
                exclude_interactive_browser_credential=True,
                exclude_shared_token_cache_credential=True,
                exclude_visual_studio_code_credential=True,
                exclude_powershell_credential=True,
                exclude_broker_credential=True,
            )

        token = credential.get_token(SQL_SCOPE).token
        # 'Connection Timeout' is rejected by this driver's connection-string
        # parser, which is why it is absent.
        conn_str = (
            f"Server={self._server};Database={self._database};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        # autocommit is False by default in this driver, and leaving it that way
        # was a bug: every read opened a transaction that nothing closed, and a
        # duplicate-key insert -- which callers here catch deliberately -- left
        # the connection with a failed transaction attached for the next
        # statement to trip over. Ordinary statements use autocommit;
        # transaction() disables it only for an explicit synchronous unit.
        return mssql_python.connect(
            conn_str,
            autocommit=True,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _token_struct(token)},
        )

    def _ensure(self) -> Any | None:
        """Return this thread's connection, opening one if needed.

        Returns ``None`` when opening a connection is unavailable. Operational
        stores fail closed and retry their read on subsequent use. A transaction
        never reconnects: that would execute its remaining writes outside the
        transaction whose connection was lost.
        """
        if getattr(self._local, "transaction_failed", False):
            raise SqlTransactionAborted("A statement failed in the current SQL transaction")
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        if getattr(self._local, "transaction_active", False):
            raise SqlTransactionAborted("The SQL transaction connection was lost")

        with self._state_lock:
            unreachable = (
                self._last_failure_at is not None
                and time.monotonic() - self._last_failure_at < self._cooldown
            )
        if unreachable:
            return None

        try:
            conn = self._connect()
        except Exception as exc:  # noqa: BLE001 - every failure is the same here
            detail = redact_text(str(exc))[:400]
            with self._state_lock:
                self._last_failure_at = time.monotonic()
                self._last_error = type(exc).__name__
                self._last_error_detail = detail
            # The message matters more than the class here. "Login failed for
            # user '<token-identified principal>'" means the identity
            # authenticated but has no database user; a timeout means the
            # server name or the network is wrong. Those need different fixes,
            # and the class name alone cannot tell them apart.
            logger.error(
                "Cannot reach Azure SQL (%s): %s: %s. Retrying in %.0fs.",
                self.target,
                type(exc).__name__,
                detail,
                self._cooldown,
            )
            return None

        self._local.conn = conn
        with self._state_lock:
            if self._last_error:
                logger.info("Reconnected to Azure SQL (%s)", self.target)
            self._last_error = ""
            self._last_error_detail = ""
            self._last_failure_at = None
        return conn

    def _drop(self) -> None:
        """Discard this thread's connection so the next call opens a fresh one.

        Explicitly does not start the unreachable cooldown. A connection that
        died mid-statement says nothing about whether the database is up, and
        treating it as an outage took every store down for 30 seconds after a
        single dropped connection.
        """
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover - already broken
                pass

    @property
    def is_available(self) -> bool:
        return self._ensure() is not None

    # --- statements --------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[AzureSqlDatabase]:
        """Join synchronous statements on this thread in one explicit transaction.

        Do not await inside this context. A caught statement error or
        interruption still aborts the transaction. An interrupted acknowledgement
        discards the connection and propagates with a reconciliation note.
        Existing stores using this handle join the same connection.
        """
        if getattr(self._local, "transaction_active", False):
            self._local.transaction_failed = True
            raise SqlTransactionAborted("Nested SQL transactions are not supported")
        conn = self._ensure()
        if conn is None:
            raise SqlUnavailable(f"Azure SQL unavailable ({self.target})")
        if not conn.autocommit:
            self._drop()
            raise SqlTransactionAborted("The SQL connection has an unowned transaction")

        self._local.transaction_active = True
        self._local.transaction_failed = False
        try:
            try:
                conn.autocommit = False
                yield self
                if self._local.transaction_failed or getattr(self._local, "conn", None) is not conn:
                    raise SqlTransactionAborted("The SQL transaction did not complete all statements")
            except BaseException as body_error:
                try:
                    conn.rollback()
                except BaseException as exc:
                    logger.error(
                        "SQL transaction rollback was not acknowledged (%s); discarding connection",
                        type(exc).__name__,
                    )
                    self._drop()
                    message = "SQL rollback was not acknowledged; reconcile before continuing"
                    # Cleanup must not turn cancellation or process exit into an
                    # ordinary database error that a caller might retry.
                    if not isinstance(body_error, Exception):
                        body_error.add_note(message)
                        raise body_error from exc
                    if not isinstance(exc, Exception):
                        exc.add_note(message)
                        raise
                    raise SqlRollbackUncertain(message) from exc
                raise
            else:
                try:
                    conn.commit()
                except BaseException as exc:
                    logger.error(
                        "SQL commit acknowledgement is uncertain (%s); reconcile before retrying",
                        type(exc).__name__,
                    )
                    self._drop()
                    message = "SQL commit was not acknowledged; reconcile the durable operation identity"
                    if not isinstance(exc, Exception):
                        exc.add_note(message)
                        raise
                    raise SqlCommitUncertain(message) from exc
        finally:
            self._local.transaction_active = False
            self._local.transaction_failed = False
            if getattr(self._local, "conn", None) is conn:
                try:
                    conn.autocommit = True
                except BaseException as exc:
                    logger.warning(
                        "Could not restore SQL connection mode (%s); discarding connection",
                        type(exc).__name__,
                    )
                    self._drop()
                    if not isinstance(exc, Exception):
                        raise

    def execute(self, sql: str, *params: Any) -> int:
        """Run a statement and return the number of rows it affected.

        ``rowcount`` is the whole point for the conditional updates that
        arbitrate claims and leases: 1 means this caller won, 0 means it lost,
        and no second round trip is needed to find out.
        """
        conn = self._ensure()
        if conn is None:
            raise SqlUnavailable(f"Azure SQL unavailable ({self.target})")
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(sql, *params)
            return cur.rowcount
        except BaseException as exc:
            if getattr(self._local, "transaction_active", False):
                self._local.transaction_failed = True
            if _is_connection_error(exc):
                self._drop()
            raise
        finally:
            self._close_cursor(cur)

    def query(self, sql: str, *params: Any) -> list[tuple]:
        conn = self._ensure()
        if conn is None:
            raise SqlUnavailable(f"Azure SQL unavailable ({self.target})")
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(sql, *params)
            # Driver rows are sequences but may compare by identity, not values.
            return [tuple(row) for row in cur.fetchall()]
        except BaseException as exc:
            if getattr(self._local, "transaction_active", False):
                self._local.transaction_failed = True
            if _is_connection_error(exc):
                self._drop()
            raise
        finally:
            self._close_cursor(cur)

    def _close_cursor(self, cursor: Any) -> None:
        if cursor is None:
            return
        try:
            cursor.close()
        except BaseException as exc:
            logger.warning("Could not close SQL cursor (%s); discarding this connection", type(exc).__name__)
            self._drop()
            if not isinstance(exc, Exception):
                if getattr(self._local, "transaction_active", False):
                    self._local.transaction_failed = True
                raise

    def integrity_error(self) -> type[Exception]:
        """The exception a duplicate primary key raises.

        Imported lazily and through the module rather than named directly, so
        the offline path never imports the driver at all.
        """
        import mssql_python

        return mssql_python.IntegrityError


class SqlUnavailable(RuntimeError):
    """The database could not be reached. Never raised for a rejected write."""


class SqlCommitUncertain(SqlUnavailable):
    """A commit might have succeeded; its durable identity must be reconciled."""


class SqlRollbackUncertain(SqlUnavailable):
    """A failed transaction rollback was not acknowledged by the database."""


class SqlTransactionAborted(RuntimeError):
    """The transaction cannot continue or commit partial work."""


#: Driver exceptions that mean the connection itself is gone, as opposed to the
#: statement being rejected. Matched by exact name so this module never has to
#: import the driver just to classify an error -- and deliberately *not* by
#: subclass: ``IntegrityError`` inherits from ``DatabaseError``, and a duplicate
#: key is an expected outcome here, not a dead connection. ``DatabaseError``
#: itself is excluded for the same reason.
_CONNECTION_ERROR_NAMES = frozenset({"OperationalError", "InterfaceError"})


def _is_connection_error(exc: BaseException) -> bool:
    """Should this thread's connection be thrown away?

    Deliberately name-based rather than probing the connection with a test
    query. Probing was the first implementation and it made things worse: the
    probe ran on a connection that was already being torn down, turning one
    failure into several.
    """
    return type(exc).__name__ in _CONNECTION_ERROR_NAMES


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
#
# Every table follows the same shape the Table Storage design used: promoted
# columns an operator can filter on, plus a ``payload`` column holding the
# authoritative JSON. The promoted columns are a convenience and are allowed to
# be redundant; the payload is what the code reads back, so adding a field to a
# model never needs a migration.
#
# Creation is idempotent for explicit deployment retries. Runtime stores inspect
# the deployed baseline and fail if it is missing; they never apply this DDL.

def schema_statements(tables: dict[str, str]) -> list[str]:
    """Return idempotent DDL for every table the accelerator uses.

    ``tables`` maps a logical name to the configured table name, so an adopter
    can prefix them to share a database with something else.
    """
    from triage.store.command_center import schema_statements as command_center_schema
    from triage.store.incident_workflow import schema_statements as incident_activity_schema

    names = DEFAULT_TABLES | tables
    t = {k: quote_identifier(v) for k, v in names.items()}
    return [
        f"""
        IF OBJECT_ID('{_bare(t["data_quality_flags"])}') IS NULL
        CREATE TABLE {t["data_quality_flags"]} (
            flag_id      NVARCHAR(128) NOT NULL PRIMARY KEY,
            request_id   NVARCHAR(200) NOT NULL,
            flagged_at   NVARCHAR(40)  NOT NULL,
            payload      NVARCHAR(MAX) NOT NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["incidents"])}') IS NULL
        CREATE TABLE {t["incidents"]} (
            incident_id  NVARCHAR(200)  NOT NULL PRIMARY KEY,
            signature    NVARCHAR(200)  NOT NULL,
            status       NVARCHAR(50)   NOT NULL,
            updated_at   NVARCHAR(40)   NOT NULL,
            payload      NVARCHAR(MAX)  NOT NULL
        )""",
        # find_open() is the hot read: it runs before every remediation.
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_incidents_signature_status')
        CREATE INDEX ix_incidents_signature_status
            ON {t["incidents"]} (signature, status)""",
        f"""
        IF OBJECT_ID('{_bare(t["processed"])}') IS NULL
        CREATE TABLE {t["processed"]} (
            fingerprint  NVARCHAR(64)   NOT NULL PRIMARY KEY,
            message_id   NVARCHAR(512)  NULL,
            received_at  NVARCHAR(40)   NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["approvals"])}') IS NULL
        CREATE TABLE {t["approvals"]} (
            request_id   NVARCHAR(200)  NOT NULL PRIMARY KEY,
            decision     NVARCHAR(40)   NULL,
            responder    NVARCHAR(200)  NULL,
            decided_at   NVARCHAR(40)   NULL,
            payload      NVARCHAR(MAX)  NOT NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["retries"])}') IS NULL
        CREATE TABLE {t["retries"]} (
            signature    NVARCHAR(200)  NOT NULL PRIMARY KEY,
            status       NVARCHAR(40)   NULL,
            due_at       NVARCHAR(40)   NULL,
            attempts     INT            NOT NULL DEFAULT 0,
            payload      NVARCHAR(MAX)  NOT NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["semantic_health"])}') IS NULL
        CREATE TABLE {t["semantic_health"]} (
            probe_key       NVARCHAR(200)  NOT NULL PRIMARY KEY,
            probe_name      NVARCHAR(200)  NULL,
            report_name     NVARCHAR(200)  NULL,
            last_max_date   NVARCHAR(40)   NULL,
            last_row_count  BIGINT         NULL,
            suspect_count   INT            NOT NULL DEFAULT 0,
            payload         NVARCHAR(MAX)  NOT NULL
        )""",
        # Leases and claims are the same primitive with different lifetimes,
        # kept apart so a reset of one cannot disturb the other.
        f"""
        IF OBJECT_ID('{_bare(t["leases"])}') IS NULL
        CREATE TABLE {t["leases"]} (
            lease_name   NVARCHAR(200)  NOT NULL PRIMARY KEY,
            owner        NVARCHAR(200)  NOT NULL,
            expires_at   DATETIME2(3)   NOT NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["claims"])}') IS NULL
        CREATE TABLE {t["claims"]} (
            claim_key    NVARCHAR(64)   NOT NULL PRIMARY KEY,
            owner        NVARCHAR(200)  NOT NULL,
            claimed_at   DATETIME2(3)   NOT NULL,
            expires_at   DATETIME2(3)   NOT NULL,
            claim_text   NVARCHAR(512)  NULL
        )""",
        # Evidence that the inbox filter refused a message. Append-only and
        # bounded; nothing reads these rows back to reconsider a message.
        f"""
        IF OBJECT_ID('{_bare(t["inbox_audit"])}') IS NULL
        CREATE TABLE {t["inbox_audit"]} (
            fingerprint  NVARCHAR(64)   NOT NULL PRIMARY KEY,
            sender       NVARCHAR(200)  NULL,
            subject      NVARCHAR(400)  NULL,
            reason       NVARCHAR(200)  NULL,
            ignored_at   NVARCHAR(40)   NOT NULL
        )""",
        f"""
        IF OBJECT_ID('{_bare(t["pipeline_reruns"])}') IS NULL
        CREATE TABLE {t["pipeline_reruns"]} (
            run_key       NVARCHAR(64)  NOT NULL PRIMARY KEY,
            workspace_id  NVARCHAR(36)  NOT NULL,
            pipeline_id   NVARCHAR(36)  NOT NULL,
            state         NVARCHAR(40)  NOT NULL,
            payload       NVARCHAR(MAX) NOT NULL
        )""",
        # The approval callback runs as a Logic App, which has no way to hold a
        # transaction open across a read and a write. Putting the whole decision
        # in one procedure gives it the same guarantee `decide()` has in Python:
        # the row moves from unanswered to answered exactly once, and the caller
        # is told whether it was the one that moved it.
        #
        # It is also the reason the callback takes no SQL from a query string.
        # The request id, decision and responder arrive from a URL that anyone
        # holding the link can edit, so they are parameters, never concatenated
        # text.
        #
        # CREATE OR ALTER rather than IF OBJECT_ID: a procedure definition has to
        # be the only statement in its batch, so it cannot sit inside an IF.
        f"""
        CREATE OR ALTER PROCEDURE dbo.triage_record_approval_decision
            @request_id  NVARCHAR(200),
            @decision    NVARCHAR(40),
            @responder   NVARCHAR(200),
            @reason      NVARCHAR(400) = '',
            @fingerprint NVARCHAR(200)
        AS
        BEGIN
            SET NOCOUNT ON;
            DECLARE @now NVARCHAR(33) = CONVERT(NVARCHAR(33), SYSDATETIMEOFFSET(), 126);

            UPDATE {t["approvals"]}
               SET decision   = @decision,
                   responder  = @responder,
                   decided_at = @now,
                   payload    = JSON_MODIFY(
                                  JSON_MODIFY(
                                    JSON_MODIFY(
                                      JSON_MODIFY(payload, '$.decision', @decision),
                                      '$.responder', @responder),
                                    '$.reason', @reason),
                                  '$.decided_at', @now)
             WHERE request_id = @request_id
               -- Web proposals require the authenticated command-center API,
               -- not an older bearer-link callback carrying a claimed actor.
               AND JSON_VALUE(payload, '$.delivery_channel') = 'teams'
               AND @decision IN ('approve', 'decline')
               AND NULLIF(LTRIM(RTRIM(@responder)), '') IS NOT NULL
               -- Unanswered. This is the single-assignment guarantee: a second
               -- click matches no row and is told so, rather than overwriting.
               AND (decision IS NULL OR decision = '')
               -- Bound to the exact action the link was issued for.
               AND DATALENGTH(@fingerprint) = 128
               AND JSON_VALUE(payload, '$.fingerprint') COLLATE Latin1_General_100_BIN2
                    = @fingerprint COLLATE Latin1_General_100_BIN2
               AND NULLIF(JSON_VALUE(payload, '$.consumed_at'), '') IS NULL
               -- Still open. An approval that arrives after the window has
               -- closed is not an approval.
               AND TRY_CAST(JSON_VALUE(payload, '$.expires_at') AS DATETIMEOFFSET)
                       > SYSDATETIMEOFFSET();

            SELECT @@ROWCOUNT AS recorded;
        END""",
    ] + command_center_schema(
        names["agent_runs"], names["agent_events"], names["agent_commands"],
    ) + incident_activity_schema(names["incident_activity"])


def _bare(quoted: str) -> str:
    """'[dbo].[x]' -> 'dbo.x', which is what OBJECT_ID expects."""
    return quoted.replace("[", "").replace("]", "")


DEFAULT_TABLES: dict[str, str] = {
    "data_quality_flags": "triage_data_quality_flags",
    "incidents": "triage_incidents",
    "processed": "triage_processed_messages",
    "approvals": "triage_approvals",
    "retries": "triage_deferred_retries",
    "semantic_health": "triage_semantic_health",
    "leases": "triage_sweep_leases",
    "claims": "triage_claims",
    "inbox_audit": "triage_inbox_audit",
    "pipeline_reruns": "triage_pipeline_reruns",
    "agent_runs": "triage_agent_runs",
    "agent_events": "triage_agent_events",
    "agent_commands": "triage_agent_commands",
    "incident_activity": "triage_incident_activity",
}


def ensure_schema(db: AzureSqlDatabase, tables: dict[str, str] | None = None) -> bool:
    """Deployment-only baseline creation, returning False on a schema failure.

    This does not upgrade existing data or make runtime identities need DDL.
    The deployment caller must verify the result before enabling runtime work.
    """
    statements = schema_statements(tables or DEFAULT_TABLES)
    try:
        for sql in statements:
            db.execute(sql)
        return True
    except SqlUnavailable:
        return False
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Could not create the triage schema in %s (%s)", db.target, type(exc).__name__
        )
        return False
