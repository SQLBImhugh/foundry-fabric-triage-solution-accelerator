"""Fabric SQL Database plumbing shared by every durable store.

Why a SQL database rather than a key-value table
------------------------------------------------
This accelerator is Fabric- and Foundry-centric, and the state it keeps is
relational: an incident has occurrences, an approval belongs to an action, a
deferred retry belongs to a signature. Keeping it in the same Fabric workspace
as the semantic models being triaged means an operator can join incident
history to the estate in one query, with one identity model, instead of
exporting a key-value table first.

Two properties made the migration worth doing rather than merely tidy:

* **A conditional ``UPDATE`` is atomic on its own.** The claim and lease stores
  previously did read-then-write guarded by an ETag, which is three round trips
  and a race the code had to reason about explicitly. ``UPDATE ... WHERE
  expires_at < SYSUTCDATETIME()`` is one statement, and ``rowcount`` says
  whether this caller won. A primary-key ``INSERT`` that raises
  ``IntegrityError`` is the same compare-and-set the old code got from
  ``ResourceExistsError``.
* **There is no key to leak.** Fabric SQL accepts Microsoft Entra tokens and
  nothing else -- there is no SQL-authentication fallback to disable, so the
  "no local auth" posture is the platform default rather than a setting that
  governance has to keep reverting.

Why ``mssql-python`` and not ``pyodbc``
--------------------------------------
The controller runs as a Foundry hosted agent: a managed Linux image built with
``dependency_resolution: remote_build``, which installs ``src/requirements.txt``
with pip and nothing else. ``pyodbc`` needs the ``msodbcsql18`` system driver,
which is an apt package and cannot be pip-installed, so it cannot work there.
``mssql-python`` ships the driver inside the wheel as a normal dependency
(``mssql-python-odbc``) and publishes a cp313 manylinux build matching the
container runtime. Verified against a real Fabric SQL Database before this was
written, not assumed.

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
from typing import Any

logger = logging.getLogger("triage.store.sql")

#: SQL_COPT_SS_ACCESS_TOKEN. Pre-login connection attribute carrying an Entra
#: token, which is how a driver authenticates without a password.
SQL_COPT_SS_ACCESS_TOKEN = 1256

#: The audience Azure SQL and Fabric SQL accept.
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


class FabricSqlDatabase:
    """Connections to a Fabric SQL Database, shared by all six stores.

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
        self._last_failure_at = 0.0
        self._last_error = ""

    def ensure_schema_once(self) -> bool:
        """Create the tables if they are missing, at most once per success.

        Stores call this on every recovery attempt, not just at startup. If the
        database was unreachable when the process began, nothing created the
        schema, and a store that only ever retried its ``SELECT`` would keep
        failing against a table that was never going to appear.
        """
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

    def _connect(self) -> Any:
        import mssql_python

        credential = self._credential
        if credential is None:
            from azure.identity import DefaultAzureCredential

            # Plain DefaultAzureCredential, unlike the mail and Power BI
            # clients, which exclude every human credential so a container
            # cannot authenticate as whoever last ran `az login`.
            #
            # This store is different because the same code runs in two places:
            # the hosted controller, where the chain resolves to the agent's own
            # identity, and the operator CLI on a laptop, where `bi-triage
            # incidents` has to read the same database as the person running it.
            # Excluding the developer credentials was tried and it broke every
            # local command. The worst case of a developer login reaching here
            # is writing demo incidents to a demo database as yourself.
            credential = DefaultAzureCredential()

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
        # statement to trip over. Every statement in this module is a single
        # atomic operation by design, so autocommit is the honest setting.
        return mssql_python.connect(
            conn_str,
            autocommit=True,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _token_struct(token)},
        )

    def _ensure(self) -> Any | None:
        """Return this thread's connection, opening one if needed.

        Returns ``None`` rather than raising when the database is unreachable.
        Callers decide what unavailability means for them: the incident store
        keeps working in memory and says so, while the claim store refuses to
        hand out a claim, because the cost of guessing differs.
        """
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn

        with self._state_lock:
            unreachable = time.monotonic() - self._last_failure_at < self._cooldown
        if unreachable:
            return None

        try:
            conn = self._connect()
        except Exception as exc:  # noqa: BLE001 - every failure is the same here
            with self._state_lock:
                self._last_failure_at = time.monotonic()
                self._last_error = type(exc).__name__
            # The message matters more than the class here. "Login failed for
            # user '<token-identified principal>'" means the identity
            # authenticated but has no database user; a timeout means the
            # server name or the network is wrong. Those need different fixes,
            # and the class name alone cannot tell them apart.
            logger.error(
                "Cannot reach Fabric SQL (%s): %s: %s. Retrying in %.0fs.",
                self.target,
                type(exc).__name__,
                str(exc)[:400],
                self._cooldown,
            )
            return None

        self._local.conn = conn
        with self._state_lock:
            if self._last_error:
                logger.info("Reconnected to Fabric SQL (%s)", self.target)
            self._last_error = ""
            self._last_failure_at = 0.0
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

    def execute(self, sql: str, *params: Any) -> int:
        """Run a statement and return the number of rows it affected.

        ``rowcount`` is the whole point for the conditional updates that
        arbitrate claims and leases: 1 means this caller won, 0 means it lost,
        and no second round trip is needed to find out.
        """
        conn = self._ensure()
        if conn is None:
            raise SqlUnavailable(f"Fabric SQL unavailable ({self.target})")
        try:
            cur = conn.cursor()
            cur.execute(sql, *params)
            return cur.rowcount
        except Exception as exc:
            if _is_connection_error(exc):
                self._drop()
            raise

    def query(self, sql: str, *params: Any) -> list[tuple]:
        conn = self._ensure()
        if conn is None:
            raise SqlUnavailable(f"Fabric SQL unavailable ({self.target})")
        try:
            cur = conn.cursor()
            cur.execute(sql, *params)
            return list(cur.fetchall())
        except Exception as exc:
            if _is_connection_error(exc):
                self._drop()
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


#: Driver exceptions that mean the connection itself is gone, as opposed to the
#: statement being rejected. Matched by exact name so this module never has to
#: import the driver just to classify an error -- and deliberately *not* by
#: subclass: ``IntegrityError`` inherits from ``DatabaseError``, and a duplicate
#: key is an expected outcome here, not a dead connection. ``DatabaseError``
#: itself is excluded for the same reason.
_CONNECTION_ERROR_NAMES = frozenset({"OperationalError", "InterfaceError"})


def _is_connection_error(exc: Exception) -> bool:
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
# Created with IF NOT EXISTS rather than a migration tool on purpose: this is a
# solution accelerator that has to come up against an empty database with no
# extra step, and the schema is small enough to state in one place.

def schema_statements(tables: dict[str, str]) -> list[str]:
    """Return idempotent DDL for every table the accelerator uses.

    ``tables`` maps a logical name to the configured table name, so an adopter
    can prefix them to share a database with something else.
    """
    t = {k: quote_identifier(v) for k, v in tables.items()}
    return [
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
            @fingerprint NVARCHAR(200) = NULL
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
               -- Unanswered. This is the single-assignment guarantee: a second
               -- click matches no row and is told so, rather than overwriting.
               AND (decision IS NULL OR decision = '')
               -- Bound to the exact action the link was issued for.
               AND (@fingerprint IS NULL
                    OR JSON_VALUE(payload, '$.fingerprint') = @fingerprint)
               -- Still open. An approval that arrives after the window has
               -- closed is not an approval.
               AND (JSON_VALUE(payload, '$.expires_at') IS NULL
                    OR TRY_CAST(JSON_VALUE(payload, '$.expires_at') AS DATETIMEOFFSET)
                       > SYSDATETIMEOFFSET());

            SELECT @@ROWCOUNT AS recorded;
        END""",
    ]


def _bare(quoted: str) -> str:
    """'[dbo].[x]' -> 'dbo.x', which is what OBJECT_ID expects."""
    return quoted.replace("[", "").replace("]", "")


DEFAULT_TABLES: dict[str, str] = {
    "incidents": "triage_incidents",
    "processed": "triage_processed_messages",
    "approvals": "triage_approvals",
    "retries": "triage_deferred_retries",
    "semantic_health": "triage_semantic_health",
    "leases": "triage_sweep_leases",
    "claims": "triage_claims",
}


def ensure_schema(db: FabricSqlDatabase, tables: dict[str, str] | None = None) -> bool:
    """Create anything missing. Returns False when the database is unreachable.

    Safe to call on every construction: each statement is guarded, and the
    whole point is that an adopter pointing at an empty database gets a working
    system without running a migration first.
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
