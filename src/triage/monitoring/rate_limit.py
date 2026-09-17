"""Shared REST request budgets; no runtime DDL and no sleeping under a work lease.

Deployment calls ``schema_statements()`` explicitly and grants the collector
SELECT, INSERT and UPDATE on this table. ``AzureSqlRateBudget`` uses the existing
Entra-authenticated AzureSqlDatabase transaction boundary. Counters and cooldowns
use SQL time and deliberately span monitoring epochs: a reset is not a new API
allowance. The in-memory implementation is an explicitly injected offline fixture.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.models import MonitoringContext
from triage.store.azure_sql import AzureSqlDatabase, quote_identifier

logger = logging.getLogger("triage.monitoring.rate_limit")

DEFAULT_RATE_TABLE = "triage_monitoring_rate_budget"


@dataclass(frozen=True)
class RatePolicy:
    requests: int
    window_seconds: int

    def __post_init__(self) -> None:
        if type(self.requests) is not int or not 1 <= self.requests <= 1_000_000:
            raise ValueError("A request budget must allow 1-1000000 requests")
        if type(self.window_seconds) is not int or not 1 <= self.window_seconds <= 86_400:
            raise ValueError("A request budget window must contain 1-86400 seconds")


@dataclass(frozen=True)
class RateDecision:
    allowed: bool
    checked_at: datetime
    retry_at: datetime


class RateBudget(Protocol):
    """One call counts one physical HTTP attempt, including failed reads.

    Implementations must coordinate by tenant and bucket, not by process or
    collector identity. A denied acquisition does not consume a request.
    """

    def acquire(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
    ) -> RateDecision: ...

    def acquire_many(
        self, context: MonitoringContext, policies: tuple[tuple[str, RatePolicy], ...],
    ) -> RateDecision: ...

    def defer(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
        *, seconds: int,
    ) -> RateDecision: ...


def _key(context: MonitoringContext, bucket: str) -> tuple[str, str]:
    if not bucket or len(bucket) > 128 or any(
        not (char.isascii() and (char.isalnum() or char in "._:-")) for char in bucket
    ):
        raise ValueError("Rate bucket names must be bounded ASCII identifiers")
    return context.tenant_id, hashlib.sha256(bucket.encode("ascii")).hexdigest()


def _delay(seconds: int) -> int:
    if type(seconds) is not int or not 0 <= seconds <= 2_147_483_647:
        raise ValueError("Retry-After must be a nonnegative SQL-sized number of seconds")
    return seconds


def _ordered_policies(
    context: MonitoringContext, policies: tuple[tuple[str, RatePolicy], ...],
) -> tuple[tuple[str, RatePolicy], ...]:
    if not 1 <= len(policies) <= 16:
        raise ValueError("Acquire between one and sixteen distinct REST request budgets")
    keys = [_key(context, bucket) for bucket, _ in policies]
    if len(set(keys)) != len(keys):
        raise ValueError("A physical request must not charge the same budget twice")
    return tuple(sorted(policies, key=lambda pair: _key(context, pair[0])))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass
class _FixtureCounter:
    policy: RatePolicy
    window_ends_at: datetime
    used: int = 0
    blocked_until: datetime | None = None


class InMemoryRateBudget:
    """Offline fixture; share this instance to exercise multiple collector replicas."""

    def __init__(self, *, clock: Callable[[], datetime]) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, str], _FixtureCounter] = {}

    def _counter(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy, now: datetime,
    ) -> _FixtureCounter:
        key = _key(context, bucket)
        counter = self._counters.get(key)
        if counter is None:
            counter = _FixtureCounter(policy, now + timedelta(seconds=policy.window_seconds))
            self._counters[key] = counter
        if counter.policy != policy:
            raise MonitoringUnavailable("Replicas disagree about the shared REST budget policy")
        if now >= counter.window_ends_at:
            counter.window_ends_at = now + timedelta(seconds=policy.window_seconds)
            counter.used = 0
        return counter

    def acquire(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
    ) -> RateDecision:
        return self.acquire_many(context, ((bucket, policy),))

    def acquire_many(
        self, context: MonitoringContext, policies: tuple[tuple[str, RatePolicy], ...],
    ) -> RateDecision:
        policies = _ordered_policies(context, policies)
        with self._lock:
            now = self._clock()
            if now.tzinfo is None:
                raise ValueError("The fixture budget clock must be timezone-aware")
            counters = [self._counter(context, bucket, policy, now) for bucket, policy in policies]
            retry_at = max((
                max(
                    now, counter.blocked_until or now,
                    counter.window_ends_at if counter.used >= counter.policy.requests else now,
                )
                for counter in counters
            ), default=now)
            if retry_at > now:
                return RateDecision(False, now, retry_at)
            for counter in counters:
                counter.used += 1
            return RateDecision(True, now, now)

    def defer(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
        *, seconds: int,
    ) -> RateDecision:
        _delay(seconds)
        with self._lock:
            now = self._clock()
            if now.tzinfo is None:
                raise ValueError("The fixture budget clock must be timezone-aware")
            counter = self._counter(context, bucket, policy, now)
            counter.blocked_until = max(
                counter.blocked_until or now, now + timedelta(seconds=seconds),
            )
            retry_at = max(
                counter.blocked_until,
                counter.window_ends_at if counter.used >= policy.requests else now,
            )
            return RateDecision(False, now, retry_at)


def schema_statements(table: str = DEFAULT_RATE_TABLE) -> list[str]:
    """Deployment-only DDL. A missing table at runtime is a deployment error."""
    quoted = quote_identifier(table)
    return [f"""
IF OBJECT_ID(N'dbo.{table}', N'U') IS NULL
CREATE TABLE {quoted} (
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    bucket_hash CHAR(64) NOT NULL,
    request_limit INT NOT NULL CHECK (request_limit > 0),
    window_seconds INT NOT NULL CHECK (window_seconds > 0),
    window_ends_at DATETIME2(7) NOT NULL,
    used INT NOT NULL CHECK (used >= 0),
    blocked_until DATETIME2(7) NULL,
    PRIMARY KEY (tenant_id, bucket_hash)
);
"""]


class _DeniedBudgets(Exception):
    def __init__(self, decision: RateDecision) -> None:
        self.decision = decision


class AzureSqlRateBudget:
    """SQL-serialized fixed windows plus a shared Retry-After cooldown.

    The range lock covers first insertion as well as later conditional updates.
    An uncertain database result fails closed; repeating a read may overcount a
    budget, but cannot create an unbudgeted HTTP request.
    """

    def __init__(
        self, database: AzureSqlDatabase, *, table: str = DEFAULT_RATE_TABLE,
    ) -> None:
        self._database = database
        self._table = quote_identifier(table)

    def _change_locked(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy, delay: int | None,
    ) -> RateDecision:
        tenant, bucket_hash = _key(context, bucket)
        if delay is not None:
            _delay(delay)
        sql = f"""
SET NOCOUNT ON;
DECLARE @tenant UNIQUEIDENTIFIER = ?, @bucket CHAR(64) = ?,
        @limit INT = ?, @seconds INT = ?, @delay INT = ?,
        @now DATETIME2(7) = SYSUTCDATETIME(), @granted BIT = 0;
IF NOT EXISTS (
    SELECT 1 FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
    WHERE tenant_id = @tenant AND bucket_hash = @bucket
)
    INSERT INTO {self._table}
        (tenant_id, bucket_hash, request_limit, window_seconds, window_ends_at, used)
    VALUES (@tenant, @bucket, @limit, @seconds, DATEADD(SECOND, @seconds, @now), 0);
IF EXISTS (
    SELECT 1 FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
    WHERE tenant_id = @tenant AND bucket_hash = @bucket
      AND (request_limit <> @limit OR window_seconds <> @seconds)
)
    THROW 51001, 'Shared REST budget policy mismatch', 1;
IF @delay IS NOT NULL
BEGIN
    UPDATE {self._table}
    SET blocked_until = CASE
        WHEN blocked_until > DATEADD(SECOND, @delay, @now) THEN blocked_until
        ELSE DATEADD(SECOND, @delay, @now) END
    WHERE tenant_id = @tenant AND bucket_hash = @bucket;
END
ELSE
BEGIN
    UPDATE {self._table} WITH (UPDLOCK, HOLDLOCK)
    SET used = CASE WHEN window_ends_at <= @now THEN 1 ELSE used + 1 END,
        window_ends_at = CASE WHEN window_ends_at <= @now
            THEN DATEADD(SECOND, @seconds, @now) ELSE window_ends_at END
    WHERE tenant_id = @tenant AND bucket_hash = @bucket
      AND (blocked_until IS NULL OR blocked_until <= @now)
      AND (window_ends_at <= @now OR used < request_limit);
    IF @@ROWCOUNT = 1 SET @granted = 1;
END;
SELECT @granted, @now,
    CASE WHEN @granted = 1 THEN @now
        WHEN blocked_until > @now AND
        (used < request_limit OR window_ends_at <= @now OR blocked_until > window_ends_at)
        THEN blocked_until
        WHEN used >= request_limit AND window_ends_at > @now THEN window_ends_at
        ELSE @now END
FROM {self._table}
WHERE tenant_id = @tenant AND bucket_hash = @bucket;
"""
        rows = self._database.query(
            sql, tenant, bucket_hash, policy.requests, policy.window_seconds, delay,
        )
        if (
            len(rows) != 1 or len(rows[0]) != 3
            or rows[0][0] not in (0, 1)
            or not isinstance(rows[0][1], datetime)
            or not isinstance(rows[0][2], datetime)
        ):
            raise MonitoringUnavailable("Shared REST request budget returned an invalid decision")
        return RateDecision(bool(rows[0][0]), _utc(rows[0][1]), _utc(rows[0][2]))

    def acquire(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
    ) -> RateDecision:
        return self.acquire_many(context, ((bucket, policy),))

    def acquire_many(
        self, context: MonitoringContext, policies: tuple[tuple[str, RatePolicy], ...],
    ) -> RateDecision:
        policies = _ordered_policies(context, policies)
        try:
            with self._database.transaction():
                decisions = [
                    self._change_locked(context, bucket, policy, None)
                    for bucket, policy in policies
                ]
                checked_at = max(value.checked_at for value in decisions)
                if any(not value.allowed for value in decisions):
                    # Roll back every provisional charge, including an allowed
                    # service bucket when the narrower API bucket is exhausted.
                    raise _DeniedBudgets(RateDecision(
                        False, checked_at, max(value.retry_at for value in decisions),
                    ))
                return RateDecision(True, checked_at, checked_at)
        except _DeniedBudgets as denied:
            return denied.decision
        except MonitoringUnavailable:
            raise
        except Exception as exc:
            logger.error("Shared REST budgets unavailable (%s)", type(exc).__name__)
            raise MonitoringUnavailable("Shared REST request budgets could not be committed") from exc

    def defer(
        self, context: MonitoringContext, bucket: str, policy: RatePolicy,
        *, seconds: int,
    ) -> RateDecision:
        try:
            with self._database.transaction():
                return self._change_locked(context, bucket, policy, seconds)
        except MonitoringUnavailable:
            raise
        except Exception as exc:
            logger.error("Shared REST budget unavailable (%s)", type(exc).__name__)
            raise MonitoringUnavailable("Shared REST request budget could not be committed") from exc
