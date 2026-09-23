"""Shared state selection and use-time persistence policy.

Choosing a SQL adapter does not establish that its last write committed. The
legacy ``is_durable`` property can also change after a failed read or write, so
health checks must not be cached as a construction-time capability.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Protocol, overload

from triage.store import azure_sql
from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable

logger = logging.getLogger("triage.store.durability")


class SqlStateSettings(Protocol):
    azure_sql_server: str
    azure_sql_database: str


class StateConfigurationError(ValueError):
    """Live/offline state selection is contradictory or incomplete."""


@overload
def select_state_database(
    settings: SqlStateSettings, *, fixture: Literal[True],
    db: AzureSqlDatabase | None = None, credential: Any = None,
    tables: dict[str, str] | None = None,
) -> None: ...


@overload
def select_state_database(
    settings: SqlStateSettings, *, fixture: Literal[False],
    db: AzureSqlDatabase | None = None, credential: Any = None,
    tables: dict[str, str] | None = None,
) -> AzureSqlDatabase: ...


@overload
def select_state_database(
    settings: SqlStateSettings, *, fixture: bool,
    db: AzureSqlDatabase | None = None, credential: Any = None,
    tables: dict[str, str] | None = None,
) -> AzureSqlDatabase | None: ...


def select_state_database(
    settings: SqlStateSettings, *, fixture: bool,
    db: AzureSqlDatabase | None = None, credential: Any = None,
    tables: dict[str, str] | None = None,
) -> AzureSqlDatabase | None:
    """Select one shared handle without connecting, installing schemas or falling back."""
    if fixture:
        if db is not None:
            raise StateConfigurationError("Explicit fixture state cannot use a live SQL handle.")
        return None
    if db is not None:
        return db
    if not settings.azure_sql_server or not settings.azure_sql_database:
        raise StateConfigurationError(
            "Live state requires AZURE_SQL_SERVER and AZURE_SQL_DATABASE."
        )
    return azure_sql.AzureSqlDatabase(
        server=settings.azure_sql_server, database=settings.azure_sql_database,
        credential=credential, tables=tables,
    )


def persistence_confirmed(store: object) -> bool:
    """Read current adapter health, not proof of a particular operation's commit.

    Older offline stores do not advertise this property. Absence is not shared
    durability; malformed values must not become success through truthiness.
    """
    value = getattr(store, "is_durable", False)
    if type(value) is not bool:
        logger.error("Store health is not boolean store_type=%s", type(store).__name__)
        raise SqlUnavailable("Store persistence health must be boolean.")
    return value


def require_shared_persistence(store: object, *, operation: str) -> None:
    """Require fresh confirmation at the point that needs it; never select a fallback."""
    if not persistence_confirmed(store):
        logger.error("Shared persistence is unconfirmed operation=%s", operation)
        raise SqlUnavailable(f"{operation} requires confirmed shared persistence.")
