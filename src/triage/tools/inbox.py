"""Inbox ingestion — the trigger.

Two mechanisms, because the customer explicitly asked to see the difference:

* **poll** — a timer reads the mailbox every N seconds. Simple, no public
  endpoint, no subscription lifecycle. Latency = half the poll interval on
  average. This is what the demo uses.
* **subscription** — Graph change notifications POST to a public HTTPS
  endpoint, typically within seconds. Requires a reachable validation endpoint
  and renewal before expiry — the ceiling for mail resources is 4230 minutes
  (just under 3 days), and most teams renew at least daily rather than riding
  the limit. A lapsed renewal is a silent outage, which is the reason to know
  about it before production.

Both produce the same :class:`BIRequest`, so the agent code never knows which
one is wired up.

Authentication choice
---------------------
The Graph path uses app-only authentication rather than delegated
authentication because the agent is unattended: a scheduled routine fires when
mail arrives, so there is no signed-in user to consent. In production, prefer a
managed identity or federated credential over a client secret.

The security caveat is important: app-only ``Mail.Read`` is **TENANT-WIDE**
unless an Exchange ``ApplicationAccessPolicy`` scopes the app. An unscoped app
that is intended to read one alerts mailbox can read every mailbox in the
tenant — including, as verified during the work-tenant spike, the global
administrator's mailbox.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from triage.models import BIRequest
from triage.store.processed import InMemoryProcessedLog, ProcessedMessageLog
from triage.tools.mail_filter import MailFilter

logger = logging.getLogger("triage.inbox")

# Deterministic extraction. Anything the regexes miss stays None and the agent
# has to cope — which is the realistic case and worth showing.
# A quoted name is the strongest signal, and it is the shape Power BI's own
# alert mail uses: "Refresh failed for 'Sales Daily Summary'". The earlier
# pattern required the literal word report/dataset/semantic model, so it never
# matched a real alert -- the report name came back None and the model was left
# to infer one, which it duly invented.
_REPORT_QUOTED = re.compile(
    r"(?:for|report|dataset|semantic model)\s*[:\-]?\s*['\"\u2018\u201c]"
    r"([^'\"\u2019\u201d]{3,60})['\"\u2019\u201d]",
    re.I,
)
_REPORT_BARE = re.compile(
    r"(?:report|dataset|semantic model)[:\s\"']+([A-Za-z0-9 _\-]{3,60})", re.I
)
_DATASET_ID = re.compile(
    r"\b(?:dataset[_ ]?id|datasetId)\W{0,3}([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.I,
)
_WORKSPACE_ID = re.compile(
    r"\b(?:workspace[_ ]?id|groupId)\W{0,3}([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.I,
)
# Upper bound raised from 40: real codes exceed it, and a code truncated
# mid-word silently changes the incident signature and stops matching
# playbooks, which is worse than not extracting one at all.
_ERROR_CODE = re.compile(r"\b(?:error[_ ]?code|code)\W{0,3}([A-Za-z][A-Za-z0-9_]{3,79})\b", re.I)


def parse_hints(subject: str, body: str) -> dict[str, str | None]:
    blob = f"{subject}\n{body}"
    report = _REPORT_QUOTED.search(blob) or _REPORT_BARE.search(blob)
    dataset = _DATASET_ID.search(blob)
    workspace = _WORKSPACE_ID.search(blob)
    code = _ERROR_CODE.search(blob)
    return {
        "report_name": report.group(1).strip() if report else None,
        "dataset_id": dataset.group(1) if dataset else None,
        "workspace_id": workspace.group(1) if workspace else None,
        "error_code": code.group(1) if code else None,
    }


class InboxSource(Protocol):
    async def fetch(self, limit: int = 10) -> list[BIRequest]: ...
    def mark_processed(self, message_id: str, *, received_at: str = "") -> None: ...


@dataclass
class MockInbox:
    """Reads `.json` messages from a directory, newest filename last."""

    directory: Path
    consumed: list[str] = field(default_factory=list)

    def mark_processed(self, message_id: str, *, received_at: str = "") -> None:
        """No-op: the offline path replays its fixtures on purpose."""

    async def fetch(self, limit: int = 10) -> list[BIRequest]:
        directory = Path(self.directory)
        if not directory.exists():
            return []

        out: list[BIRequest] = []
        for path in sorted(directory.glob("*.json")):
            if path.name in self.consumed:
                continue
            out.append(self.load(path))
            self.consumed.append(path.name)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def load(path: str | Path) -> BIRequest:
        raw: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
        hints = parse_hints(raw.get("subject", ""), raw.get("body", ""))
        kwargs: dict[str, Any] = {
            "request_id": raw.get("request_id") or Path(path).stem,
            "sender": raw.get("sender", ""),
            "subject": raw.get("subject", ""),
            "body": raw.get("body", ""),
            "report_name": raw.get("report_name") or hints["report_name"],
            "dataset_id": raw.get("dataset_id") or hints["dataset_id"],
            "workspace_id": raw.get("workspace_id") or hints["workspace_id"],
            "error_code": raw.get("error_code") or hints["error_code"],
            "source": "mock",
        }
        # Let the model's default factory supply `received_at` when absent.
        if raw.get("received_at"):
            kwargs["received_at"] = raw["received_at"]
        return BIRequest(**kwargs)

    def reset(self) -> None:
        self.consumed.clear()


class GraphInbox:
    """Polls a monitored mailbox via Microsoft Graph.

    Uses an admin-consented application permission (``Mail.Read``). Marks
    nothing as read — the agent cannot write to the mailbox at all, which is
    deliberate — so "already handled" is tracked in the agent's own durable
    log instead. See ``store/processed.py`` for why that has to be durable.
    """

    _BASE = "https://graph.microsoft.com/v1.0"
    _SCOPE = "https://graph.microsoft.com/.default"

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str | None = None,
        mailbox: str,
        mail_filter: MailFilter | None = None,
        processed_log: ProcessedMessageLog | None = None,
    ):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret or ""
        self._mailbox = mailbox
        # In-memory by default so a caller that forgets to supply one behaves
        # as this class did before, rather than silently re-triaging.
        self._processed: ProcessedMessageLog = processed_log or InMemoryProcessedLog()
        self._token: str = ""
        self._token_expires_at: float = 0.0
        # Fail closed: without an explicit filter the agent would act on
        # every message in the mailbox, which makes it steerable by anyone
        # who can send it mail.
        self._filter = mail_filter

    def mark_processed(self, message_id: str, *, received_at: str = "") -> None:
        """Record that this message reached a terminal outcome.

        Called by the caller *after* the incident is persisted, not during
        ``fetch``. If the run dies in between, the alert is triaged again next
        sweep -- noisy but safe -- rather than being dropped unseen.
        """
        self._processed.mark(message_id, received_at=received_at)

    async def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        if not self._client_secret:
            # Credential-free path. This is the production posture: the process
            # authenticates as its own Entra agent identity, so there is no
            # secret to store or rotate. That matters under tenant governance
            # that purges Entra app secrets on a schedule -- a secret-based
            # design starts failing about a month after go-live.
            #
            # A plain ManagedIdentityCredential is NOT enough. Inside a Foundry
            # hosted agent the identity arrives as a *federated* workload
            # identity, not an IMDS managed identity, and asking only for the
            # latter returns a token Graph rejects with 401.
            #
            # DefaultAzureCredential handles both, but its chain also includes
            # the developer's Azure CLI login, so a misconfigured container
            # could silently authenticate as whoever last ran `az login` and
            # read that person's mail. Excluding every human credential keeps
            # the convenience of the chain without that failure mode. verify()
            # rejects a token carrying a 'upn' claim as defence in depth.
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential(
                exclude_cli_credential=True,
                exclude_developer_cli_credential=True,
                exclude_interactive_browser_credential=True,
                exclude_shared_token_cache_credential=True,
                exclude_visual_studio_code_credential=True,
                managed_identity_client_id=self._client_id or None,
            )
            token = await asyncio.to_thread(credential.get_token, self._SCOPE)
            self._token = token.token
            try:
                self._token_expires_at = float(token.expires_on)
            except (AttributeError, TypeError, ValueError):
                self._token_expires_at = 0.0
            return self._token

        if not self._client_id:
            raise ValueError("graph_client_id is required for client-secret authentication")

        import httpx

        url = f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": self._SCOPE,
                },
            )
            resp.raise_for_status()
            payload = resp.json()

        self._token = payload["access_token"]
        try:
            self._token_expires_at = time.time() + float(payload.get("expires_in", 0))
        except (TypeError, ValueError):
            # A token without a usable lifetime is never reused. Caching it
            # forever would turn a transient configuration mistake into a
            # persistent authentication failure.
            self._token_expires_at = 0.0
        return self._token

    #: How many Graph pages one sweep will walk before giving up. Bounds the
    #: work per sweep on a busy mailbox while still being deep enough that a run
    #: of irrelevant mail cannot hide an alert underneath it.
    _MAX_PAGES = 10

    #: Graph page size. Larger than the caller's limit on purpose: most messages
    #: in a shared mailbox are not Power BI alerts, so a page buys several
    #: candidates rather than one.
    _PAGE_SIZE = 50

    async def fetch(self, limit: int = 10) -> list[BIRequest]:
        """Return up to ``limit`` alerts that have not been triaged yet.

        Paginates, and that is the whole point of this method's shape. It used
        to request a single page of ``limit`` messages ordered newest-first and
        stop. Once ``limit`` messages that the filter rejects -- or that were
        already triaged -- occupied the top of the inbox, every later sweep
        retrieved exactly those, skipped them all, and returned nothing. Older
        mail beneath them was never reached again.

        That is a denial of service available to anyone who can email the
        mailbox, and it needs no privilege: ten newsletters disable triage
        permanently while the logs report "No new alerts."

        Messages the filter rejects are deliberately **not** marked processed.
        Marking them would make later sweeps cheaper, but the filter fails
        closed -- including when its own subject pattern is invalid -- so one bad
        pattern would mark an entire mailbox as handled and fixing the pattern
        would not bring any of it back. Re-reading is the recoverable choice.
        """
        import httpx

        token = await self._get_token()
        url = (
            f"{self._BASE}/users/{self._mailbox}/mailFolders/inbox/messages"
            f"?$top={self._PAGE_SIZE}&$orderby=receivedDateTime desc"
            f"&$select=id,subject,body,from,receivedDateTime"
        )

        out: list[BIRequest] = []
        skipped = 0
        already = 0
        pages = 0

        async with httpx.AsyncClient(timeout=30) as client:
            while url and pages < self._MAX_PAGES and len(out) < limit:
                resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
                if resp.status_code >= 400:
                    # Surface Graph's own explanation. Without it a 401 here is
                    # indistinguishable between a bad token, a missing app role
                    # and an Exchange policy denial -- three very different fixes.
                    try:
                        detail = str(resp.json().get("error", {}).get("message", ""))[:400]
                    except Exception:
                        detail = resp.text[:400]
                    logger.error("Graph mail read failed: HTTP %s %s", resp.status_code, detail)
                resp.raise_for_status()

                payload = resp.json()
                url = payload.get("@odata.nextLink", "")
                pages += 1

                for msg in payload.get("value", []):
                    if len(out) >= limit:
                        break
                    outcome = self._classify(msg)
                    if outcome == "processed":
                        already += 1
                    elif outcome == "filtered":
                        skipped += 1
                    elif outcome is not None:
                        out.append(outcome)

        if skipped:
            logger.info(
                "Ignored %d message(s) that were not Power BI refresh alerts", skipped
            )
        if already:
            logger.info("Skipped %d message(s) already triaged on an earlier sweep", already)
        if pages > 1:
            logger.info("Walked %d page(s) of mail to find %d alert(s)", pages, len(out))
        if url and len(out) < limit:
            # Ran out of page budget with mail still unexamined. Said out loud,
            # because quietly returning fewer alerts than exist is the failure
            # this pagination was added to remove.
            logger.warning(
                "Stopped after %d page(s) with mail still unread and only %d alert(s) "
                "found. Older alerts may be waiting: tighten GRAPH_SUBJECT_PATTERN "
                "or raise _MAX_PAGES if this persists.",
                pages,
                len(out),
            )
        return out

    def _classify(self, msg: dict[str, Any]) -> BIRequest | str | None:
        """One message: a request, ``"processed"``, ``"filtered"``, or ``None``.

        Split out of ``fetch`` so the pagination loop stays readable. The string
        sentinels keep the counters that let an operator tell "nothing new"
        apart from "not looking", which the logs have to preserve.
        """
        mid = msg.get("id", "")
        if self._processed.seen(mid):
            return "processed"

        subject = msg.get("subject", "") or ""
        sender = ((msg.get("from") or {}).get("emailAddress") or {}).get("address", "")

        if self._filter is not None:
            accepted, reason = self._filter.accepts(sender=sender, subject=subject)
            if not accepted:
                # Counted, not silently dropped: an operator needs to know the
                # agent saw this and chose not to act.
                logger.info("Skipped message (%s)", reason)
                return "filtered"

        body = ((msg.get("body") or {}).get("content") or "")[:20000]
        hints = parse_hints(subject, body)
        return BIRequest(
            request_id=mid,
            received_at=msg.get("receivedDateTime", ""),
            sender=sender,
            subject=subject,
            body=body,
            report_name=hints["report_name"],
            dataset_id=hints["dataset_id"],
            workspace_id=hints["workspace_id"],
            error_code=hints["error_code"],
            source="graph",
        )

    @staticmethod
    def _decode_jwt_payload(token: str) -> dict[str, Any]:
        """Decode the JWT payload without validating or exposing the token."""
        parts = token.split(".")
        if len(parts) < 2:
            raise ValueError("Graph access token is not a JWT")

        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Graph access token payload is not a JSON object")
        return payload

    async def verify(self) -> dict[str, Any]:
        """Return safe metadata about Graph authentication for preflight."""
        try:
            token = await self._get_token()
            payload = self._decode_jwt_payload(token)
        except Exception as exc:
            # Do not include exception details: an auth library can echo
            # request data, and the access token must never reach logs.
            logger.warning(
                "Graph inbox authentication verification failed (%s)",
                type(exc).__name__,
            )
            return {"ok": False, "roles": [], "has_upn": False}

        roles = payload.get("roles", [])
        if isinstance(roles, str):
            roles = [roles]
        elif not isinstance(roles, list):
            roles = []

        return {
            "ok": True,
            "roles": [str(role) for role in roles],
            "has_upn": "upn" in payload,
            # Which principal the token actually belongs to. Worth surfacing:
            # a container can hold a perfectly valid token for the *wrong*
            # identity, which fails as 401 at the API and looks like a
            # permission bug rather than an identity mix-up.
            "app_id": str(payload.get("appid") or payload.get("azp") or ""),
            "object_id": str(payload.get("oid") or ""),
            "tenant_id": str(payload.get("tid") or ""),
        }

    async def verify_scope(self, canary_mailbox: str) -> dict[str, Any]:
        """Prove the app registration is confined to its mailbox.

        App-only ``Mail.Read`` is **tenant-wide by default**. We verified that
        directly: an app created to read one demo mailbox happily read the
        global administrator's inbox. The fix is an Exchange
        ``ApplicationAccessPolicy`` scoping it to a single mailbox.

        This method proves the scope is in force by attempting to read a mailbox
        the agent has no business reading. A **403 is the passing result**. A 200
        means the policy is missing or has not propagated, and the caller should
        refuse to run rather than quietly ingest from an over-permissioned app.

        Reads only message ids, and never returns message content.
        """
        import httpx

        if not canary_mailbox or canary_mailbox.lower() == self._mailbox.lower():
            return {"checked": False, "reason": "no distinct canary mailbox configured"}

        try:
            token = await self._get_token()
            url = (
                f"{self._BASE}/users/{canary_mailbox}/mailFolders/inbox/messages"
                f"?$top=1&$select=id"
            )
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        except Exception as exc:
            logger.warning("Scope check could not run (%s)", type(exc).__name__)
            return {"checked": False, "reason": type(exc).__name__}

        # 403 = the policy denied it, which is what we want. 404 can mean the
        # mailbox does not exist, which proves nothing either way.
        if resp.status_code == 403:
            return {"checked": True, "scoped": True, "canary": canary_mailbox, "status": 403}
        if resp.status_code == 200:
            return {
                "checked": True,
                "scoped": False,
                "canary": canary_mailbox,
                "status": 200,
                "reason": (
                    f"The app read {canary_mailbox}, which it should not be able to. "
                    "Apply an Exchange ApplicationAccessPolicy restricting it to "
                    f"{self._mailbox}."
                ),
            }
        return {
            "checked": False,
            "canary": canary_mailbox,
            "status": resp.status_code,
            "reason": f"inconclusive (HTTP {resp.status_code})",
        }


def mailbox_scope_refusal(
    *,
    scope: dict[str, object] | None,
    canary_mailbox: str,
    mailbox: str,
) -> str | None:
    """Return why mail must not be read, or ``None`` when it is proven safe.

    App-only ``Mail.Read`` is tenant-wide unless Exchange scopes it to a mailbox
    with an ``ApplicationAccessPolicy``, so an unscoped app registration can read
    every mailbox in the tenant.

    All three unproven cases refuse:

    * no canary configured, so nothing was ever tested -- and unset is the
      shipped default, which is why this mattered
    * the check did not complete, because inconclusive is not proof
    * the check completed and the agent could read the canary

    This lives here rather than in the hosted entry point so it can be tested
    without the hosting library. The previous version returned early on the
    first two cases and read the mailbox anyway, while the surrounding comment
    and the documentation both said it failed closed.
    """
    if not canary_mailbox:
        return (
            "Refusing to read mail: GRAPH_CANARY_MAILBOX is not set, so there is "
            f"no evidence this agent is confined to {mailbox}. Set it to a "
            "mailbox this agent must NOT be able to read."
        )
    scope = scope or {}
    reason = str(scope.get("reason", "")).strip()
    if not scope.get("checked"):
        return f"Refusing to read mail: the mailbox scope check did not complete. {reason}".strip()
    if not scope.get("scoped"):
        return f"Refusing to read mail: this agent is not confined to {mailbox}. {reason}".strip()
    return None
