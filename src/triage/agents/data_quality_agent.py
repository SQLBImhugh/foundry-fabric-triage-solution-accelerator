"""The Data Quality agent — a genuinely separate agent, not a tool.

It has its own provider, its own system prompt, its own tool, and its own
small controller loop. The Triage agent reaches it only through the
``consult_data_quality_agent`` tool, and receives a typed
:class:`DataQualityFinding` back.

The important property: **deterministic evidence overrides the model.** The
scan produces the numbers; the model produces the sentence. If the model
contradicts the scan, the scan wins and the disagreement is logged. An agent
that can talk itself out of its own evidence is not one you can put in front
of an operations team.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from triage.models import BIRequest, DataQualityFinding, DuplicateEvidence
from triage.observability import tool_span, with_agent_context
from triage.prompts import load_prompt, prompt_version_hash
from triage.providers.base import BaseProvider
from triage.tools.dataset import DatasetSource, detect_duplicates

logger = logging.getLogger("triage.agent.dq")

PROMPT_FILE = "data_quality_system.md"


class DataQualityAgent:
    AGENT_NAME = "DataQualityAgent"

    def __init__(self, provider: BaseProvider, *, max_turns: int = 4):
        self._provider = provider
        self._max_turns = max_turns

    @property
    def model_info(self) -> str:
        return f"{self._provider.model_name} -> {self._provider.provider_name}"

    @property
    def prompt_hash(self) -> str:
        return prompt_version_hash(PROMPT_FILE)

    @with_agent_context(AGENT_NAME)
    async def investigate(
        self,
        *,
        request: BIRequest,
        datasets: dict[str, DatasetSource],
        reason: str = "",
        ledger: Any = None,
    ) -> DataQualityFinding:
        """Scan deterministically, then let the agent interpret the evidence.

        The scan runs **here, in the orchestrator, with no model involved**, and
        the resulting counts are passed to the agent as ground truth. The agent
        writes the sentence; it does not produce the numbers.

        That split is deliberate and load-bearing:

        * the numbers are identical on every rehearsal, so the demo is
          reproducible and the flag table is trustworthy;
        * the Data Quality agent needs no tools of its own, which means it can
          be a plain Foundry prompt agent rather than something that has to be
          hosted and made server-callable;
        * a model that contradicts the scan loses (see :meth:`_reconcile`).

        ``ledger`` is the orchestrator's :class:`PolicyLedger`. It is shared
        rather than per-agent on purpose: a budget that only covers the
        orchestrator is not a budget for the run.
        """
        if not datasets:
            return DataQualityFinding(
                has_issue=False,
                issue_type="unknown",
                confidence=0.0,
                detail="No tables are registered for inspection.",
                agent_name=self.AGENT_NAME,
            )

        # --- deterministic evidence, before any model call ------------------
        #
        # Every registered table is scanned, not just the first. This used to
        # read `next(iter(datasets.values()))` while reporting `list(datasets)`
        # as checked, so a two-table consultation opened one file and claimed
        # both were clean. A finding that names a table it never read is worse
        # than no finding: it is evidence the reader will trust.
        #
        # Collect everything, then let reporting priority decide what is
        # announced. Stopping at the first clean table is the same defect in a
        # different costume -- a model that looked healthy hiding one that is not.
        evidences: list[DuplicateEvidence] = []
        scan_errors: list[str] = []
        scanned: list[str] = []

        for source in datasets.values():
            with tool_span("check_duplicates", table=source.name):
                if not source.exists():
                    scan_errors.append(f"{source.name}: no file at {source.path}")
                else:
                    evidences.append(
                        detect_duplicates(
                            path=source.path,
                            key_columns=source.key_columns,
                            table_name=source.name,
                        )
                    )
                    scanned.append(source.name)
            # Charged per table, because that is how many scans really ran. A
            # budget that under-counts is a budget that does not bind.
            if ledger is not None:
                ledger.charge_tool_call("check_duplicates")

        # A defect anywhere outranks cleanliness everywhere else.
        evidence: DuplicateEvidence | None = next(
            (item for item in evidences if item.duplicate_row_count > 0),
            evidences[0] if evidences else None,
        )
        scan_error = "; ".join(scan_errors)

        if evidence is None:
            return DataQualityFinding(
                has_issue=False,
                issue_type="unknown",
                confidence=0.0,
                detail=f"Deterministic scan could not run: {scan_error}",
                checked_tables=scanned,
                recommended_action="escalate",
                agent_name=self.AGENT_NAME,
            )

        # --- the agent interprets it ----------------------------------------
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": load_prompt(PROMPT_FILE)},
            {
                "role": "user",
                "content": (
                    f"A BI failure was reported.\n"
                    f"subject: {request.subject}\n"
                    f"report: {request.report_name or 'unknown'}\n"
                    f"consultation reason: {reason or 'routine data quality gate'}\n\n"
                    f"Deterministic scan evidence (ground truth):\n"
                    f"{evidence.model_dump_json()}\n\n"
                    "Return your finding as a single JSON object."
                ),
            },
        ]

        if ledger is not None:
            ledger.charge_llm_turn()
        resp = await self._provider.complete(messages=messages)
        if ledger is not None:
            ledger.charge_tokens(resp.total_tokens)

        model_payload = _parse_json_object(resp.content)
        return self._reconcile(model_payload, evidence, scanned)

    # --- reconciliation ----------------------------------------------------

    def _reconcile(
        self,
        payload: dict[str, Any],
        evidence: DuplicateEvidence | None,
        checked: list[str],
    ) -> DataQualityFinding:
        """Build the finding, letting deterministic evidence win any conflict."""
        detail = str(payload.get("detail") or "").strip()
        confidence = _as_float(payload.get("confidence"), default=0.0)
        recommended = payload.get("recommended_action")
        if recommended not in ("flag_and_notify", "no_action", "escalate"):
            recommended = None

        if evidence is None:
            return DataQualityFinding(
                has_issue=bool(payload.get("has_issue")),
                issue_type="unknown",
                confidence=min(confidence, 0.5),
                detail=detail or "No deterministic scan was completed.",
                checked_tables=checked,
                recommended_action=recommended or "escalate",
                agent_name=self.AGENT_NAME,
            )

        truth = evidence.duplicate_row_count > 0
        claimed = payload.get("has_issue")
        if claimed is not None and bool(claimed) != truth:
            logger.warning(
                "DQ agent claimed has_issue=%s but the scan found %d duplicate rows in %s "
                "— deferring to the scan.",
                claimed,
                evidence.duplicate_row_count,
                evidence.table,
            )

        if truth:
            # The model's prose is discarded when it contradicts the scan.
            # Returning has_issue=True alongside "looks fine to me" would put a
            # self-contradicting sentence in a Teams message.
            contradicted = claimed is False
            return DataQualityFinding(
                has_issue=True,
                issue_type="duplicates",
                confidence=1.0,
                detail=evidence.headline() if contradicted else (detail or evidence.headline()),
                evidence=evidence,
                checked_tables=checked,
                recommended_action=(
                    "flag_and_notify" if contradicted else (recommended or "flag_and_notify")
                ),
                agent_name=self.AGENT_NAME,
            )

        contradicted = claimed is True
        return DataQualityFinding(
            has_issue=False,
            issue_type="none",
            confidence=1.0,
            detail=(
                f"No duplicate keys found in {evidence.table}."
                if contradicted
                else (detail or f"No duplicate keys found in {evidence.table}.")
            ),
            evidence=evidence,
            checked_tables=checked,
            recommended_action="no_action",
            agent_name=self.AGENT_NAME,
        )

    async def close(self) -> None:
        await self._provider.close()


def _parse_json_object(content: str) -> dict[str, Any]:
    """Tolerate fenced code blocks and leading prose around the JSON."""
    text = (content or "").strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1] if text.count("```") >= 2 else text.strip("`")
        text = text.removeprefix("json").strip()

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if 0 <= start < end:
            try:
                parsed = json.loads(text[start : end + 1])
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                pass
    logger.warning("Data Quality agent returned unparseable output")
    return {}


def _as_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
