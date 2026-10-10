import logging
import os
from typing import Any, ClassVar

from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner import PlanDraft, PlanningContext
from yggdrasil.watchers import EventType

from .recipes import (
    UPSERT_X_FLOWCELL,
    VALIDATE_RUNFOLDER,
    branch_namespace,
    build_demux_plan,
)
from .utils import group_samplesheet_branches, normalize_flowcell_id

logger = logging.getLogger(__name__)


def _source_ref(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    """Identify a source document for provenance, or None if it is unknown."""
    if not doc:
        return None
    return {"_id": doc.get("_id"), "_rev": doc.get("_rev")}


def _describe_issue(issue: dict[str, Any]) -> str:
    index = issue["entry_index"]
    location = "samplesheets" if index is None else f"samplesheets[{index}]"
    return f"{location}: {issue['reason']}"


class DemuxHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "demux_handler"
    # Shared steps each branch's first step depends on. Adding UPSERT_X_FLOWCELL
    # makes every branch wait for, and be blocked by, the metadata update.
    branch_prerequisites: ClassVar[tuple[str, ...]] = (VALIDATE_RUNFOLDER,)

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Since build_scope already generates the clean dict, but derive_scope is required
        for direct handler triggers or injections, we can parse it from doc.
        """
        fcid = doc.get("flowcell_id", doc.get("_id", "unknown"))
        return {"kind": "flowcell", "id": normalize_flowcell_id(fcid)}

    def _diagnostic(
        self,
        plan_id: str,
        scope: dict[str, Any],
        status: str,
        note: str,
        preview: dict[str, Any],
    ) -> PlanDraft:
        """Return a non-runnable draft (no steps, not auto-run) explaining why."""
        return PlanDraft(
            plan=Plan(
                plan_id=plan_id,
                realm=self._require_realm_id(),
                scope=scope,
                steps=[],
            ),
            auto_run=False,
            approvals_required=[],
            notes=f"{status.capitalize()}: {note}",
            preview={"status": status, "reason": note, **preview},
        )

    def _deferred(self, ctx: PlanningContext, source_db: str, note: str) -> PlanDraft:
        """Defer a trigger whose canonical flowcell is not known yet.

        Keeps the trigger's own scope and identity rather than guessing a flowcell.
        """
        trigger_id = ctx.scope.get("id") or "unknown"
        return self._diagnostic(
            f"{self._require_realm_id()}:{trigger_id}",
            dict(ctx.scope),
            "deferred",
            note,
            {"triggering_source": source_db},
        )

    def _flowcell_diagnostic(
        self, canonical_fcid: str, status: str, note: str, preview: dict[str, Any]
    ) -> PlanDraft:
        """Non-runnable draft under the flowcell's combined plan identity."""
        return self._diagnostic(
            self._plan_id(canonical_fcid),
            {"kind": "flowcell", "id": canonical_fcid},
            status,
            note,
            {"canonical_flowcell_id": canonical_fcid, **preview},
        )

    def _plan_id(self, canonical_fcid: str) -> str:
        return f"{self._require_realm_id()}:{canonical_fcid}:demux"

    def _get_last_event(self, events: list, event_type: str) -> dict | None:
        """Helper to find the last occurring event of a certain type"""
        matches = [
            e
            for e in events
            if e.get("event") == event_type
            or e.get("type") == event_type
            or e.get("event_type") == event_type
        ]
        if not matches:
            return None
        return matches[-1]

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        ctx: PlanningContext = payload["planning_ctx"]
        source_db = payload.get("source", "unknown")

        logger.info(f"Planning demux triggered by {source_db}")

        demux_db = ctx.data.couchdb("demux_sample_info_db")
        fc_db = ctx.data.couchdb("flowcell_status_db")

        if source_db == "demux_sample_info":
            # Fetch by the change's document ID: the event may not carry the document,
            # in which case the trigger scope holds that UUID, not a flowcell ID.
            doc_id = payload.get("doc_id") or ctx.scope.get("id", "")
            if not doc_id:
                return [
                    self._deferred(
                        ctx, source_db, "demux_sample_info trigger missing doc_id."
                    )
                ]
            if doc_id.startswith("_"):
                # Design docs and internal CouchDB documents — should be filtered by
                # WatchSpec.filter_expr but guard here as defense-in-depth.
                return [
                    self._deferred(
                        ctx,
                        source_db,
                        f"Skipping internal CouchDB document '{doc_id}'.",
                    )
                ]

            demux_doc = await demux_db.get(doc_id)
            if not demux_doc:
                return [
                    self._deferred(
                        ctx,
                        source_db,
                        f"demux_sample_info document '{doc_id}' not found or deleted.",
                    )
                ]

            canonical_fcid = normalize_flowcell_id(demux_doc.get("flowcell_id", ""))
            if not canonical_fcid:
                return [
                    self._deferred(
                        ctx,
                        source_db,
                        "demux_sample_info document missing flowcell_id.",
                    )
                ]

            fc_doc = await fc_db.find_one(
                {"flowcell_id": {"$in": [canonical_fcid, f"A{canonical_fcid}"]}}
            )

        else:
            # flowcell_status: _id is a UUID hash; flowcell_id is a separate field.
            fc_doc = payload.get("doc") or None
            if not fc_doc:
                return [
                    self._deferred(
                        ctx,
                        source_db,
                        "flowcell_status trigger missing document in payload.",
                    )
                ]

            canonical_fcid = normalize_flowcell_id(fc_doc.get("flowcell_id", ""))
            if not canonical_fcid:
                return [
                    self._deferred(
                        ctx, source_db, "flowcell_status document missing flowcell_id."
                    )
                ]

            demux_doc = await demux_db.find_one(
                {"flowcell_id": {"$in": [canonical_fcid, f"A{canonical_fcid}"]}}
            )

        logger.info(f"Planning demux for {canonical_fcid} triggered by {source_db}")

        provenance = {
            "triggering_source": source_db,
            "sources": {
                "flowcell_status": _source_ref(fc_doc),
                "demux_sample_info": _source_ref(demux_doc),
            },
        }

        def defer(note: str) -> list[PlanDraft]:
            return [
                self._flowcell_diagnostic(canonical_fcid, "deferred", note, provenance)
            ]

        if not fc_doc:
            return defer(
                f"No flowcell_status document found for flowcell_id '{canonical_fcid}'."
            )
        if not demux_doc:
            return defer(
                f"No demux_sample_info document found for flowcell_id '{canonical_fcid}'."
            )

        # Readiness Checks
        events = fc_doc.get("events", [])
        transferred_event = self._get_last_event(events, "transferred_to_hpc")
        final_transfer_event = self._get_last_event(events, "final_transfer_started")

        if not transferred_event:
            return defer("flowcell_status missing 'transferred_to_hpc' event.")

        if not final_transfer_event:
            return defer("flowcell_status missing 'final_transfer_started' event.")

        destination_path = final_transfer_event.get("data", {}).get("destination_path")
        if not destination_path:
            return defer("final_transfer_started event missing destination_path.")

        runfolder_id = fc_doc.get("runfolder_id", fc_doc.get("runfolder_name"))
        if not runfolder_id:
            return defer("flowcell_status missing runfolder_id.")

        samplesheets = demux_doc.get("samplesheets")
        if samplesheets is None or samplesheets == []:
            return defer("demux_sample_info missing samplesheets.")

        hpc_base = os.environ.get("DMX_HPC_BASE_PATH", "")
        if hpc_base:
            hpc_runfolder_path = os.path.join(
                hpc_base, destination_path.lstrip("/"), runfolder_id
            )
            logger.debug(
                "DMX_HPC_BASE_PATH='%s'; resolved runfolder path: %s",
                hpc_base,
                hpc_runfolder_path,
            )
        else:
            hpc_runfolder_path = os.path.join(destination_path, runfolder_id)
        # A relative path would resolve against the executing process's cwd.
        if not os.path.isabs(hpc_runfolder_path):
            return [
                self._flowcell_diagnostic(
                    canonical_fcid,
                    "rejected",
                    f"runfolder path '{hpc_runfolder_path}' is not absolute; "
                    "DMX_HPC_BASE_PATH or destination_path must be absolute.",
                    provenance,
                )
            ]

        branches, issues = group_samplesheet_branches(samplesheets)
        if issues:
            logger.warning(
                "Rejecting demux proposal for %s: %d samplesheet issue(s).",
                canonical_fcid,
                len(issues),
            )
            note = (
                f"{len(issues)} samplesheet issue(s) in demux_sample_info; "
                f"no work is planned: "
                + "; ".join(_describe_issue(issue) for issue in issues)
            )
            return [
                self._flowcell_diagnostic(
                    canonical_fcid, "rejected", note, {**provenance, "issues": issues}
                )
            ]

        plan = build_demux_plan(
            plan_id=self._plan_id(canonical_fcid),
            realm=self._require_realm_id(),
            canonical_fcid=canonical_fcid,
            runfolder_id=runfolder_id,
            hpc_runfolder_path=hpc_runfolder_path,
            samplesheets=samplesheets,
            uploaded_lims_info=demux_doc.get("uploaded_lims_info", []),
            metadata=demux_doc.get("metadata", {}),
            branches=branches,
            branch_prerequisites=self.branch_prerequisites,
        )
        branch_summaries = []
        for branch in branches:
            prefix = f"{branch_namespace(branch.lane_id, branch.settings_index)}__"
            branch_summaries.append(
                {
                    "lane_id": branch.lane_id,
                    "settings_index": branch.settings_index,
                    "source_index": branch.source_index,
                    "step_ids": [
                        s.step_id for s in plan.steps if s.step_id.startswith(prefix)
                    ],
                }
            )
        logger.info(
            "Planned %s with %d lane/settings branch(es).",
            plan.plan_id,
            len(branches),
        )
        return [
            PlanDraft(
                plan=plan,
                auto_run=True,
                approvals_required=[],
                notes=(
                    f"Ready for demultiplexing {canonical_fcid}: "
                    f"{len(branches)} lane/settings branch(es)."
                ),
                preview={
                    "status": "ready",
                    "canonical_flowcell_id": canonical_fcid,
                    **provenance,
                    "runfolder_id": runfolder_id,
                    "hpc_runfolder_path": hpc_runfolder_path,
                    "failure_policy": plan.failure_policy,
                    "shared_step_ids": [VALIDATE_RUNFOLDER, UPSERT_X_FLOWCELL],
                    "branch_prerequisites": list(self.branch_prerequisites),
                    "metadata_required": UPSERT_X_FLOWCELL in self.branch_prerequisites,
                    "branches": branch_summaries,
                    "step_count": len(plan.steps),
                },
            )
        ]
