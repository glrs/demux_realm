"""Plans built by the realm's handler, run by the real Yggdrasil engine.

External writes are faked at the DataAccess boundary; work and event spool
directories are temporary. Runtime failures are injected by swapping a realm
step for another @step function, never through plan parameters.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from realm_support import (
    REALM_ID,
    RUN_INFO_XML_TEXT,
    RUN_PARAMETERS_XML_TEXT,
    demux_sample_info_doc,
    flowcell_status_doc,
    lane_entry,
    planning_ctx,
)
from yggdrasil.core import engine as engine_module
from yggdrasil.core.engine import Engine
from yggdrasil.daemon.plan_execution import (
    DAEMON_CLAIM,
    ExecutionStatus,
    PlanExecutionCoordinator,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.outcomes import ExecutionOutcome, StepOutcome, TerminationReason
from yggdrasil.flow.step import step
from yggdrasil.storage import build_internal_storage
from yggdrasil.storage.plan_eligibility import is_plan_eligible
from yggdrasil.storage.sqlite import SQLiteInternalStore

from demux_realm import steps as realm_steps
from demux_realm.handler import DemuxHandler
from demux_realm.recipes import UPSERT_X_FLOWCELL, VALIDATE_RUNFOLDER

SUCCEEDED = StepOutcome.SUCCEEDED
REUSED = StepOutcome.REUSED
FAILED = StepOutcome.FAILED
BLOCKED = StepOutcome.BLOCKED

RUNFOLDER_ID = "20260312_SH01140_0005_ASC2177698-SC3"
FCID = "SC2177698-SC3"
LANE_1 = [
    f"lane_1_settings_0__{stage}"
    for stage in (
        "materialize_config",
        "generate_samplesheet",
        "execute_demux",
        "collect_results",
        "upload_results",
    )
]
LANE_2 = [step_id.replace("lane_1_", "lane_2_") for step_id in LANE_1]


class FakeXFlowcells:
    """Execution-phase x_flowcells_db client that records saves."""

    def __init__(self) -> None:
        self.saved: list[tuple[dict, dict]] = []
        self.error: Exception | None = None

    def save(self, doc: dict, **kwargs) -> SimpleNamespace:
        if self.error is not None:
            raise self.error
        self.saved.append((doc, kwargs))
        return SimpleNamespace(status="created", doc_id="xfc-1")


@pytest.fixture(autouse=True)
def x_flowcells(monkeypatch):
    """Replace every DataAccess the engine builds, so no test reads real config."""
    client = FakeXFlowcells()

    class FakeDataAccess:
        def __init__(self, realm_id, phase, cfg=None, trace_context=None):
            assert (realm_id, phase) == (REALM_ID, "execution")

        def connection(self, name):
            assert name == "x_flowcells_db"
            return client

    monkeypatch.setattr(engine_module, "DataAccess", FakeDataAccess)
    return client


@pytest.fixture
def ws(tmp_path, monkeypatch):
    monkeypatch.delenv("DMX_HPC_BASE_PATH", raising=False)
    monkeypatch.setenv("YGG_WORK_ROOT", str(tmp_path / "work"))
    monkeypatch.setenv("YGG_EVENT_SPOOL", str(tmp_path / "spool"))
    incoming = tmp_path / "incoming"
    runfolder = incoming / RUNFOLDER_ID
    runfolder.mkdir(parents=True)
    (runfolder / "RunInfo.xml").write_text(RUN_INFO_XML_TEXT)
    (runfolder / "RunParameters.xml").write_text(RUN_PARAMETERS_XML_TEXT)
    return SimpleNamespace(
        root=tmp_path,
        incoming=incoming,
        runfolder=runfolder,
        work=tmp_path / "work",
        spool=tmp_path / "spool",
    )


async def draft_for(ws, samplesheets):
    handler = DemuxHandler()
    handler.realm_id = REALM_ID
    fc_doc = flowcell_status_doc(
        FCID, destination_path=str(ws.incoming), runfolder_id=RUNFOLDER_ID
    )
    ctx = planning_ctx(
        FCID, demux_doc=demux_sample_info_doc(samplesheets, FCID), fc_doc=fc_doc
    )
    (draft,) = await handler.generate_plan_drafts(
        {"source": "flowcell_status", "doc": fc_doc, "planning_ctx": ctx}
    )
    assert draft.auto_run is True
    return draft


def plan_for(ws, samplesheets):
    return asyncio.run(draft_for(ws, samplesheets)).plan


def engine_for(ws) -> Engine:
    return Engine(work_root=ws.work, emitter=FileSpoolEmitter(spool_dir=ws.spool))


def instrument(monkeypatch, name, calls, fail_when=None):
    """Swap a realm step for a @step that records its calls and may fail."""
    body = getattr(realm_steps, name).__wrapped__

    @step(name=name)
    def instrumented(ctx, scenario):
        calls.append((name, scenario.get("lane_id"), scenario.get("settings_index")))
        if fail_when is not None and fail_when(scenario):
            raise RuntimeError(f"planned failure in {name}")
        return body(ctx, scenario)

    monkeypatch.setattr(realm_steps, name, instrumented)


def artifact_events(spool: Path, execution_id: str) -> dict[str, dict[str, str]]:
    """Return {step_id: {artifact key: path}} from one attempt's artifact events."""
    found: dict[str, dict[str, str]] = {}
    for path in spool.rglob("*.json"):
        event = json.loads(path.read_text())
        if (
            event.get("type") == "step.artifact"
            and event.get("execution_id") == execution_id
        ):
            artifact = event["artifact"]
            found.setdefault(event["step_id"], {})[artifact["key"]] = artifact["path"]
    return found


def test_early_branch_failure_leaves_later_branch_running(ws, monkeypatch):
    calls: list = []
    instrument(
        monkeypatch, "execute_demux", calls, fail_when=lambda s: s["lane_id"] == "1"
    )
    instrument(monkeypatch, "upload_results", calls)
    plan = plan_for(ws, [lane_entry(1), lane_entry(2)])

    report = engine_for(ws).run(plan)

    assert report.termination_reason is TerminationReason.COMPLETED
    assert report.outcome is ExecutionOutcome.FAILED
    assert report.step_outcomes == {
        VALIDATE_RUNFOLDER: SUCCEEDED,
        UPSERT_X_FLOWCELL: SUCCEEDED,
        LANE_1[0]: SUCCEEDED,
        LANE_1[1]: SUCCEEDED,
        LANE_1[2]: FAILED,
        LANE_1[3]: BLOCKED,
        LANE_1[4]: BLOCKED,
        **dict.fromkeys(LANE_2, SUCCEEDED),
    }
    assert report.direct_blockers[LANE_1[3]] == [LANE_1[2]]
    assert report.failed_ancestors[LANE_1[4]] == [LANE_1[2]]
    # Lane 1 had already failed when lane 2 went on to its upload.
    assert calls == [
        ("execute_demux", "1", "0"),
        ("execute_demux", "2", "0"),
        ("upload_results", "2", "0"),
    ]


def test_validation_failure_blocks_metadata_and_every_branch(
    ws, monkeypatch, x_flowcells
):
    instrument(monkeypatch, "validate_runfolder", [], fail_when=lambda s: True)
    plan = plan_for(ws, [lane_entry(1), lane_entry(2)])

    report = engine_for(ws).run(plan)

    assert report.outcome is ExecutionOutcome.FAILED
    assert report.step_outcomes == {
        VALIDATE_RUNFOLDER: FAILED,
        UPSERT_X_FLOWCELL: BLOCKED,
        **dict.fromkeys(LANE_1 + LANE_2, BLOCKED),
    }
    assert x_flowcells.saved == []


def test_metadata_failure_lets_independent_branches_finish(ws, x_flowcells):
    x_flowcells.error = RuntimeError("x_flowcells unavailable")
    plan = plan_for(ws, [lane_entry(1), lane_entry(2)])

    report = engine_for(ws).run(plan)

    assert report.termination_reason is TerminationReason.COMPLETED
    assert report.outcome is ExecutionOutcome.FAILED
    assert report.step_outcomes == {
        VALIDATE_RUNFOLDER: SUCCEEDED,
        UPSERT_X_FLOWCELL: FAILED,
        **dict.fromkeys(LANE_1 + LANE_2, SUCCEEDED),
    }


def test_required_metadata_failure_blocks_every_branch(ws, monkeypatch, x_flowcells):
    monkeypatch.setattr(
        DemuxHandler, "branch_prerequisites", (VALIDATE_RUNFOLDER, UPSERT_X_FLOWCELL)
    )
    x_flowcells.error = RuntimeError("x_flowcells unavailable")
    plan = plan_for(ws, [lane_entry(1), lane_entry(2)])

    report = engine_for(ws).run(plan)

    assert report.outcome is ExecutionOutcome.FAILED
    assert report.step_outcomes == {
        VALIDATE_RUNFOLDER: SUCCEEDED,
        UPSERT_X_FLOWCELL: FAILED,
        **dict.fromkeys(LANE_1 + LANE_2, BLOCKED),
    }
    assert report.direct_blockers[LANE_1[0]] == [UPSERT_X_FLOWCELL]


def test_one_settings_branch_fails_while_its_sibling_continues(ws, monkeypatch):
    calls: list = []
    instrument(monkeypatch, "validate_runfolder", calls)
    instrument(
        monkeypatch,
        "execute_demux",
        calls,
        fail_when=lambda s: s["settings_index"] == "0",
    )
    plan = plan_for(
        ws, [lane_entry(1, settings_index=0), lane_entry(1, settings_index=1)]
    )

    report = engine_for(ws).run(plan)

    settings_1 = [step_id.replace("settings_0", "settings_1") for step_id in LANE_1]
    assert [report.step_outcomes[step_id] for step_id in LANE_1] == [
        SUCCEEDED,
        SUCCEEDED,
        FAILED,
        BLOCKED,
        BLOCKED,
    ]
    assert [report.step_outcomes[step_id] for step_id in settings_1] == [SUCCEEDED] * 5
    assert [call for call in calls if call[0] == "validate_runfolder"] == [
        ("validate_runfolder", None, None)
    ]


def test_unchanged_rerun_reuses_and_missing_output_reruns_its_producer(ws):
    plan = plan_for(ws, [lane_entry(1), lane_entry(2)])
    engine = engine_for(ws)
    step_ids = [spec.step_id for spec in plan.steps]
    plan_dir = ws.work / plan.plan_id

    first = engine.run(plan)

    assert first.outcome is ExecutionOutcome.SUCCEEDED
    assert first.step_outcomes == dict.fromkeys(step_ids, SUCCEEDED)
    artifacts = artifact_events(ws.spool, first.execution_id)
    for spec in plan.steps:
        declared = {
            key: plan_dir / spec.step_id / name for key, name in spec.outputs.items()
        }
        assert all(path.is_file() for path in declared.values())
        assert artifacts.get(spec.step_id, {}) == {
            key: str(path) for key, path in declared.items()
        }
    lane_1_sheet = plan_dir / LANE_1[1] / "SampleSheet.csv"
    assert "L1S0_1" in lane_1_sheet.read_text()
    assert "L2S0_1" not in lane_1_sheet.read_text()

    second = engine.run(plan)

    assert second.outcome is ExecutionOutcome.SUCCEEDED
    assert second.step_outcomes == dict.fromkeys(step_ids, REUSED)

    lane_1_sheet.unlink()
    third = engine.run(plan)

    assert third.step_outcomes == {
        **dict.fromkeys(step_ids, REUSED),
        LANE_1[1]: SUCCEEDED,
    }
    assert lane_1_sheet.is_file()


def test_runfolder_xml_change_reruns_only_the_metadata_update(ws, x_flowcells):
    plan = plan_for(ws, [lane_entry(1)])
    engine = engine_for(ws)
    engine.run(plan)
    (ws.runfolder / "RunParameters.xml").write_text(
        RUN_PARAMETERS_XML_TEXT.replace(
            "<RunCounter>5</RunCounter>", "<RunCounter>6</RunCounter>"
        )
    )

    report = engine.run(plan)

    assert report.step_outcomes == {
        **dict.fromkeys((spec.step_id for spec in plan.steps), REUSED),
        UPSERT_X_FLOWCELL: SUCCEEDED,
    }
    (first_doc, first_kwargs), (second_doc, _) = x_flowcells.saved
    assert first_kwargs == {
        "selector": {"name": {"$eq": "20260312_ASC2177698-SC3"}},
        "mode": "upsert",
    }
    assert first_doc["RunInfo"]["Flowcell"] == FCID
    assert [row["Sample_ID"] for row in first_doc["samplesheet_csv"]] == ["L1S0_1"]
    assert first_doc["RunParameters"]["RunCounter"] == "5"
    assert second_doc["RunParameters"]["RunCounter"] == "6"


@pytest.mark.asyncio
async def test_finished_failed_continuation_needs_a_new_run_request(ws, monkeypatch):
    instrument(
        monkeypatch, "execute_demux", [], fail_when=lambda s: s["lane_id"] == "1"
    )
    plan = (await draft_for(ws, [lane_entry(1), lane_entry(2)])).plan
    database = ws.root / "ygg.sqlite3"
    storage = build_internal_storage(
        {"internal_storage": {"backend": "sqlite", "sqlite": {"path": str(database)}}},
        dev_mode=True,
    )
    doc_id = storage.plans.save_plan(plan, plan.realm, plan.scope, auto_run=True)
    coordinator = PlanExecutionCoordinator(
        engine=engine_for(ws), plan_store=storage.plans
    )

    first = await coordinator.execute(doc_id, DAEMON_CLAIM)

    assert first.status is ExecutionStatus.FINALIZED
    assert first.report.termination_reason is TerminationReason.COMPLETED
    assert first.report.outcome is ExecutionOutcome.FAILED
    doc = storage.plans.fetch_plan(doc_id)
    assert doc["status"] == "approved"
    assert doc["executed_run_token"] == doc["run_token"] == 0
    assert doc["last_finalized_execution"]["outcome"] == "failed"
    assert not is_plan_eligible(doc)
    again = await coordinator.execute(doc_id, DAEMON_CLAIM)
    assert again.status is ExecutionStatus.NOT_ELIGIBLE

    # An operator's rerun request: a higher run_token, written at the read revision.
    doc["run_token"] += 1
    SQLiteInternalStore(database).put_document(
        "plans", doc_id, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
    )
    rerun = await coordinator.execute(doc_id, DAEMON_CLAIM)

    assert rerun.status is ExecutionStatus.FINALIZED
    assert rerun.report.step_outcomes == {
        VALIDATE_RUNFOLDER: REUSED,
        UPSERT_X_FLOWCELL: REUSED,
        LANE_1[0]: REUSED,
        LANE_1[1]: REUSED,
        LANE_1[2]: FAILED,
        LANE_1[3]: BLOCKED,
        LANE_1[4]: BLOCKED,
        **dict.fromkeys(LANE_2, REUSED),
    }
    assert storage.plans.fetch_plan(doc_id)["executed_run_token"] == 1
