from typing import Any

from yggdrasil.core.realm import RealmDescriptor
from yggdrasil.watchers import EventType, WatchSpec

from .handler import DemuxHandler
from .utils import normalize_flowcell_id


def _build_demux_sample_info_scope(raw_event: Any) -> dict[str, str]:
    # Without a document, fall back to the change's _id (a UUID); the handler
    # fetches the document and plans under the canonical flowcell scope.
    doc = getattr(raw_event, "doc", None) or {}
    fcid = doc.get("flowcell_id", "")
    if fcid:
        return {"kind": "flowcell", "id": normalize_flowcell_id(fcid)}
    return {"kind": "flowcell", "id": getattr(raw_event, "id", "")}


def _build_demux_sample_info_payload(raw_event: Any) -> dict[str, Any]:
    doc = getattr(raw_event, "doc", None) or {}
    doc_id = getattr(raw_event, "id", "")
    return {
        "doc": doc,
        "doc_id": doc_id,
        "source": "demux_sample_info",
        "reason": f"doc_change:{doc_id or 'unknown'}",
    }


def _build_flowcell_status_scope(raw_event: Any) -> dict[str, str]:
    doc = getattr(raw_event, "doc", None) or {}
    # flowcell_status uses a UUID _id; flowcell_id is a separate field.
    # The watcher fetches each document individually, so doc is populated for non-deleted events.
    # raw_event.id (UUID) is a fallback only if the doc fetch somehow returned nothing.
    fcid = doc.get("flowcell_id", "") or getattr(raw_event, "id", "")
    return {"kind": "flowcell", "id": normalize_flowcell_id(fcid)}


def _build_flowcell_status_payload(raw_event: Any) -> dict[str, Any]:
    doc = getattr(raw_event, "doc", None) or {}
    doc_id = getattr(raw_event, "id", "") or doc.get("_id", "unknown")
    return {
        "doc": doc,
        "source": "flowcell_status",
        "reason": f"doc_change:{doc_id}",
    }


# JSON Logic filter applied to RawWatchEvent before any handler is called.
# Excludes:
#   - deleted document tombstones  ("deleted" field is True when a doc is removed)
#   - CouchDB internal design documents (id starts with "_design/")
# Both are surfaced by the _changes feed but have no business meaning here.
_SKIP_INTERNAL_CHANGES: dict = {
    "and": [
        {"!": {"var": "deleted"}},
        {"!": {"in": ["_design/", {"var": "id"}]}},
    ]
}


def _get_watchspecs() -> list[WatchSpec]:
    return [
        WatchSpec(
            backend="couchdb",
            connection="demux_sample_info_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            filter_expr=_SKIP_INTERNAL_CHANGES,
            build_scope=_build_demux_sample_info_scope,
            build_payload=_build_demux_sample_info_payload,
            target_handlers=["demux_handler"],
        ),
        WatchSpec(
            backend="couchdb",
            connection="flowcell_status_db",
            event_type=EventType.COUCHDB_DOC_CHANGED,
            filter_expr=_SKIP_INTERNAL_CHANGES,
            build_scope=_build_flowcell_status_scope,
            build_payload=_build_flowcell_status_payload,
            target_handlers=["demux_handler"],
        ),
    ]


def get_realm_descriptor() -> RealmDescriptor:
    return RealmDescriptor(
        realm_id="dmx_realm",
        handler_classes=[DemuxHandler],
        watchspecs=_get_watchspecs,
    )
