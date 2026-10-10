# demux_realm

`demux_realm` contains the active Yggdrasil realm for demultiplexing planning and
pre-demux metadata preparation.

The realm currently:

* registers a `ygg.realm` entry point named `dmx_realm`;
* watches `demux_sample_info` and `flowcell_status` CouchDB changes;
* builds one Yggdrasil plan per flowcell: shared runfolder validation and
  metadata preparation, plus one independent branch per lane/settings
  combination;
* prepares the pre-demux `x_flowcells` document from runfolder XML files,
  sample-sheet payloads, and uploaded LIMS metadata.

## Setup

### Create and activate a Python 3.11 environment:

```bash
conda create -n ygg python=3.11
conda activate ygg
```

### Install Yggdrasil and this repository in editable mode:

The realm needs a Yggdrasil revision with independent-branch execution
(`failure_policy="continue_independent"`). That support is on the `dev` branch
and not yet on `main`, so cloning Yggdrasil's default branch is not enough.
There is no released minimum version yet. The realm was tested against `dev` at
`a461df6bd8606cd97ad0b669ef6e4c4f23160e43`.

```bash
git clone https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git
git -C Yggdrasil checkout dev
# Or the tested revision:
# git -C Yggdrasil checkout a461df6bd8606cd97ad0b669ef6e4c4f23160e43
pip install -e Yggdrasil

git clone https://github.com/NationalGenomicsInfrastructure/demux_realm.git
pip install -e "demux_realm[dev]"
```

Yggdrasil is kept out of the base install so the package can still be
installed without fetching a Git dependency. The `ygg` extra installs
Yggdrasil's `dev` branch instead of a local clone:

```bash
pip install -e ".[ygg,dev]"
```

To pin the tested revision without a local clone:

```bash
pip install "yggdrasil @ git+https://github.com/NationalGenomicsInfrastructure/Yggdrasil.git@a461df6bd8606cd97ad0b669ef6e4c4f23160e43"
```

## Yggdrasil Entry Point

On startup, Yggdrasil discovers the realm through `pyproject.toml`:

```toml
[project.entry-points."ygg.realm"]
dmx_realm = "demux_realm.descriptor:get_realm_descriptor"
```

The descriptor returns the `dmx_realm` realm with:

* `DemuxHandler`
* CouchDB watch specs for `demux_sample_info_db`
* CouchDB watch specs for `flowcell_status_db`

## Planning

A change to either source document is planned the same way. The handler fetches
the counterpart document, matching `SC...` and `ASC...` flowcell IDs, and checks
that the flowcell has been transferred (`transferred_to_hpc` and
`final_transfer_started` with a `destination_path`). The runfolder path is
`<destination_path>/<runfolder_id>`, prefixed by `DMX_HPC_BASE_PATH` when set.
The result must be absolute; a relative path would depend on the executing
process's working directory, so the proposal is rejected instead.

For a ready flowcell with `N` valid lane/settings combinations the handler
returns one plan, run automatically, with `2 + 5N` steps:

| Field | Value |
|---|---|
| `plan_id` | `dmx_realm:<canonical_flowcell_id>:demux` |
| `scope` | `{"kind": "flowcell", "id": "<canonical_flowcell_id>"}` |
| `failure_policy` | `continue_independent` |

```text
validate_runfolder
├── upsert_x_flowcell_pre_demux
├── lane_1_settings_0__materialize_config
│   └── lane_1_settings_0__generate_samplesheet
│       └── lane_1_settings_0__execute_demux
│           └── lane_1_settings_0__collect_results
│               └── lane_1_settings_0__upload_results
└── lane_2_settings_0__materialize_config
    └── ...
```

Both triggers produce the same plan for the same source data. Dependencies alone
decide execution order; steps run one at a time.

**Metadata relationship.** The metadata update and each branch's first step
depend only on `validate_runfolder`. If the metadata update fails, the branches
still run, and the attempt ends failed. To make branches wait for it instead,
add `upsert_x_flowcell_pre_demux` to `DemuxHandler.branch_prerequisites`: its
failure then blocks every branch.

**Branch identity.** Every samplesheet entry needs a top-level `lane`: a
non-negative integer or a string of digits (`1`, `"1"` and `"01"` are the same
lane). `settings_index` is a non-negative integer or a string of digits. A
lane's only entry may omit it and gets settings `0`; a lane with several entries
needs an explicit, distinct `settings_index` on each. Every
`BCLConvert_Data` row's `Lane` must match its entry's lane. Branches are ordered
by lane, then settings index, whatever the order of the source entries.

**Deferred and rejected proposals.** Neither produces work: the handler returns
one draft with no steps and `auto_run=False`, under the combined plan ID once
the flowcell is known. A proposal is deferred (`Deferred: ...`) while a
prerequisite is missing, such as the counterpart document, a transfer event, or
the samplesheets. It is rejected (`Rejected: ...`) when the runfolder path is
not absolute or any samplesheet entry is invalid; for samplesheet problems, the
draft's `preview["issues"]` lists every one with its entry
index. No shared step or valid branch runs for a rejected proposal.

Yggdrasil stores every draft under its plan ID, so a deferred or rejected draft
replaces the flowcell's stored plan. It does not cancel an attempt that is
already running.

**Parameters and preview.** Step parameters hold only what the steps use:
validation gets the runfolder identity, the metadata update gets every
samplesheet and the LIMS information, and each branch gets its own entry and
`demux_sample_info` metadata. Source document IDs, revisions and the trigger
source are recorded in the draft's preview only, so they never invalidate reuse.

## Execution

| Step | What it does now |
|---|---|
| `validate_runfolder` | Placeholder that always succeeds |
| `upsert_x_flowcell_pre_demux` | Reads `RunInfo.xml` and `RunParameters.xml`, then writes the `x_flowcells` document through DataAccess |
| `materialize_config` | Writes `extra_config_demultiplex.config` |
| `generate_samplesheet` | Validates the entry again and writes `SampleSheet.csv` |
| `execute_demux`, `collect_results`, `upload_results` | Simulations that only log |

`materialize_config` and `generate_samplesheet` declare their files as required
outputs, relative to their own step work directories. The metadata update
declares both XML files as inputs, so editing either re-runs it. A step is
reused on a later attempt when its parameters, declared inputs and declared
outputs are unchanged and its outputs still exist; removing a declared output
re-runs its producer. Re-running a step does not by itself re-run the steps that
depend on it.

When a branch step fails, the rest of that branch is blocked and every other
branch continues. The attempt then ends failed and its request is finished.
Approving the plan again does not rerun it: raise the plan document's
`run_token` with a revision-checked update, as described in Yggdrasil's plan
execution reference. Changing a source document instead regenerates the plan as
a new generation.

> **Warning: `x_flowcells` documents are replaced, not merged.** The metadata
> update saves by `name` with `mode="upsert"`. When a matching document exists,
> its whole body is replaced; only `_id` and `_rev` are kept, so fields written
> by other processes (for example post-demux `Json_Stats`) are lost. Plan and
> step IDs changed with the single-plan layout, so the first combined run for a
> flowcell repeats the metadata update and overwrites any existing document.
> Test against a disposable `x_flowcells` destination. A field-preserving write
> is a separate follow-up, needed before running against documents whose other
> fields must survive.

## Manual smoke test

1. Run one daemon with its own test internal storage, work root and event spool
   (`work_root` or `YGG_WORK_ROOT`, `YGG_EVENT_SPOOL`). Configure the
   `demux_sample_info_db`, `flowcell_status_db` and `x_flowcells_db`
   connections to test databases. `--dev` changes only the default work and
   spool paths; it does not redirect DataAccess writes.
2. Before enabling combined planning for a flowcell, let any old
   `dmx_realm:<fcid>:init` and `...:lane_<n>:settings_<m>` plans finish, or stop
   them, and make sure none will run again. Yggdrasil cannot tell that old and
   new plans overlap, because their plan IDs differ. Keep their records; nothing
   needs deleting or migrating.
3. Prepare matching source documents and a test runfolder containing
   `RunInfo.xml` and `RunParameters.xml`. With the watchers running, change the
   `demux_sample_info` document. Check that exactly one new plan
   `dmx_realm:<fcid>:demux` appears, with the flowcell scope, the
   `continue_independent` policy and the graph above. Repeat with a change to
   the `flowcell_status` document.
4. Inspect the attempt report or ops snapshot, the files under
   `<work_root>/dmx_realm:<fcid>:demux/<step_id>/`, and the saved `x_flowcells`
   document. Branch-failure behavior is covered by the automated tests; a
   malformed source document is rejected at planning and runs nothing.
5. Request a rerun by raising the plan's `run_token` with a revision-checked
   update, and check that unchanged steps are reused.

The first combined run repeats work: its plan and step IDs are new, so there are
no earlier results to reuse. Do not copy old success markers. Repeated source
changes may regenerate the plan more than once.

## Repository Layout

```text
demux_realm/
├── demux_realm/
│   ├── __init__.py
│   ├── descriptor.py
│   ├── handler.py
│   ├── recipes.py
│   ├── steps.py
│   └── utils.py
├── tests/
├── README.md
├── pyproject.toml
└── LICENSE
```

## Development

Run the test suite:

```bash
pytest
```

The execution tests run plans through the real Yggdrasil engine and coordinator
in temporary directories, with DataAccess replaced by a fake; they write to no
database.

Check that the package imports from the repository root:

```bash
python -c "import demux_realm; from demux_realm.descriptor import get_realm_descriptor"
```
