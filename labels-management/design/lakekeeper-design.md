# Lakekeeper — `UpdateLabels` REST Verb Design

**Implementation branch:** [`laskoviymishka/lakekeeper` `labels-crud-verb`](https://github.com/laskoviymishka/lakekeeper/tree/labels-crud-verb) (commit [`5d75cacf`](https://github.com/laskoviymishka/lakekeeper/commit/5d75cacf))
**Companion proposal:** [`labels-crud-followup.md`](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md)
**Status:** Draft PR / static-review only (sandbox cannot reach crates.io for `cargo check`)

## Goal

Implement the IRC `UpdateLabels` REST verb in Lakekeeper as the reference catalog
implementation. The notebook in this folder
([`labels-management.ipynb`](../notebook/labels-management.ipynb)) drives this
implementation end-to-end.

Wire shape (split shape, no envelope):

```http
POST /catalog/v1/{prefix}/namespaces/{ns}/tables/{tbl}/labels
Content-Type: application/json

{
  "updates":  [{"key": "domain", "value": "customer"},
               {"field-id": 3, "key": "pii-type", "value": "email"}],
  "removals": [{"key": "tier"},
               {"field-id": 3, "key": "sensitivity"}]
}

→ 200 OK
{
  "labels":        {"domain": "customer"},
  "column-labels": [{"field-id": 3, "labels": {"pii-type": "email"}}]
}
```

## Cross-PR map

| PR | Repo | Branch | Commit |
|----|------|--------|--------|
| 1  | [apache/iceberg](https://github.com/laskoviymishka/iceberg/tree/labels-crud-verb) | `labels-crud-verb` | [`037d84e`](https://github.com/laskoviymishka/iceberg/commit/037d84e) — SDK + wire types |
| 2  | **lakekeeper/lakekeeper (this PR)** | [`labels-crud-verb`](https://github.com/laskoviymishka/lakekeeper/tree/labels-crud-verb) | [`5d75cacf`](https://github.com/laskoviymishka/lakekeeper/commit/5d75cacf) |
| 3  | unitycatalog/unitycatalog | _pending — OSS UC reference impl_ | — |
| 4  | [trinodb/trino](https://github.com/laskoviymishka/trino/tree/labels-crud-verb) | `labels-crud-verb` | [`89504645`](https://github.com/laskoviymishka/trino/commit/89504645) — `ALTER TABLE … SET LABEL` DDL |
| 5  | [laskoviymishka/irc-labels](https://github.com/laskoviymishka/irc-labels/tree/labels-management) | `labels-management` | [`aec0dd6`](https://github.com/laskoviymishka/irc-labels/commit/aec0dd6) — e2e demo + notebook |

## Architecture

```
HTTP POST .../labels
        │
        ▼
crates/lakekeeper/src/api/iceberg/v1/tables.rs::router
        │  (axum route added for GET + POST on /labels)
        ▼
crates/lakekeeper/src/server/tables.rs::TablesService::update_labels
        │  (impl on CatalogServer, delegates to labels.rs handler)
        ▼
crates/lakekeeper/src/server/tables/labels.rs::update_labels
        │
        ├─► assert_writable(&request.updates) ── 403 LabelKeyNotWritable
        ├─► assert_writable(&request.removals) ── 403 LabelKeyNotWritable
        │       (atomic check — before any mutation)
        │
        └─► apply_request(&table, &request)
                │
                ▼
        LABEL_STORE: RwLock<HashMap<TableIdent, Labels>>
        (PoC in-memory storage; productionization swaps for Postgres)
```

The wire response is the **same shape** as what `LoadLabelsResponse` returns for the
read path, so the client gets the full post-update state without a second call.

## File-level breakdown

Build on top of the prior [`labels-poc`](https://github.com/laskoviymishka/lakekeeper/tree/labels-poc)
branch, which added `Labels` to `LoadTableResult`. The split shape replaces the
old nested envelope, and the new dedicated `/labels` endpoint joins the existing
LoadTable-inlined read.

| Layer | File | What |
|---|---|---|
| Wire types | `crates/iceberg-ext/src/catalog/rest/table.rs` | `Labels` (split shape: flat `labels` + `column_labels` keyed by `field-id`), `ColumnLabels`, `LabelEntry`, `UpdateLabelsRequest`; `impl_into_response!(Labels)` for axum |
| Re-exports | `crates/iceberg-ext/src/catalog/mod.rs` | Export the new types under `iceberg_ext::catalog::rest::*` |
| Endpoint enum | `crates/lakekeeper/src/api/endpoints.rs` | `CatalogV1::LoadLabels(GET, ...)` + `CatalogV1::UpdateLabels(POST, ...)` — picked up by `/v1/config` advertisement |
| Postgres migration | `crates/lakekeeper/migrations/20260512120000_add_labels_endpoints.sql` | `ALTER TYPE api_endpoints ADD VALUE …` for `catalog-v1-load-labels` and `catalog-v1-update-labels` |
| Routes + trait | `crates/lakekeeper/src/api/iceberg/v1/tables.rs` | `TablesService` trait gains `load_labels` + `update_labels`; axum router adds `GET`/`POST` on `/labels`; test stubs added to both `TestService` impls |
| Re-exports | `crates/lakekeeper/src/api/iceberg/mod.rs` | Export `Labels` + `UpdateLabelsRequest` under `api::iceberg::v1` |
| Handler | `crates/lakekeeper/src/server/tables/labels.rs` (new) | Atomic-check-before-mutate, 403 with spec error envelope, in-memory store keyed by `TableIdent` |
| Wiring | `crates/lakekeeper/src/server/tables.rs` | Imports `Labels`, `UpdateLabelsRequest`; adds `load_labels` + `update_labels` methods on `CatalogServer`'s `TablesService` impl that delegate to the handler |
| LoadTable migration | `crates/lakekeeper/src/server/tables/load_table.rs` | Migrate the old `Labels { table: Option<HashMap<…>> }` shape to the new split shape with `column_labels: Vec::new()` for V1 |

## Three key implementation decisions

### 1. Atomic check **before** any mutation

The spec is explicit that label updates are all-or-nothing. We enforce by validating
every entry (updates and removals together) against the catalog-managed read-only set
**before** taking the store lock for writes:

```rust
pub(super) async fn update_labels<…>(
    parameters: TableParameters,
    request: UpdateLabelsRequest,
    …
) -> Result<Labels> {
    let TableParameters { prefix: _, table } = parameters;

    // Atomic-check-before-mutate: validate the full request against the read-only
    // policy first; only mutate if every entry passes.
    assert_writable(&request.updates)?;
    assert_writable(&request.removals)?;

    Ok(apply_request(&table, &request))
}
```

`assert_writable` returns `Err(IcebergErrorResponse)` on the first read-only-key hit;
the early return means **`apply_request` is never reached** when any entry is invalid.
The notebook's scenario #8 verifies this externally: snapshot labels → send mixed
valid+invalid request → 403 → re-snapshot → state unchanged.

### 2. PoC storage is in-memory; productionization is a swap

```rust
static LABEL_STORE: LazyLock<RwLock<HashMap<TableIdent, Labels>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
```

A real Lakekeeper deployment needs Postgres-backed storage (one row per
`(table_id, key)` plus a `column_labels` join table). For a wire-contract PoC the
in-memory `RwLock<HashMap>` is sufficient and clearly tagged as PoC-only in
[the handler module docstring](https://github.com/laskoviymishka/lakekeeper/blob/labels-crud-verb/crates/lakekeeper/src/server/tables/labels.rs).

This choice is intentional: the goal of PR 2 is to demonstrate the verb shape, the
atomic semantics, and the error contract. Storage is mechanical — the wire shape
and handler signature don't change when we move to Postgres. The
[`labels-poc`](https://github.com/laskoviymishka/lakekeeper/tree/labels-poc) branch's
approach (labels as namespace-property prefix) is preserved for the LoadTable inline
read; the dedicated `/labels` endpoint gets fresh storage to avoid the
namespace-inheritance leak (every table in a namespace shares labels — wrong for
per-table writes).

### 3. Error envelope: `ErrorModel` builder, type + key

The spec error envelope is `{type, message, key}`. Lakekeeper's `ErrorModel` has a
`stack` field for structured detail, so we use that for the typed `key=…` marker
while keeping the human-readable key in `message` too:

```rust
fn label_key_not_writable(key: &str) -> ErrorModel {
    ErrorModel::builder()
        .message(format!(
            "Label key '{key}' is catalog-managed and cannot be written via the API."
        ))
        .r#type("LabelKeyNotWritable")
        .code(403)
        .stack(vec![format!("key={key}")])
        .build()
}
```

Engines that need to programmatically extract the offending key can parse the
`stack[0]`; engines that just want to render the error use the `message`. This
matches the spec contract under §Technical Specification → Error contracts.

## Read-only key policy

V1 hardcodes the policy at module scope:

```rust
const READ_ONLY_KEYS: &[&str] = &[
    "last-accessed-at",
    "last-edit-by",
    "query-count-24h",
    "sla-latency-p99",
    "migration-timestamp",
];
```

These match the canonical examples from the spec proposal. A productionization pass
moves the list to per-warehouse policy in Postgres, with a management API for the
warehouse admin to update. The wire contract doesn't change — the catalog still
returns `403 LabelKeyNotWritable` with the offending key; the key set is just
configurable instead of hardcoded.

## Endpoint advertisement via `/v1/config`

Lakekeeper's `CatalogV1Endpoint` enum (generated by the `generate_endpoints!` macro
in [`api/endpoints.rs`](https://github.com/laskoviymishka/lakekeeper/blob/labels-crud-verb/crates/lakekeeper/src/api/endpoints.rs))
drives the `endpoints[]` array returned by `GET /v1/config`. Adding the two variants
is one line each:

```rust
LoadLabels(GET,  "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/labels"),
UpdateLabels(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/labels"),
```

The macro produces `EndpointFlat::CatalogV1LoadLabels` and `…UpdateLabels`, which
serialize to `catalog-v1-load-labels` and `catalog-v1-update-labels` in JSON (kebab
case via `strum`). Engines that don't see these in the advertisement should refuse
the SQL surface at parse time — exactly what Trino PR 4's `ConnectorMetadata.setTableLabels`
default `NOT_SUPPORTED` throw enables.

## Postgres enum migration

`EndpointFlat` is also a Postgres `api_endpoints` enum (used by the
`endpoint_statistics` table). Adding new variants requires a migration:

```sql
-- 20260512120000_add_labels_endpoints.sql
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'catalog-v1-load-labels';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'catalog-v1-update-labels';
```

Without this, runtime endpoint-stats writes would fail. The Lakekeeper migration
binary runs this on startup (the docker-compose `migrate` service).

## Inheritance from `labels-poc`

The prior [`labels-poc`](https://github.com/laskoviymishka/lakekeeper/tree/labels-poc)
branch (commit [`1a7643b9`](https://github.com/laskoviymishka/lakekeeper/commit/1a7643b9))
added:

- A `Labels` struct with the **old nested envelope** (`Labels { table: Option<HashMap<…>> }`)
- A read-only path: `LoadTableResult.labels` populated from namespace properties with
  the `label.*` prefix

`labels-crud-verb` builds on this branch and:

- **Replaces the envelope** with the spec's split shape (flat `labels` + `column_labels`)
- **Adds the dedicated `/labels` endpoint** for read + write, decoupled from `LoadTable`
- **Preserves the namespace-inheritance read** in `load_table.rs` as a convenience
  (skip a round-trip when loading a table), with the shape migrated to split

The two paths coexist: column-labels go only through the dedicated endpoint;
table-level labels can come from either source. A productionization pass picks one
canonical source — the dedicated endpoint, with namespace inheritance as a defaulting
layer below the per-table override.

## V1 scope cuts

| Cut | Why |
|---|---|
| Postgres-backed storage | PoC uses in-memory store. The wire shape, atomic semantics, and error contract don't depend on storage; switching to a Postgres `labels` table is a follow-up commit. |
| Authorization on labels read/write | Production-grade catalogs add the same `LoadTable`-style authz check. Current handler skips it. |
| ETag / `If-Match` optimistic concurrency | Last-writer-wins for V1. The Iceberg SDK supports `mutationHeaders`; Lakekeeper can opt into the contract when needed. |
| Per-warehouse / per-project read-only-key policy | Global hardcoded list for now. Future: management API to configure the writable-key policy per warehouse. |
| `Idempotency-Key` header support | The spec aligns with IRC's existing `idempotency-key-lifetime` config. Easy to add via `mutationHeaders`; not exercised in the PoC. |

## How to build and run

The docker-compose in this folder builds the patched Lakekeeper from your local
checkout. Setup:

```bash
cd ~
git clone git@github.com:laskoviymishka/lakekeeper.git
cd lakekeeper
git checkout labels-crud-verb

cd ~/irc-labels/labels-management
cp .env.example .env  # ensure LAKEKEEPER_SRC points at ~/lakekeeper
docker-compose up -d  # first build ~5 min for Rust release
```

Then open [`notebook/labels-management.ipynb`](../notebook/labels-management.ipynb)
in Jupyter at `http://localhost:8888/lab` (token: `demo`) and run cells top to bottom.
Every scenario asserts a spec invariant; the notebook is also a runnable test of the
implementation.

## Open questions

1. **Per-warehouse `endpoints[]` filtering.** Should a warehouse admin be able to
   opt out of labels writes (keep reads enabled) per warehouse, so the `/v1/config`
   for that warehouse omits `catalog-v1-update-labels`? The Iceberg SDK already
   enforces — engines call `Endpoint.check(endpoints, V1_UPDATE_LABELS)` before
   issuing — so the catalog just needs to filter the advertised set.
2. **Initial values for catalog-managed keys.** The spec's open question #6: should
   Lakekeeper return synthetic values for read-only keys (e.g. `last-accessed-at`
   defaulting to table creation time) so reads aren't empty before any observation?
   Easy to implement; question is whether it's spec-mandated or implementation
   choice. PoC chooses to NOT synthesize.
3. **Bulk writes across tables.** A classifier updating labels on 10k tables today
   makes 10k requests. Worth a bulk-write verb? Out of scope for V1; revisit if
   bulk becomes a real bottleneck for real adopters.

## References

- [labels-crud-followup.md](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md) — the proposal
- [discussion-split-shape.md](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/discussion-split-shape.md) — why split, not nested envelope
- [trino-design.md](trino-design.md) — companion engine integration
