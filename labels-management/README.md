# Labels Management — Write-Path E2E

End-to-end demo of the **write path** for IRC labels: catalog-scoped label CRUD via the
`UpdateLabels` REST verb proposed in [labels-crud-followup.md](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md)
and prototyped across four coordinated PRs.

This complements the read-path demo in the parent repo (which exercises labels surfaced
via `LoadTableResponse.labels`). Together they show the full read+write loop for IRC labels.

## The PR chain

| PR | Repo | Branch | What |
|----|------|--------|------|
| 1 | [apache/iceberg](https://github.com/laskoviymishka/iceberg/tree/labels-crud-verb) | `labels-crud-verb` | Wire shape: `UpdateLabelsRequest`, `LoadLabelsResponse`, `RESTSessionCatalog.updateLabels(...)`, `Endpoint.V1_UPDATE_LABELS` |
| 2 | [lakekeeper/lakekeeper](https://github.com/laskoviymishka/lakekeeper/tree/labels-crud-verb) | `labels-crud-verb` | Reference impl: `GET` + `POST /v1/{prefix}/.../labels`, atomic check, 403 LabelKeyNotWritable |
| 3 | unitycatalog/unitycatalog | _pending_ | OSS UC reference impl (parallel to PR 2) |
| 4 | [trinodb/trino](https://github.com/laskoviymishka/trino/tree/labels-crud-verb) (local) | `labels-crud-verb` | `ALTER TABLE … SET LABEL` SQL grammar + SPI + IcebergMetadata impl |
| 5 | this folder | — | E2E demo wiring it together |

## What this demo proves

1. **Split-shape wire** — `{labels: {...}, "column-labels": [{"field-id": N, "labels": {...}}]}` is
   symmetric for read (GET) and write (POST). No envelope, no `subject` field.
2. **Atomic check before mutate** — the catalog validates *every* entry against the read-only
   policy before applying any mutation. One bad entry rejects the whole request.
3. **`403 LabelKeyNotWritable`** carries the offending key so engines can render a clear error
   to the user without a parse-time schema endpoint.
4. **Capability discovery** — `GET /v1/config` advertises `catalog-v1-load-labels` and
   `catalog-v1-update-labels` in the `endpoints[]` array. Engines that see them enable the
   SQL surface; otherwise it's a parse-time error.
5. **Lifecycle invariant** — label writes never touch `metadata.json`, never create snapshots,
   never appear in time-travel queries.

## Stack

- **Postgres 17** — Lakekeeper's metadata store
- **MinIO** — S3-compatible storage for Iceberg data
- **Lakekeeper** (PR 2 branch, built from source) — IRC catalog with `/labels` endpoints
- **Jupyter** — notebook driving the demo

No Trino in this round — the demo uses direct REST calls so the wire flow is visible.
For the Trino DDL surface, see PR 4 (`ALTER TABLE … SET LABEL`); a follow-up demo can
build a patched Trino image once the PR 1 Iceberg SDK is published.

## Setup

You need a local checkout of the patched Lakekeeper:

```bash
cd ~
git clone git@github.com:laskoviymishka/lakekeeper.git
cd lakekeeper
git checkout labels-crud-verb
```

The demo's `docker-compose.yaml` builds Lakekeeper from this checkout. Set the path:

```bash
cd ~/irc-labels/labels-management
export LAKEKEEPER_SRC=~/lakekeeper      # path to the PR 2 checkout
docker-compose up -d
```

First start takes ~5 minutes (Rust release build of Lakekeeper). Subsequent starts are
near-instant due to docker layer caching.

Once up:

- Lakekeeper:   `http://localhost:8181/v1/config`
- MinIO:        `http://localhost:9001` (admin / password)
- Jupyter:      `http://localhost:8888/lab` (token: `demo`)

Open the notebook at `notebook/labels-management.ipynb` and run the cells top to bottom.

## Files

```
labels-management/
├── README.md                        # this file
├── docker-compose.yaml              # services + build directives
├── lakekeeper.Dockerfile            # Rust release build of Lakekeeper from PR 2 source
├── .env.example                     # LAKEKEEPER_SRC pointer + warehouse defaults
├── design/
│   ├── README.md                    # cross-PR map + branch links + sequencing
│   ├── lakekeeper-design.md         # catalog reference impl (PR 2) design notes
│   └── trino-design.md              # ALTER TABLE ... SET LABEL design notes (PR 4)
└── notebook/
    ├── labels-management.ipynb      # the demo walkthrough
    ├── irc_labels_client.py         # thin Python client for IRC labels endpoints
    ├── install.py                   # pip install requirements on container start
    └── requirements.txt
```

For the design-level deep-dives on each implementation PR, see
[`design/`](design/). The cross-PR map with all branch + commit links is in
[`design/README.md`](design/README.md).

## Scenarios in the notebook

1. **Endpoint discovery** — `GET /v1/config`, confirm `catalog-v1-load-labels` and
   `catalog-v1-update-labels` are advertised in `endpoints[]`.
2. **Warehouse + table bootstrap** — create a Lakekeeper warehouse backed by MinIO, create
   a namespace + table with a schema that has a `field-id` for an `email` column.
3. **Read initial state** — `GET /v1/{prefix}/.../labels` returns the empty split shape.
4. **Write table labels** — `POST` with `updates: [{key, value}]`, verify the response echoes
   the post-update state.
5. **Write column labels** — `POST` with `updates: [{field-id: N, key, value}]`, verify the
   response carries the new `column-labels` array entry.
6. **403 LabelKeyNotWritable** — `POST` an update targeting `last-accessed-at` (catalog-managed),
   verify the request is rejected with the spec error envelope.
7. **Atomicity** — `POST` a request mixing one valid update + one update to a read-only key.
   Verify the whole request is rejected and the valid update did NOT take effect.
8. **Mixed update + removal** — `POST` a request with both `updates` and `removals` lists.
9. **metadata.json untouched** — `GET` the table's metadata location before and after a label
   write; verify the path is unchanged. (Lifecycle invariant: labels are not table state.)

## Spec reference

- [labels-crud-followup.md](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md) — the proposal
- [discussion-split-shape.md](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/discussion-split-shape.md) — why split, not nested envelope

## License

Apache 2.0.
