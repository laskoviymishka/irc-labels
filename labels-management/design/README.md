# Design Docs — IRC Labels CRUD

Companion design notes for the four-PR effort prototyping IRC catalog-scoped labels.

## All branches in one place

| PR | Repo | Branch | Commit | View | Open PR |
|----|------|--------|--------|------|---------|
| 1  | apache/iceberg | `labels-crud-verb` | [`037d84e`](https://github.com/laskoviymishka/iceberg/commit/037d84e) | [tree](https://github.com/laskoviymishka/iceberg/tree/labels-crud-verb) | [new PR](https://github.com/laskoviymishka/iceberg/pull/new/labels-crud-verb) |
| 2  | lakekeeper/lakekeeper | `labels-crud-verb` | [`5d75cacf`](https://github.com/laskoviymishka/lakekeeper/commit/5d75cacf) | [tree](https://github.com/laskoviymishka/lakekeeper/tree/labels-crud-verb) | [new PR](https://github.com/laskoviymishka/lakekeeper/pull/new/labels-crud-verb) |
| 3  | unitycatalog/unitycatalog | _pending_ | — | — | — |
| 4  | trinodb/trino | `labels-crud-verb` | [`89504645`](https://github.com/laskoviymishka/trino/commit/89504645) | [tree](https://github.com/laskoviymishka/trino/tree/labels-crud-verb) | [new PR](https://github.com/laskoviymishka/trino/pull/new/labels-crud-verb) |
| 5  | laskoviymishka/irc-labels | `labels-management` | [`aec0dd6`](https://github.com/laskoviymishka/irc-labels/commit/aec0dd6) | [tree](https://github.com/laskoviymishka/irc-labels/tree/labels-management) | [new PR](https://github.com/laskoviymishka/irc-labels/pull/new/labels-management) |

Earlier read-side proposal (already-live PRs for reference):

| Component | PR |
|-----------|----|
| Iceberg spec (read path) | [apache/iceberg#15750](https://github.com/apache/iceberg/pull/15750) |
| PyIceberg client | [apache/iceberg-python#3191](https://github.com/apache/iceberg-python/pull/3191) |
| Polaris (read path) | [apache/polaris#4048](https://github.com/apache/polaris/pull/4048) |
| Unity Catalog (read path) | [unitycatalog/unitycatalog#1417](https://github.com/unitycatalog/unitycatalog/pull/1417) |
| Lakekeeper (read path) | [lakekeeper/lakekeeper#1676](https://github.com/lakekeeper/lakekeeper/pull/1676) |

## Design docs in this folder

- [`lakekeeper-design.md`](lakekeeper-design.md) — REST endpoint, atomic check-before-mutate, storage model, error envelope, endpoint advertisement via `/v1/config` and Postgres enum migration
- [`trino-design.md`](trino-design.md) — `ALTER TABLE … SET LABEL` grammar + SPI cascade, `field-id` resolution defending against schema evolution, `LabelEntry` carrier, error mapping to `LABEL_KEY_NOT_WRITABLE`

## Spec proposal

The spec the four PRs implement: [`labels-crud-followup.md`](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md).

Background context:

- [`active-threads-context.md`](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/active-threads-context.md) — gdoc thread state per stakeholder
- [`discussion-split-shape.md`](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/discussion-split-shape.md) — why flat `labels` + `column-labels` keyed by `field-id`, not a nested envelope

## Sequencing

The four PRs depend on each other in a strict order; the design docs assume this chain:

```
PR 1 (Iceberg core SDK + wire types)
  ├─► PR 2 (Lakekeeper consumes the wire shape server-side)
  ├─► PR 3 (UC consumes the wire shape server-side, parallel to PR 2)
  └─► PR 4 (Trino consumes RESTSessionCatalog.updateLabels from PR 1)
              │
              ▼
         PR 5 (e2e demo wiring PR 2 to a real running catalog
               via Jupyter; PR 4's DDL is the SQL surface that
               translates to the same wire flow)
```

PR 4 currently relies on PR 1's local SNAPSHOT build (`./gradlew publishToMavenLocal`)
until upstream Iceberg ships the verb. The notebook in PR 5 exercises PR 2 directly
via REST — no Trino build required for the demo.
