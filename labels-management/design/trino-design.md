# Trino — `ALTER TABLE … SET LABEL` Design

**Implementation branch:** [`laskoviymishka/trino` `labels-crud-verb`](https://github.com/laskoviymishka/trino/tree/labels-crud-verb) (commit [`89504645`](https://github.com/laskoviymishka/trino/commit/89504645))
**Companion proposal:** [`labels-crud-followup.md`](https://github.com/laskoviymishka/slop/tree/main/projects/iceberg-governance/labels/labels-crud-followup.md)
**Status:** Draft PR / static-review only (no JDK on the build host)

## Cross-PR map

| PR | Repo | Branch | Commit |
|----|------|--------|--------|
| 1  | [apache/iceberg](https://github.com/laskoviymishka/iceberg/tree/labels-crud-verb) | `labels-crud-verb` | [`037d84e`](https://github.com/laskoviymishka/iceberg/commit/037d84e) — SDK + wire types |
| 2  | [lakekeeper/lakekeeper](https://github.com/laskoviymishka/lakekeeper/tree/labels-crud-verb) | `labels-crud-verb` | [`5d75cacf`](https://github.com/laskoviymishka/lakekeeper/commit/5d75cacf) — catalog reference impl |
| 3  | unitycatalog/unitycatalog | _pending — OSS UC reference impl_ | — |
| 4  | **trinodb/trino (this PR)** | [`labels-crud-verb`](https://github.com/laskoviymishka/trino/tree/labels-crud-verb) | [`89504645`](https://github.com/laskoviymishka/trino/commit/89504645) |
| 5  | [laskoviymishka/irc-labels](https://github.com/laskoviymishka/irc-labels/tree/labels-management) | `labels-management` | [`aec0dd6`](https://github.com/laskoviymishka/irc-labels/commit/aec0dd6) — e2e demo + notebook |

## Goal

Add a portable SQL DDL surface for IRC catalog-scoped labels:

```sql
ALTER TABLE cat.schema.table SET   LABEL ('domain' = 'customer', 'tier' = 'gold');
ALTER TABLE cat.schema.table UNSET LABEL ('tier');
ALTER TABLE cat.schema.table ALTER COLUMN email SET   LABEL ('pii-type' = 'email');
ALTER TABLE cat.schema.table ALTER COLUMN email UNSET LABEL ('pii-type');
```

These DDL statements translate to the IRC `UpdateLabels` REST verb (PR 1) and produce
the same wire payloads the Lakekeeper notebook exercises in this folder. Catalog-managed
keys are surfaced as `TrinoException(LABEL_KEY_NOT_WRITABLE, …)` carrying the offending
key, so the user sees a clean error at the SQL surface.

## Why grammar, not procedure

The minimal-effort engine integration would be a `CALL system.set_label(…)` procedure:
~80 LOC, no grammar touch. We rejected that path because:

1. **Every competitor catalog ships DDL.** Snowflake `ALTER TABLE … SET TAG`,
   Databricks `ALTER TABLE … ALTER COLUMN c SET TAGS`, BigQuery
   `ALTER TABLE … SET OPTIONS(labels=…)`. A procedure-only surface for Iceberg signals
   second-class status against the rest of the ecosystem.
2. **The whole spec argument is portability.** Procedures pinpoint a vendor-specific
   verb at the SQL surface; DDL is the user-facing language already standardized for
   label-like writes.
3. **Trino's structural cost is up-front, once.** The grammar lives in core (`SqlBase.g4`)
   so the SPI addition + IcebergMetadata override land alongside it. After that, any
   future catalog connector overriding `setTableLabels` automatically gets the SQL
   surface.

Asymmetry to flag in the spec: **Spark** can do this entirely within `apache/iceberg`'s
Spark extensions repo (parser plugin hook). **Trino** has no equivalent plugin-level
grammar hook — the change has to land in core Trino. That's a real difference per-engine
that the spec should call out under §SQL Syntax Design.

## Architecture

```
SQL: ALTER TABLE iceberg.sales.orders SET LABEL ('domain' = 'customer')
                                        │
                                        ▼
              core/trino-grammar/.../SqlBase.g4
              (LABEL token + statement alts)
                                        │
                                        ▼
              core/trino-parser/.../tree/SetTableLabels.java
              (AST node — extends Statement)
                                        │
                                        ▼
              core/trino-parser/.../parser/AstBuilder.visitSetTableLabels
                                        │
                                        ▼
              core/trino-main/.../analyzer/StatementAnalyzer.visitSetTableLabels
              (scope assignment; no parse-time policy check in V1)
                                        │
                                        ▼
              core/trino-main/.../execution/SetTableLabelsTask.execute
              (resolves TableHandle, calls MetadataManager)
                                        │
                                        ▼
              core/trino-main/.../metadata/MetadataManager.setTableLabels
              (routes to ConnectorMetadata via toConnectorSession)
                                        │
                                        ▼
              core/trino-spi/.../connector/ConnectorMetadata.setTableLabels
              (default: throws NOT_SUPPORTED; connectors override)
                                        │
                                        ▼
              plugin/trino-iceberg/.../IcebergMetadata.setTableLabels
              (builds LabelEntry list, dispatches to TrinoCatalog)
                                        │
                                        ▼
              plugin/trino-iceberg/.../catalog/rest/TrinoRestCatalog.updateLabels
              (constructs UpdateLabelsRequest, calls RESTSessionCatalog)
                                        │
                                        ▼
              org.apache.iceberg.rest.RESTSessionCatalog.updateLabels  [from PR 1]
              POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}/labels
                                        │
                                        ▼
                                  Lakekeeper [PR 2]
```

Eleven concrete files touched. Full diff at the [`labels-crud-verb` branch](https://github.com/laskoviymishka/trino/tree/labels-crud-verb)
(commit `89504645`, ~1444 LOC).

## File-level breakdown

| Layer | File | What |
|---|---|---|
| Grammar | `core/trino-grammar/.../SqlBase.g4` | Adds `LABEL` token (non-reserved), four statement alts |
| AST | `core/trino-parser/.../tree/{Set,Unset}{Table,Column}Labels.java` | Four new `Statement` subclasses |
| AST builder | `core/trino-parser/.../parser/AstBuilder.java` | Four `visit…Labels` methods |
| AST visitor | `core/trino-parser/.../tree/AstVisitor.java` | Four `visit…Labels` defaults dispatching to `visitStatement` |
| Formatter | `core/trino-parser/.../sql/SqlFormatter.java` | Four format methods + `joinIdentifiers` helper |
| Analyzer | `core/trino-main/.../sql/analyzer/StatementAnalyzer.java` | Four `visit…Labels` methods — scope only, no parse-time validation |
| Tasks | `core/trino-main/.../execution/{Set,Unset}{Table,Column}LabelsTask.java` | Four `DataDefinitionTask<T>` impls |
| Registration | `core/trino-main/.../util/StatementUtils.java` + `server/QueryExecutionFactoryModule.java` | Four `dataDefinitionStatement` registrations + Guice bindings |
| Routing | `core/trino-main/.../metadata/Metadata.java` + `MetadataManager.java` + `tracing/TracingMetadata.java` + `test/.../AbstractMockMetadata.java` | Four-method extension on the Metadata interface + impls + tracing wrapper |
| SPI | `core/trino-spi/.../connector/ConnectorMetadata.java` | Four `default` methods, all throw `NOT_SUPPORTED` |
| Iceberg impl | `plugin/trino-iceberg/.../IcebergMetadata.java` | Four `@Override` impls + `resolveFieldId` helper |
| Catalog SPI | `plugin/trino-iceberg/.../catalog/TrinoCatalog.java` + `catalog/rest/TrinoRestCatalog.java` | Connector-level `updateLabels` interface method + REST impl |
| Carrier | `plugin/trino-iceberg/.../labels/LabelEntry.java` | Plugin-internal type so `TrinoCatalog` doesn't leak Iceberg SDK types |
| Error | `plugin/trino-iceberg/.../IcebergErrorCode.java` | `LABEL_KEY_NOT_WRITABLE(19, USER_ERROR)` |
| Test stub | `plugin/trino-iceberg/.../test/.../TestIcebergLabelsCrud.java` | `@Disabled` placeholder with TODO list for MockWebServer wiring |

## Two key implementation decisions

### 1. `field-id` resolution at execute time, not analyze time

The grammar accepts column names (`ALTER COLUMN email`) because forcing users to type
`ALTER COLUMN <field-id=3>` would be hostile. The translation to `field-id: 3` happens
at execution, not analyze time, in `IcebergMetadata.resolveFieldId`:

```java
Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
NestedField field = icebergTable.schema().findField(columnHandle.getName());
return field.fieldId();
```

We re-load the table inside the execute method (rather than relying on
`IcebergColumnHandle.getId()` captured at analyze time) **as a defense against
schema evolution between handle creation and execution.** If the column has been
renamed or dropped, `findField` returns null and we surface a clean error.

The schema is already in `TableMetadata`; no extra round-trip beyond the existing
`loadTable`.

### 2. `LabelEntry` plugin-internal carrier

`TrinoCatalog` is a cross-plugin interface — Hive, Glue, JDBC, Nessie all implement
it. Leaking `org.apache.iceberg.rest.requests.UpdateLabelsRequest.Entry` into that
interface would couple every catalog to the IRC REST package.

The plugin-internal `LabelEntry` (`plugin/trino-iceberg/…/labels/LabelEntry.java`)
sits between `IcebergMetadata` and `TrinoCatalog`. Only `TrinoRestCatalog.updateLabels`
maps it onto `UpdateLabelsRequest.Entry`; other `TrinoCatalog` impls keep the default
`NOT_SUPPORTED` throw.

## Error mapping

The Iceberg REST client returns `ForbiddenException` for any HTTP 403. Catalog-managed-key
rejections come back with the spec error envelope. `TrinoRestCatalog.updateLabels` catches
and re-throws as a typed Trino exception:

```java
catch (ForbiddenException e) {
    throw new TrinoException(
            LABEL_KEY_NOT_WRITABLE,
            "Label update rejected by catalog: " + e.getMessage(),
            e);
}
```

`USER_ERROR` ensures Trino renders the message to the SQL client (not as an internal
fault). The catalog's message preserves the offending key, so the user sees:

```
io.trino.spi.TrinoException: Label update rejected by catalog:
  Label key 'last-accessed-at' is catalog-managed and cannot be written via the API.
```

A finer-grained error type per failure mode (`LabelValueNotAllowed`,
`LabelKeyUnknown`, `LabelStructuralConstraint`) is a V2 follow-up once enough catalogs
implement writes to know the common rejection patterns.

## V1 scope cuts

| Cut | Why |
|---|---|
| Nested-column labels (`ALTER COLUMN a.b.c SET LABEL`) | Iceberg's `Schema.findField(String)` resolves dot-paths, but the addressing semantics for label writes on nested fields aren't pinned down. V1 supports top-level columns only; the analyzer should reject multi-part column names. Open follow-up. |
| `SHOW LABELS` DDL | `updateLabels` already returns the full post-update label set in `LoadLabelsResponse`. We discard it today (fire-and-forget). A `SHOW LABELS` path that plumbs the response back through the engine is a clean follow-up. |
| Dedicated `AccessControl#checkCanSet{Table,Column}Labels` hooks | Currently piggybacks on existing table/column ACLs. A dedicated hook is cleaner; engines can deny label edits without revoking column write access. |
| ETag / `If-Match` optimistic concurrency | Iceberg SDK supports it (`mutationHeaders`), but the engine plumbing for the prior ETag isn't yet wired. Last-writer-wins for V1. |

## How to build and run

PR 4 depends on PR 1's Iceberg SDK additions (`Endpoint.V1_UPDATE_LABELS`,
`UpdateLabelsRequest`, `LoadLabelsResponse`, `RESTSessionCatalog.updateLabels`).
The Iceberg PR 1 isn't published; for a local build:

```bash
cd ~/iceberg
git checkout labels-crud-verb
./gradlew publishToMavenLocal       # publish 1.x.x-SNAPSHOT to ~/.m2

cd ~/trino
git checkout labels-crud-verb
# bump iceberg version in pom.xml to the SNAPSHOT
./mvnw -pl :trino-iceberg install -DskipTests
```

Then point a running Trino at a Lakekeeper from PR 2 (the `labels-management/docker-compose.yaml`
in this folder is the convenient way to get one running) and exercise:

```sql
CREATE TABLE iceberg.sales.orders (id BIGINT, email VARCHAR);
ALTER TABLE iceberg.sales.orders SET LABEL ('domain' = 'customer');
ALTER TABLE iceberg.sales.orders ALTER COLUMN email SET LABEL ('pii-type' = 'email');
```

A pre-built docker image for the patched Trino is a follow-up — Trino's release build is
~30 minutes and benefits from real CI infrastructure, not a one-off Dockerfile.

## Cross-engine parity reference

Spark parity is a parallel ~400-LOC patch in `apache/iceberg`'s Spark extensions (the
`SET IDENTIFIER FIELDS` pattern). Flink parity is blocked: the Iceberg Flink connector
hasn't adopted Flink's procedure SPI (Flink 1.18+), and there's no grammar plugin hook.
The spec's §SQL Syntax Design should call out these per-engine adoption costs explicitly
— landing as `CALL system.set_label(…)` first would ship faster across all three but
breaks the "every catalog uses DDL" portability frame.
