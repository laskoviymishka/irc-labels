"""
Label-driven governance for ClickHouse via system.iceberg_labels pattern.

Reads labels from IRC (via PyIceberg), populates a ClickHouse iceberg_labels table,
and generates governed views with masking/blocking driven by label values.

This demonstrates the "trusted engine" architecture:
  Catalog classifies → Labels travel via IRC → Engine enforces natively
"""

from pyiceberg.catalog import Catalog

# Masking expressions by PII type
PII_MASKS = {
    "email": "replaceRegexpOne({col}, '^(.{{2}})[^@]*(@.*)$', '\\\\1****\\\\2')",
    "full_name": "concat(substring({col}, 1, 1), '****')",
    "date_of_birth": "'****-**-**'",
    "phone_number": "replaceRegexpOne({col}, '^(.{{3}}).*(.{{4}})$', '\\\\1-***-\\\\2')",
    "ssn": "concat('***-**-', substring({col}, -4))",
    "patient_identifier": "concat('ID-', toString(cityHash64({col})))",
    "clinical_diagnosis": "NULL",
}


def populate_iceberg_labels(catalog: Catalog, ch_client, namespace: str):
    """
    Read labels from IRC via PyIceberg and populate ClickHouse iceberg_labels table.
    Simulates what a native system.iceberg_labels would do.
    """
    ch_client.command("""
        CREATE TABLE IF NOT EXISTS iceberg_labels (
            database String,
            table_name String,
            scope String,
            field_id Nullable(UInt32),
            column_name Nullable(String),
            label_key String,
            label_value String
        ) ENGINE = MergeTree()
        ORDER BY (database, table_name, scope, label_key)
    """)
    ch_client.command("TRUNCATE TABLE iceberg_labels")

    rows = []
    for table_id in catalog.list_tables(namespace):
        table = catalog.load_table(table_id)
        table_name = table_id[1]
        schema = table.schema()

        # Table-level labels
        for k, v in table.table_labels.items():
            rows.append([namespace, table_name, "table", None, None, k, v])

        # Column-level labels
        for col_entry in table.column_labels:
            field_id = col_entry.get("field-id")
            col_labels = col_entry.get("labels", {})
            # Resolve field name from schema
            col_name = None
            for field in schema.fields:
                if field.field_id == field_id:
                    col_name = field.name
                    break
            for k, v in col_labels.items():
                rows.append([namespace, table_name, "column", field_id, col_name, k, v])

    if rows:
        ch_client.insert(
            "iceberg_labels", rows,
            column_names=["database", "table_name", "scope", "field_id",
                          "column_name", "label_key", "label_value"],
        )

    return len(rows)


def generate_governed_view(ch_client, database: str, table_name: str, role: str = "analyst"):
    """
    Generate a governed view for a table based on its labels in iceberg_labels.

    - Columns with pii_type/phi_type → masked for non-admin roles
    - Columns with sensitivity=restricted → blocked (NULL) for non-admin roles
    - Other columns → pass through
    """
    # In ClickHouse DataLakeCatalog, table names include the namespace:
    # e.g. "healthcare.patients" as the table name inside database "healthcare"
    ch_table = f"{database}.{table_name}"

    # Get schema via DESCRIBE (system.columns doesn't see DataLakeCatalog tables)
    columns = ch_client.query(
        f"DESCRIBE {database}.`{ch_table}`"
    ).result_rows
    # DESCRIBE returns (name, type, default_type, default_expr, comment, codec, ttl)
    columns = [(row[0], row[1]) for row in columns]

    if not columns:
        return None

    # Get column labels
    label_rows = ch_client.query(
        f"SELECT column_name, label_key, label_value FROM iceberg_labels "
        f"WHERE database = '{database}' AND table_name = '{table_name}' AND scope = 'column'"
    ).result_rows

    # Build label lookup: column_name -> {label_key: label_value}
    col_labels = {}
    for col_name, lk, lv in label_rows:
        col_labels.setdefault(col_name, {})[lk] = lv

    # Build column expressions
    col_exprs = []
    for col_name, col_type in columns:
        labels = col_labels.get(col_name, {})
        pii_type = labels.get("pii_type") or labels.get("phi_type")
        sensitivity = labels.get("sensitivity")

        if sensitivity == "restricted" and pii_type:
            # Restricted + PII → block entirely
            col_exprs.append(f"NULL AS {col_name}")
        elif pii_type and pii_type in PII_MASKS:
            # Has PII type → mask
            mask_expr = PII_MASKS[pii_type].format(col=col_name)
            col_exprs.append(
                f"CASE WHEN currentUser() IN ('{role}') THEN {mask_expr} "
                f"ELSE {col_name} END AS {col_name}"
            )
        elif sensitivity in ("high", "restricted"):
            # High sensitivity without specific PII type → block
            col_exprs.append(
                f"CASE WHEN currentUser() IN ('{role}') THEN NULL "
                f"ELSE {col_name} END AS {col_name}"
            )
        else:
            col_exprs.append(col_name)

    select = ",\n    ".join(col_exprs)
    view_name = f"governed_{table_name}"

    # Create view in default database (DataLakeCatalog databases don't support views)
    ddl = (
        f"CREATE OR REPLACE VIEW default.`{view_name}` AS\n"
        f"SELECT\n    {select}\nFROM {database}.`{ch_table}`"
    )
    ch_client.command(ddl)
    return view_name, ddl


def apply_label_governance(catalog: Catalog, ch_client, namespace: str, role: str = "analyst"):
    """
    End-to-end: read labels from IRC → populate iceberg_labels → create governed views.

    Returns dict with created views and their DDL.
    """
    # Step 1: Populate labels
    label_count = populate_iceberg_labels(catalog, ch_client, namespace)
    print(f"Loaded {label_count} labels into iceberg_labels table")

    # Step 2: Generate governed views for each table
    views = {}
    for table_id in catalog.list_tables(namespace):
        table_name = table_id[1]
        result = generate_governed_view(ch_client, namespace, table_name, role)
        if result:
            view_name, ddl = result
            views[table_name] = {"view": view_name, "ddl": ddl}
            print(f"Created governed view: {namespace}.{view_name}")

    return views
