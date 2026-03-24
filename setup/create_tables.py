"""
Create the healthcare demo tables in UC OSS with synthetic data.

Run this after docker-compose up to populate the catalog.
Uses PyIceberg to create tables and write Parquet data to MinIO.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    DateType,
    TimestampType,
    DoubleType,
    NestedField,
)

# --- Schemas ---

PATIENTS_SCHEMA = Schema(
    NestedField(1, "patient_id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "email", StringType(), required=True),
    NestedField(4, "diagnosis", StringType(), required=False),
    NestedField(5, "dob", DateType(), required=True),
    NestedField(6, "insurance_id", StringType(), required=False),
)

VISITS_SCHEMA = Schema(
    NestedField(1, "visit_date", DateType(), required=True),
    NestedField(2, "department", StringType(), required=True),
    NestedField(3, "visit_count", IntegerType(), required=True),
    NestedField(4, "avg_wait_time", DoubleType(), required=True),
    NestedField(5, "satisfaction_score", DoubleType(), required=True),
)

BILLING_SCHEMA = Schema(
    NestedField(1, "invoice_id", IntegerType(), required=True),
    NestedField(2, "patient_id", IntegerType(), required=True),
    NestedField(3, "amount", DoubleType(), required=True),
    NestedField(4, "insurance_code", StringType(), required=False),
    NestedField(5, "billing_date", DateType(), required=True),
)

# --- Synthetic Data ---

PATIENTS_DATA = pa.table({
    "patient_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    "name": [
        "Alice Johnson", "Bob Smith", "Carol Davis", "Dan Wilson",
        "Eva Martinez", "Frank Brown", "Grace Lee", "Henry Taylor",
    ],
    "email": [
        "alice.j@example.com", "bob.s@example.com", "carol.d@example.com",
        "dan.w@example.com", "eva.m@example.com", "frank.b@example.com",
        "grace.l@example.com", "henry.t@example.com",
    ],
    "diagnosis": [
        "Hypertension", "Type 2 Diabetes", "Asthma", "Migraine",
        "Hypertension", "Anxiety Disorder", "Type 1 Diabetes", "COPD",
    ],
    "dob": [
        date(1985, 3, 15), date(1972, 7, 22), date(1990, 11, 8),
        date(1968, 1, 30), date(1995, 5, 12), date(1980, 9, 3),
        date(1988, 12, 19), date(1955, 4, 7),
    ],
    "insurance_id": [
        "INS-AA1001", "INS-BB2002", "INS-CC3003", "INS-DD4004",
        "INS-EE5005", "INS-FF6006", "INS-GG7007", "INS-HH8008",
    ],
})

VISITS_DATA = pa.table({
    "visit_date": [
        date(2026, 1, 5), date(2026, 1, 5), date(2026, 1, 5),
        date(2026, 1, 12), date(2026, 1, 12), date(2026, 1, 12),
        date(2026, 2, 1), date(2026, 2, 1), date(2026, 2, 1),
        date(2026, 2, 15), date(2026, 2, 15), date(2026, 2, 15),
        date(2026, 3, 1), date(2026, 3, 1), date(2026, 3, 1),
    ],
    "department": [
        "Cardiology", "Neurology", "General",
        "Cardiology", "Neurology", "General",
        "Cardiology", "Neurology", "General",
        "Cardiology", "Neurology", "General",
        "Cardiology", "Neurology", "General",
    ],
    "visit_count": [
        45, 32, 128,
        52, 28, 135,
        48, 35, 142,
        55, 30, 138,
        60, 33, 145,
    ],
    "avg_wait_time": [
        22.5, 18.3, 35.2,
        24.1, 17.8, 38.5,
        21.0, 19.2, 33.8,
        25.3, 16.5, 36.1,
        23.7, 18.9, 34.5,
    ],
    "satisfaction_score": [
        4.2, 4.5, 3.8,
        4.1, 4.6, 3.7,
        4.3, 4.4, 3.9,
        4.0, 4.7, 3.6,
        4.4, 4.5, 3.8,
    ],
})

BILLING_DATA = pa.table({
    "invoice_id": [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010],
    "patient_id": [1001, 1002, 1003, 1001, 1004, 1005, 1006, 1007, 1008, 1002],
    "amount": [
        1250.00, 890.50, 425.00, 2100.00, 675.00,
        1890.00, 320.00, 1450.00, 3200.00, 780.00,
    ],
    "insurance_code": [
        "BCBS-100", "AETNA-200", "BCBS-100", "BCBS-100", "UHC-300",
        "CIGNA-400", "AETNA-200", "UHC-300", "BCBS-100", "AETNA-200",
    ],
    "billing_date": [
        date(2026, 1, 10), date(2026, 1, 12), date(2026, 1, 15),
        date(2026, 1, 20), date(2026, 1, 25), date(2026, 2, 1),
        date(2026, 2, 5), date(2026, 2, 10), date(2026, 2, 15),
        date(2026, 2, 20),
    ],
})


def setup_catalog():
    """Create tables in UC OSS via PyIceberg."""
    catalog = load_catalog(
        "unity",
        **{
            "type": "rest",
            "uri": "http://localhost:8080",
            "warehouse": "healthcare",
            "prefix": "api/2.1/unity-catalog/iceberg",
        },
    )

    # Create namespace
    try:
        catalog.create_namespace("healthcare")
        print("Created namespace: healthcare")
    except Exception as e:
        print(f"Namespace exists or error: {e}")

    # Create tables and insert data
    tables_to_create = [
        ("healthcare", "patients", PATIENTS_SCHEMA, PATIENTS_DATA),
        ("healthcare", "visits_summary", VISITS_SCHEMA, VISITS_DATA),
        ("healthcare", "billing", BILLING_SCHEMA, BILLING_DATA),
    ]

    for ns, name, schema, data in tables_to_create:
        full_name = f"{ns}.{name}"
        try:
            table = catalog.create_table(full_name, schema=schema)
            table.append(data)
            print(f"Created and populated: {full_name} ({len(data)} rows)")
        except Exception as e:
            print(f"Table {full_name} exists or error: {e}")
            try:
                table = catalog.load_table(full_name)
                table.append(data)
                print(f"Appended data to existing: {full_name}")
            except Exception as e2:
                print(f"Could not append to {full_name}: {e2}")


if __name__ == "__main__":
    setup_catalog()
    print("\nDone! Tables created in UC OSS with synthetic healthcare data.")
