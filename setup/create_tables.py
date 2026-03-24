"""
Create the healthcare demo tables in Lakekeeper with synthetic data.

Run this after docker-compose up to:
1. Create a warehouse in Lakekeeper (management API)
2. Create namespace + tables via PyIceberg IRC
3. Populate with synthetic healthcare data
"""

import sys
import time
import httpx
import pyarrow as pa
from datetime import date
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, DateType, DoubleType, NestedField

LAKEKEEPER_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8181"
PROXY_URL = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8182"
MINIO_URL = "http://minio:9000"  # internal Docker network

# --- Step 1: Create warehouse via Lakekeeper management API ---

def create_warehouse():
    """Create a warehouse with S3 storage profile pointing to MinIO."""
    client = httpx.Client(base_url=LAKEKEEPER_URL, timeout=30)

    # Check if warehouse already exists
    resp = client.get("/management/v1/warehouse")
    if resp.status_code == 200:
        warehouses = resp.json().get("warehouses", [])
        for w in warehouses:
            if w.get("warehouse-name") == "demo":
                print(f"Warehouse 'demo' already exists (id={w['id']})")
                return w["id"]

    # Create warehouse
    resp = client.post("/management/v1/warehouse", json={
        "warehouse-name": "demo",
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "region": "us-east-1",
            "endpoint": MINIO_URL,
            "path-style-access": True,
            "flavor": "minio",
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "admin",
            "aws-secret-access-key": "password",
        },
    })
    resp.raise_for_status()
    wid = resp.json()["id"]
    print(f"Created warehouse 'demo' (id={wid})")
    return wid


# --- Step 2: Create tables via PyIceberg ---

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

PATIENTS_DATA = pa.table({
    "patient_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    "name": ["Alice Johnson", "Bob Smith", "Carol Davis", "Dan Wilson",
             "Eva Martinez", "Frank Brown", "Grace Lee", "Henry Taylor"],
    "email": ["alice.j@example.com", "bob.s@example.com", "carol.d@example.com",
              "dan.w@example.com", "eva.m@example.com", "frank.b@example.com",
              "grace.l@example.com", "henry.t@example.com"],
    "diagnosis": ["Hypertension", "Type 2 Diabetes", "Asthma", "Migraine",
                  "Hypertension", "Anxiety Disorder", "Type 1 Diabetes", "COPD"],
    "dob": [date(1985, 3, 15), date(1972, 7, 22), date(1990, 11, 8), date(1968, 1, 30),
            date(1995, 5, 12), date(1980, 9, 3), date(1988, 12, 19), date(1955, 4, 7)],
    "insurance_id": ["INS-AA1001", "INS-BB2002", "INS-CC3003", "INS-DD4004",
                     "INS-EE5005", "INS-FF6006", "INS-GG7007", "INS-HH8008"],
})

VISITS_DATA = pa.table({
    "visit_date": [date(2026, 1, 5)]*3 + [date(2026, 1, 12)]*3 + [date(2026, 2, 1)]*3 +
                  [date(2026, 2, 15)]*3 + [date(2026, 3, 1)]*3,
    "department": ["Cardiology", "Neurology", "General"] * 5,
    "visit_count": [45, 32, 128, 52, 28, 135, 48, 35, 142, 55, 30, 138, 60, 33, 145],
    "avg_wait_time": [22.5, 18.3, 35.2, 24.1, 17.8, 38.5, 21.0, 19.2, 33.8, 25.3, 16.5, 36.1, 23.7, 18.9, 34.5],
    "satisfaction_score": [4.2, 4.5, 3.8, 4.1, 4.6, 3.7, 4.3, 4.4, 3.9, 4.0, 4.7, 3.6, 4.4, 4.5, 3.8],
})

BILLING_DATA = pa.table({
    "invoice_id": [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010],
    "patient_id": [1001, 1002, 1003, 1001, 1004, 1005, 1006, 1007, 1008, 1002],
    "amount": [1250.0, 890.5, 425.0, 2100.0, 675.0, 1890.0, 320.0, 1450.0, 3200.0, 780.0],
    "insurance_code": ["BCBS-100", "AETNA-200", "BCBS-100", "BCBS-100", "UHC-300",
                       "CIGNA-400", "AETNA-200", "UHC-300", "BCBS-100", "AETNA-200"],
    "billing_date": [date(2026, 1, 10), date(2026, 1, 12), date(2026, 1, 15), date(2026, 1, 20),
                     date(2026, 1, 25), date(2026, 2, 1), date(2026, 2, 5), date(2026, 2, 10),
                     date(2026, 2, 15), date(2026, 2, 20)],
})


def create_tables():
    """Create tables via PyIceberg pointing at the labels proxy."""
    catalog = load_catalog("demo", **{
        "type": "rest",
        "uri": PROXY_URL,
        "warehouse": "demo",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
    })

    try:
        catalog.create_namespace("healthcare")
        print("Created namespace: healthcare")
    except Exception as e:
        print(f"Namespace: {e}")

    for ns, name, schema, data in [
        ("healthcare", "patients", PATIENTS_SCHEMA, PATIENTS_DATA),
        ("healthcare", "visits_summary", VISITS_SCHEMA, VISITS_DATA),
        ("healthcare", "billing", BILLING_SCHEMA, BILLING_DATA),
    ]:
        full = f"{ns}.{name}"
        try:
            table = catalog.create_table(full, schema=schema)
            table.append(data)
            print(f"Created: {full} ({len(data)} rows)")
        except Exception as e:
            print(f"{full}: {e}")
            try:
                table = catalog.load_table(full)
                table.append(data)
                print(f"Appended to existing: {full}")
            except Exception as e2:
                print(f"Failed: {full}: {e2}")


if __name__ == "__main__":
    print("Step 1: Creating warehouse...")
    create_warehouse()
    print("\nStep 2: Creating tables...")
    create_tables()
    print("\nDone!")
