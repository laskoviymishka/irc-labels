#!/usr/bin/env python3
"""
Bootstrap the demo: create project, warehouse, namespace, and tables in Lakekeeper.
Run once after docker-compose up.

Usage:
    python setup/bootstrap.py [--lakekeeper-url http://localhost:8181] [--proxy-url http://localhost:8182]
"""

import argparse
import sys
import httpx
from datetime import date
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, DateType, DoubleType, NestedField


def create_project_and_warehouse(lk_url: str, minio_url: str = "http://minio:9000") -> str:
    """Create project and warehouse, return warehouse id."""
    client = httpx.Client(base_url=lk_url, timeout=30)

    # Lakekeeper's catalog config endpoint defaults to the zero-UUID project.
    # Create that project so warehouses are discoverable without extra headers.
    project_id = "00000000-0000-0000-0000-000000000000"
    resp = client.post("/management/v1/project", json={
        "project-id": project_id,
        "project-name": "default",
    })
    if resp.status_code in (200, 201):
        print(f"Created default project (id={project_id})")
    elif resp.status_code == 409:
        print(f"Default project exists (id={project_id})")
    else:
        # Older Lakekeeper versions may not support project-id in create
        resp2 = client.post("/management/v1/project", json={"project-name": "default"})
        if resp2.status_code in (200, 201):
            project_id = resp2.json()["project-id"]
            print(f"Created project (id={project_id})")
        else:
            print(f"Project creation failed: {resp.status_code} {resp.text}")
            sys.exit(1)

    # Warehouse — check if exists
    resp = client.get("/management/v1/warehouse")
    warehouses = resp.json().get("warehouses", [])
    existing = [w for w in warehouses if (w.get("warehouse-name") or w.get("name")) == "healthcare"]

    if existing:
        wid = existing[0]["id"]
        if not project_id:
            project_id = existing[0].get("project-id")
        print(f"Warehouse 'healthcare' exists (id={wid})")
        return wid

    if not project_id:
        print("ERROR: No project-id available. Run: docker-compose down -v && docker-compose up -d")
        sys.exit(1)

    resp = client.post("/management/v1/warehouse", json={
        "warehouse-name": "healthcare",
        "project-id": project_id,
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "region": "us-east-1",
            "endpoint": minio_url,
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "admin",
            "aws-secret-access-key": "password",
        },
    })
    if resp.status_code not in (200, 201):
        print(f"Warehouse creation failed: {resp.status_code} {resp.text}")
        sys.exit(1)
    wid = resp.json()["id"]
    print(f"Created warehouse 'healthcare' (id={wid})")
    return wid


def create_tables(catalog_url: str):
    """Create namespace and tables with synthetic data."""
    catalog = load_catalog("healthcare", **{
        "type": "rest",
        "uri": catalog_url,
        "warehouse": "healthcare",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    })

    # Namespace
    try:
        catalog.create_namespace("healthcare")
        print("Created namespace: healthcare")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Namespace 'healthcare' exists")
        else:
            print(f"Namespace: {e}")

    # Tables
    tables = [
        ("patients", Schema(
            NestedField(1, "patient_id", LongType()),
            NestedField(2, "name", StringType()),
            NestedField(3, "email", StringType()),
            NestedField(4, "diagnosis", StringType()),
            NestedField(5, "dob", DateType()),
            NestedField(6, "insurance_id", StringType()),
        ), pa.table({
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
        })),
        ("visits_summary", Schema(
            NestedField(1, "visit_date", DateType()),
            NestedField(2, "department", StringType()),
            NestedField(3, "visit_count", LongType()),
            NestedField(4, "avg_wait_time", DoubleType()),
            NestedField(5, "satisfaction_score", DoubleType()),
        ), pa.table({
            "visit_date": [date(2026, 1, 5)] * 3 + [date(2026, 1, 12)] * 3 + [date(2026, 2, 1)] * 3 +
                          [date(2026, 2, 15)] * 3 + [date(2026, 3, 1)] * 3,
            "department": ["Cardiology", "Neurology", "General"] * 5,
            "visit_count": [45, 32, 128, 52, 28, 135, 48, 35, 142, 55, 30, 138, 60, 33, 145],
            "avg_wait_time": [22.5, 18.3, 35.2, 24.1, 17.8, 38.5, 21.0, 19.2, 33.8, 25.3, 16.5, 36.1, 23.7, 18.9, 34.5],
            "satisfaction_score": [4.2, 4.5, 3.8, 4.1, 4.6, 3.7, 4.3, 4.4, 3.9, 4.0, 4.7, 3.6, 4.4, 4.5, 3.8],
        })),
        ("billing", Schema(
            NestedField(1, "invoice_id", LongType()),
            NestedField(2, "patient_id", LongType()),
            NestedField(3, "amount", DoubleType()),
            NestedField(4, "insurance_code", StringType()),
            NestedField(5, "billing_date", DateType()),
        ), pa.table({
            "invoice_id": [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010],
            "patient_id": [1001, 1002, 1003, 1001, 1004, 1005, 1006, 1007, 1008, 1002],
            "amount": [1250.0, 890.5, 425.0, 2100.0, 675.0, 1890.0, 320.0, 1450.0, 3200.0, 780.0],
            "insurance_code": ["BCBS-100", "AETNA-200", "BCBS-100", "BCBS-100", "UHC-300",
                               "CIGNA-400", "AETNA-200", "UHC-300", "BCBS-100", "AETNA-200"],
            "billing_date": [date(2026, 1, 10), date(2026, 1, 12), date(2026, 1, 15), date(2026, 1, 20),
                             date(2026, 1, 25), date(2026, 2, 1), date(2026, 2, 5), date(2026, 2, 10),
                             date(2026, 2, 15), date(2026, 2, 20)],
        })),
    ]

    for name, schema, data in tables:
        full = f"healthcare.{name}"
        try:
            t = catalog.create_table(full, schema=schema)
            t.append(data)
            print(f"Created: {full} ({len(data)} rows)")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Exists: {full}")
            else:
                print(f"Error {full}: {e}")


def verify(catalog_url: str):
    """Verify tables and labels."""
    catalog = load_catalog("verify", **{
        "type": "rest",
        "uri": catalog_url,
        "warehouse": "healthcare",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    })
    print("\nVerification:")
    for table_id in catalog.list_tables("healthcare"):
        t = catalog.load_table(table_id)
        labels = t.labels if hasattr(t, "labels") else {}
        scan = t.scan().to_arrow()
        print(f"  {table_id[1]:20s}  rows={len(scan)}  labels={'yes' if labels else 'no'}")
        if labels:
            for k, v in t.table_labels.items():
                print(f"    {k}: {v}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap IRC Labels demo")
    parser.add_argument("--lakekeeper-url", default="http://localhost:8181")
    parser.add_argument("--proxy-url", default="http://localhost:8182")
    parser.add_argument("--minio-url", default="http://minio:9000",
                        help="MinIO URL as seen by Lakekeeper (internal Docker network)")
    args = parser.parse_args()

    catalog_url = f"{args.proxy_url}/catalog"

    print("=== Step 1: Project & Warehouse ===")
    create_project_and_warehouse(args.lakekeeper_url, args.minio_url)

    print("\n=== Step 2: Namespace & Tables ===")
    create_tables(catalog_url)

    print("\n=== Step 3: Verify ===")
    verify(catalog_url)

    print("\nDone! Open demo.ipynb or governance.ipynb.")
