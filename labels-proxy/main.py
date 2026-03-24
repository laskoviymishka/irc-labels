"""
IRC Labels Proxy — enriches LoadTableResponse with labels.

A thin FastAPI proxy that sits in front of any Iceberg REST Catalog and adds
a `labels` field to LoadTableResponse based on configured label mappings.

This demonstrates the proposed IRC Labels spec change without modifying
the upstream catalog implementation.
"""

import json
import os
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

UPSTREAM_CATALOG = os.environ.get("UPSTREAM_CATALOG_URL", "http://polaris:8181")
CONFIG_PATH = os.environ.get("LABELS_CONFIG_PATH", "/app/labels_config.yaml")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "8182"))

app = FastAPI(title="IRC Labels Proxy", version="0.1.0")

# Load label definitions
_labels_store: dict = {}


def load_labels_config():
    global _labels_store
    path = Path(CONFIG_PATH)
    if path.exists():
        with open(path) as f:
            config = yaml.safe_load(f)
        _labels_store = config.get("tables", {})
        print(f"Loaded labels for {len(_labels_store)} tables")
    else:
        print(f"Warning: labels config not found at {path}")


@app.on_event("startup")
async def startup():
    load_labels_config()


def resolve_table_key(catalog: str, namespace: str, table: str) -> str:
    """Build a lookup key matching the labels config format."""
    # Try multiple key formats for flexible matching
    candidates = [
        f"{namespace}.{table}",
        f"{catalog}.{namespace}.{table}",
        table,
    ]
    for key in candidates:
        if key in _labels_store:
            return key
    return ""


def enrich_response(body: dict, catalog: str, namespace: str, table: str) -> dict:
    """Inject labels into a LoadTableResponse JSON body."""
    key = resolve_table_key(catalog, namespace, table)
    if not key:
        return body

    label_def = _labels_store[key]
    labels = {}

    if "table" in label_def:
        labels["table"] = label_def["table"]

    if "columns" in label_def:
        labels["columns"] = label_def["columns"]

    if labels:
        body["labels"] = labels

    return body


def is_load_table_request(method: str, path: str) -> tuple[bool, str, str, str]:
    """
    Check if this is a LoadTable request and extract path components.
    IRC LoadTable: GET /v1/{prefix}/namespaces/{ns}/tables/{table}
    UC variant:    GET /api/2.1/unity-catalog/iceberg/v1/{catalog}/namespaces/{ns}/tables/{table}
    """
    if method != "GET":
        return False, "", "", ""

    # Match UC-style path:
    # /api/2.1/unity-catalog/iceberg/v1/{catalog}/namespaces/{ns}/tables/{table}
    parts = path.rstrip("/").split("/")

    # Find the pattern: .../v1/{catalog}/namespaces/{ns}/tables/{table}
    try:
        v1_idx = None
        for i, p in enumerate(parts):
            if p == "v1":
                v1_idx = i
                break

        if v1_idx is None:
            return False, "", "", ""

        remaining = parts[v1_idx + 1 :]

        # Pattern: {catalog}/namespaces/{ns}/tables/{table}
        if len(remaining) >= 5 and remaining[1] == "namespaces" and remaining[3] == "tables":
            catalog = remaining[0]
            namespace = remaining[2]
            table = remaining[4]
            return True, catalog, namespace, table

    except (IndexError, ValueError):
        pass

    return False, "", "", ""


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    """Proxy all requests to upstream catalog, enriching LoadTable responses."""
    # Handle local label endpoints before proxying
    if path == "labels":
        return _labels_store
    if path.startswith("labels/flat/"):
        ns = path.split("/", 2)[2]
        return await get_flat_labels(ns)
    if path.startswith("labels/"):
        parts = path.split("/")
        if len(parts) == 3:  # labels/{namespace}/{table}
            return await get_table_labels(parts[1], parts[2])

    upstream_url = f"{UPSTREAM_CATALOG}/{path}"

    # Forward query params
    if request.query_params:
        upstream_url += f"?{request.query_params}"

    # Forward headers (except host)
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }

    # Forward body if present
    body = await request.body()

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(
            method=request.method,
            url=upstream_url,
            headers=headers,
            content=body if body else None,
        )

    # Check if this is a LoadTable response we should enrich
    full_path = f"/{path}"
    is_load, catalog, namespace, table = is_load_table_request(request.method, full_path)

    if is_load and resp.status_code == 200:
        try:
            resp_body = resp.json()
            enriched = enrich_response(resp_body, catalog, namespace, table)
            return JSONResponse(
                content=enriched,
                status_code=resp.status_code,
                headers=dict(resp.headers),
            )
        except (json.JSONDecodeError, Exception):
            pass

    # Pass through unmodified
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
        media_type=resp.headers.get("content-type"),
    )


# --- Convenience endpoints for the demo ---

@app.get("/labels")
async def list_all_labels():
    """List all configured labels (demo/debug endpoint)."""
    return _labels_store


@app.get("/labels/{namespace}/{table}")
async def get_table_labels(namespace: str, table: str):
    """Get labels for a specific table (demo/debug endpoint)."""
    key = f"{namespace}.{table}"
    if key in _labels_store:
        return _labels_store[key]
    return JSONResponse(status_code=404, content={"error": f"No labels for {key}"})


@app.get("/labels/flat/{namespace}")
async def get_flat_labels(namespace: str):
    """Return all labels in flat format for ClickHouse dictionary ingestion."""
    rows = []
    for table_key, label_def in _labels_store.items():
        if not table_key.startswith(f"{namespace}."):
            continue
        table_name = table_key.split(".", 1)[1]

        # Table-level labels
        for k, v in label_def.get("table", {}).items():
            rows.append({"table_name": table_name, "scope": "table",
                         "field_id": 0, "column_name": "", "label_key": k, "label_value": v})

        # Column-level labels
        for col in label_def.get("columns", []):
            fid = col.get("field-id", 0)
            for k, v in col.get("labels", {}).items():
                rows.append({"table_name": table_name, "scope": "column",
                             "field_id": fid, "column_name": "", "label_key": k, "label_value": v})

    return rows
