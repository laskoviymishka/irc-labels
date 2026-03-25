"""
Minimal IRC (Iceberg REST Catalog) client that reads labels.

Talks to the Labels Proxy (or any IRC endpoint that serves labels).
"""

import httpx


class IRCClient:
    """Client for Iceberg REST Catalog with labels support."""

    def __init__(self, base_url: str, catalog: str = "unity"):
        self.base_url = base_url.rstrip("/")
        self.catalog = catalog
        self._client = httpx.Client(timeout=30.0)
        self._prefix = f"{self.base_url}/api/2.1/unity-catalog/iceberg/v1/{catalog}"

    def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog."""
        resp = self._client.get(f"{self._prefix}/namespaces")
        resp.raise_for_status()
        data = resp.json()
        return [".".join(ns) for ns in data.get("namespaces", [])]

    def list_tables(self, namespace: str) -> list[str]:
        """List all tables in a namespace."""
        resp = self._client.get(f"{self._prefix}/namespaces/{namespace}/tables")
        resp.raise_for_status()
        data = resp.json()
        return [
            ident.get("name", ident["namespace"][-1] if "namespace" in ident else "")
            for ident in data.get("identifiers", [])
        ]

    def load_table(self, namespace: str, table: str) -> dict:
        """
        Load table metadata + labels via IRC LoadTable.

        Returns the full LoadTableResponse including the `labels` field
        if the catalog/proxy provides it.
        """
        resp = self._client.get(
            f"{self._prefix}/namespaces/{namespace}/tables/{table}"
        )
        resp.raise_for_status()
        return resp.json()

    def get_labels(self, namespace: str, table: str) -> dict | None:
        """Extract just the labels from a LoadTable response."""
        response = self.load_table(namespace, table)
        return response.get("labels")

    def get_schema(self, namespace: str, table: str) -> dict | None:
        """Extract schema from table metadata."""
        response = self.load_table(namespace, table)
        metadata = response.get("metadata", {})
        schemas = metadata.get("schemas", [])
        if schemas:
            # Return the current schema (highest schema-id)
            return max(schemas, key=lambda s: s.get("schema-id", 0))
        return metadata.get("schema")

    def discover_tables_with_labels(self, namespace: str) -> list[dict]:
        """
        Discover all tables in a namespace with their labels and schema.

        Returns a list of dicts with keys: name, labels, schema
        """
        tables = self.list_tables(namespace)
        result = []
        for table_name in tables:
            response = self.load_table(namespace, table_name)
            metadata = response.get("metadata", {})
            schemas = metadata.get("schemas", [])
            schema = max(schemas, key=lambda s: s.get("schema-id", 0)) if schemas else None

            result.append({
                "name": f"{namespace}.{table_name}",
                "labels": response.get("labels", {}),
                "schema": schema,
                "properties": metadata.get("properties", {}),
            })
        return result

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
