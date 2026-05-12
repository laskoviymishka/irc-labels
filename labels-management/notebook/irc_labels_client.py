"""
Thin Python client for the IRC labels endpoints proposed in
labels-crud-followup.md.

This is the read+write path implemented by PR 2 (Lakekeeper) and PR 1
(Iceberg core SDK). The client is intentionally minimal — it hits the
two new endpoints directly so the wire shape is visible in notebook
cells, not hidden behind abstraction.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import requests


# Catalog-managed (read-only) keys hardcoded in the Lakekeeper PoC.
# Writing to any of these triggers 403 LabelKeyNotWritable.
READ_ONLY_KEYS = (
    "last-accessed-at",
    "last-edit-by",
    "query-count-24h",
    "sla-latency-p99",
    "migration-timestamp",
)


@dataclass
class IrcLabelsClient:
    """Direct REST client for IRC label CRUD."""

    base_url: str
    warehouse: str
    timeout: float = 10.0

    @property
    def prefix(self) -> str:
        """IRC `prefix` for the warehouse — used in every endpoint path."""
        return self.warehouse

    def _labels_url(self, namespace: str, table: str) -> str:
        return (
            f"{self.base_url}/catalog/v1/{self.prefix}"
            f"/namespaces/{namespace}/tables/{table}/labels"
        )

    def config(self) -> dict[str, Any]:
        """GET /v1/config — endpoint capability discovery."""
        r = requests.get(
            f"{self.base_url}/catalog/v1/config",
            params={"warehouse": self.warehouse},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def supports(self, endpoint: str) -> bool:
        """True if the catalog advertises `endpoint` in /v1/config endpoints[]."""
        cfg = self.config()
        return endpoint in cfg.get("endpoints", [])

    def load_labels(self, namespace: str, table: str) -> dict[str, Any]:
        """GET split-shape labels for a table."""
        r = requests.get(self._labels_url(namespace, table), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def update_labels(
        self,
        namespace: str,
        table: str,
        updates: Optional[list[dict[str, Any]]] = None,
        removals: Optional[list[dict[str, Any]]] = None,
    ) -> tuple[int, dict[str, Any]]:
        """
        POST atomic update + removal request. Returns (status_code, body).

        Wire shape per labels-crud-followup.md:
          updates : list of {"key": ..., "value": ..., "field-id"?: int}
          removals: list of {"key": ..., "field-id"?: int}

        On success (200): body is the full post-update label set in
        split shape. On rejection: body carries the spec error envelope
        with type=LabelKeyNotWritable for catalog-managed key writes.
        """
        body = {"updates": updates or [], "removals": removals or []}
        r = requests.post(
            self._labels_url(namespace, table),
            json=body,
            timeout=self.timeout,
        )
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, {"raw": r.text}


# --- ergonomic builders -----------------------------------------------------


def update_table(key: str, value: str) -> dict[str, Any]:
    """Build a table-scoped update entry."""
    return {"key": key, "value": value}


def update_column(field_id: int, key: str, value: str) -> dict[str, Any]:
    """Build a column-scoped update entry."""
    return {"field-id": field_id, "key": key, "value": value}


def remove_table(key: str) -> dict[str, Any]:
    """Build a table-scoped removal entry."""
    return {"key": key}


def remove_column(field_id: int, key: str) -> dict[str, Any]:
    """Build a column-scoped removal entry."""
    return {"field-id": field_id, "key": key}
