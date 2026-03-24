"""
Label-aware data agent — uses IRC labels for table selection and governance.

Vendor-neutral: works with any OpenAI-compatible API (Claude, OpenAI, Ollama, etc.)
"""

import json
import os
from dataclasses import dataclass

from openai import OpenAI

from irc_client import IRCClient


@dataclass
class AgentConfig:
    irc_url: str = "http://localhost:8181"
    catalog: str = "unity"
    namespace: str = "healthcare"
    llm_base_url: str | None = None
    llm_api_key: str = ""
    llm_model: str = "claude-sonnet-4-5-20250514"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    duckdb_path: str = ":memory:"

    @classmethod
    def from_env(cls):
        return cls(
            irc_url=os.environ.get("IRC_URL", "http://localhost:8181"),
            catalog=os.environ.get("IRC_CATALOG", "unity"),
            namespace=os.environ.get("IRC_NAMESPACE", "healthcare"),
            llm_base_url=os.environ.get("LLM_BASE_URL"),
            llm_api_key=os.environ.get("LLM_API_KEY", ""),
            llm_model=os.environ.get("LLM_MODEL", "claude-sonnet-4-5-20250514"),
            clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        )


def format_table_context(tables: list[dict]) -> str:
    """Format discovered tables with labels into a context string for the LLM."""
    parts = []
    for t in tables:
        labels = t.get("labels", {})
        table_labels = labels.get("table", {})
        column_labels = labels.get("columns", [])
        schema = t.get("schema", {})
        fields = schema.get("fields", []) if schema else []

        part = f"### Table: `{t['name']}`\n"

        if table_labels:
            part += "**Table labels:**\n"
            for k, v in table_labels.items():
                part += f"  - {k}: {v}\n"

        if fields:
            part += "**Columns:**\n"
            # Build a field-id to label mapping
            col_label_map = {cl["field-id"]: cl.get("labels", {}) for cl in column_labels}
            for field in fields:
                fid = field.get("id")
                fname = field.get("name", "?")
                ftype = field.get("type", "?")
                cl = col_label_map.get(fid, {})
                meaning = cl.get("meaning", "")
                sensitivity = cl.get("sensitivity", "")
                pii = cl.get("pii_type", cl.get("phi_type", ""))

                extras = []
                if meaning:
                    extras.append(f'meaning="{meaning}"')
                if sensitivity:
                    extras.append(f"sensitivity={sensitivity}")
                if pii:
                    extras.append(f"pii/phi={pii}")

                extra_str = f" ({', '.join(extras)})" if extras else ""
                part += f"  - `{fname}` {ftype}{extra_str}\n"

        parts.append(part)

    return "\n".join(parts)


SYSTEM_PROMPT = """You are a data agent that helps users query a healthcare data lake.
You have access to tables via an Iceberg REST Catalog that provides rich metadata labels.

IMPORTANT RULES:
1. Use labels to select the RIGHT table — prefer safe/aggregated tables over sensitive ones.
2. RESPECT governance labels:
   - If a table has sensitivity=restricted, DO NOT query it unless the user explicitly has authorization.
   - If columns have pii_type or phi_type labels, WARN the user and suggest alternatives.
   - If regulatory_scope includes HIPAA, treat all PII/PHI columns as protected.
3. Explain your reasoning: which labels influenced your table/column choice.
4. Generate SQL compatible with both ClickHouse and DuckDB (standard SQL).
5. If the user's question can be answered from a less-sensitive table, prefer that.

When you generate SQL, wrap it in ```sql blocks.
When you explain your reasoning, reference specific labels."""


class DataAgent:
    """Label-aware data agent."""

    def __init__(self, config: AgentConfig):
        self.config = config
        self.irc = IRCClient(config.irc_url, config.catalog)
        self.llm = OpenAI(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
        )
        self._table_context: str | None = None
        self._tables: list[dict] | None = None

    def discover(self) -> str:
        """Discover tables and build context from labels."""
        self._tables = self.irc.discover_tables_with_labels(self.config.namespace)
        self._table_context = format_table_context(self._tables)
        return self._table_context

    def ask(self, question: str, with_labels: bool = True) -> dict:
        """
        Ask a natural language question about the data.

        Args:
            question: Natural language question
            with_labels: If True, provide labels context. If False, only schema (for comparison).

        Returns:
            dict with keys: answer, sql, reasoning, governance_warnings
        """
        if self._tables is None:
            self.discover()

        if with_labels:
            context = self._table_context
        else:
            # Strip labels — only provide schema (the "without labels" baseline)
            context = self._format_schema_only()

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"""Available tables and metadata:

{context}

---

User question: {question}

Respond with:
1. **Reasoning**: Which table(s) you chose and why (reference specific labels if available)
2. **Governance**: Any sensitivity/PII warnings
3. **SQL**: The query to answer the question (standard SQL)
4. **Answer approach**: How to interpret the results""",
            },
        ]

        response = self.llm.chat.completions.create(
            model=self.config.llm_model,
            messages=messages,
            temperature=0,
            max_tokens=2000,
        )

        return {
            "answer": response.choices[0].message.content,
            "model": self.config.llm_model,
            "labels_used": with_labels,
        }

    def _format_schema_only(self) -> str:
        """Format tables with only schema info (no labels) for baseline comparison."""
        parts = []
        for t in self._tables:
            schema = t.get("schema", {})
            fields = schema.get("fields", []) if schema else []

            part = f"### Table: `{t['name']}`\n"
            part += "**Columns:**\n"
            for field in fields:
                fname = field.get("name", "?")
                ftype = field.get("type", "?")
                part += f"  - `{fname}` {ftype}\n"

            parts.append(part)

        return "\n".join(parts)

    def compare(self, question: str) -> dict:
        """
        Ask the same question with and without labels to show the difference.

        Returns dict with keys: with_labels, without_labels
        """
        return {
            "with_labels": self.ask(question, with_labels=True),
            "without_labels": self.ask(question, with_labels=False),
        }

    def close(self):
        self.irc.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
