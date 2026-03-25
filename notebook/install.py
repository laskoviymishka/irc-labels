"""Notebook container startup: install deps."""
import subprocess, sys

packages = ["httpx", "duckdb", "clickhouse-connect", "pyarrow", "pyiceberg"]
subprocess.run(
    [sys.executable, "-m", "pip", "install", "--no-cache-dir"] + packages,
    check=False,
)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "--no-cache-dir", "openai"],
    check=False,
)
print("Install complete")
