"""Install notebook dependencies on container start."""
from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    req = os.path.join(os.path.dirname(__file__), "requirements.txt")
    print(f"Installing dependencies from {req} ...")
    return subprocess.call(
        [sys.executable, "-m", "pip", "install", "--quiet", "-r", req]
    )


if __name__ == "__main__":
    sys.exit(main())
