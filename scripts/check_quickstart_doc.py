"""Execute every Python snippet in docs/quickstart.md as one session.

The guide promises its snippets are copy-pasteable and form one continuous
session; this script holds it to that. Runs in a temporary working directory
so the guide's relative archive path never touches the repo.

Usage: python scripts/check_quickstart_doc.py
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import textwrap
from pathlib import Path

PYTHON_BLOCK_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    guide = root / "docs" / "quickstart.md"
    blocks = PYTHON_BLOCK_RE.findall(guide.read_text(encoding="utf-8"))
    if not blocks:
        print("no python blocks found in the quickstart guide")
        return 1

    # blocks nested in list items carry the list indentation
    session = "\n\n".join(textwrap.dedent(block) for block in blocks)
    workdir = tempfile.mkdtemp(prefix="foreledger_quickstart_doc_")
    cwd = os.getcwd()
    try:
        os.chdir(workdir)
        exec(compile(session, str(guide), "exec"), {"__name__": "__main__"})
    finally:
        os.chdir(cwd)
    print(f"quickstart guide executed: {len(blocks)} snippets, one continuous session")
    return 0


if __name__ == "__main__":
    sys.exit(main())
