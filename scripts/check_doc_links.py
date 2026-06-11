"""Verify that relative markdown links in the repo's docs point at real files.

Usage: python scripts/check_doc_links.py
Exits non-zero listing any broken link. External (http/mailto) and pure
anchor links are skipped.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")


def doc_files(root: Path) -> list[Path]:
    return [*root.glob("*.md"), *(root / "docs").glob("*.md")]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    broken: list[str] = []
    for doc in doc_files(root):
        for match in LINK_RE.finditer(doc.read_text(encoding="utf-8")):
            target = match.group(1)
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            resolved = (doc.parent / target.split("#", 1)[0]).resolve()
            if not resolved.exists():
                broken.append(f"{doc.relative_to(root)}: {target}")
    if broken:
        print("broken relative links:")
        for entry in broken:
            print(f"  {entry}")
        return 1
    print(f"all relative links resolve across {len(doc_files(root))} markdown files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
