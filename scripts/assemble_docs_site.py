"""Assemble the Cloudflare Pages artifact around the MkDocs output."""

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"
DOCS = SITE / "docs"
LANDING_SOURCE = ROOT / "scripts" / "docs_landing.html"
NOT_FOUND_SOURCE = ROOT / "scripts" / "docs_404.html"


def main() -> None:
    docs_index = DOCS / "index.html"
    root_files = ("_headers", "_redirects")

    if not docs_index.is_file():
        msg = "Expected MkDocs output at site/docs/index.html before assembly."
        raise SystemExit(msg)
    for name in root_files:
        if not (DOCS / name).is_file():
            msg = f"Expected docs/{name} to be copied into the MkDocs output."
            raise SystemExit(msg)

    (SITE / "index.html").write_text(LANDING_SOURCE.read_text(), encoding="utf-8")
    (SITE / "404.html").write_text(NOT_FOUND_SOURCE.read_text(), encoding="utf-8")
    for name in root_files:
        shutil.move(DOCS / name, SITE / name)

    print("Assembled Cloudflare Pages artifact: / landing page, /docs documentation.")


if __name__ == "__main__":
    main()
