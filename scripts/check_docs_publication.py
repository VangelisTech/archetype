#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reject publication or references to archived documentation."""

from __future__ import annotations

import argparse
import json
import posixpath
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]
RETIRED = frozenset(
    {
        "archive",
        "compatibility",
        "design",
        "planning",
        "history",
        "reports",
        "incidents",
        "research",
        "maintainers",
        "experiments",
    }
)
LINK = re.compile(r"\]\(([^)\s]+)|(?:href|src)=[\"']([^\"']+)")


def archived_url(value: str) -> bool:
    parsed = urlsplit(value)
    if parsed.netloc and parsed.hostname not in {"archetype.vangelis.tech", "github.com"}:
        return False
    if parsed.hostname == "github.com" and not unquote(parsed.path).startswith(
        "/VangelisTech/archetype/"
    ):
        return False
    path = posixpath.normpath(unquote(parsed.path))
    return bool(RETIRED.intersection(Path(path).parts))


def _links(path: Path):
    if path.suffix in {".md", ".html"}:
        for match in LINK.finditer(path.read_text()):
            yield match.group(1) or match.group(2)
    elif path.name == "_redirects":
        for line in path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                yield from line.split()[:2]


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def audit(root: Path = ROOT, site: Path | None = None, assembled: Path | None = None) -> list[str]:
    errors = []
    config = yaml.safe_load((root / "mkdocs.yml").read_text())
    docs = (root / config["docs_dir"]).resolve()
    archive = (root / "archive").resolve()
    if docs == archive or docs in archive.parents or archive in docs.parents:
        errors.append("archive must be outside docs_dir")
    for path in docs.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(docs)
        if not path.resolve().is_relative_to(docs):
            errors.append(f"published source escapes docs_dir: {relative}")
        if RETIRED.intersection(relative.parts):
            errors.append(f"historical source remains publishable: {relative}")
        if any(archived_url(value) for value in _links(path)):
            errors.append(f"published source links to archive: {relative}")
    if any(archived_url(value) for value in _strings(config.get("nav", []))):
        errors.append("navigation references archived documentation")
    if site is not None:
        index = site / "search/search_index.json"
        if not index.is_file():
            errors.append("missing built search index")
        else:
            for document in json.loads(index.read_text())["docs"]:
                if archived_url(document["location"]):
                    errors.append(f"archived search entry: {document['location']}")
        for path in site.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(site)
            if RETIRED.intersection(relative.parts):
                errors.append(f"historical output remains published: {relative}")
            if any(archived_url(value) for value in _links(path)):
                errors.append(f"built page links to archive: {relative}")
    if assembled is not None:
        for name in ("index.html", "404.html", "_headers", "_redirects", "docs/index.html"):
            if not (assembled / name).is_file():
                errors.append(f"missing assembled file: {name}")
        for path in assembled.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(assembled)
            if RETIRED.intersection(relative.parts):
                errors.append(f"historical assembled output: {relative}")
            if any(archived_url(value) for value in _links(path)):
                errors.append(f"assembled page links to archive: {relative}")
    return sorted(set(errors))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path)
    parser.add_argument("--assembled", type=Path)
    args = parser.parse_args()
    errors = audit(site=args.site, assembled=args.assembled)
    for error in errors:
        print(error)
    if not errors:
        print("Documentation publication contains no archived pages or links")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
