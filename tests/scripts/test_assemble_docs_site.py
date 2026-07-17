# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the Cloudflare Pages artifact assembly."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import assemble_docs_site as assembly  # noqa: E402


def _configure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    site = tmp_path / "site"
    docs = site / "docs"
    docs.mkdir(parents=True)
    landing = tmp_path / "landing.html"
    not_found = tmp_path / "404.html"
    landing.write_text("landing", encoding="utf-8")
    not_found.write_text("missing", encoding="utf-8")
    monkeypatch.setattr(assembly, "SITE", site)
    monkeypatch.setattr(assembly, "DOCS", docs)
    monkeypatch.setattr(assembly, "LANDING_SOURCE", landing)
    monkeypatch.setattr(assembly, "NOT_FOUND_SOURCE", not_found)
    return site, docs


def test_assembly_promotes_cloudflare_control_files(tmp_path, monkeypatch):
    site, docs = _configure(tmp_path, monkeypatch)
    (docs / "index.html").write_text("docs", encoding="utf-8")
    (docs / "_headers").write_text("headers", encoding="utf-8")
    (docs / "_redirects").write_text("redirects", encoding="utf-8")

    assembly.main()

    assert (site / "index.html").read_text(encoding="utf-8") == "landing"
    assert (site / "404.html").read_text(encoding="utf-8") == "missing"
    assert (site / "_headers").read_text(encoding="utf-8") == "headers"
    assert (site / "_redirects").read_text(encoding="utf-8") == "redirects"
    assert not (docs / "_headers").exists()
    assert not (docs / "_redirects").exists()


def test_assembly_fails_when_redirect_rules_are_missing(tmp_path, monkeypatch):
    _, docs = _configure(tmp_path, monkeypatch)
    (docs / "index.html").write_text("docs", encoding="utf-8")
    (docs / "_headers").write_text("headers", encoding="utf-8")

    with pytest.raises(SystemExit, match=r"docs/_redirects"):
        assembly.main()
