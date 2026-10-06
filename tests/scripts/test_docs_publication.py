# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Publication oracles: navigation hiding cannot substitute for exclusion."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.check_docs_publication import audit


@pytest.fixture
def publication(tmp_path: Path) -> tuple[Path, Path]:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "index.md").write_text("# Current runtime\n")
    archive = tmp_path / "archive/docs/planning"
    archive.mkdir(parents=True)
    (archive / "old.md").write_text("# Retired design\n")
    (tmp_path / "mkdocs.yml").write_text(
        "docs_dir: docs\nnav:\n  - Runtime: index.md\nnot_in_nav: |\n  planning/**\n"
    )
    site = tmp_path / "site"
    (site / "search").mkdir(parents=True)
    (site / "search/search_index.json").write_text(
        json.dumps({"docs": [{"location": "index.html", "title": "Runtime", "text": "Current"}]})
    )
    (site / "index.html").write_text('<a href="index.html">Runtime</a>')
    return tmp_path, site


def test_archives_outside_docs_are_not_published(publication):
    root, site = publication
    assert audit(root, site) == []


def test_not_in_nav_does_not_admit_historical_sources(publication):
    root, site = publication
    (root / "docs/planning").mkdir()
    (root / "docs/planning/old.md").write_text("# Retired design\n")
    assert any("historical source" in error for error in audit(root, site))


@pytest.mark.parametrize(
    "link",
    [
        "../../archive/docs/planning/old.md",
        "https://archetype.vangelis.tech/docs/compatibility/0.6/guide/runtime/",
        "../%61rchive/docs/history/",
    ],
)
def test_generated_reference_cannot_link_to_archive(publication, link):
    root, site = publication
    (root / "docs/index.md").write_text(f"[Old design]({link})\n")
    assert any("source links" in error for error in audit(root, site))


def test_built_search_entry_is_checked_independently(publication):
    root, site = publication
    (site / "search/search_index.json").write_text(
        json.dumps(
            {"docs": [{"location": "planning/old/#design", "title": "Old", "text": "Retired"}]}
        )
    )
    assert any("archived search entry" in error for error in audit(root, site))


def test_built_output_is_checked_independently(publication):
    root, site = publication
    (site / "history").mkdir()
    (site / "history/index.html").write_text("Retired history")
    assert any("historical output" in error for error in audit(root, site))


def test_built_links_are_checked_independently(publication):
    root, site = publication
    (site / "index.html").write_text('<a href="../archive/docs/old.html">Old</a>')
    assert any("built page links" in error for error in audit(root, site))


def test_missing_search_index_fails_closed(publication):
    root, site = publication
    (site / "search/search_index.json").unlink()
    assert "missing built search index" in audit(root, site)


def test_navigation_cannot_restore_archived_pages(publication):
    root, site = publication
    (root / "mkdocs.yml").write_text(
        "docs_dir: docs\nnav:\n  - Old: ../archive/docs/planning/old.md\n"
    )
    assert "navigation references archived documentation" in audit(root, site)


def test_source_symlink_cannot_publish_archived_record(publication):
    root, site = publication
    (root / "docs/old.md").symlink_to(root / "archive/docs/planning/old.md")
    assert any("escapes docs_dir" in error for error in audit(root, site))


@pytest.mark.parametrize(
    "link",
    [
        "https://upstream.test/history/programs",
        "history/../guide/runtime/",
        "https://archetype.vangelis.tech/docs/history/../guide/runtime/",
    ],
)
def test_external_history_and_normalized_active_paths_are_valid(publication, link):
    root, site = publication
    (root / "docs/index.md").write_text(f"[Reference]({link})\n")
    assert audit(root, site) == []


@pytest.fixture
def assembled(publication):
    root, docs_site = publication
    site = root / "assembled"
    (site / "docs").mkdir(parents=True)
    (site / "docs/index.html").write_text("Current docs")
    (site / "index.html").write_text('<a href="docs/">Docs</a>')
    (site / "404.html").write_text('<a href="docs/">Docs</a>')
    (site / "_headers").write_text("/*\n  X-Content-Type-Options: nosniff\n")
    (site / "_redirects").write_text("/ /index.html 200\n")
    return root, docs_site, site


@pytest.mark.parametrize("name", ["index.html", "404.html", "_redirects"])
def test_final_assembled_pages_and_redirects_cannot_reintroduce_archives(assembled, name):
    root, docs_site, site = assembled
    assert audit(root, docs_site, site) == []
    text = (
        "/old /docs/compatibility/0.6/ 301\n"
        if name == "_redirects"
        else '<a href="archive/docs/history/">Old</a>'
    )
    (site / name).write_text(text)
    assert any("assembled page links" in error for error in audit(root, docs_site, site))


def test_final_assembled_required_files_are_checked(assembled):
    root, docs_site, site = assembled
    (site / "404.html").unlink()
    assert "missing assembled file: 404.html" in audit(root, docs_site, site)
