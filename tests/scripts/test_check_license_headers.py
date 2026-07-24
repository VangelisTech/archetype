# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the license-header checker.

The checker guards two live conventions (long-form 2025 Apache header,
compact SPDX header) and must never stack a second copyright block onto a
file that already has one — the 2026-07-24 `--fix` incident rewrote 130+
correctly headered files under a green exit.
"""

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import check_license_headers as checker  # noqa: E402

LONG_HEADER = """# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

x = 1
"""

COMPACT_HEADER = """# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

x = 1
"""


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "module.py"
    path.write_text(content, encoding="utf-8")
    return path


def test_long_form_2025_header_is_valid(tmp_path: Path) -> None:
    assert checker.has_license_header(_write(tmp_path, LONG_HEADER))


def test_compact_spdx_2026_header_is_valid(tmp_path: Path) -> None:
    assert checker.has_license_header(_write(tmp_path, COMPACT_HEADER))


def test_year_range_header_is_valid(tmp_path: Path) -> None:
    content = COMPACT_HEADER.replace("Copyright 2026", "Copyright 2025-2026")
    assert checker.has_license_header(_write(tmp_path, content))


def test_license_mention_outside_the_head_is_not_a_header(tmp_path: Path) -> None:
    filler = "x = 1\n" * 600
    content = filler + COMPACT_HEADER
    assert not checker.has_license_header(_write(tmp_path, content))


def test_fix_adds_compact_header_once(tmp_path: Path) -> None:
    path = _write(tmp_path, "x = 1\n")
    assert not checker.has_license_header(path)

    assert checker.add_license_header(path)
    assert checker.has_license_header(path)
    stamped = path.read_text(encoding="utf-8")
    assert stamped.count("Vangelis Technologies Inc.") == 1
    assert "SPDX-License-Identifier: Apache-2.0" in stamped

    # A second fix pass must be a no-op: the file now has a valid header, and
    # add_license_header refuses files whose head already carries a copyright.
    assert not checker.add_license_header(path)
    assert path.read_text(encoding="utf-8") == stamped


def test_fix_preserves_shebang_line(tmp_path: Path) -> None:
    path = _write(tmp_path, "#!/usr/bin/env python3\nx = 1\n")
    assert checker.add_license_header(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "#!/usr/bin/env python3"
    assert lines[1].startswith("# Copyright")


def test_fix_never_stacks_a_second_copyright_block(tmp_path: Path) -> None:
    # Copyright line present but no recognized license marker: the header is
    # malformed, and the only safe automated action is to refuse and stay red.
    content = "# Copyright 2026 Vangelis Technologies Inc.\n\nx = 1\n"
    path = _write(tmp_path, content)
    assert not checker.has_license_header(path)
    assert not checker.add_license_header(path)
    assert path.read_text(encoding="utf-8") == content


def test_fix_run_with_unrepairable_file_exits_red(tmp_path: Path, monkeypatch, capsys) -> None:
    path = _write(tmp_path, "# Copyright 2026 Vangelis Technologies Inc.\n\nx = 1\n")
    monkeypatch.setattr(sys, "argv", ["check_license_headers.py", "--fix", str(path)])
    assert checker.main() == 1
    assert "Refusing to stack" in capsys.readouterr().out


def test_fix_run_that_repairs_everything_exits_green(tmp_path: Path, monkeypatch) -> None:
    path = _write(tmp_path, "x = 1\n")
    monkeypatch.setattr(sys, "argv", ["check_license_headers.py", "--fix", str(path)])
    assert checker.main() == 0
    assert checker.has_license_header(path)


def test_empty_file_is_skipped_by_fix(tmp_path: Path) -> None:
    path = _write(tmp_path, "\n")
    assert not checker.add_license_header(path)


# --- the gate must fail loudly, never silently ------------------------------
#
# A checker only protects what it actually inspects. Each case below is a way
# the gate could report success while auditing nothing.


def test_an_empty_audit_set_is_an_error(tmp_path: Path) -> None:
    not_python = tmp_path / "README.md"
    not_python.write_text("# not python\n", encoding="utf-8")

    files, errors = checker.resolve_files([str(not_python)])

    assert files == []
    assert errors


def test_a_run_that_audits_nothing_exits_red(tmp_path: Path, monkeypatch) -> None:
    not_python = tmp_path / "README.md"
    not_python.write_text("# not python\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["check_license_headers.py", str(not_python)])

    assert checker.main() == 1


def test_a_missing_source_root_is_an_error_not_an_empty_pass(tmp_path: Path, monkeypatch) -> None:
    # No arguments means "audit src/". If that root is absent, the audit set is
    # empty for a structural reason and must not read as success.
    monkeypatch.setattr(checker, "__file__", str(tmp_path / "scripts" / "checker.py"))

    files, errors = checker.resolve_files([])

    assert files == []
    assert any("source root does not exist" in message for message in errors)


def test_a_nonexistent_path_is_an_error_not_a_missing_header(tmp_path: Path) -> None:
    absent = tmp_path / "gone.py"

    files, errors = checker.resolve_files([str(absent)])

    assert files == []
    assert any("path does not exist" in message for message in errors)


def test_fix_cannot_launder_a_nonexistent_path_into_a_pass(tmp_path: Path, monkeypatch) -> None:
    absent = tmp_path / "gone.py"
    monkeypatch.setattr(sys, "argv", ["check_license_headers.py", "--fix", str(absent)])

    assert checker.main() == 1
    assert not absent.exists()


def test_dot_github_is_audited_while_dot_git_is_skipped() -> None:
    # ".git" as a substring also matches ".github", which silently exempted
    # every workflow helper. Skip rules match whole path components.
    assert not checker.should_skip_file(Path(".github/workflows/helper.py"))
    assert checker.should_skip_file(Path(".git/hooks/helper.py"))
    assert checker.should_skip_file(Path("src/archetype/__pycache__/mod.py"))


def test_a_github_file_reaches_the_audit_set(tmp_path: Path) -> None:
    scripts_dir = tmp_path / ".github" / "scripts"
    scripts_dir.mkdir(parents=True)
    helper = scripts_dir / "helper.py"
    helper.write_text("x = 1\n", encoding="utf-8")

    files, errors = checker.resolve_files([str(helper)])

    assert files == [helper]
    assert not errors


def test_every_shipped_source_file_is_audited_and_licensed(monkeypatch) -> None:
    """Regression lock: the state this checker failed to report for 142 files."""
    files, errors = checker.resolve_files([])

    assert not errors
    assert files, "the default audit set must never resolve to nothing"

    monkeypatch.setattr(sys, "argv", ["check_license_headers.py"])
    assert checker.main() == 0
