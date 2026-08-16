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

import pytest

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


def test_default_scan_includes_every_workspace_source_root(tmp_path: Path) -> None:
    expected: set[Path] = set()
    for index, relative in enumerate(checker.WORKSPACE_SOURCE_ROOTS):
        path = tmp_path / relative / "archetype" / f"module_{index}.py"
        path.parent.mkdir(parents=True)
        path.write_text(COMPACT_HEADER, encoding="utf-8")
        expected.add(path)

    assert set(checker.default_python_files(tmp_path)) == expected


def test_default_scan_fails_closed_for_partial_workspace(tmp_path: Path) -> None:
    (tmp_path / checker.WORKSPACE_SOURCE_ROOTS[0]).mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="workspace source roots are incomplete"):
        checker.default_python_files(tmp_path)
