"""MkDocs plugin that transforms ``{ .python .live }`` code blocks into interactive Pyodide cells.

Usage in markdown::

    ``` { .python .live }
    print("Hello from Pyodide!")
    ```

The ``codehilite`` extension renders this as::

    <div class="live codehilite"><pre><span></span><code>...</code></pre></div>

The plugin matches that wrapper div and replaces the entire block with an
interactive cell (CodeMirror editor + Run/Reset buttons + output panel).
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

from mkdocs.config import config_options
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.plugins import BasePlugin
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page

# ── Pattern A: codehilite/superfences output ─────────────────────────────────
# <div class="live codehilite"><pre>...<code>HIGHLIGHTED</code></pre></div>
# <div class="live highlight"><pre>...<code>HIGHLIGHTED</code></pre></div>
_CODEHILITE_RE = re.compile(
    r'<div\s+class="[^"]*\blive\b[^"]*(?:\bcodehilite\b|\bhighlight\b)[^"]*"[^>]*>'
    r"\s*<pre[^>]*>(?:<span></span>)?\s*<code>(.*?)</code>\s*</pre>\s*</div>",
    re.DOTALL,
)

# ── Pattern B: plain fenced_code output (no codehilite) ─────────────────────
# <pre><code class="language-python live">CODE</code></pre>
_FENCED_RE = re.compile(
    r'<pre[^>]*>\s*<code[^>]*class="[^"]*language-python[^"]*\blive\b[^"]*"[^>]*>'
    r"(.*?)"
    r"</code>\s*</pre>",
    re.DOTALL,
)

_ASSET_DIR = Path(__file__).parent

# Regex to strip HTML tags (for extracting plain text from syntax-highlighted code)
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")


def _strip_tags(s: str) -> str:
    """Remove all HTML tags, returning plain text."""
    return _STRIP_TAGS_RE.sub("", s)


class PyodidePlugin(BasePlugin):
    config_scheme = (
        ("packages", config_options.Type(list, default=[])),
        ("theme", config_options.Choice(("dark", "light", "auto"), default="auto")),
        ("pyodide_version", config_options.Type(str, default="0.27.6")),
        ("lazy_load", config_options.Type(bool, default=True)),
    )

    def __init__(self) -> None:
        super().__init__()
        # Track which pages have live cells (MkDocs batches all on_page_content
        # calls before any on_post_page calls, so a single boolean won't work).
        self._pages_with_cells: set[str] = set()
        # Cache bundled assets to avoid repeated disk reads during build.
        self._js_cache: str | None = None
        self._css_cache: str | None = None

    # ------------------------------------------------------------------
    # MkDocs hooks
    # ------------------------------------------------------------------

    def on_config(self, config: MkDocsConfig) -> MkDocsConfig:
        self._pages_with_cells.clear()
        return config

    def on_page_content(
        self, content: str, *, page: Page, config: MkDocsConfig, files: Files
    ) -> str:
        """Find live code blocks and wrap them as interactive cells."""
        found = False

        def _replace(match: re.Match) -> str:
            nonlocal found
            found = True
            raw_code = match.group(1)
            # Strip syntax-highlighting spans, then decode HTML entities
            source = html.unescape(_strip_tags(raw_code)).strip()
            escaped = html.escape(source, quote=True)
            cell_id = f"pyodide-cell-{id(match)}"

            return (
                f'<div class="pyodide-cell" id="{cell_id}" data-source="{escaped}">'
                f'  <div class="pyodide-editor"></div>'
                f'  <div class="pyodide-toolbar">'
                f'    <button class="pyodide-run" title="Run (Shift+Enter)">Run</button>'
                f'    <button class="pyodide-reset" title="Reset to original">Reset</button>'
                f'    <span class="pyodide-status"></span>'
                f"  </div>"
                f'  <pre class="pyodide-output"></pre>'
                f"</div>"
            )

        # Try codehilite pattern first, then plain fenced_code fallback
        transformed = _CODEHILITE_RE.sub(_replace, content)
        if not found:
            transformed = _FENCED_RE.sub(_replace, content)

        if found:
            page_path = page.file.src_path if page and page.file else ""
            self._pages_with_cells.add(page_path)

        return transformed

    def on_post_page(self, output: str, *, page: Page, config: MkDocsConfig) -> str:
        """Inject JS/CSS assets into pages that contain live cells."""
        page_path = page.file.src_path if page and page.file else ""
        if page_path not in self._pages_with_cells:
            return output

        pyodide_ver = self.config["pyodide_version"]
        packages_json = json.dumps(self.config["packages"])
        theme = self.config["theme"]
        lazy = json.dumps(self.config["lazy_load"])

        # Read bundled assets (cached across pages)
        if self._js_cache is None:
            self._js_cache = (_ASSET_DIR / "js" / "pyodide-runner.js").read_text()
        if self._css_cache is None:
            self._css_cache = (_ASSET_DIR / "css" / "pyodide-runner.css").read_text()
        js_content = self._js_cache
        css_content = self._css_cache

        head_inject = f"\n<style>{css_content}</style>\n"

        body_inject = f"""
<script>
  window.__PYODIDE_CONFIG__ = {{
    version: "{pyodide_ver}",
    packages: {packages_json},
    theme: "{theme}",
    lazyLoad: {lazy}
  }};
</script>
<script type="module">{js_content}</script>
"""

        output = output.replace("</head>", head_inject + "</head>", 1)
        output = output.replace("</body>", body_inject + "</body>", 1)
        return output
