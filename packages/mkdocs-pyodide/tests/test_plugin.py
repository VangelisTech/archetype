"""Tests for mkdocs-pyodide plugin HTML transformation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mkdocs_pyodide.plugin import PyodidePlugin, _CODEHILITE_RE, _FENCED_RE


def _make_page(src_path: str = "index.md") -> MagicMock:
    page = MagicMock()
    page.file.src_path = src_path
    return page


# ── Regex tests: codehilite pattern ──────────────────────────────────────────


class TestCodehiliteRegex:
    def test_matches_live_codehilite_block(self):
        html = '<div class="live codehilite"><pre><span></span><code><span class="nb">print</span><span class="p">(</span><span class="mi">1</span><span class="p">)</span></code></pre></div>'
        assert _CODEHILITE_RE.search(html) is not None

    def test_does_not_match_codehilite_without_live(self):
        html = '<div class="codehilite"><pre><span></span><code>print(1)</code></pre></div>'
        assert _CODEHILITE_RE.search(html) is None

    def test_captures_highlighted_code(self):
        html = '<div class="live codehilite"><pre><span></span><code><span class="n">x</span> <span class="o">=</span> <span class="mi">1</span></code></pre></div>'
        m = _CODEHILITE_RE.search(html)
        assert m is not None
        assert "<span" in m.group(1)


# ── Regex tests: fenced_code fallback ────────────────────────────────────────


class TestFencedRegex:
    def test_matches_python_live_block(self):
        html = '<pre><code class="language-python live">print("hi")</code></pre>'
        assert _FENCED_RE.search(html) is not None

    def test_does_not_match_plain_python(self):
        html = '<pre><code class="language-python">print("hi")</code></pre>'
        assert _FENCED_RE.search(html) is None


# ── Plugin HTML transformation ───────────────────────────────────────────────


class TestPluginTransform:
    @pytest.fixture
    def plugin(self):
        p = PyodidePlugin()
        p.config = {
            "packages": [],
            "theme": "auto",
            "pyodide_version": "0.27.6",
            "lazy_load": True,
        }
        return p

    def test_transforms_codehilite_live_block(self, plugin):
        html = '<div class="live codehilite"><pre><span></span><code><span class="nb">print</span><span class="p">(</span><span class="s2">&quot;hello&quot;</span><span class="p">)</span></code></pre></div>'
        page = _make_page()
        result = plugin.on_page_content(html, page=page, config=None, files=None)

        assert 'class="pyodide-cell"' in result
        assert 'class="pyodide-editor"' in result
        assert 'class="pyodide-run"' in result
        assert "data-source=" in result
        assert "index.md" in plugin._pages_with_cells

    def test_source_strips_html_tags(self, plugin):
        html = '<div class="live codehilite"><pre><span></span><code><span class="n">x</span> <span class="o">=</span> <span class="mi">42</span></code></pre></div>'
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert 'data-source="x = 42"' in result

    def test_transforms_fenced_fallback(self, plugin):
        html = '<pre><code class="language-python live">print("hello")</code></pre>'
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert 'class="pyodide-cell"' in result
        assert "index.md" in plugin._pages_with_cells

    def test_preserves_plain_codehilite(self, plugin):
        html = '<div class="codehilite"><pre><span></span><code>print(1)</code></pre></div>'
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert result == html
        assert len(plugin._pages_with_cells) == 0

    def test_preserves_plain_python(self, plugin):
        html = '<pre><code class="language-python">print("hello")</code></pre>'
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert result == html
        assert len(plugin._pages_with_cells) == 0

    def test_decodes_html_entities_in_source(self, plugin):
        html = '<div class="live codehilite"><pre><span></span><code>if x &gt; 0:\n    print(&quot;yes&quot;)</code></pre></div>'
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert "x &gt; 0" in result

    def test_multiple_cells(self, plugin):
        html = (
            '<div class="live codehilite"><pre><span></span><code>a = 1</code></pre></div>'
            "<p>Some text</p>"
            '<div class="live codehilite"><pre><span></span><code>b = 2</code></pre></div>'
        )
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert result.count('class="pyodide-cell"') == 2

    def test_mixed_live_and_static(self, plugin):
        html = (
            '<div class="live codehilite"><pre><span></span><code>a = 1</code></pre></div>'
            '<div class="codehilite"><pre><span></span><code>b = 2</code></pre></div>'
        )
        result = plugin.on_page_content(html, page=_make_page(), config=None, files=None)

        assert result.count('class="pyodide-cell"') == 1
        assert 'class="codehilite"' in result


# ── Asset injection ──────────────────────────────────────────────────────────


class TestAssetInjection:
    @pytest.fixture
    def plugin(self):
        p = PyodidePlugin()
        p.config = {
            "packages": ["numpy"],
            "theme": "dark",
            "pyodide_version": "0.27.6",
            "lazy_load": True,
        }
        return p

    def test_injects_assets_when_cells_present(self, plugin):
        page = _make_page()
        html = '<div class="live codehilite"><pre><span></span><code>pass</code></pre></div>'
        plugin.on_page_content(html, page=page, config=None, files=None)

        page_html = "<html><head></head><body></body></html>"
        result = plugin.on_post_page(page_html, page=page, config=None)

        assert "__PYODIDE_CONFIG__" in result
        assert '"numpy"' in result

    def test_skips_injection_without_cells(self, plugin):
        page = _make_page()
        html = '<div class="codehilite"><pre><span></span><code>pass</code></pre></div>'
        plugin.on_page_content(html, page=page, config=None, files=None)

        page_html = "<html><head></head><body></body></html>"
        result = plugin.on_post_page(page_html, page=page, config=None)

        assert result == page_html

    def test_injects_only_for_live_pages(self, plugin):
        """Simulates MkDocs batching: all on_page_content, then all on_post_page."""
        live_page = _make_page("index.md")
        static_page = _make_page("guide/api.md")

        # Phase 1: all content hooks
        plugin.on_page_content(
            '<div class="live codehilite"><pre><span></span><code>pass</code></pre></div>',
            page=live_page,
            config=None,
            files=None,
        )
        plugin.on_page_content(
            '<div class="codehilite"><pre><span></span><code>pass</code></pre></div>',
            page=static_page,
            config=None,
            files=None,
        )

        page_html = "<html><head></head><body></body></html>"

        # Phase 2: all post-page hooks
        live_result = plugin.on_post_page(page_html, page=live_page, config=None)
        static_result = plugin.on_post_page(page_html, page=static_page, config=None)

        assert "__PYODIDE_CONFIG__" in live_result
        assert static_result == page_html
