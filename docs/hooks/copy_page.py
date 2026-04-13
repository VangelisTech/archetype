"""MkDocs hook: inject raw markdown into template context for the copy-page button."""

_page_markdown_cache: dict[str, str] = {}


def on_page_markdown(markdown, page, config, files):
    """Cache the raw markdown before it gets converted to HTML."""
    _page_markdown_cache[page.file.src_path] = markdown
    return markdown


def on_page_context(context, page, config, nav):
    """Inject the cached markdown into the template context."""
    context["raw_page_markdown"] = _page_markdown_cache.pop(
        page.file.src_path, ""
    )
    return context
