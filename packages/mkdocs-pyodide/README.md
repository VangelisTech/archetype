# mkdocs-pyodide

A [MkDocs](https://www.mkdocs.org/) plugin that transforms fenced Python code blocks into live, editable cells powered by [Pyodide](https://pyodide.org/) and [CodeMirror 6](https://codemirror.net/).

## Features

- **Live Python execution** — runs entirely in the browser via WebAssembly; no server required
- **Editable cells** — CodeMirror 6 editor with Python syntax highlighting
- **Shift+Enter** to run, Reset button to restore the original code
- **Lazy-loads Pyodide** on first Run click for fast page loads
- **Dark/light/auto theme** support

## Installation

```bash
pip install mkdocs-pyodide
```

## Usage

In your `mkdocs.yml`:

```yaml
plugins:
  - search
  - pyodide:
      theme: auto          # dark | light | auto
      lazy_load: true      # defer Pyodide init until first Run click
      pyodide_version: "0.27.6"
      packages: []         # extra micropip packages to pre-install
```

In your markdown, add `.live` to any fenced Python block:

````markdown
``` { .python .live }
print("Hello from Pyodide!")
x = 6 * 7
x
```
````

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

## License

MIT
