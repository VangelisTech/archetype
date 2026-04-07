/**
 * mkdocs-pyodide: Live Python code cells powered by Pyodide + CodeMirror 6.
 *
 * Finds all .pyodide-cell elements, initialises CodeMirror editors, and runs
 * code via a shared Pyodide instance (lazy-loaded on first Run click).
 */

const CFG = window.__PYODIDE_CONFIG__ || {};
const PYODIDE_CDN = `https://cdn.jsdelivr.net/pyodide/v${CFG.version || "0.27.6"}/full/`;

// esm.sh deduplicates shared dependencies (e.g. @codemirror/state) across
// packages, avoiding the "multiple instances" error that jsdelivr causes.
const ESM = "https://esm.sh";

// ── CodeMirror 6 (loaded from CDN) ──────────────────────────────────────────

async function loadCodeMirror() {
  if (window.__CM_LOADED__) return window.__CM_LOADED__;

  const [cmView, cmState, cmPython, cmOneDark] = await Promise.all([
    import(`${ESM}/@codemirror/view`),
    import(`${ESM}/@codemirror/state`),
    import(`${ESM}/@codemirror/lang-python`),
    import(`${ESM}/@codemirror/theme-one-dark`),
  ]);

  window.__CM_LOADED__ = { cmView, cmState, cmPython, cmOneDark };
  return window.__CM_LOADED__;
}

// ── Pyodide (lazy-loaded) ───────────────────────────────────────────────────

let pyodidePromise = null;

async function ensurePyodide() {
  if (pyodidePromise) return pyodidePromise;

  pyodidePromise = (async () => {
    if (!window.loadPyodide) {
      await new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = `${PYODIDE_CDN}pyodide.js`;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
      });
    }

    const pyodide = await window.loadPyodide({ indexURL: PYODIDE_CDN });

    const packages = CFG.packages || [];
    if (packages.length > 0) {
      await pyodide.loadPackage("micropip");
      const micropip = pyodide.pyimport("micropip");
      await micropip.install(packages);
    }

    return pyodide;
  })();

  return pyodidePromise;
}

// ── Cell initialisation ─────────────────────────────────────────────────────

async function initCells() {
  const cells = document.querySelectorAll(".pyodide-cell");
  if (cells.length === 0) return;

  const cm = await loadCodeMirror();

  function isDark() {
    if (CFG.theme === "dark") return true;
    if (CFG.theme === "light") return false;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ||
      document.documentElement.getAttribute("data-theme") === "dark" ||
      document.body.classList.contains("dark");
  }

  for (const cell of cells) {
    const source = cell.dataset.source;
    const editorEl = cell.querySelector(".pyodide-editor");
    const runBtn = cell.querySelector(".pyodide-run");
    const resetBtn = cell.querySelector(".pyodide-reset");
    const statusEl = cell.querySelector(".pyodide-status");
    const outputEl = cell.querySelector(".pyodide-output");

    const extensions = [
      cm.cmView.lineNumbers(),
      cm.cmView.highlightActiveLine(),
      cm.cmPython.python(),
      cm.cmView.keymap.of([
        {
          key: "Shift-Enter",
          run: () => { runBtn.click(); return true; },
        },
      ]),
    ];
    if (isDark()) {
      extensions.push(cm.cmOneDark.oneDark);
    }

    const state = cm.cmState.EditorState.create({
      doc: source,
      extensions,
    });

    const view = new cm.cmView.EditorView({
      state,
      parent: editorEl,
    });

    // ── Run ──
    let running = false;
    runBtn.addEventListener("click", async () => {
      if (running) return;
      running = true;
      runBtn.disabled = true;
      statusEl.textContent = "Loading Pyodide...";
      outputEl.textContent = "";
      outputEl.classList.remove("pyodide-error");

      try {
        const pyodide = await ensurePyodide();
        statusEl.textContent = "Running...";

        let stdout = "";
        let stderr = "";
        pyodide.setStdout({ batched: (text) => { stdout += text + "\n"; } });
        pyodide.setStderr({ batched: (text) => { stderr += text + "\n"; } });

        const code = view.state.doc.toString();
        let result;
        try {
          result = await pyodide.runPythonAsync(code);
        } catch (err) {
          stderr += err.message;
        }

        let output = stdout;
        if (result !== undefined && result !== null && !stdout.includes(String(result))) {
          output += String(result);
        }
        if (stderr) {
          outputEl.classList.add("pyodide-error");
          output += stderr;
        }
        outputEl.textContent = output.trimEnd();
        statusEl.textContent = "";
      } catch (err) {
        outputEl.classList.add("pyodide-error");
        outputEl.textContent = `Failed to load Pyodide: ${err.message}`;
        statusEl.textContent = "";
      } finally {
        running = false;
        runBtn.disabled = false;
      }
    });

    // ── Reset ──
    resetBtn.addEventListener("click", () => {
      view.dispatch({
        changes: {
          from: 0,
          to: view.state.doc.length,
          insert: source,
        },
      });
      outputEl.textContent = "";
      outputEl.classList.remove("pyodide-error");
    });

    if (!CFG.lazyLoad) {
      ensurePyodide().then(() => {});
    }
  }
}

// ── Bootstrap ───────────────────────────────────────────────────────────────

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initCells);
} else {
  initCells();
}
