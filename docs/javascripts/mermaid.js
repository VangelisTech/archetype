// Material swaps page content during instant navigation without reloading scripts.
// Re-run Mermaid whenever that new document becomes active.
mermaid.initialize({ startOnLoad: false });
document$.subscribe(() => {
  mermaid.run({ querySelector: ".mermaid" });
});
