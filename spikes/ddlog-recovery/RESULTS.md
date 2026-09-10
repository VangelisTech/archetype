# Verified controlled checkpoint restart

The original September 5, 2026 proof passed: complete input checkpoint through
Iceberg, fresh native host restoration, and eight full-set BFS comparisons.
The cleaned standalone harness was rerun before publication: **PASS**, 6.553 s,
three native activations, zero native compilations and zero provider calls.
All ten facts (seven vertices, three edges) were restored at boundary 2;
unpublished boundary 3 was excluded. Bridge insertion and deletion after restore
matched the uninterrupted host and independent BFS. Cleanup had no errors.

[Sanitized acceptance summary](results/acceptance.json) retains every output
comparison and the raw local receipt hash. Local paths, registry content, process
identities, binaries, catalogs and raw run directories are not published.
The host/native hashes in `artifact.example.json` identify the tested artifacts.

Three transport-free oracle contracts, Python compilation and Rust formatting
passed. Adapter build used the existing offline locked dependency/target cache;
this is not a clean dependency acquisition or fresh native compilation result.
The historical 6.427-second result is distinct from the cleaned harness rerun.
See [README.md](README.md) for reproduction and scope limitations.
