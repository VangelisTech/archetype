# Contributing

Archetype's supported execution owner is the native DDlog `WorldManager`.
MCP and CLI are canonical; HTTP and Python share their operation contract.
Start with [the specification](docs/guide/specification.md), the focused native
contract tests, and the implementation that owns the behavior.

For nontrivial work, record the observable behavior, owning layer, normative
source, executable oracle, invariants at risk, validation and affected docs in
an issue. Fix the smallest owning layer and update the focused contract when
behavior changes. Source and specification disagreements must be recorded.

Native physical storage owns complete cut visibility, immutable object integrity,
artifact occurrence identity and retained recovery evidence. Runtime wrappers
own lazy activation and process lifetime. Authentication and resource grants
are enforced before native lookup. Preserve these boundaries when adding a
provider; remote data placement does not establish remote control authority.

Use these checks:

| Command | Evidence |
| --- | --- |
| `make ci` | Required static and current contract profile |
| `make verify-full` | Reliability, coverage, examples, package and docs evidence |
| `make verify-release` | Exact installed-wheel and actual DDlog acceptance |
| `make docs` | Generated references, strict build and source/search/assembled archive exclusion |

Open a PR, wait for required `Static` and `Tests (3.12)`, address concrete
advisory findings, obtain an eligible independent approval and merge normally.
A required-check failure gets one rerun after reading and classifying its
receipt; a second failure of the same kind is a harness defect to file.
Do not bypass the normal review or merge process.

Build matching native and Python artifacts. Candidate wheels and public release
status require exact source, artifact hashes and installed acceptance evidence.
Publication is owned by the hosted release workflow; local test success does
not authorize a release or deployment.

Research and runtime compatibility products are removed. Historical documentation
and planning live in `archive/`, outside the MkDocs `docs_dir`. Never link archived
material from public docs, regenerate it into docs, or hide it only with navigation
settings: the publication guard checks source, output, search and final site links.

Use conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`). Add shipped
requirements to the owning distribution and repository tooling to root dependency
groups. Update the lockfile and verify it with `uv lock --check` after dependency
changes. Test data must be synthetic and isolated; provider writes and cleanup
require an explicitly authorized target and prefix.
