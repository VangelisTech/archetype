# API stability and docstrings

Archetype separates compatibility from prominence. A symbol can be supported
without being the first interface shown to a new user.

## API tiers

| Tier | Contract | Documentation |
| --- | --- | --- |
| Recommended | Default application interface | Complete reference and workflow examples |
| Extension | Supported customization interface | Complete semantics and focused examples |
| Integration | Supported host and service interface | Advanced reference without tutorial repetition |
| Compatibility | Stable, frozen, or deprecated interface | Terse reference with migration direction |
| Internal | No compatibility promise | Maintainer context in source only |

The recommended interface is `ArchetypeRuntime` and its world handles. Components,
processors, resources, and their configuration form the extension interface. The
service layer is for hosts that need explicit authorization or custom wiring. The
synchronous educational engine remains a compatibility interface.

## What counts as public

A name is public when it is exported from `archetype`. Types that appear in the
arguments or return values of those names are public dependencies even when they
live in a submodule. Exporting a name from a lower-level package does not promote
it to the recommended interface.

Names beginning with an underscore are internal. Modules explicitly labeled
experimental may change without the compatibility guarantees of the main API.

Public exports are additive within a release line. Removing or changing their
meaning requires a versioned migration. Every export change must update the Python
reference manifest; the docs build rejects missing or stale entries.

## Docstring standard

Public docstrings use Google style and begin with one direct summary sentence.
Additional prose should explain only behavior that the signature cannot:

- lifecycle and ownership;
- persistence or mutation semantics;
- concurrency guarantees;
- intentional exceptions;
- surprising defaults or side effects.

Use `Args`, `Returns`, and `Raises` when their semantics are not obvious from names
and annotations. Do not repeat types or defaults already present in the signature.
Examples belong on recommended entry points and non-obvious workflows, not on every
method. Prefer a guide when an example spans several calls.

Public docstrings must not contain issue numbers, implementation shorthand,
development TODOs, or references to private services. Put that context in code
comments, specifications, or development guides.

Internal docstrings describe invariants and rationale for maintainers. They do not
need user-facing examples or exhaustive argument sections.
