---
name: design-coherence
description: "Review a change for repository-native simplicity, pattern consistency, and concrete design without turning taste into a merge blocker."
user_invocable: true
---

# Design Coherence Review

Review whether a change is the simplest repository-native implementation of
its concrete requirement.

This is not the footgun detector and not a linter. Every finding is advisory
design evidence for a human. A reproducible defect belongs in the footgun
review; formatting, naming taste, and generic “clean code” advice are not
findings.

## Evidence contract

Every finding must:

1. Anchor to a changed line.
2. Cite at least one comparable repository path and symbol.
3. Describe the shape introduced by the change.
4. Name a concrete comprehension, ownership, testing, or change-amplification
   cost.
5. Offer a simpler alternative that preserves behavior.

“I would write this differently” is invalid. If the repository has no
comparable pattern, the cost is speculative, or the alternative changes
behavior, do not report the finding. Record behavior-changing uncertainty in
the human design brief instead. The structured validator confirms that every
cited protected-base path exists and contains the named symbol.

For Daft-specific code, read `.claude/skills/daft-antipatterns/SKILL.md` and do
not reclassify its established wrong-shape categories as generic cleanliness
findings.

## Design categories

#### Mixed responsibility

One changed unit owns multiple concerns that the repository keeps in distinct
components, processors, handlers, services, or composition code. Cite the
nearby separation pattern and show the concrete change-amplification cost.

#### Obscured control flow

Indirection, callbacks, branching, or implicit mutation hides the actual
execution sequence compared with a concrete local pattern. Complexity alone is
not evidence; name the state transition or ownership decision that becomes
harder to follow.

#### Dead or ceremonial scaffolding

New wrappers, fields, hooks, parameters, or layers exist only to satisfy an
anticipated shape and have no current behavioral role. Do not duplicate the
footgun detector's `dead-code-contracts`: this category concerns advisory
design ceremony, not a false runtime promise.

#### Local pattern divergence

The change solves the same local problem differently from an established
repository pattern without a requirement that explains the deviation. Cite
the comparable implementation and the maintenance cost of carrying both
shapes.

#### Duplicated mechanism

The change introduces a second implementation of an existing mechanism rather
than extending or reusing its canonical owner. Identify both mechanisms and
the future synchronization burden.

#### Parallel source of truth

The change adds another non-authoritative representation of the same policy,
registry, mapping, lifecycle state, or configuration. A concrete authority
violation is a footgun; this category covers advisory design drift before
behavior has demonstrably diverged.

#### Single use abstraction

An interface, protocol, factory, adapter, generic, or wrapper has one
implementation and one consumer, while a direct concrete dependency would
preserve the same supported boundary.

#### Premature generalization

The change models hypothetical variation not required by current behavior,
accepted specifications, or a second concrete use. Identify the speculative
dimension and the simpler current type or function.

#### Extension point without variation

A registry, plugin hook, strategy seam, callback surface, or configurable
dispatch point is introduced without multiple behaviors that need selection.
Show that a direct call or explicit branch preserves the current requirement.

#### Indirection without policy

A layer forwards values or calls without enforcing ownership, lifecycle,
authorization, transformation, retry, or another named policy. Cite the
repository's concrete boundary and explain why the forwarding layer obscures
rather than owns it.

## Result policy

- Findings are advisory-only and appear in the human design brief.
- Corroboration raises confidence; it never promotes design taste into a hard
  blocker.
- A justified deviation is not a finding.
- An empty result is correct when the change follows repository-native
  patterns or its additional shape is concretely justified.
