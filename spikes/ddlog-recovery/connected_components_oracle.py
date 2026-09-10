"""Independent BFS oracle for exact minimum-label connected components.

The vertex universe is explicit vertices plus every edge endpoint. Edges are
undirected for connectivity, while input facts retain directed-tuple set
semantics: inserting an exact duplicate is idempotent, and deleting (u, v)
leaves (v, u) as independent support. Self loops introduce a singleton vertex.

No production/compiler imports or Large-Star/Small-Star steps are used here.
`trace()` is a transport-free acceptance skeleton: each entry supplies one
MCP-shaped mutation batch and the exact expected labels after that batch.
"""
from collections import deque
import json
import re


ARITIES = {'vertices': 1, 'edges': 2}


def _row(predicate, values):
    if predicate not in ARITIES:
        raise ValueError(f'Unknown input predicate: {predicate}')
    try:
        row = tuple(values)
    except TypeError as error:
        raise ValueError(f'{predicate} requires {ARITIES[predicate]} integer fields') from error
    if len(row) != ARITIES[predicate] or any(type(value) is not int for value in row):
        raise ValueError(f'{predicate} requires {ARITIES[predicate]} integer fields')
    return row


def _copy(facts):
    if set(facts) != set(ARITIES):
        raise ValueError('Expected exactly vertices and edges input collections')
    return {name: {_row(name, value) for value in facts[name]} for name in ARITIES}


def expected(facts):
    """Return the exact set of (vertex, minimum component vertex) labels."""
    inputs = _copy(facts)
    adjacent = {vertex: set() for vertex, in inputs['vertices']}
    for left, right in inputs['edges']:
        adjacent.setdefault(left, set()).add(right)
        adjacent.setdefault(right, set()).add(left)
    visited, labels = set(), set()
    for start in sorted(adjacent):
        if start in visited:
            continue
        component = {start}
        visited.add(start)
        frontier = deque([start])
        while frontier:
            vertex = frontier.popleft()
            for neighbor in adjacent[vertex]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    component.add(neighbor)
                    frontier.append(neighbor)
        representative = min(component)
        labels.update((vertex, representative) for vertex in component)
    return labels


def apply(facts, changes):
    """Apply an ordered insert/delete batch to a copy of finite-set inputs."""
    staged = _copy(facts)
    for change in changes:
        if set(change) != {'op', 'predicate', 'values'}:
            raise ValueError('Each change requires exactly op, predicate and values')
        predicate, operation = change['predicate'], change['op']
        row = _row(predicate, change['values'])
        if operation == 'insert':
            staged[predicate].add(row)
        elif operation == 'delete':
            staged[predicate].discard(row)
        else:
            raise ValueError(f'Unknown input operation: {operation}')
    return staged


def rows(text, predicate='labels'):
    """Parse exact two-column DDlog output, rejecting malformed/duplicate rows."""
    pattern = re.compile(r'R_' + re.escape(predicate) + r'\{\.f0 = (-?\d+), \.f1 = (-?\d+)\}')
    result = set()
    for line in text.splitlines():
        match = pattern.fullmatch(line)
        if match is None:
            raise ValueError(f'Malformed {predicate} row: {line}')
        row = tuple(map(int, match.groups()))
        if row in result:
            raise ValueError(f'Duplicate {predicate} row: {line}')
        result.add(row)
    return result

