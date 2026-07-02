# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Structured prompt archetype for product-facing example surfaces."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

PRODUCT_SURFACE_OUTPUT_GRAMMAR = r"""
product_surface_output ::= object_start
  surface_name_pair comma
  user_promise_pair comma
  real_api_path_pair comma
  runnable_example_pair comma
  banners_pair comma
  docs_pair comma
  smoke_tests_pair comma
  done_when_pair
object_end

surface_name_pair ::= quote "surface_name" quote colon string
user_promise_pair ::= quote "user_promise" quote colon string
real_api_path_pair ::= quote "real_api_path" quote colon attributed_item
runnable_example_pair ::= quote "runnable_example" quote colon runnable_example
banners_pair ::= quote "banners" quote colon array_start banner (comma banner)* array_end
docs_pair ::= quote "docs" quote colon array_start doc_ref (comma doc_ref)* array_end
smoke_tests_pair ::= quote "smoke_tests" quote colon array_start smoke_test (comma smoke_test)* array_end
done_when_pair ::= quote "done_when" quote colon array_start string (comma string)* array_end

attributed_item ::= object_start
  quote "claim" quote colon string comma
  quote "source_refs" quote colon array_start string (comma string)* array_end
object_end

runnable_example ::= object_start
  quote "path" quote colon string comma
  quote "command" quote colon string comma
  quote "smoke_command" quote colon string comma
  quote "source_refs" quote colon array_start string (comma string)* array_end
object_end

banner ::= object_start
  quote "title" quote colon string comma
  quote "use_case" quote colon string comma
  quote "code_refs" quote colon array_start string (comma string)* array_end comma
  quote "required_env" quote colon array_start string? (comma string)* array_end
object_end

doc_ref ::= object_start
  quote "path" quote colon string comma
  quote "section" quote colon string comma
  quote "source_refs" quote colon array_start string (comma string)* array_end
object_end

smoke_test ::= object_start
  quote "path" quote colon string comma
  quote "assertion" quote colon string comma
  quote "command" quote colon string
object_end

object_start ::= "{"
object_end ::= "}"
array_start ::= "["
array_end ::= "]"
comma ::= ","
colon ::= ":"
quote ::= "\""
string ::= valid_json_string
""".strip()


PRODUCT_SURFACE_JSON_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "surface_name",
        "user_promise",
        "real_api_path",
        "runnable_example",
        "banners",
        "docs",
        "smoke_tests",
        "done_when",
    ],
    "properties": {
        "surface_name": {"type": "string"},
        "user_promise": {"type": "string"},
        "real_api_path": {
            "type": "object",
            "additionalProperties": False,
            "required": ["claim", "source_refs"],
            "properties": {
                "claim": {"type": "string"},
                "source_refs": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string"},
                },
            },
        },
        "runnable_example": {
            "type": "object",
            "additionalProperties": False,
            "required": ["path", "command", "smoke_command", "source_refs"],
            "properties": {
                "path": {"type": "string"},
                "command": {"type": "string"},
                "smoke_command": {"type": "string"},
                "source_refs": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string"},
                },
            },
        },
        "banners": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "use_case", "code_refs", "required_env"],
                "properties": {
                    "title": {"type": "string"},
                    "use_case": {"type": "string"},
                    "code_refs": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"},
                    },
                    "required_env": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "docs": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["path", "section", "source_refs"],
                "properties": {
                    "path": {"type": "string"},
                    "section": {"type": "string"},
                    "source_refs": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "smoke_tests": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["path", "assertion", "command"],
                "properties": {
                    "path": {"type": "string"},
                    "assertion": {"type": "string"},
                    "command": {"type": "string"},
                },
            },
        },
        "done_when": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "string"},
        },
    },
}


PRODUCT_SURFACE_PROMPT = """
You produce a product-surface archetype for an Archetype repo change.

Return only JSON matching the provided schema/grammar. The output must be
mechanical enough for an implementation agent to execute.

Rules:
- The surface must use a real public API path, not a mock or alternate subsystem.
- Every claim must include source_refs pointing to existing code, docs, tests, or
  planned files.
- Every banner must map to actual code that can be written or already exists.
- The runnable example must be safe by default and must include a smoke command.
- Smoke tests must prove the real API path, not merely import constants.
- Keep provider/use-case banners focused on user orientation, not marketing copy.
- If a source is uncertain, mark it in done_when instead of inventing a reference.

The ideal shape is:
real API path -> runnable example -> dedicated banners -> smoke tests -> docs.
""".strip()


@dataclass(frozen=True)
class AttributedItem:
    claim: str
    source_refs: tuple[str, ...]


@dataclass(frozen=True)
class RunnableExample:
    path: str
    command: str
    smoke_command: str
    source_refs: tuple[str, ...]


@dataclass(frozen=True)
class ProductBanner:
    title: str
    use_case: str
    code_refs: tuple[str, ...]
    required_env: tuple[str, ...]


@dataclass(frozen=True)
class ProductDocRef:
    path: str
    section: str
    source_refs: tuple[str, ...]


@dataclass(frozen=True)
class ProductSmokeTest:
    path: str
    assertion: str
    command: str


@dataclass(frozen=True)
class ProductSurfacePlan:
    surface_name: str
    user_promise: str
    real_api_path: AttributedItem
    runnable_example: RunnableExample
    banners: tuple[ProductBanner, ...]
    docs: tuple[ProductDocRef, ...]
    smoke_tests: tuple[ProductSmokeTest, ...]
    done_when: tuple[str, ...]


def _string_tuple(value: object, *, field: str, min_items: int = 0) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise TypeError(f"{field} must be a list of strings")
    items = tuple(str(item) for item in value)
    if len(items) < min_items:
        raise ValueError(f"{field} must contain at least {min_items} item(s)")
    return items


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    return cast("Mapping[str, Any]", value)


def _attributed_item(value: object, *, field: str) -> AttributedItem:
    item = _mapping(value, field=field)
    return AttributedItem(
        claim=str(item.get("claim") or ""),
        source_refs=_string_tuple(
            item.get("source_refs"), field=f"{field}.source_refs", min_items=1
        ),
    )


def product_surface_plan_from_structured_output(
    output: Mapping[str, Any],
) -> ProductSurfacePlan:
    """Parse structured prompt output into a typed product-surface plan."""
    example = _mapping(output.get("runnable_example"), field="runnable_example")
    banners = output.get("banners")
    docs = output.get("docs")
    tests = output.get("smoke_tests")

    if not isinstance(banners, list):
        raise TypeError("banners must be a list")
    if not isinstance(docs, list):
        raise TypeError("docs must be a list")
    if not isinstance(tests, list):
        raise TypeError("smoke_tests must be a list")

    return ProductSurfacePlan(
        surface_name=str(output.get("surface_name") or ""),
        user_promise=str(output.get("user_promise") or ""),
        real_api_path=_attributed_item(output.get("real_api_path"), field="real_api_path"),
        runnable_example=RunnableExample(
            path=str(example.get("path") or ""),
            command=str(example.get("command") or ""),
            smoke_command=str(example.get("smoke_command") or ""),
            source_refs=_string_tuple(
                example.get("source_refs"),
                field="runnable_example.source_refs",
                min_items=1,
            ),
        ),
        banners=tuple(
            ProductBanner(
                title=str(_mapping(banner, field="banner").get("title") or ""),
                use_case=str(_mapping(banner, field="banner").get("use_case") or ""),
                code_refs=_string_tuple(
                    _mapping(banner, field="banner").get("code_refs"),
                    field="banner.code_refs",
                    min_items=1,
                ),
                required_env=_string_tuple(
                    _mapping(banner, field="banner").get("required_env", []),
                    field="banner.required_env",
                ),
            )
            for banner in banners
        ),
        docs=tuple(
            ProductDocRef(
                path=str(_mapping(doc, field="doc").get("path") or ""),
                section=str(_mapping(doc, field="doc").get("section") or ""),
                source_refs=_string_tuple(
                    _mapping(doc, field="doc").get("source_refs"),
                    field="doc.source_refs",
                    min_items=1,
                ),
            )
            for doc in docs
        ),
        smoke_tests=tuple(
            ProductSmokeTest(
                path=str(_mapping(test, field="smoke_test").get("path") or ""),
                assertion=str(_mapping(test, field="smoke_test").get("assertion") or ""),
                command=str(_mapping(test, field="smoke_test").get("command") or ""),
            )
            for test in tests
        ),
        done_when=_string_tuple(output.get("done_when"), field="done_when", min_items=1),
    )


def render_product_surface_checklist(plan: ProductSurfacePlan) -> str:
    """Render a terse implementation checklist for a product-surface plan."""
    lines = [
        f"# {plan.surface_name}",
        "",
        f"Promise: {plan.user_promise}",
        f"API path: {plan.real_api_path.claim}",
        "API refs:",
        *[f"- {ref}" for ref in plan.real_api_path.source_refs],
        "",
        "Runnable example:",
        f"- path: {plan.runnable_example.path}",
        f"- command: {plan.runnable_example.command}",
        f"- smoke: {plan.runnable_example.smoke_command}",
        "",
        "Banners:",
    ]
    for banner in plan.banners:
        env = ", ".join(banner.required_env) if banner.required_env else "none"
        lines.append(f"- {banner.title}: {banner.use_case} (env: {env})")
    lines.extend(["", "Docs:"])
    lines.extend(f"- {doc.path}#{doc.section}" for doc in plan.docs)
    lines.extend(["", "Smoke tests:"])
    lines.extend(f"- {test.command}: {test.assertion}" for test in plan.smoke_tests)
    lines.extend(["", "Done when:"])
    lines.extend(f"- {item}" for item in plan.done_when)
    return "\n".join(lines)
