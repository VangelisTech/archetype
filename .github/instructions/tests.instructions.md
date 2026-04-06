---
applyTo: "tests/**"
---

# Test Review Guidelines

- Framework: `pytest` + `pytest-asyncio` with function-scoped async fixtures.
- Use `tmp_path` fixture for storage isolation — never write to shared paths.
- Integration tests go in `tests/integration/`, unit tests alongside their module.
- Every new feature or processor needs tests.
- Minimum 60% coverage threshold on `src/archetype/`.
- Test commands flow through `ServiceContainer` the same way production does — don't mock the broker or skip RBAC in integration tests.
