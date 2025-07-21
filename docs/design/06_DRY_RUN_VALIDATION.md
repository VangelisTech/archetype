# 06: Dry Run Validation for Integrity

This document proposes an optional, parallel validation phase in the tick lifecycle, designed to ensure data integrity, prevent corruption, and aid in debugging complex, high-stakes simulations.

## 1. Core Principles

-   **Trust, but Verify**: While the system is designed for correctness, a dedicated validation step provides an explicit guarantee that the state produced by the system logic is valid *before* it is committed to the durable store.
-   **Fail Fast**: Detect data corruption, schema mismatches, or broken business logic invariants at the earliest possible moment—within the tick they occur, not at some later point when the consequences are harder to trace.
-   **Separation of Concerns**: The core simulation logic (Processors) should focus on the "happy path." The validation logic is handled separately by the `Updater`, which is the ultimate guardian of state.
-   **Configurable Overhead**: This feature must be optional and disabled by default, as it introduces computational overhead. It should be selectively enabled for testing, CI, or in production for the most critical archetypes.

## 2. The `validate()` Method

To implement this, we will extend the `iAsyncUpdateManager` protocol with a new, optional `validate` method.

```python
# In src/archetype/core/aio/async_interfaces.py

class iAsyncUpdateManager(Protocol):
    # ... existing methods: update, materialize_spawns, etc. ...

    async def validate(self, df: DataFrame, sig: ArchetypeSignature) -> None:
        """
        Performs a series of fast, in-memory checks on a DataFrame before it is committed.
        Raises a ValidationError if any check fails. This method should NOT perform I/O.
        """
        ...
```

The default implementation on `AsyncUpdateManager` would simply be `pass`, but it can be overridden with specific checks.

We can also trigger a fully validated dataframe by leveraging the underlying features of pydantic. This is achieved by converting the dataframe back to a list of pydicts using `df.to_pylist()` by breaking the df columns of the archetype back into components and running a loop over `component.model_validate(x) for x in pylist` within a try except block. We COULD use a list comprehension but lost the debugging. It would be cool to see different sampling techniques used here with an optional argument for sample size.

Here We would also print the df.explain() with options for the log or mermaid chart. The introspection into the query plan should be able to be compared against the processor priority system for any given archetype and juxtapose the visualization against a track strace flame graph. 

See [visualization](./07_VISUALIZATION_ENTRY_POINTS.md) for more details on the `df.explain()` . 

#### Types of Validations

The `validate` method can perform a hierarchy of checks:

1.  **Schema Validation**: Does the resulting DataFrame's schema exactly match the canonical schema for its archetype signature? This catches bugs where a processor incorrectly adds, removes, or renames a column.
2.  **Constraint Validation**: Are there any `null` values in columns that should never be null? Are all `entity_id`s unique within the DataFrame?
3.  **Business Logic Invariants**: This is the most powerful aspect for financial simulations. The validator can check for domain-specific rules that must always hold true.
    *   *Example 1 (Trading):* In an 'Orderbook' archetype, does `ask_price` always exceed `bid_price`?
    *   *Example 2 (Ledger):* In a 'Treasury' archetype, does the sum of all account balances equal the expected total?
    *   *Example 3 (Risk):* Is any single 'Portfolio' archetype's `exposure` value greater than the global limit?

## 3. Implementation in `AsyncWorld`

The "Dry Run" is integrated as a new, optional phase (Phase 2.5) in the `AsyncWorld.step` method, directly after the parallel execution and before the final commit.

```python
# Inside AsyncWorld.step()

# ... after Phase 2: Parallel Execution ...
results: List[Tuple[DataFrame, ArchetypeSignature]] = await asyncio.gather(*tasks, return_exceptions=True)

# PHASE 2.5: OPTIONAL DRY RUN VALIDATION
if self.config.get("enable_validation", False):
    validation_tasks = []
    for res in results:
        if not isinstance(res, Exception):
            df, sig = res
            validation_tasks.append(self.updater.validate(df, sig))
    
    validation_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
    for val_res in validation_results:
        if isinstance(val_res, Exception):
            # Log or raise a critical error, as this indicates a validation failure.
            logger.critical("Data validation failed!", exc_info=val_res)
            # Depending on policy, you might halt the simulation here.

# PHASE 3: ATOMIC COMMIT
# ... proceeds as normal ...
```



## 5. CI/CD Integration and Staging

As noted by o3, this feature is perfectly suited for a Continuous Integration pipeline.

-   **CI Pipeline Job**: A dedicated job in the GitHub Actions workflow can run the entire test suite with the `enable_validation` flag set to `True`.
-   **Pre-Merge Check**: This ensures that no pull request can be merged if it introduces a change that violates data integrity, even if the standard unit tests pass. It acts as a powerful safety net against subtle, emergent bugs.
-   **Staging Environment**: The validation can be permanently enabled in a staging environment that mirrors production, providing a final line of defense before deployment.

## 6. Near-Term Implementation Plan

1.  **Extend Interface**: Add the optional `validate` method to the `iAsyncUpdateManager` protocol.
2.  **Implement Basic Validator**: Add a default `validate` implementation to `AsyncUpdateManager` that performs a basic schema check.
3.  **Feature Flag `AsyncWorld`**: Add the `if self.config.get("enable_validation", False):` block to the `AsyncWorld.step` method. The world's `__init__` will need to accept a `config: Dict` object.
4.  **Update CI Workflow**: Add a new test job to a workflow file (e.g., `.github/workflows/main.yml`) that sets an environment variable or test configuration to enable the validation flag during the test run.
