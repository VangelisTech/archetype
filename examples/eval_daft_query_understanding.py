#!/usr/bin/env python3
# /// script
# description = "Evaluate Daft Query Understanding with Structured Outputs + LLM-as-a-Judge"
# requires-python = ">=3.10, <3.13"
# dependencies = ["daft>=0.7.1", "openai", "pydantic", "matplotlib", "numpy"]
# ///
"""
Evaluating Daft Query Understanding at Scale with Structured Outputs
====================================================================

An end-to-end example of **Structured Outputs** with Daft and LLM-as-a-Judge
for training a model to write excellent Daft DataFrame queries.

Inspired by: daft-examples/use_cases/image_understanding_eval/

Pipeline:
    1. Run structured output inference on schema+question prompts
    2. Conduct ablation study (with vs. without schema) to isolate understanding
    3. Classify results into diagnostic quadrants
    4. Use LLM-as-a-Judge to explain failures
    5. Apply RLVR (Reinforcement Learning with Verifiable Rewards)

Quadrant Analysis:
    | Quadrant          | With Schema | Without Schema | Interpretation           |
    |-------------------|-------------|----------------|--------------------------|
    | Both Correct      | ✓           | ✓              | Easy/common pattern      |
    | Schema Helped     | ✓           | ✗              | True schema understanding|
    | Schema Hurt       | ✗           | ✓              | Over-reliance on schema  |
    | Both Incorrect    | ✗           | ✗              | Hard query or limitation |

Run:
    export OPENROUTER_API_KEY=your_key_here
    cd archetype
    uv run python examples/eval_daft_query_understanding.py
"""

import os
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import daft
from daft import DataFrame, DataType, Series, col, lit
from daft.functions import (
    format,
    prompt,
    regexp_extract_all,
    when,
)
from pydantic import BaseModel, Field

# Load environment variables from project root
try:
    from dotenv import load_dotenv

    # Load from project root (archetype/.env)
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)
except ImportError:
    pass


# =============================================================================
# Configuration
# =============================================================================


# Use OpenRouter with responses API (structured outputs)
MODEL_ID = "google/gemini-2.5-flash-lite"  # Cheaper for dataset building
JUDGE_MODEL_ID = "google/gemini-2.5-flash-lite"

LIMIT = 10  # Keep low for interactive demo
# Responses API uses max_output_tokens, not max_tokens
PARAMS = {"temperature": 0.0}
JUDGE_PARAMS = {"temperature": 0.0}


# =============================================================================
# Structured Output Models
# =============================================================================


class DaftQueryResponse(BaseModel):
    """Structured output for Daft query generation."""

    query: str = Field(..., description="The Daft DataFrame query starting with 'df.'")
    confidence: float = Field(default=0.8, description="Confidence in the query (0-1)")


class JudgeResponse(BaseModel):
    """Structured diagnostic feedback from the LLM judge."""

    reasoning: str = Field(..., description="Why did the model generate this query?")
    error_type: str = Field(
        ...,
        description="Type of error: 'syntax', 'wrong_operation', 'wrong_columns', 'logic', 'none'",
    )
    suggestion: str = Field(..., description="How to fix the query")
    difficulty: str = Field(
        default="medium", description="Query difficulty: 'easy', 'medium', 'hard'"
    )


# =============================================================================
# Setup
# =============================================================================


def setup_provider() -> bool:
    """Configure Daft to use OpenRouter with responses API."""
    api_key = os.environ.get("OPENROUTER_API_KEY")

    if not api_key:
        print("⚠️  OPENROUTER_API_KEY not set. Using mock mode.")
        return False

    daft.set_provider("openai", api_key=api_key, base_url="https://openrouter.ai/api/v1")
    print(f"✓ OpenRouter configured (model: {MODEL_ID}) - Using responses API")
    return True


# =============================================================================
# Data Loading
# =============================================================================


def load_training_data(limit: int = LIMIT) -> DataFrame:
    """
    Generate training data for Daft query evaluation.

    Each row contains:
        - schema: Table schema description
        - question: Natural language question
        - expected_query: Ground truth query
        - difficulty: easy/medium/hard
    """
    import random

    # Schema definitions (from our actual saved data)
    schemas = [
        {
            "name": "training_metrics",
            "columns": "step: Int64, loss: Float64, reward: Float64, kl: Float64, accuracy: Float64",
            "description": "RL training metrics over time",
        },
        {
            "name": "reward_results",
            "columns": "name: String, success_rate: Float64, avg_steps: Float64, avg_reward: Float64",
            "description": "Reward function comparison results",
        },
        {
            "name": "simulation",
            "columns": "entity_id: String, tick: Int64, position__x: Float64, position__y: Float64, health: Float64",
            "description": "Entity simulation state",
        },
    ]

    # Question templates with expected queries and difficulty
    templates = [
        # Easy - single operation
        ("What is the average {col}?", 'df.agg(col("{col}").mean().alias("avg_{col}"))', "easy"),
        ("Show all rows.", "df", "easy"),
        ("Count the number of rows.", "df.count_rows()", "easy"),
        # Medium - two operations
        ("Get the top {n} rows by {col}.", 'df.sort(col("{col}"), desc=True).limit({n})', "medium"),
        (
            "Filter to rows where {col} > {threshold}.",
            'df.where(col("{col}") > {threshold})',
            "medium",
        ),
        (
            "Calculate mean and standard deviation of {col}.",
            'df.agg(col("{col}").mean().alias("mean"), col("{col}").stddev().alias("std"))',
            "medium",
        ),
        # Hard - multiple operations
        (
            "Group by {group_col} and compute average {col}, then sort by the average descending.",
            'df.groupby("{group_col}").agg(col("{col}").mean().alias("avg_{col}")).sort(col("avg_{col}"), desc=True)',
            "hard",
        ),
        (
            "Filter to rows where {col} is between {low} and {high}, then select only {col} and {col2}.",
            'df.where((col("{col}") >= {low}) & (col("{col}") <= {high})).select(col("{col}"), col("{col2}"))',
            "hard",
        ),
        ("Get the row with maximum {col}.", 'df.sort(col("{col}"), desc=True).limit(1)', "medium"),
    ]

    examples = []

    for i in range(limit):
        schema = random.choice(schemas)
        template_q, template_a, difficulty = random.choice(templates)

        # Parse columns from schema
        cols = [c.split(":")[0].strip() for c in schema["columns"].split(",")]
        numeric_cols = [c for c in cols if c not in ["name", "entity_id"]]

        col_name = random.choice(numeric_cols) if numeric_cols else cols[0]
        col2 = random.choice([c for c in cols if c != col_name]) if len(cols) > 1 else col_name
        group_col = random.choice([c for c in cols if c != col_name])

        question = template_q.format(
            col=col_name,
            col2=col2,
            group_col=group_col,
            n=random.choice([5, 10]),
            threshold=0.5,
            low=0,
            high=1,
        )

        expected = template_a.format(
            col=col_name,
            col2=col2,
            group_col=group_col,
            n=random.choice([5, 10]),
            threshold=0.5,
            low=0,
            high=1,
        )

        schema_text = f"Table: {schema['name']}\nDescription: {schema['description']}\nColumns: {schema['columns']}"

        examples.append(
            {
                "id": i,
                "schema": schema_text,
                "question": question,
                "expected_query": expected,
                "difficulty": difficulty,
            }
        )

    return daft.from_pylist(examples)


# =============================================================================
# Inference
# =============================================================================


SYSTEM_PROMPT_WITH_SCHEMA = """You are a Daft DataFrame expert. Generate precise, correct Daft queries.

Key patterns:
- Select: df.select(col("a"), col("b"))
- Filter: df.where(col("x") > 0)
- Aggregate: df.agg(col("x").mean().alias("avg"))
- Group: df.groupby("a").agg(col("b").sum())
- Sort: df.sort(col("x"), desc=True)
- Limit: df.limit(10)

Aggregations: mean(), sum(), min(), max(), count(), stddev()

Respond with JSON only: {"query": "df.your_query_here()", "confidence": 0.9}"""


SYSTEM_PROMPT_NO_SCHEMA = """You are a Daft DataFrame expert. Generate a query based on the question alone.
Assume common column names like 'value', 'id', 'name', 'count'.

Respond with JSON only: {"query": "df.your_query_here()", "confidence": 0.9}"""


def run_inference(
    df: DataFrame,
    with_schema: bool = True,
    model: str = MODEL_ID,
) -> DataFrame:
    """
    Run inference on the dataset using chat completions.

    Uses JSON in system prompt for structured output (OpenRouter compatible).

    Args:
        df: DataFrame with schema, question columns
        with_schema: Whether to include schema in prompt
        model: Model to use

    Returns:
        DataFrame with result column containing JSON string
    """
    if with_schema:
        prompt_col = format(
            "Schema:\n{}\n\nQuestion: {}",
            col("schema"),
            col("question"),
        )
        system_prompt = SYSTEM_PROMPT_WITH_SCHEMA
        result_col = "result_with_schema"
    else:
        prompt_col = format("Question: {}", col("question"))
        system_prompt = SYSTEM_PROMPT_NO_SCHEMA
        result_col = "result_no_schema"

    # Structured outputs with Pydantic via responses API
    return df.with_column(
        result_col,
        prompt(
            messages=prompt_col,
            system_message=system_prompt,
            model=model,
            return_format=DaftQueryResponse,
            **PARAMS,
        ),
    )


# =============================================================================
# Evaluation
# =============================================================================


@daft.func.batch(return_dtype=DataType.bool())
def check_syntax(queries: Series) -> Series:
    """Check if queries have valid Python syntax."""
    results = []
    for q in queries.to_pylist():
        try:
            q = str(q).strip()
            q = re.sub(r"```python\n?", "", q)
            q = re.sub(r"```\n?", "", q)
            compile(q, "<string>", "eval")
            results.append(True)
        except Exception:
            results.append(False)
    return Series.from_pylist(results)


@daft.func.batch(return_dtype=DataType.float64())
def compute_similarity(generated: Series, expected: Series) -> Series:
    """Compute similarity between generated and expected queries."""
    col_pattern = r'col\(["\']([^"\']+)["\']\)'
    ops_pattern = r"\.(select|where|groupby|agg|sort|limit|mean|sum|stddev|min|max)\("

    results = []
    for g, e in zip(generated.to_pylist(), expected.to_pylist(), strict=False):
        g, e = str(g), str(e)

        # Column overlap
        gen_cols = set(re.findall(col_pattern, g))
        exp_cols = set(re.findall(col_pattern, e))
        col_score = len(gen_cols & exp_cols) / max(len(exp_cols), 1)

        # Operation overlap
        gen_ops = set(re.findall(ops_pattern, g))
        exp_ops = set(re.findall(ops_pattern, e))
        ops_score = len(gen_ops & exp_ops) / max(len(exp_ops), 1)

        results.append(0.5 * col_score + 0.5 * ops_score)

    return Series.from_pylist(results)


def compute_similarity_native(df: DataFrame) -> DataFrame:
    """
    Compute similarity using native Daft functions.

    Uses regexp_extract_all for column and operation extraction.
    """
    # Extract columns from queries using regexp
    col_pattern = r'col\(["\'][^"\']+["\']\)'
    ops_pattern = r"\.(select|where|groupby|agg|sort|limit)"

    # Extract from generated queries
    df = df.with_column("gen_cols", regexp_extract_all(col("query_with_schema"), col_pattern))
    df = df.with_column("exp_cols", regexp_extract_all(col("expected_query"), col_pattern))

    # Extract operations
    df = df.with_column("gen_ops", regexp_extract_all(col("query_with_schema"), ops_pattern))
    df = df.with_column("exp_ops", regexp_extract_all(col("expected_query"), ops_pattern))

    return df


@daft.func
def extract_query(result: dict) -> str:
    """Extract query from structured output (Pydantic model as dict)."""
    if isinstance(result, dict):
        return str(result.get("query", ""))
    return str(result)


def evaluate_results(df: DataFrame) -> DataFrame:
    """
    Evaluate query correctness.

    Adds columns:
        - is_correct_with_schema
        - is_correct_no_schema
        - similarity_with_schema
        - similarity_no_schema
    """
    # Extract queries from structured output
    df = df.with_column("query_with_schema", extract_query(col("result_with_schema")))
    df = df.with_column("query_no_schema", extract_query(col("result_no_schema")))

    # Check syntax validity
    df = df.with_column("syntax_valid_with", check_syntax(col("query_with_schema")))
    df = df.with_column("syntax_valid_no", check_syntax(col("query_no_schema")))

    # Compute similarity scores
    df = df.with_column(
        "similarity_with", compute_similarity(col("query_with_schema"), col("expected_query"))
    )
    df = df.with_column(
        "similarity_no", compute_similarity(col("query_no_schema"), col("expected_query"))
    )

    # Correctness threshold
    threshold = 0.7
    df = df.with_column(
        "is_correct_with_schema",
        (col("syntax_valid_with") == True) & (col("similarity_with") >= threshold),
    )
    df = df.with_column(
        "is_correct_no_schema",
        (col("syntax_valid_no") == True) & (col("similarity_no") >= threshold),
    )

    return df


# =============================================================================
# Quadrant Classification
# =============================================================================


def classify_quadrants(df: DataFrame) -> DataFrame:
    """
    Classify results into diagnostic quadrants.

    | Quadrant          | With Schema | Without Schema |
    |-------------------|-------------|----------------|
    | Both Correct      | ✓           | ✓              |
    | Schema Helped     | ✓           | ✗              |
    | Schema Hurt       | ✗           | ✓              |
    | Both Incorrect    | ✗           | ✗              |
    """
    return df.with_column(
        "quadrant",
        when(
            (col("is_correct_with_schema") == True) & (col("is_correct_no_schema") == True),
            "Both Correct",
        )
        .when(
            (col("is_correct_with_schema") == True) & (col("is_correct_no_schema") == False),
            "Schema Helped",
        )
        .when(
            (col("is_correct_with_schema") == False) & (col("is_correct_no_schema") == True),
            "Schema Hurt",
        )
        .otherwise("Both Incorrect"),
    )


# =============================================================================
# LLM-as-a-Judge
# =============================================================================


JUDGE_SYSTEM_PROMPT = """You are an expert code reviewer analyzing Daft DataFrame queries.

Inspect the generated query and provide diagnostic feedback:
1. Identify the specific error type (syntax, wrong_operation, wrong_columns, logic, none)
2. Explain why the model made this mistake
3. Suggest how to fix it
4. Assess the query difficulty

Be concise but precise."""


def run_judge(df: DataFrame, model: str = JUDGE_MODEL_ID) -> DataFrame:
    """
    Run LLM-as-a-Judge on failure cases.

    Focuses on:
        - Schema Hurt: correct without schema, incorrect with schema
        - Both Incorrect: incorrect in both conditions
    """
    # Filter to failures
    df_failures = df.where(
        (col("quadrant") == "Schema Hurt") | (col("quadrant") == "Both Incorrect")
    )

    judge_prompt = format(
        """Analyze this Daft query generation failure:

Question: {}
Expected Query: {}
Generated Query (with schema): {}
Generated Query (no schema): {}
Quadrant: {}

Provide diagnostic feedback as JSON: {{"reasoning": "...", "error_type": "syntax|wrong_operation|wrong_columns|logic|none", "suggestion": "..."}}""",
        col("question"),
        col("expected_query"),
        col("query_with_schema"),
        col("query_no_schema"),
        col("quadrant"),
    )

    # Responses API with structured output
    return df_failures.with_column(
        "judge_response",
        prompt(
            messages=judge_prompt,
            system_message=JUDGE_SYSTEM_PROMPT,
            model=model,
            return_format=JudgeResponse,
            **JUDGE_PARAMS,
        ),
    )


# =============================================================================
# Mock Mode (No API)
# =============================================================================


@daft.func
def mock_query_generator(prompt_text: str) -> dict:
    """Mock generator for testing without API."""
    import random

    prompt_lower = prompt_text.lower()

    if "average" in prompt_lower or "mean" in prompt_lower:
        query = 'df.agg(col("value").mean().alias("avg"))'
    elif "top" in prompt_lower:
        query = 'df.sort(col("value"), desc=True).limit(10)'
    elif "filter" in prompt_lower or "where" in prompt_lower:
        query = 'df.where(col("value") > 0.5)'
    elif "group" in prompt_lower:
        query = 'df.groupby("name").agg(col("value").mean())'
    elif "count" in prompt_lower:
        query = "df.count_rows()"
    else:
        query = 'df.select(col("value"))'

    # Add some noise for realistic results
    if random.random() < 0.2:
        query = query.replace("value", random.choice(["x", "y", "data"]))

    return {"query": query, "confidence": random.uniform(0.6, 0.95)}


def run_mock_pipeline(df: DataFrame) -> DataFrame:
    """Run full pipeline in mock mode."""
    # Mock with schema
    df = df.with_column(
        "_prompt_with", format("Schema:\n{}\n\nQuestion: {}", col("schema"), col("question"))
    )
    df = df.with_column("result_with_schema", mock_query_generator(col("_prompt_with")))

    # Mock without schema
    df = df.with_column("_prompt_no", format("Question: {}", col("question")))
    df = df.with_column("result_no_schema", mock_query_generator(col("_prompt_no")))

    # Clean up temp columns
    df = df.exclude("_prompt_with", "_prompt_no")

    return df


# =============================================================================
# Main Pipeline
# =============================================================================


def run_full_pipeline(has_api: bool = True) -> dict:
    """Run the complete evaluation pipeline."""
    results = {}

    # 1. Load data
    print("\n" + "=" * 70)
    print("1. Loading Training Data")
    print("=" * 70)
    df = load_training_data(LIMIT)
    print(f"Loaded {df.count_rows()} examples")
    df.select("question", "difficulty", "expected_query").show(3)

    # 2. Run inference (with and without schema)
    print("\n" + "=" * 70)
    print("2. Running Inference (Ablation Study)")
    print("=" * 70)

    if has_api:
        print("Running with schema...")
        start = time.time()
        df = run_inference(df, with_schema=True)
        print(f"  Done in {time.time() - start:.1f}s")

        print("Running without schema...")
        start = time.time()
        df = run_inference(df, with_schema=False)
        df = df.collect()
        print(f"  Done in {time.time() - start:.1f}s")
    else:
        print("Running in mock mode...")
        df = run_mock_pipeline(df).collect()

    # 3. Evaluate results
    print("\n" + "=" * 70)
    print("3. Evaluating Results")
    print("=" * 70)
    df = evaluate_results(df)

    # Calculate accuracies
    total = df.count_rows()
    acc_with = df.where(col("is_correct_with_schema") == True).count_rows() / total
    acc_no = df.where(col("is_correct_no_schema") == True).count_rows() / total

    print(f"Accuracy (with schema):    {acc_with:.1%}")
    print(f"Accuracy (without schema): {acc_no:.1%}")
    print(f"Delta:                     {acc_with - acc_no:+.1%}")

    results["accuracy_with_schema"] = acc_with
    results["accuracy_without_schema"] = acc_no

    # 4. Classify quadrants
    print("\n" + "=" * 70)
    print("4. Quadrant Classification")
    print("=" * 70)
    df = classify_quadrants(df)

    # Show distribution
    quadrant_counts = (
        df.groupby("quadrant")
        .count()
        .select("quadrant", col("id").alias("count"))
        .with_column("percentage", (col("count") / lit(total) * 100))
        .collect()
    )

    quadrant_counts.show()
    results["quadrant_counts"] = quadrant_counts.to_pylist()

    # 5. Analyze by difficulty
    print("\n" + "=" * 70)
    print("5. Analysis by Difficulty")
    print("=" * 70)

    for difficulty in ["easy", "medium", "hard"]:
        subset = df.where(col("difficulty") == difficulty)
        count = subset.count_rows()
        if count > 0:
            correct = subset.where(col("is_correct_with_schema") == True).count_rows()
            print(f"  {difficulty}: {correct}/{count} correct ({correct / count:.1%})")

    # 6. LLM-as-a-Judge (on failures)
    print("\n" + "=" * 70)
    print("6. LLM-as-a-Judge (Failure Analysis)")
    print("=" * 70)

    failure_count = df.where(
        (col("quadrant") == "Schema Hurt") | (col("quadrant") == "Both Incorrect")
    ).count_rows()

    print(f"Failures to analyze: {failure_count}")

    if has_api and failure_count > 0:
        print("Running judge...")
        start = time.time()
        df_judged = run_judge(df).collect()
        print(f"  Judged {df_judged.count_rows()} failures in {time.time() - start:.1f}s")

        # Show sample judgments (handle string vs struct response)
        try:
            df_judged.select(
                "question",
                "quadrant",
                col("judge_response").get("error_type").alias("error_type"),
                col("judge_response").get("suggestion").alias("suggestion"),
            ).show(3)
        except Exception:
            # Judge returned string instead of struct
            df_judged.select("question", "quadrant", "judge_response").show(3)

        results["judged_count"] = df_judged.count_rows()
    else:
        print("  (Skipped - no API or no failures)")

    # 7. Sample results
    print("\n" + "=" * 70)
    print("7. Sample Results by Quadrant")
    print("=" * 70)

    for quadrant in ["Schema Helped", "Schema Hurt", "Both Correct", "Both Incorrect"]:
        subset = df.where(col("quadrant") == quadrant).limit(2)
        count = subset.count_rows()
        if count > 0:
            print(f"\n{quadrant} ({count} samples):")
            subset.select(
                "question",
                col("query_with_schema").alias("with_schema"),
                col("query_no_schema").alias("no_schema"),
            ).show(2)

    # Include DataFrame for CSV export
    results["df"] = df
    return results


# =============================================================================
# Main
# =============================================================================


def main():
    print("=" * 70)
    print("Evaluating Daft Query Understanding")
    print("with Structured Outputs + LLM-as-a-Judge")
    print("=" * 70)

    has_api = setup_provider()
    results = run_full_pipeline(has_api)

    # Save results
    output_dir = os.path.abspath(".archetype_examples/query_eval")
    os.makedirs(output_dir, exist_ok=True)

    # Save full results to CSV for review
    if "df" in results:
        csv_path = os.path.join(output_dir, "eval_results.csv")
        # Unnest struct columns and select for CSV export
        export_df = results["df"].select(
            "question",
            "schema",
            "expected_query",
            "difficulty",
            "query_with_schema",
            "query_no_schema",
            "is_correct_with_schema",
            "is_correct_no_schema",
            "quadrant",
            # Unnest the raw LLM response structs
            col("result_with_schema").get("confidence").alias("confidence_with_schema"),
            col("result_no_schema").get("confidence").alias("confidence_no_schema"),
        )
        export_df.write_csv(csv_path)
        print(f"\n✓ Full results saved to: {csv_path}")

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Accuracy (with schema):    {results.get('accuracy_with_schema', 0):.1%}")
    print(f"Accuracy (without schema): {results.get('accuracy_without_schema', 0):.1%}")
    print()
    print("Quadrant Distribution:")
    for q in results.get("quadrant_counts", []):
        print(f"  {q['quadrant']}: {q['count']} ({q['percentage']:.1f}%)")

    print()
    print("=" * 70)
    print("Key Insights")
    print("=" * 70)
    print("""
• Schema Helped: Model genuinely uses schema information
• Schema Hurt: Model gets confused by extra context  
• Both Correct: Query pattern is common/memorized
• Both Incorrect: Fundamental limitation

Next Steps:
• Use 'Schema Helped' cases for RLVR positive examples
• Use judge feedback to improve prompts
• Track error_type distribution over training
""")

    print(f"\nResults saved to: {output_dir}")


if __name__ == "__main__":
    main()
