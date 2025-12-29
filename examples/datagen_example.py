# Copyright 2025 Vangelis Technologies Inc.
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

"""
Training Data Generation with daft.functions.prompt
====================================================

Generate synthetic SQL training data using Daft's native LLM integration.

This example demonstrates:
    1. Using daft.functions.prompt for parallel LLM calls
    2. Pydantic structured outputs → Daft structs
    3. Generating question-SQL pairs at scale
    4. Creating reward model preference pairs

Requirements:
    - Set OPENAI_API_KEY environment variable
    - Or configure another provider via daft.set_provider()

Reference: https://docs.getdaft.io/en/stable/custom-code/func/
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import daft
from daft import col
from daft.functions import format, prompt, unnest
from pydantic import BaseModel, Field

# =============================================================================
# Check for API key
# =============================================================================


def check_api_key():
    if not os.environ.get("OPENAI_API_KEY"):
        print("=" * 60)
        print("OPENAI_API_KEY not set - running in mock mode")
        print("=" * 60)
        print()
        print("To run with real LLM:")
        print("  export OPENAI_API_KEY=sk-...")
        print()
        return False
    return True


# =============================================================================
# Pydantic Models for Structured Outputs
# =============================================================================


class SQLExample(BaseModel):
    """Structured output for SQL training example."""

    question: str = Field(description="Natural language question")
    sql: str = Field(description="SQL query answering the question")
    explanation: str = Field(description="How the SQL works")
    difficulty: str = Field(description="easy, medium, or hard")


class PreferencePair(BaseModel):
    """Structured output for reward model training."""

    preferred: str = Field(description="Better SQL query")
    rejected: str = Field(description="Worse SQL query")
    reason: str = Field(description="Why preferred is better")


# =============================================================================
# Mock Mode (no API key)
# =============================================================================


def run_mock_example():
    """Demonstrate the pipeline structure without LLM calls."""
    print("Running mock example (no API calls)")
    print("-" * 50)
    print()

    # Simulated output
    mock_data = [
        {
            "schema": "CREATE TABLE users (id INT, name TEXT, age INT)",
            "topic": "user analytics",
            "question": "How many users are older than 30?",
            "sql": "SELECT COUNT(*) FROM users WHERE age > 30",
            "explanation": "Filters users by age > 30 and counts them",
            "difficulty": "easy",
        },
        {
            "schema": "CREATE TABLE users (id INT, name TEXT, age INT)",
            "topic": "user analytics",
            "question": "What is the average age of all users?",
            "sql": "SELECT AVG(age) FROM users",
            "explanation": "Computes mean of the age column",
            "difficulty": "easy",
        },
        {
            "schema": "CREATE TABLE orders (id INT, user_id INT, amount DECIMAL)",
            "topic": "sales analysis",
            "question": "Which user has spent the most?",
            "sql": "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id ORDER BY total DESC LIMIT 1",
            "explanation": "Groups by user, sums amounts, orders descending, takes top",
            "difficulty": "medium",
        },
    ]

    df = daft.from_pylist(mock_data)

    print("Generated training data (mock):")
    print()
    df.show()

    print()
    print("Schema (would be Pydantic → Daft struct in real run):")
    print(df.schema())

    return df


# =============================================================================
# Real Example with LLM
# =============================================================================


def run_real_example():
    """Generate real training data with LLM."""
    print("Generating training data with LLM")
    print("-" * 50)
    print()

    # Define database schemas
    schemas = [
        """CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP
);""",
        """CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount DECIMAL(10,2),
    status TEXT,
    created_at TIMESTAMP
);""",
    ]

    # Create seeds
    seeds = daft.from_pylist(
        [
            {"schema": schemas[0], "topic": "user management"},
            {"schema": schemas[0], "topic": "user analytics"},
            {"schema": schemas[1], "topic": "order tracking"},
            {"schema": schemas[1], "topic": "sales reporting"},
        ]
    )

    print(f"Created {seeds.count_rows()} seeds")
    print()

    # System prompt for SQL generation
    system_message = """You are an expert SQL instructor creating training data.
Generate a natural language question and its corresponding SQL query.
The question should be clear and practical.
The SQL should be correct, efficient, and follow best practices."""

    # Generate using prompt with structured output
    print("Generating examples with gpt-4o-mini...")
    print("(This will make API calls)")
    print()

    df = seeds.with_column(
        "generated",
        prompt(
            messages=format(
                "Database schema:\n{}\n\nGenerate a SQL question about: {}",
                col("schema"),
                col("topic"),
            ),
            system_message=system_message,
            model="gpt-4o-mini",
            provider="openai",
            return_format=SQLExample,
            temperature=0.7,
        ),
    )

    # Unnest structured output into columns
    df = df.select(
        col("schema"),
        col("topic"),
        unnest(col("generated")),
    )

    print("Generated training data:")
    df.show()

    return df


# =============================================================================
# Main
# =============================================================================


def main():
    print("=" * 70)
    print("Training Data Generation with daft.functions.prompt")
    print("=" * 70)
    print()

    print("Key concept: Use Pydantic models for structured LLM outputs")
    print()
    print("  class SQLExample(BaseModel):")
    print("      question: str")
    print("      sql: str")
    print("      explanation: str")
    print("      difficulty: str")
    print()
    print("  df = df.with_column(")
    print('      "generated",')
    print("      prompt(..., return_format=SQLExample)")
    print("  )")
    print()
    print("This auto-converts to Daft struct type!")
    print()

    has_key = check_api_key()

    if has_key:
        try:
            df = run_real_example()
        except Exception as e:
            print(f"LLM call failed: {e}")
            print("Falling back to mock example")
            df = run_mock_example()
    else:
        df = run_mock_example()

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print()
    print("The generated DataFrame can be used for:")
    print()
    print("  1. Fine-tuning LLMs on SQL generation")
    print("  2. Training reward models (add preference pairs)")
    print("  3. GRPO training with group-relative advantages")
    print()
    print("Pipeline:")
    print("  Seeds → prompt() → Pydantic struct → DataFrame → Training")
    print()
    print("See: https://docs.getdaft.io/en/stable/custom-code/func/")
    print()


if __name__ == "__main__":
    main()
