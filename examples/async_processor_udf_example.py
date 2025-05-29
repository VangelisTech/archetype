"""
Async Processor with Daft UDF Pattern

This demonstrates how processors in the archetype system would use Daft UDFs
similar to the pattern shown in the Daft team's analyze_repo.py example.
"""

import asyncio
from typing import List, Dict, Any, Type
import daft
from daft import col, DataFrame
from pydantic import BaseModel
import instructor
from openai import AsyncOpenAI

# Example Components
class Health(BaseModel):
    current: int
    max: int
    regeneration_rate: float

class Position(BaseModel):
    x: float
    y: float
    z: float

class CombatStats(BaseModel):
    damage: int
    armor: int
    critical_chance: float


# Example UDF-based Processor following the Daft pattern
class HealthRegenerationProcessor:
    """
    Processor that uses Daft UDFs to handle health regeneration.
    Similar to how analyze_repo.py uses UDFs for async processing.
    """
    
    priority: int = 10
    components = (Health,)
    
    @staticmethod
    @daft.udf(
        return_dtype=daft.DataType.struct(
            dict(
                current=daft.DataType.int32(),
                max=daft.DataType.int32(),
                regeneration_rate=daft.DataType.float32(),
            )
        ),
    )
    def regenerate_health_udf(
        health_current: List[int],
        health_max: List[int],
        health_regeneration_rate: List[float],
        dt: float,
        max_concurrent_requests: int = 64,
    ):
        """
        UDF that processes health regeneration for multiple entities concurrently.
        This follows the same pattern as analyze_repo_readme_and_description.
        """
        
        async def regenerate_single_entity(current, max_health, regen_rate):
            # Simulate async operation (could be AI inference, database lookup, etc.)
            await asyncio.sleep(0.001)  # Simulate work
            
            # Calculate new health
            healing = regen_rate * dt
            new_health = min(current + healing, max_health)
            
            return {
                "current": int(new_health),
                "max": max_health,
                "regeneration_rate": regen_rate
            }
        
        # Use semaphore for concurrency control, just like analyze_repo.py
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        async def regenerate_with_semaphore(*args):
            async with semaphore:
                return await regenerate_single_entity(*args)
        
        # Create tasks for all entities
        tasks = [
            regenerate_with_semaphore(current, max_health, regen_rate)
            for current, max_health, regen_rate in zip(
                health_current,
                health_max,
                health_regeneration_rate
            )
        ]
        
        async def run_tasks():
            return await asyncio.gather(*tasks)
        
        # Run all tasks concurrently
        results = asyncio.run(run_tasks())
        return results
    
    async def process_archetype_stream(
        self,
        archetype_name: str,
        df: DataFrame,
        dt: float
    ) -> DataFrame:
        """
        Process method that applies the UDF to update health components.
        """
        # Apply the UDF with proper concurrency control
        health_regenerator = self.regenerate_health_udf.with_concurrency(1)
        
        # Build the transformation expression (lazy evaluation)
        df = df.with_column(
            "health__updated",
            health_regenerator(
                df["health__current"],
                df["health__max"],
                df["health__regeneration_rate"],
                dt
            )
        )
        
        # Unpack the struct into individual columns
        df = df.with_columns({
            "health__current": col("health__updated").struct.get("current"),
            "health__max": col("health__updated").struct.get("max"),
            "health__regeneration_rate": col("health__updated").struct.get("regeneration_rate"),
        }).exclude("health__updated")
        
        return df


class AIDecisionProcessor:
    """
    More complex processor that uses AI to make decisions, 
    following the same pattern as the repo analysis example.
    """
    
    priority: int = 20
    components = (Position, CombatStats, Health)
    
    def __init__(self, openai_api_key: str):
        self.client = instructor.from_openai(
            AsyncOpenAI(api_key=openai_api_key, timeout=60)
        )
        self.model = "gpt-4o-mini"
    
    class CombatDecision(BaseModel):
        action: str  # "attack", "defend", "retreat", "heal"
        target_entity_id: int
        confidence: float
    
    @daft.udf(
        return_dtype=daft.DataType.struct(
            dict(
                action=daft.DataType.string(),
                target_entity_id=daft.DataType.int64(),
                confidence=daft.DataType.float32(),
            )
        ),
    )
    def make_combat_decisions_udf(
        self,
        entity_ids: List[int],
        positions_x: List[float],
        positions_y: List[float],
        health_current: List[int],
        combat_damage: List[int],
        max_concurrent_requests: int = 32,
    ):
        """
        UDF that uses AI to make combat decisions for entities.
        Similar to how analyze_repo uses AI for repository analysis.
        """
        
        async def decide_for_entity(entity_id, pos_x, pos_y, health, damage):
            prompt = f"""
            You are an AI combat strategist. Given the entity's current state:
            - Position: ({pos_x}, {pos_y})
            - Health: {health}
            - Damage: {damage}
            
            Decide the best combat action: attack, defend, retreat, or heal.
            Also specify a target entity ID if applicable.
            """
            
            from tenacity import retry, stop_after_attempt, wait_exponential
            
            @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
            async def fetch_decision():
                return await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=100,
                    response_model=self.CombatDecision,
                )
            
            try:
                decision = await fetch_decision()
                return decision.model_dump()
            except Exception as e:
                # Fallback decision
                return {
                    "action": "defend",
                    "target_entity_id": entity_id,
                    "confidence": 0.0
                }
        
        # Semaphore for API rate limiting
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        async def decide_with_semaphore(*args):
            async with semaphore:
                return await decide_for_entity(*args)
        
        tasks = [
            decide_with_semaphore(eid, px, py, h, d)
            for eid, px, py, h, d in zip(
                entity_ids, positions_x, positions_y, health_current, combat_damage
            )
        ]
        
        async def run_tasks():
            return await asyncio.gather(*tasks)
        
        results = asyncio.run(run_tasks())
        return results
    
    async def process_archetype_stream(
        self,
        archetype_name: str,
        df: DataFrame,
        dt: float
    ) -> DataFrame:
        """
        Apply AI decision making to entities in this archetype.
        """
        # Create the UDF with concurrency control
        decision_maker = self.make_combat_decisions_udf.with_concurrency(1)
        
        # Apply the UDF (lazy evaluation)
        df = df.with_column(
            "combat_decision",
            decision_maker(
                df["entity_id"],
                df["position__x"],
                df["position__y"],
                df["health__current"],
                df["combatstats__damage"]
            )
        )
        
        # Store decision as new columns (could be stored as separate component)
        df = df.with_columns({
            "decision__action": col("combat_decision").struct.get("action"),
            "decision__target": col("combat_decision").struct.get("target_entity_id"),
            "decision__confidence": col("combat_decision").struct.get("confidence"),
        }).exclude("combat_decision")
        
        return df


# Example of batch processing with aggregation
class DamageAggregationProcessor:
    """
    Processor that aggregates damage across entities using Daft's native operations.
    Shows how to combine UDFs with Daft's powerful aggregation capabilities.
    """
    
    priority: int = 30
    components = (CombatStats, Health)
    
    async def process_archetype_stream(
        self,
        archetype_name: str,
        df: DataFrame,
        dt: float
    ) -> DataFrame:
        """
        Aggregate and apply damage based on combat decisions.
        """
        # Example: Calculate total damage per target
        # This uses Daft's native aggregation - no UDF needed
        damage_by_target = (
            df.where(col("decision__action") == "attack")
            .groupby("decision__target")
            .agg(
                col("combatstats__damage").sum().alias("total_incoming_damage")
            )
        )
        
        # Join back to apply damage
        df = df.join(
            damage_by_target,
            left_on="entity_id",
            right_on="decision__target",
            how="left"
        )
        
        # Apply damage (with null handling for entities not targeted)
        df = df.with_column(
            "health__current",
            (col("health__current") - col("total_incoming_damage").fill_null(0)).clip(0, col("health__max"))
        )
        
        return df


"""
Key Patterns from analyze_repo.py Applied to Archetype System:

1. **UDF with Async Processing**:
   - Each processor defines UDFs that handle concurrent operations
   - Semaphores control concurrency just like in analyze_repo.py
   
2. **Structured Return Types**:
   - Use Pydantic models for complex returns
   - Define clear Daft schemas for UDF outputs
   
3. **Error Handling**:
   - Retry logic for external services (AI, databases)
   - Graceful fallbacks for failures
   
4. **Lazy Evaluation**:
   - Build expression trees with df.with_column()
   - Only materialize at the end (during append)
   
5. **Concurrency Control**:
   - with_concurrency(1) to control UDF parallelism
   - Internal semaphores for fine-grained control

This pattern allows processors to:
- Handle complex async operations (AI inference, external APIs)
- Process entities concurrently within safety bounds
- Maintain the lazy evaluation benefits of Daft
- Scale to millions of entities efficiently
""" 