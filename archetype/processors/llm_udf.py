#  llm_udf.py
from typing import Tuple
from daft import col
from daft.functions import llm_generate
from .processors import processor
from core.base import Component
import pandera
# ───────── Components ─────────
class Prompt(Component):
    system: pa
    sampling_params: dict

class Completion(Component):
    text: str



# Define Request / Response Schemas for each type of inference call (dict, vs json, vs feather)
class SchemaFeather(InSchema):
    class Config:
        from_format = "feather" # Expects new data to be in feather format
        to_format = "feather"   # Returns data in feather format
        to_format_kwargs = {"orient": "records"}

class SchemaDict(OutSchema):
    class Config:
        from_format = "dict"
        to_format = "dict"
        to_format_kwargs = {"orient": "records"}

class SchemaJson(OutSchema):
    class Config:
        from_format = "json"
        to_format = "json"
        to_format_kwargs = {"orient": "records"}



def get_interchange_schema(format: str, task: str) -> pandera.DataFrameSchema:
    if format == "dict":
        return SchemaDict
    elif format == "json":
        return SchemaJson
    elif format == "feather":
        return SchemaFeather
    else:
        raise ValueError(f"Invalid format: {format}")
    

# ───────── Processor ─────────
@processor(Prompt)
class LLMGenerateProcessor:
    """Batch‑runs llm_generate over every Prompt component that has
    no matching Completion yet, then writes the result back."""
    def preprocess(self, ) -> DataFrame:
        self,

        model: str = "nvidia/Llama-3.1-Nemotron-8B-UltraLong-4M-Instruct",
        tp: int = 1,
        batch: int = 32,
        concur: int = 4,
        **gen_cfg,
    ):
        super().__init__(world)
        self.model   = model
        self.tp      = tp
        self.batch   = batch
        self.concur  = concur
        self.gen_cfg = gen_cfg

    def process(self, dt, df):
        # run llm_generate UDF – Daft handles batching & concurrency
        df2 = df.with_column(
            "response",
            llm_generate(
                col("prompt__text"),
                model=self.model,
                concurrency=self.concur,
                batch_size=self.batch,
                tensor_parallel_size=self.tp,  # forwarded into vLLM engine
                **self.gen_cfg,
            ),
        )

        # massage into Completion component structure
        updates = df2.select(
            col("entity_id"),
            col("completion").alias("completion__text"),
        )

        return updates
