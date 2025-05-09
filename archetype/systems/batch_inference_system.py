
import os
from typing import Any, Dict, List, Callable
import daft
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig


def build_vllm_cfg(
    model: str = "microsoft/Phi-4-mini-instruct",
    engine_kwargs: dict = {
        "tensor_parallel_size": 1,         # single GPU
        "max_model_len": 10_000,          # keep full context window
        "paged_kv": True,                  # default in vLLM ≥0.6
        "gpu_memory_utilization": 0.9,     # leave ~10 % for RT
        "max_num_batched_tokens": 4096,
        "enable_chunked_prefill": True,
    },
    batch_size: int  = 32,        # micro-batch; climb to 48 if GPU mem <90 %
    concurrency: int = 1,
    accelerator_type: str = "L4", 
):
    return dict(
        model_source = model,
        engine_kwargs = engine_kwargs,
        runtime_env=dict(
            env_vars=dict(
                HF_TOKEN=os.environ.get("HUGGINGFACE_TOKEN"),
            ),
        ),
        batch_size  = batch_size,        
        concurrency = concurrency,
        accelerator_type=accelerator_type, 
    )


class BaseLLMProcessor(Processor):
    """
    Generic Ray‑Data + vLLM processor.

    Sub‑classes only implement `build_prompt(row)` to return the dict that
    build_llm_processor expects for the *preprocess* stage, and optionally
    `postprocess(row)` if they want something fancier than returning the
    generated text.
    """
    def __init__(
        self,
        model_source: str,
        batch_size: int = 16,
        concurrency: int = 1,
        sampling_params: Dict[str, Any] | None = None,
        engine_kwargs: Dict[str, Any] | None = None,
    ):
        self.model_source = model_source
        self.batch_size = batch_size
        self.concurrency = concurrency
        self._sampling_params = sampling_params or {}
        self._engine_kwargs = engine_kwargs or {}
        self._processor: Callable | None = None

    # ---------- hooks for subclasses ----------
    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Return a preprocess‑compatible dict.  MUST be overridden."""
        raise NotImplementedError

    def postprocess(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Default postprocess – just forward generated_text."""
        return {"resp": row["generated_text"]}

    # ---------- public API ----------
    def process(self, ds: "ray.data.Dataset"):  # noqa: F821 quotes avoid forward ref
        if self._processor is None:
            cfg = vLLMEngineProcessorConfig(
                model_source=self.model_source,
                engine_kwargs=self._engine_kwargs,
                batch_size=self.batch_size,
                concurrency=self.concurrency,
            )
            self._processor = build_llm_processor(
                cfg, preprocess=self.build_prompt, postprocess=self.postprocess
            )
        return self._processor(ds)


# --------------------------------------------------------------------------- #
#  Specialised processors
# --------------------------------------------------------------------------- #

class ChatProcessor(BaseLLMProcessor):
    def __init__(
        self,
        system_prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 256,
        **base_kwargs,
    ):
        self._system_prompt = system_prompt
        self._temperature = temperature
        self._max_tokens = max_tokens
        super().__init__(**base_kwargs)

    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            messages=[
                {"role": "system", "content": self._system_prompt},
                {"role": "user", "content": row["input"]},
            ],
            sampling_params=dict(
                temperature=self._temperature,
                max_tokens=self._max_tokens,
            ),
        )

class ChoiceProcessor(BaseLLMProcessor):
    def __init__(
        self,
        choices: List[str],
        temperature: float = 0.0,
        **base_kwargs,
    ):
        self._choices = choices
        self._temperature = temperature
        super().__init__(**base_kwargs)

    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            prompt=row["input"],
            sampling_params=dict(
                temperature=self._temperature,
                guided_decoding=dict(choice=self._choices),
            ),
        )

class RegexProcessor(BaseLLMProcessor):
    def __init__(
        self,
        pattern: str,
        temperature: float = 0.0,
        **base_kwargs,
    ):
        self._pattern = pattern
        self._temperature = temperature
        super().__init__(**base_kwargs)

    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            prompt=row["input"],
            sampling_params=dict(
                temperature=self._temperature,
                guided_decoding=dict(regex=self._pattern),
            ),
        )

class JSONSchemaProcessor(BaseLLMProcessor):
    def __init__(
        self,
        json_schema: str,
        temperature: float = 0.3,
        **base_kwargs,
    ):
        self._json_schema = json_schema
        self._temperature = temperature
        super().__init__(**base_kwargs)

    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            prompt=row["input"],
            sampling_params=dict(
                temperature=self._temperature,
                guided_decoding=dict(json=self._json_schema),
            ),
        )

class GrammarProcessor(BaseLLMProcessor):
    def __init__(
        self,
        grammar: str,
        temperature: float = 0.3,
        **base_kwargs,
    ):
        self._grammar = grammar
        self._temperature = temperature
        super().__init__(**base_kwargs)

    def build_prompt(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            prompt=row["input"],
            sampling_params=dict(
                temperature=self._temperature,
                guided_decoding=dict(grammar=self._grammar),
            ),
        )

# Optional: Processor registry for lookup by name
PROCESSOR_REGISTRY: Dict[str, type[BaseLLMProcessor]] = {
    "chat": ChatProcessor,
    "choice": ChoiceProcessor,
    "regex": RegexProcessor,
    "json_schema": JSONSchemaProcessor,
    "grammar": GrammarProcessor,
}

class LLMInferenceSystem(System):
    def __init__(self, world, clock_hz=1):
        self.world = world
        self.next_tick = time.time()
        self.period = 1.0 / clock_hz
        # cache { (model, schema_id) : processor } objects
        self._processors = {}

    def _get_processor(self, cfgmodel: str, schema_json: str):
        key = (model, schema_json)
        if key not in self._processors:
            
            self._processors[key] = build_llm_processor(
                cfg,
                preprocess=_make_prompt(schema_json),
                postprocess=lambda row: {"resp": row["generated_text"]},
            )
        return self._processors[key]

    def update(self):
        if time.time() < self.next_tick:
            return
        self.next_tick += self.period                
        # --------- TICK ----------
        agents = self.world.query(NeedsLLMStep, PromptTemplate)
        if not agents:
            return
        # 1. make a Daft / pandas df then ray.data.from_pandas(...)
        batches = group_by_model_and_schema(agents)
        outputs = []
        for (model, schema), df in batches.items():
            ds = ray.data.from_pandas(df)
            ds = self._get_processor(model, schema)(ds).materialize()
            outputs.extend(ds.take_all())
        # 2. write back to components
        for agent, out in zip(agents, outputs):
            agent.add_component(LLMResponse(out["resp"]))
            agent.remove_component(NeedsLLMStep)





