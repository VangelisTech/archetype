
class LLMInferenceSystem(System):
    def __init__(self, world, clock_hz=1):
        self.world = world
        self.next_tick = time.time()
        self.period = 1.0 / clock_hz
        # cache { (model, schema_id) : processor } objects
        self._processors = {}

    def _get_processor(self, model: str, schema_json: str, batch_size, concurrency, engine_kwargs, ):
        key = (model, schema_json)
        if key not in self._processors:
            cfg = vLLMEngineProcessorConfig(model_source=model,
                                            batch_size=16,
                                            concurrency=1,
                                            engine_kwargs={
                                                "guided_decoding_backend": "xgrammar",
                                                "dtype": "half",
                                                "max_model_len": 1024,
                                            })
            self._processors[key] = build_llm_processor(
                cfg,
                preprocess=_make_prompt(schema_json),
                postprocess=lambda row: {"resp": row["generated_text"]},
            )
        return self._processors[key]

    def execute(self):
        if time.time() < self.next_tick:
            return
        self.next_tick += self.period                # --------- TICK ----------
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
