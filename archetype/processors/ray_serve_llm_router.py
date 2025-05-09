from ray import serve
from ray.serve.llm import LLMConfig, LLMServer, LLMRouter
from typing import Union
import os



llm_config1 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars=dict(
            HF_TOKEN=os.environ["HF_TOKEN"],
            HF_HUB_ENABLE_HF_TRANSFER="1"
        )
    ),
)

llm_config2 = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-1.5b",
        model_source="Qwen/Qwen2.5-1.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars=dict(
            HF_TOKEN=os.environ["HF_TOKEN"],
            HF_HUB_ENABLE_HF_TRANSFER="1"
        )
    ),
)

# Deploy the application
deployment1 = LLMServer.as_deployment(llm_config1.get_serve_options(name_prefix="vLLM:")).bind(llm_config1)
deployment2 = LLMServer.as_deployment(llm_config2.get_serve_options(name_prefix="vLLM:")).bind(llm_config2)
llm_app = LLMRouter.as_deployment().bind([deployment1, deployment2])
serve.run(llm_app)

class Messages(Component):
    role: 
    content: 

class ImageURLMessage(Component):
    type: str = "image_url"
    image_url: 


class VLLMInferenceSystem(System):
    def __init__(self, llm_configs: List[LLMConfig]= None):
        if llm_configs is not None: 
            self.deployments = [LLMServer.as_deployment(llm_config.get_serve_options(name_prefix="vLLM:")).bind(llm_config) for llm_config in llm_configs]
            self._bind_deployments_to_router()
        else:
            self.deployments = []
        
    def startup(self):
        serve.run(app)


    def _bind_deployments_to_router(self):
        self.router = LLMRouter.as_deployment().bind(self.deployments)

    def add_llm_config(self, llm_config: LLMConfig):
        this_deployment = LLMServer.as_deployment(llm_config.get_serve_options(name_prefix="vLLM:")).bind(llm_config)
        self.deployments.append(this_deployment)
        


from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Basic chat completion with streaming
response = client.chat.completions.create(
    model="qwen-0.5b",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)