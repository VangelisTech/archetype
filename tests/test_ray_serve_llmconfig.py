import ray
from ray import serve
from ray.serve.llm import LLMRouter, LLMConfig, LLMServer
from openai import AsyncOpenAI
import os
import pytest 
import daft 
import wandb

config_file = "./model_config.yaml"
HF_TOKEN = os.environ["HF_TOKEN"]
WANDB_API_KEY = os.environ["WANDB_API_KEY"]


# Build and deploy the app
app = build_openai_app({"llm_configs": [config_file]})
serve.run(app)

# Instantiate OAI Client
client = AsyncOpenAI(base_url="http://localhost:800/v1", api_key="no-key")




# Draft Client Inference Calls for each model 
async def test_chat():
    response = client.chat.completions.create(
        model="qwen-0.5b",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that outputs JSON."
            },
            {
                "role": "user",
                "content": "List three colors in JSON format"
            }
        ],
        stream=True,
    )
client.chat.completions.create
async def chat_image():
    response = client.chat.completions.create(
        model="qwen-0.5b",
        response_format={"type": "image_url"},
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that outputs an image URL."
            },
            {
                "role": "user",
