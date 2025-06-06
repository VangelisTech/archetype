from enum import Enum
from daft import DataFrame
import asyncio
from archetype.core.base import Component
from archetype.core.processor import AsyncProcessor
import daft
import os
from openai import AsyncOpenAI
from vllm import SamplingParams 
from ray.serve.llm import LLMConfig, LLMServer, build_openai_app

from archetype.utils.llm import OpenAIClientFactory


class LLMProvider(str, Enum):
    OPENAI = "OpenAI"
    VLLM = "vLLM"
    GROK = "Grok"
    OPENROUTER = "OpenRouter"

class Prompt(Component):
    provider: LLMProvider
    model_id: str
    system: str
    user: str
    sampling_params: dict = {}



    


class PromptProcessor(AsyncProcessor):
    def __init__(self, prompt: str):
        self.prompt = prompt

    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, prompt: Prompt) -> DataFrame:
        pass

    @daft.udf(
        return_dtype=daft.DataType.struct(
            dict(
                languages=daft.DataType.list(daft.DataType.string()),
                keywords=daft.DataType.list(daft.DataType.string()),
            )
        ),
        num_cpus=1,
        batch_size=1,
        concurrency=1,
    )
    async def prompt_udf(provider: str, model_id: str, system: str, user: str, sampling_params: dict, repo_name: str, readme: str, description: str, max_concurrent_requests=64, max_tokens=256):
        client_factory = OpenAIClientFactory()
        client = await client_factory(provider)
        model = model_id

        async def analyze_single_readme_and_description(client, repo_name, readme, description):
            print(f"Analyzing {repo_name}")
            try:
                format = guess_format(readme)
                if format == "markdown":
                    readme = pypandoc.convert_text(readme, to="plain", format="markdown")
                elif format == "rst":
                    readme = pypandoc.convert_text(readme, to="plain", format="rst")
            except Exception as e:
                readme = readme

            prompt = f"""You are an expert at analyzing GitHub repositories. Your task is to analyze this GitHub repository to:
            1. Determine the programming languages used in the repository.
            2. Generate a list of relevant keywords that describe the repository. 
            The keywords should contain the most important concepts and ideas in the repository, 
            such as the main problem it solves, the main features, the tools and frameworks used, etc.

            Repository details:
            Name: {repo_name}
            Description: {description}
            README: {readme}

            Based on the GitHub repository details above, provide:
            1. A list of the top 2 programming languages used in this repository. Make sure to not produce more than 2 languages.
            2. A list of the top 10 relevant keywords that describe the repository. Make sure to not produce more than 10 keywords. If the keywords are compound words, use a hyphen to join them.
            """

            from tenacity import retry, stop_after_attempt, wait_exponential

            @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60), reraise=True)
            async def fetch_completion(client, model, prompt, max_tokens):
                return await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    response_model=ProjectAnalysis,
                )

            try:
                result = await fetch_completion(client, model, prompt, max_tokens)
                print(f"Analyzed {repo_name} with {model}")
                result_dict = result.model_dump()
                result_dict['languages'] = sorted(result_dict['languages'])
                result_dict['keywords'] = sorted([keyword.replace(" ", "-") for keyword in result_dict['keywords']])
                return result_dict
            except Exception as e:
                print(f"Error analyzing {repo_name}: {e}")
                return None

        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def analyze_with_semaphore(*args):
            async with semaphore:
                return await analyze_single_readme_and_description(*args)

        tasks = [
            analyze_with_semaphore(client, repo_name, readme, description)
            for repo_name, readme, description in zip(
                repo_name,
                readme,
                description,
            )
        ]

        async def run_tasks():
            return await asyncio.gather(*tasks)

        results = asyncio.run(run_tasks())
        return results