import asyncio
import ulid
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple, AsyncGenerator, Type

import daft
from daft import col, lit, DataFrame
import instructor
# Use aiohttp for async HTTP requests
import aiohttp 
from openai import AsyncOpenAI # Still useful for type hints if client mimics it
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

from archetype.core.interfaces import iAsyncQuerier
from .components import (
    NodeID, GraphNodeComponent, GraphEdgeComponent, 
    KnowledgeGraphComponent, SpacyDocComponent,
    LLMAgentState, MessageComponent, AgentMailboxComponent, ChatHistoryComponent
)
from .async_processor import AsyncGraphProcessor, async_graph_processor
from archetype.utils.async_utils import run_async_tasks_with_semaphore

# --- Pydantic Models for LLM Interactions ---
class LLMThought(BaseModel):
    reasoning: str
    confidence: float
    next_action: str
    action_parameters: Optional[Dict[str, Any]] = None

class LLMResponse(BaseModel):
    response_text: str
    structured_data: Optional[Dict[str, Any]] = None # For tool use or structured output
    # Reference to a SpacyDocComponent if text is processed
    processed_text_id: Optional[str] = None 

# --- Real Ray Serve Client using aiohttp ---
class RayServeLLMClient:
    def __init__(self, endpoint_url: str, api_key: str = "fake-key"):
        self.endpoint_url = endpoint_url.rstrip('/') # Ensure no trailing slash
        self.api_key = api_key # OpenAI compatible services often expect a bearer token
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # You might want to configure connector limits, timeouts, etc.
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.api_key}"}
            )
        return self._session

    async def generate(
        self, 
        model: str, 
        messages: List[Dict[str,str]], 
        response_model: Type[BaseModel], 
        max_tokens: int, 
        temperature: float
    ) -> BaseModel:
        session = await self._get_session()
        target_url = f"{self.endpoint_url}/chat/completions" # Assuming /v1 is part of endpoint_url
        
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            # For structured output with Pydantic via vLLM/OpenAI API:
            "response_format": {"type": "json_object", "schema": response_model.model_json_schema()}
        }
        
        print(f"[RayServeLLMClient] Requesting: {target_url} with model {model}, response_model {response_model.__name__}")
        
        try:
            async with session.post(target_url, json=payload) as response:
                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
                response_json = await response.json()
                
                # Extract the content that should match the Pydantic model
                # OpenAI API typically puts this in response['choices'][0]['message']['content']
                # or response['choices'][0]['message']['tool_calls'][0]['function']['arguments'] for tool use
                # For direct JSON object mode with schema, it might be directly in 'content' of the message.
                
                # Adjust based on actual Ray Serve vLLM OpenAI-compatible output structure for JSON mode.
                # Often, the structured JSON is a string within the 'content' field.
                message_content_str = response_json['choices'][0]['message']['content']
                
                try:
                    # The content itself should be the JSON string matching the schema
                    parsed_data = response_model.model_validate_json(message_content_str)
                    return parsed_data
                except ValidationError as ve:
                    print(f"[RayServeLLMClient] Pydantic validation error: {ve}")
                    print(f"[RayServeLLMClient] Received content string: {message_content_str}")
                    raise # Re-raise the validation error to be handled by tenacity or caller
                except Exception as e_parse: # Catch other parsing errors
                    print(f"[RayServeLLMClient] Error parsing LLM JSON response: {e_parse}")
                    print(f"[RayServeLLMClient] Received content string: {message_content_str}")
                    raise

        except aiohttp.ClientError as e_http:
            print(f"[RayServeLLMClient] HTTP error: {e_http}")
            raise
        except Exception as e_general: # Catch-all for other unexpected errors
            print(f"[RayServeLLMClient] General error in generate: {e_general}")
            raise

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        print("[RayServeLLMClient] Session closed.")

# --- LLM Agent Processor ---

# Static method for core thinking logic, to be used with the async helper
# This can be outside the class or static, as it doesn't rely on `self` directly for client anymore.
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30))
async def _think_for_single_agent_static(
    agent_id: NodeID, current_mode: str, persona: str, messages_json: Optional[List[Dict[str, Any]]], 
    model_name: str, temp: float, max_tok: int, 
    llm_client_endpoint: str, 
    LLMThoughtModel: Type[LLMThought]
) -> Dict[str, Any]:
    
    llm_client = RayServeLLMClient(llm_client_endpoint)
    fallback_thought = LLMThoughtModel(reasoning="Fallback due to error", confidence=0.0, next_action="error_handle_think").model_dump()
    try:
        prompt_messages = [{"role": "system", "content": persona}]
        if messages_json:
            for msg_dict in messages_json:
                # Ensure content is a string
                content_str = str(msg_dict.get('content', '')) 
                role = "user" if msg_dict.get('sender_id') != agent_id else "assistant"
                prompt_messages.append({"role": role, "content": content_str})
        prompt_messages.append({"role": "user", "content": f"Current mode: {current_mode}. Based on your persona and recent messages (if any), what is your next reasoned thought, your confidence in it, and the immediate next action you propose? Respond in the required JSON format."})
        
        thought: LLMThought = await llm_client.generate(
            model=model_name, 
            messages=prompt_messages, 
            response_model=LLMThoughtModel, 
            max_tokens=max_tok, 
            temperature=temp
        )
        return thought.model_dump()
    except RetryError as re:
        print(f"LLM error for agent {agent_id} after retries: {re}")
        return fallback_thought
    except Exception as e:
        print(f"LLM error for agent {agent_id} in _think_for_single_agent_static: {e}")
        return fallback_thought # Return a valid Pydantic model dict on error
    finally:
        await llm_client.close()

@async_graph_processor(
    components=(LLMAgentState, GraphNodeComponent, AgentMailboxComponent), 
    priority=50
)
class LLMAgentProcessor(AsyncGraphProcessor):
    def __init__(self, ray_serve_endpoint_url: str):
        self.ray_serve_endpoint_url = ray_serve_endpoint_url

    # --- Core UDF for Agent Thinking --- 
    # This UDF would be called by `process_stream`
    # It encapsulates the LLM call and structured response parsing.
    @daft.udf(
        return_dtype=daft.DataType.struct(LLMThought.model_json_schema()["properties"])
    )
    def think_udf(
        self, 
        agent_ids: daft.Series,
        current_modes: daft.Series,
        personas: daft.Series,
        mailbox_contents_json: daft.Series, 
        model_names: daft.Series,
        temperatures: daft.Series,
        max_tokens_list: daft.Series,
        max_concurrent_llm_calls: int = 5 # Reduced default for potentially slower real calls
    ):
        items_data_iterable = zip(
            agent_ids.to_pylist(),
            current_modes.to_pylist(),
            personas.to_pylist(),
            mailbox_contents_json.to_pylist(),
            model_names.to_pylist(),
            temperatures.to_pylist(),
            max_tokens_list.to_pylist()
        )

        shared_args = (self.ray_serve_endpoint_url, LLMThought)
        
        udf_results_with_exceptions = asyncio.run(
            run_async_tasks_with_semaphore(
                _think_for_single_agent_static, 
                items_data_iterable,
                max_concurrent_llm_calls,
                *shared_args
            )
        )
        
        processed_results = []
        for res in udf_results_with_exceptions:
            if isinstance(res, Exception):
                print(f"Error in UDF gather for LLM agent: {res}") 
                processed_results.append(LLMThought(reasoning=f"UDF Gather Error: {res}", confidence=0.0, next_action="udf_error").model_dump())
            elif not isinstance(res, dict): # Ensure it's a dict, as expected by Daft struct
                print(f"Unexpected result type in UDF gather: {type(res)}, {res}")
                processed_results.append(LLMThought(reasoning=f"UDF Type Error", confidence=0.0, next_action="udf_type_error").model_dump())
            else:
                processed_results.append(res)
        return processed_results

    async def process_stream(self, archetype_name: str, agent_df: DataFrame, dt: float) -> DataFrame:
        print(f"LLMAgentProcessor processing: {archetype_name} for step {agent_df.get_column('step').to_pylist()[0] if len(agent_df) > 0 else 'N/A'}")
        if len(agent_df) == 0:
            return agent_df
        
        agent_df = agent_df.with_column(
            "mailbox_json_for_udf", 
            col("AgentMailboxComponent__incoming_messages").apply(
                lambda msg_list: [msg.model_dump() for msg in msg_list] if msg_list else [], 
                return_dtype=daft.DataType.python()
            )
        )

        agent_think_udf_func = self.think_udf

        agent_df = agent_df.with_column(
            "llm_thought_struct",
            agent_think_udf_func(
                agent_df["GraphNodeComponent__node_id"],
                agent_df["LLMAgentState__current_mode"],
                agent_df["LLMAgentState__persona_description"],
                agent_df["mailbox_json_for_udf"],
                agent_df["LLMAgentState__model_name"],
                agent_df["LLMAgentState__temperature"],
                agent_df["LLMAgentState__max_tokens"],
                # Pass max_concurrent_llm_calls to the UDF if it's a parameter of the UDF itself
                # otherwise, it's handled by run_async_tasks_with_semaphore internal default or what's passed to it.
                # For now, assuming the UDF definition above takes it as a direct parameter.
            )
        ).drop("mailbox_json_for_udf")

        agent_df = agent_df.with_columns({
            "llmagentstate__next_action": col("llm_thought_struct").struct.get("next_action"),
            "llmagentstate__action_parameters": col("llm_thought_struct").struct.get("action_parameters"),
            "llmagentstate__reasoning": col("llm_thought_struct").struct.get("reasoning"),
            "llmagentstate__confidence": col("llm_thought_struct").struct.get("confidence")
        }).drop("llm_thought_struct")
        
        return agent_df

    async def shutdown(self):
        await super().shutdown()
        print("LLMAgentProcessor shutdown complete.") 