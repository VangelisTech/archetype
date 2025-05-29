import argparse
import os
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

DEFAULT_MODEL_ID = "Qwen/Qwen2-0.5B-Instruct" # A small, fast model for testing
DEFAULT_ACCELERATOR_TYPE = "CPU" # Default to CPU for broader compatibility, change if GPUs available

def main():
    parser = argparse.ArgumentParser(description="Deploy an LLM model using Ray Serve with an OpenAI-compatible API.")
    parser.add_argument(
        "--model-id", 
        type=str, 
        default=os.environ.get("RAY_SERVE_MODEL_ID", DEFAULT_MODEL_ID),
        help=f"The HuggingFace model ID to deploy (e.g., 'meta-llama/Llama-2-7b-chat-hf'). Defaults to {DEFAULT_MODEL_ID}. Can also be set with RAY_SERVE_MODEL_ID env var."
    )
    parser.add_argument(
        "--accelerator-type",
        type=str,
        default=os.environ.get("RAY_SERVE_ACCELERATOR_TYPE", DEFAULT_ACCELERATOR_TYPE),
        help=f"The accelerator type to use (e.g., 'CPU', 'A10G', 'L4'). Defaults to {DEFAULT_ACCELERATOR_TYPE}. Can also be set with RAY_SERVE_ACCELERATOR_TYPE env var."
    )
    parser.add_argument(
        "--num-replicas",
        type=int,
        default=int(os.environ.get("RAY_SERVE_NUM_REPLICAS", 1)),
        help="Number of replicas for the model deployment. Defaults to 1. Can also be set with RAY_SERVE_NUM_REPLICAS env var."
    )
    parser.add_argument(
        "--tensor-parallel-size",
        type=int,
        default=int(os.environ.get("RAY_SERVE_TENSOR_PARALLEL_SIZE", 1)),
        help="Tensor parallelism size for vLLM. Defaults to 1 (no tensor parallelism). Can also be set with RAY_SERVE_TENSOR_PARALLEL_SIZE env var."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("RAY_SERVE_PORT", 8000)),
        help="Port for the Ray Serve endpoint. Defaults to 8000. Can also be set with RAY_SERVE_PORT env var."
    )
    parser.add_argument(
        "--route-prefix",
        type=str,
        default=os.environ.get("RAY_SERVE_ROUTE_PREFIX", "/v1"),
        help="Route prefix for the OpenAI API. Defaults to /v1. Can also be set with RAY_SERVE_ROUTE_PREFIX env var."
    )
    parser.add_argument(
        "--hf-token-env-var",
        type=str,
        default="HF_TOKEN",
        help="Name of the environment variable holding the HuggingFace Hub token for gated models. Defaults to HF_TOKEN."
    )

    args = parser.parse_args()

    print(f"Starting Ray Serve LLM deployment with the following configuration:")
    print(f"  Model ID: {args.model_id}")
    print(f"  Accelerator Type: {args.accelerator_type}")
    print(f"  Number of Replicas: {args.num_replicas}")
    print(f"  Tensor Parallel Size: {args.tensor_parallel_size}")
    print(f"  Port: {args.port}")
    print(f"  Route Prefix: {args.route_prefix}")

    # Prepare runtime_env for gated models if HF_TOKEN is set
    runtime_env = {}
    hf_token = os.environ.get(args.hf_token_env_var)
    if hf_token:
        print(f"  Using HuggingFace Hub token from env var: {args.hf_token_env_var}")
        runtime_env["env_vars"] = {"HF_TOKEN": hf_token, "HF_HUB_ENABLE_HF_TRANSFER": "1"}
    else:
        print(f"  Warning: {args.hf_token_env_var} not set. This might be an issue for gated models.")
        runtime_env["env_vars"] = {"HF_HUB_ENABLE_HF_TRANSFER": "1"} # Enable fast download even for public models

    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=args.model_id, # Use the model_id from args
            model_source=args.model_id, # Assuming model_id is also the source path on HuggingFace
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=args.num_replicas,
                max_replicas=args.num_replicas, # For now, fixed replicas based on arg
            )
        ),
        accelerator_type=args.accelerator_type,
        engine_kwargs=dict(
            tensor_parallel_size=args.tensor_parallel_size,
            # Add other vLLM engine specific kwargs here if needed
            # e.g., max_model_len, dtype, etc.
        ),
        runtime_env=runtime_env if runtime_env else None # Only pass if not empty
    )

    # Build the OpenAI compatible application
    # We can deploy multiple models by passing a list of LLMConfig objects
    app = build_openai_app(llm_configs=[llm_config], model_id_key="model_id") 

    print(f"\nDeploying to Ray Serve on port {args.port} with route prefix {args.route_prefix}...")
    serve.run(
        app, 
        name="openai_compat_vllm_server", # Give a name to the application
        host="0.0.0.0", # Bind to all interfaces
        port=args.port, 
        route_prefix=args.route_prefix, 
        blocking=True
    )
    print("Ray Serve LLM deployment is running.")

if __name__ == "__main__":
    # Note: Ray Serve applications should typically be started from the command line
    # using `serve run deploy_llm_service:app` after defining `app` globally, 
    # or this script can be run directly if `serve.run(blocking=True)` is used.
    # Ensure Ray is initialized if not running via `serve run` CLI.
    # import ray
    # if not ray.is_initialized():
    #     ray.init(address="auto" if os.environ.get("RAY_ADDRESS") else None, ignore_reinit_error=True)
    main() 