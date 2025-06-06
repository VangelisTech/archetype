import asyncio
from typing import Callable, Iterable, List, Any, Coroutine, Tuple

async def run_async_tasks_with_semaphore(
    semaphore: asyncio.Semaphore,
    core_async_func: Callable[..., Coroutine[Any, Any, Any]], 
    items_iterable: Iterable[Tuple[Any, ...]], # Iterable of tuples, where each tuple contains args for one call to core_async_func
    max_concurrent: int, 
    *shared_args: Any, 
    **shared_kwargs: Any
) -> List[Any]:
    """
    Runs an async function over a list of items with a concurrency limit using asyncio.Semaphore.

    Args:
        core_async_func: The async function to execute for each item.
        items_iterable: An iterable of tuples, where each tuple contains the specific arguments
                        for one call to `core_async_func`.
        max_concurrent: The maximum number of concurrent calls to `core_async_func`.
        *shared_args: Positional arguments to be passed to every call of `core_async_func`.
        **shared_kwargs: Keyword arguments to be passed to every call of `core_async_func`.

    Returns:
        A list of results from core_async_func, in the order of task completion (if gather(return_exceptions=False))
        or in the order of original tasks if return_exceptions=True and then processed.
        Currently returns in order of task completion for successful tasks when return_exceptions=True is used.
    """
    
    
    async def worker(item_specific_args: Tuple[Any, ...]) -> Any:
        async with semaphore:
            return await core_async_func(*item_specific_args, *shared_args, **shared_kwargs)

    tasks = [worker(item_args) for item_args in items_iterable]
    
    # return_exceptions=True is crucial for not having the entire gather call fail on one task's error.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results # Caller should handle exceptions in the results list 