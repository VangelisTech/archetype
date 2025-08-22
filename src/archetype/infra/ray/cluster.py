from contextlib import asynccontextmanager
import ray

@asynccontextmanager
async def local_ray_cluster(num_l4=1):
    ray.init(num_gpus=0, )                           # head
    for _ in range(num_l4):
        ray.worker._global_node.add_resources({f"GPU": 1})
    try:
        yield
    finally:
        ray.shutdown()
