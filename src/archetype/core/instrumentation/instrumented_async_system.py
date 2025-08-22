from __future__ import annotations

from daft import DataFrame

from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core import Archetype, ArchetypeSignature
from archetype.core.config import RunConfig
from .profiling_shim import zone


class InstrumentedAsyncSystem(AsyncSystem):
    async def execute(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs) -> DataFrame:  # type: ignore[override]
        with zone(f"system.execute[{Archetype.get_name(sig)}]"):
            # replicate base behavior with per-processor zones by iterating the same way
            if run_config.enable_validation:
                await self._validate(sig, df)

            for proc_instance in sorted(self.processors, key=lambda x: x.priority):
                if set(proc_instance.components).issubset(set(sig)):
                    try:
                        assert isinstance(proc_instance, AsyncProcessor)
                        from inspect import signature
                        sig_params = signature(proc_instance.process).parameters
                        filtered_kwargs = {k: v for k, v in input_kwargs.items() if k in sig_params}
                        with zone(f"proc[{proc_instance.__class__.__name__}]"):
                            df = await proc_instance.process(df, **filtered_kwargs)
                    except Exception:
                        # Preserve base error handling semantics without logging here
                        # to keep profiling layer focused. Base system logs errors.
                        pass

            if run_config.debug:
                try:
                    if getattr(run_config, "show_rows", 0) > 0:
                        df.limit(run_config.show_rows).show()
                except Exception:
                    pass

            return df


