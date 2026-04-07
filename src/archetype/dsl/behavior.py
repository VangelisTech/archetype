"""@behavior decorator — turns a plain class into an AsyncProcessor."""

from __future__ import annotations

from archetype.core.aio.async_processor import AsyncProcessor


def behavior(cls):
    """Decorate a class with ``requires``, ``priority``, and ``process()`` into an AsyncProcessor.

    Usage::

        @behavior
        class GainExperience:
            requires = [Agent]
            priority = 10

            async def process(self, df, **kwargs):
                return df.with_column(...)
    """
    requires = getattr(cls, "requires", [])
    priority = getattr(cls, "priority", 0)
    process_fn = getattr(cls, "process", None)

    if not requires:
        raise TypeError(f"@behavior class {cls.__name__} must define 'requires' (list of Component types)")
    if process_fn is None:
        raise TypeError(f"@behavior class {cls.__name__} must define 'async def process(self, df, **kwargs)'")

    # Build a real AsyncProcessor subclass
    processor_cls = type(
        cls.__name__,
        (AsyncProcessor,),
        {
            "components": tuple(requires),
            "priority": priority,
            "process": process_fn,
            "__doc__": cls.__doc__,
            "__module__": cls.__module__,
        },
    )
    return processor_cls
