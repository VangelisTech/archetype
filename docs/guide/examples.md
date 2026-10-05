# Examples and compatibility

`examples/native_simulation.py` is the current supported 0.7 two-program
quickstart. It requires a matched native library and real driver; it has no
simulated execution fallback. See [quickstart](quickstart.md).

The existing generic inference and Biome examples remain teaching material for
matched 0.6 source/wheels. They are not promoted as runnable 0.7 live examples.
Research remains an explicit 0.6 compatibility library. The independent Smol
examples remain runnable with `archetype-smol==0.6.3`.

Current release gates run the installed native example and the public contract
scenario. Old ecosystem examples and operational/eval harnesses retain their
versioned 0.6 contracts; run them against that source line rather than relabelling
old tick semantics as DDlog behavior.
