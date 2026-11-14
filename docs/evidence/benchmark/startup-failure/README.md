# Benchmark01 startup failure

The [original console](console.log) and [failure receipt](receipt.json) record `ModuleNotFoundError: psutil` in the clean locked Python 3.11 environment. Execution stopped before a benchmark work directory or services were created. psutil was subsequently added as an explicit pinned development dependency and installed from the regenerated frozen lock. No performance or business-result claim is attached to this failed attempt.
