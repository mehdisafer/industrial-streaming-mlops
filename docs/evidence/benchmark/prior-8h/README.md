# Prior benchmark02: zero-alert limitation

The [original report](benchmark.json), [protocol](protocol.json) and [console](console.log) are preserved without modification. It completed in 69.717897 seconds with its original status `passed`.

| Variant | Simulated hours | Raw records | Predictions | Alerts | Worker wall s |
| --- | ---: | ---: | ---: | ---: | ---: |
| h02-p1-w1 | 2 | 205 | 85 | 0 | 12.441 |
| h08-p1-w1 | 8 | 781 | 373 | 0 | 13.830 |
| h08-p3-w1 | 8 | 781 | 373 | 0 | 13.785 |
| h08-p3-w2 | 8 | 781 | 373 | 0 | 13.386 |

All variants emitted zero alerts. The 8-hour fixture ended before the failure windows exercised by the later 24-hour fixture. The original result supports its feature/score and transport checks; it does not establish nonzero alert production, emitted-alert identity/evidence equivalence or meaningful cooldown suppression. Benchmark03 adds 24-hour inputs and explicit nonzero-alert/cooldown checks. The earlier report's status and checks are preserved as historical evidence, not expanded after the fact.
