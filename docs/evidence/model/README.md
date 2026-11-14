# Original model evaluation evidence

These files are exact copies from the training phase of native attempt **01**. The datasets are generated industrial sensor data, with separate delayed labels. They contain no customer or production measurements. Their checksums and original paths are recorded in the [evidence manifest](../attempts/evidence-manifest.json).

- [training-report.json](training-report.json) preserves the complete original evaluation, validation-selected thresholds, qualification policy, source identities, model versions and MLflow run identifiers.
- [holdout-predictions.csv](holdout-predictions.csv) preserves all original holdout rows, timestamps, delayed-label availability and baseline/candidate scores.
- [native-01-source-manifest.json](native-01-source-manifest.json) preserves the original native attempt source manifest. The report also contains its own training-source manifest.

## What the original evaluation observed

The split policy is disjoint synthetic datasets ordered as train, validation and test, with labels mature before the next split. Each split has 5,166 available labeled feature rows. The held-out test includes 24 observed failures over 17.9375 machine-days. Thresholds were selected using validation; these are the original test results with a 1,800-second alert cooldown.

| Model | Threshold | Event precision | Event recall | Alert precision | Useful failure recall | False alerts / machine-day | Median alert lead, seconds | Model-only p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline (v1) | 0.500 | 44.40% | 77.08% | 42.00% | 87.50% | 1.61672 | 919.36 | 0.186049 |
| Candidate (v2) | 0.975 | 88.55% | 80.56% | 96.00% | 100.00% | 0.05575 | 962.61 | 0.693303 |

Event classification metrics count labeled feature rows. Operational alert metrics apply the cooldown and evaluate useful detection of distinct failures. A useful alert requires at least 300 seconds of lead from feature availability to the simulated failure. Timing values come from the synthetic event/availability timeline. Latency is a warm-model, one-row-per-call measurement with 128 samples; it excludes Kafka, Spark, storage and dashboard time.

Both models satisfy the recorded model gates in this evaluation. The training function records `promotion_performed: false`; registry activation and pipeline recovery are separate evidence. Native attempt 01 subsequently failed on producer configuration, so this training evidence does not establish aggregate integration success. The original failures are retained in [the attempts folder](../attempts/README.md).

## Identity and scope

The report records the training code identity as `working-tree-sha256:0198d952137fe7439ed7818dbae0fbb81074f53e8e8da0032f365a2acab1db53`, evaluation ID `5639521de699a9b00af7789ad7ea52d727e3ff56f309b3e613f2cce6d2e37eb1`, feature version `v1` and feature signature `d6901bc8f738c50d99ca4406a9fd9ad7eab6c001ef9e1677e7cdc1c93034ba95`.

These are the identities actually recorded at execution time. No Git commit identifier has been substituted. Model quality on generated data establishes the behavior of this demonstration protocol; it does not establish generalization to a real industrial site or to an unseen machine population. The completed [native08 qualification](../native/README.md) provides the current runtime receipt. That run reused this original training report without retraining; its embedded report is identical and all seven recorded training-source file hashes remain unchanged. The original model identity and the later runtime source identity are preserved separately.
