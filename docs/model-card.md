# Model card: industrial maintenance risk

## Intended use

I use this model to demonstrate an auditable maintenance workflow on **synthetic** machines. An operator sees the site, machine, observed sensor window, risk score, model version and eventual label behind an alert. These are experiments on a controlled simulator, not evidence of failure prediction on real industrial assets.

The target is a failure in the next 30 minutes of **event time**. The live system buffers observations for the configured lateness allowance. An alert only counts as useful when its feature was actually available at least five minutes before the failure. Processing a historical timestamp does not create advance warning in wall-clock time; the accelerated demonstration reports simulated operational lead and separate measured processing latency.

## Features and baseline

The feature contract `v1` uses the last 30 minutes for the same dataset/site/machine: temperature mean/max, vibration mean/max, pressure mean, temperature/vibration slope per minute and sample count. At least four observations are required. Sources must be at or before the anchor's business timestamp and must have arrived before feature closure. Identifiers, labels, future failure timestamps and scenario state never enter the model matrix.

The baseline is an explicit monotone score for high vibration or high temperature. A baseline score of 0.5 corresponds to a vibration maximum of 5.5 mm/s or temperature maximum of 80°C. Its coefficients are fixed. Only the operating decision threshold is selected on validation data.

The candidate is a class-balanced logistic regression after `StandardScaler`. Both components are fitted only on training rows. This small model is appropriate for a low-dimensional controlled simulator and exposes coefficients. It does not justify a claim that a larger or deep model would improve a real maintenance problem. Scores are **not calibrated probabilities**; model calibration and cost estimation on representative field data remain future work.

## Evaluation protocol fixed before the final run

- Training: synthetic seed 101, starting 2026-01-01.
- Validation: independent seed 202, starting 2026-01-05.
- Final holdout: independent seed 303, starting 2026-01-09.
- Default exposure: six machines, two sites, 72 hours per split. A bounded 24-hour profile is also available.
- Dataset identities are disjoint. All observations and delayed labels from one split must be available before the following split starts.
- Model and scaler are fitted once on training. A fixed grid of 39 thresholds from 0.05 through 1.0 is evaluated on validation, with a 30-minute alert cooldown per machine.
- Among validation thresholds within the false-alert budget, select useful failure recall, then failure recall, then alert precision, then lower false-alert rate, then higher threshold. Freeze that threshold before evaluating the final holdout.
- Do not retrain or change the threshold after inspecting the final holdout. A future model change requires a newly designated, previously unseen final holdout.

The current experiment measures longitudinal generalization within a controlled simulator. It does not establish transfer to an unseen physical machine, site, sensor manufacturer or failure mode. No machine/site identity is a model feature, and no machine-specific normalization is fitted.

## Promotion gates

The same policy is recorded in `qualification.json` and model-version tags. A model is eligible only when its final holdout has at least three observed failures and one machine-day of exposure, at least 99% label coverage, at most **2 false alerts per machine-day**, at least **70% useful failure recall** with **at least 300 seconds actionable lead**, and warm single-row model inference **p95 ≤ 1,000 ms**. The latency test excludes Kafka, watermark delay, artifact loading and target persistence; those are measured by the pipeline evidence separately.

The candidate must also have no worse useful recall and no higher false-alert rate than the baseline, with a strict improvement in useful recall, false-alert rate or median actionable lead. Equal performance does not justify replacing the baseline. These simple gates express the demonstration's maintenance budget. Field deployment would need a representative prevalence, uncertainty intervals, actual downtime/inspection costs and a business review.

Event-level precision/recall are reported alongside **emitted-alert** precision, false alerts per machine-day and **failure-level** recall. They answer different questions. Repeated scores above threshold within cooldown do not create repeated maintenance alerts. An alert that only becomes available after the failure counts as a false maintenance alert even if its anchor label is positive.

## Tracking and serving

Training uses a real MLflow HTTP server with persistent artifact storage and database-backed registry. Runs record code identity, complete dataset manifests, feature order and semantic signature, threshold, gate decisions, and the training drift reference. The signature includes window length, lateness, minimum observations, target horizon, units and feature order; changing those semantics requires a compatible model. Each model version records the exact `model.skops` SHA-256; loading verifies those bytes before deserializing the trusted artifact. The loader permits the reviewed scikit-learn types and explicitly allows only this project's baseline class when necessary. It rejects an artifact's broader trust list, alternate serializer or bundled loader code. The application package matching the recorded training source must be installed. A content digest detects mismatch, not independent certification or a hostile registry administrator.

Training and registration do not promote a model. Promotion changes the `champion` registry alias only after reading the recorded gates and loading a compatible artifact. The worker's numeric active version is a separate deployment record. The alias is resolved once to a numeric version; metadata and scores cannot silently come from different alias values. Rollback names an explicit known compatible version and requires a separate activation.

An unqualified version can only be used through an explicitly selected **shadow** demonstration deployment. Shadow outputs must remain marked as such. Model unavailability, missing features, non-finite scores or a feature-version mismatch must fail visibly; they must never produce a default healthy score.

## Monitoring and limitations

Training features define finite histogram bins. Population Stability Index compares a current window against that reference; at least 100 samples are required and PSI ≥ 0.2 is the predeclared investigation threshold. Prediction mean/positive-rate shifts and performance after delayed labels mature are separate signals. None automatically changes an alias or deployment.

The synthetic generator is fully known and easier to model than real telemetry. Reported point estimates are conditional on this simulator and exposure. Sensor downtime, interventions, censoring, changing maintenance policy, calibration, confidence intervals and external validation remain limitations. See the generated training report for **observed** numbers; this card intentionally contains no unexecuted benchmark result.
