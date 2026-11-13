import json
from types import SimpleNamespace

import numpy as np
import pytest

from industrial_mlops.features import FEATURE_NAMES
from industrial_mlops.registry import LoadedModel, RegistryClient, RegistryError


class ConstantScore:
    def __init__(self, score):
        self.score = score

    def predict_proba(self, X):
        return np.array([[1 - self.score, self.score] for _ in range(len(X))])


def test_registry_requires_real_server_not_file_backed_substitute():
    with pytest.raises(ValueError, match="real HTTP"):
        RegistryClient("file:/tmp/mlruns")
    with pytest.raises(ValueError, match="credentials"):
        RegistryClient("http://secret:password@localhost:5000")


def test_loaded_model_rejects_missing_features_incompatible_version_and_bad_score():
    model = LoadedModel("industrial-risk", "3", "run", "v1", 0.5, ConstantScore(0.8))
    feature = {"feature_version": "v1", "values": {name: 1.0 for name in FEATURE_NAMES}}
    assert model.predict(feature) == 0.8
    model.feature_signature = "expected-window-semantics"
    with pytest.raises(RegistryError, match="semantics"):
        model.predict(feature)
    feature["feature_signature"] = "expected-window-semantics"
    assert model.predict(feature) == 0.8
    feature["feature_version"] = "v2"
    with pytest.raises(RegistryError, match="incompatible"):
        model.predict(feature)
    feature["feature_version"] = "v1"
    feature["values"].pop(FEATURE_NAMES[0])
    with pytest.raises(RegistryError, match="inference failed"):
        model.predict(feature)
    feature["values"][FEATURE_NAMES[0]] = 1.0
    model.estimator = ConstantScore(float("nan"))
    with pytest.raises(RegistryError, match="invalid risk score"):
        model.predict(feature)


def test_qualification_rejects_missing_failed_or_incompatible_evidence():
    registry = RegistryClient.__new__(RegistryClient)
    registry.feature_version = "v1"
    gates = {key: True for key in ("label_coverage", "failure_evidence", "exposure_evidence", "false_alert_budget", "useful_failure_recall", "model_latency_p95")}
    tags = {"qualification.eligible": "true", "feature.version": "v1", "publisher": "industrial-streaming-mlops",
            "evaluation.id": "known-evaluation", "qualification.gates": json.dumps(gates), "role": "baseline"}
    version = SimpleNamespace(tags=tags)
    assert registry._qualified(version)
    tags["role"] = "candidate"
    assert not registry._qualified(version)
    gates["improvement_over_baseline"] = True
    tags["qualification.gates"] = json.dumps(gates)
    assert registry._qualified(version)
    gates["false_alert_budget"] = False
    tags["qualification.gates"] = json.dumps(gates)
    assert not registry._qualified(version)


def test_promotion_stops_before_alias_mutation_on_failed_evidence():
    registry = RegistryClient.__new__(RegistryClient)
    registry.feature_version = "v1"
    registry._version = lambda version: SimpleNamespace(tags={}, version=version)
    registry.client = SimpleNamespace()  # a mutation would fail the test
    with pytest.raises(RegistryError, match="heldout promotion gates"):
        registry.promote("7")
