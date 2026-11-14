"""A real MLflow HTTP registry; validation, alias promotion and activation differ.

Registry artifacts are executable Python models. This reference accepts only
models published by its trusted training path; do not point it at an untrusted
registry. Content hashes detect corruption, not a malicious registry owner.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .features import FEATURE_NAMES


class RegistryError(RuntimeError):
    pass


@dataclass
class LoadedModel:
    model_name: str
    model_version: str
    run_id: str
    feature_version: str
    threshold: float
    estimator: Any = field(repr=False)
    model_digest: str = ""
    qualified: bool = False
    evaluation_id: str = ""
    reference_profile: dict[str, Any] = field(default_factory=dict)
    feature_signature: str = ""
    qualification_policy: dict[str, Any] = field(default_factory=dict)

    def predict(self, feature: dict[str, Any]) -> float:
        from .modeling import feature_frame

        if feature.get("feature_version") != self.feature_version:
            raise RegistryError("feature version is incompatible with the active model")
        if self.feature_signature and feature.get("feature_signature") != self.feature_signature:
            raise RegistryError("feature semantics are incompatible with the active model")
        try:
            probability = float(self.estimator.predict_proba(feature_frame([feature]))[0, 1])
        except Exception as exc:
            raise RegistryError(f"model inference failed: {type(exc).__name__}") from exc
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise RegistryError("model produced an invalid risk score")
        return probability


class RegistryClient:
    def __init__(self, tracking_uri: str, model_name: str = "industrial-risk",
                 *, feature_version: str = "v1"):
        parsed = urlparse(tracking_uri)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("a real HTTP(S) MLflow tracking server is required")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("MLflow URL must not contain credentials, query parameters or fragment")
        if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,127}", model_name):
            raise ValueError("invalid registered model name")
        from mlflow.tracking import MlflowClient

        self.tracking_uri = tracking_uri.rstrip("/")
        self.model_name = model_name
        self.feature_version = feature_version
        self.client = MlflowClient(tracking_uri=self.tracking_uri)

    def _version(self, version: str) -> Any:
        if not str(version).isdigit() or int(version) < 1:
            raise ValueError("an explicit positive numeric model version is required")
        return self.client.get_model_version(self.model_name, str(version))

    def _qualified(self, model_version: Any) -> bool:
        tags = model_version.tags or {}
        if tags.get("qualification.eligible") != "true" or tags.get("feature.version") != self.feature_version:
            return False
        if tags.get("publisher") != "industrial-streaming-mlops" or not tags.get("evaluation.id"):
            return False
        try:
            gates = json.loads(tags["qualification.gates"])
        except (KeyError, ValueError, TypeError):
            return False
        required = {"label_coverage", "failure_evidence", "exposure_evidence", "false_alert_budget", "useful_failure_recall", "model_latency_p95"}
        if tags.get("role") == "candidate":
            required.add("improvement_over_baseline")
        return bool(required <= set(gates) and all(value is True for value in gates.values()))

    def is_qualified(self, version: str) -> bool:
        return self._qualified(self._version(version))

    def register(self, estimator: Any, *, role: str, threshold: float, feature_version: str,
                 feature_signature: str,
                 code_sha: str, evaluation_id: str, qualification: dict[str, Any],
                 metrics: dict[str, Any], input_example: Any, dataset_manifest: dict[str, Any],
                 reference_features: list[dict[str, Any]]) -> dict[str, Any]:
        import mlflow
        import mlflow.sklearn
        from mlflow.models import infer_signature

        from .monitoring import reference_profile

        if role not in {"baseline", "candidate"} or not 0 <= threshold <= 1:
            raise ValueError("reviewed model role and threshold are required")
        if feature_version != self.feature_version:
            raise RegistryError("cannot register a model with incompatible feature version")
        if not re.fullmatch(r"[a-f0-9]{64}", feature_signature):
            raise RegistryError("a reviewed feature semantics signature is required")
        if not code_sha or not evaluation_id:
            raise ValueError("code and evaluation identities are required")
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_registry_uri(self.tracking_uri)
        mlflow.set_experiment("industrial-monitoring")
        tags = {
            "publisher": "industrial-streaming-mlops", "role": role,
            "feature.version": feature_version,
            "feature.signature": feature_signature,
            "feature.names": json.dumps(list(FEATURE_NAMES)),
            "code.sha": code_sha, "evaluation.id": evaluation_id,
            "decision.threshold": repr(float(threshold)),
            "qualification.eligible": str(bool(qualification["eligible"])).lower(),
            "qualification.gates": json.dumps(qualification["gates"], sort_keys=True),
            "qualification.policy": json.dumps(qualification["policy"], sort_keys=True),
        }
        with mlflow.start_run(run_name=f"{role}-{evaluation_id[:12]}", tags=tags) as run:
            mlflow.log_params({"feature_version": feature_version, "threshold": threshold,
                               "role": role, "code_sha": code_sha})
            scalar_metrics = {k: float(v) for k, v in metrics.items() if isinstance(v, (int, float)) and math.isfinite(float(v))}
            mlflow.log_metrics(scalar_metrics)
            mlflow.log_dict(qualification, "qualification.json")
            mlflow.log_dict(dataset_manifest, "datasets.json")
            profile = reference_profile(reference_features)
            mlflow.log_dict(profile, "reference-profile.json")
            signature = infer_signature(input_example, estimator.predict_proba(input_example))
            requirements = [f"{name}=={importlib.metadata.version(name)}" for name in ("mlflow", "scikit-learn", "numpy", "pandas", "skops")]
            trusted_types = ["industrial_mlops.modeling.BaselineRiskModel"] if role == "baseline" else []
            info = mlflow.sklearn.log_model(
                sk_model=estimator, name="risk-model", signature=signature,
                input_example=input_example, pip_requirements=requirements,
                pyfunc_predict_fn="predict_proba", metadata={"feature_version": feature_version},
                serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_SKOPS,
                skops_trusted_types=trusted_types,
            )
            local = Path(mlflow.artifacts.download_artifacts(artifact_uri=info.model_uri, tracking_uri=self.tracking_uri))
            artifact = local / "model.skops"
            if not artifact.is_file():
                raise RegistryError("registered sklearn artifact has no model.skops")
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            tags["artifact.sha256"] = digest
            tags["artifact.format"] = "skops"
            version = mlflow.register_model(info.model_uri, self.model_name, await_registration_for=120, tags=tags)
            run_id = run.info.run_id
        return {"model_name": self.model_name, "model_version": str(version.version),
                "run_id": run_id, "model_uri": f"models:/{self.model_name}/{version.version}",
                "model_digest": digest}

    def load(self, version: str | None = None, alias: str = "champion") -> LoadedModel:
        import mlflow.artifacts
        import mlflow.sklearn

        # Resolve the alias exactly once. Artifact and metadata always refer to
        # this resolved numeric version, even if an operator moves the alias.
        mv = self._version(version) if version is not None else self.client.get_model_version_by_alias(self.model_name, alias)
        resolved = str(mv.version)
        tags = mv.tags or {}
        if tags.get("publisher") != "industrial-streaming-mlops":
            raise RegistryError("model was not published by the trusted project training path")
        if tags.get("feature.version") != self.feature_version:
            raise RegistryError("registered feature version is incompatible")
        try:
            if json.loads(tags["feature.names"]) != list(FEATURE_NAMES):
                raise RegistryError("registered feature order is incompatible")
            threshold = float(tags["decision.threshold"])
            if not math.isfinite(threshold) or not 0 <= threshold <= 1:
                raise RegistryError("registered threshold is invalid")
            expected_digest = tags["artifact.sha256"]
            signature = tags["feature.signature"]
            if not re.fullmatch(r"[a-f0-9]{64}", signature):
                raise RegistryError("registered feature signature is invalid")
            policy = json.loads(tags["qualification.policy"])
            if not isinstance(policy, dict):
                raise RegistryError("registered qualification policy is invalid")
        except (KeyError, ValueError, TypeError) as exc:
            raise RegistryError("model is missing valid reviewed metadata") from exc
        # Resolve MLflow 3's logged-model indirection through this explicit
        # client. The models:/ artifact repository otherwise consults global
        # tracking/registry configuration even when download kwargs are set.
        source = mv.source
        if not re.fullmatch(r"models:/m-[a-f0-9]+", source):
            raise RegistryError("model artifact source is not a reviewed MLflow logged model")
        logged = self.client.get_logged_model(source.split("/")[-1])
        if logged.source_run_id != mv.run_id:
            raise RegistryError("registered model and logged artifact have different run identities")
        local = Path(mlflow.artifacts.download_artifacts(
            artifact_uri=logged.artifact_location, tracking_uri=self.tracking_uri,
            registry_uri=self.tracking_uri))
        import yaml

        # MLflow reads its loader metadata before constructing the estimator.
        # Keep the permitted serializer/type set local and explicit, instead of
        # accepting a broader trust list from the artifact itself.
        metadata = yaml.safe_load((local / "MLmodel").read_text(encoding="utf-8"))
        flavor = metadata.get("flavors", {}).get("sklearn", {})
        trusted_types = ["industrial_mlops.modeling.BaselineRiskModel"] if tags.get("role") == "baseline" else []
        if (tags.get("role") not in {"baseline", "candidate"} or tags.get("artifact.format") != "skops"
                or flavor.get("serialization_format") != "skops" or flavor.get("code")
                or flavor.get("pickled_model") != "model.skops"
                or (flavor.get("skops_trusted_types") or []) != trusted_types):
            raise RegistryError("model loader metadata exceeds the reviewed serializer/type policy")
        artifact = local / "model.skops"
        if not artifact.is_file() or hashlib.sha256(artifact.read_bytes()).hexdigest() != expected_digest:
            raise RegistryError("model artifact content does not match its recorded digest")
        model = mlflow.sklearn.load_model(str(local))
        run_id = mv.run_id or ""
        profile = {}
        if run_id:
            profile_path = self.client.download_artifacts(run_id, "reference-profile.json")
            profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))
        return LoadedModel(self.model_name, resolved, run_id, self.feature_version, threshold, model,
                           expected_digest, self._qualified(mv), tags.get("evaluation.id", ""), profile,
                           signature, policy)

    def promote(self, version: str, *, alias: str = "champion") -> dict[str, Any]:
        if alias != "champion":
            raise ValueError("only the reviewed champion promotion operation is exposed")
        mv = self._version(version)
        if not self._qualified(mv):
            raise RegistryError("model has not met the recorded heldout promotion gates")
        # Load and check compatibility/content before changing the registry pointer.
        loaded = self.load(version=str(mv.version))
        # The REST server reports an absent alias as INVALID_PARAMETER_VALUE;
        # inspecting the registered model avoids treating that as a network or
        # model-not-found error and avoids swallowing unrelated failures.
        registered = self.client.get_registered_model(self.model_name)
        prior_value = (registered.aliases or {}).get(alias)
        prior = str(prior_value) if prior_value is not None else None
        if prior is not None and prior != loaded.model_version:
            self.client.set_registered_model_alias(self.model_name, "previous", prior)
        self.client.set_registered_model_alias(self.model_name, alias, loaded.model_version)
        observed = self.client.get_model_version_by_alias(self.model_name, alias)
        if str(observed.version) != loaded.model_version:
            raise RegistryError("promotion readback differs; reconcile concurrent registry writers")
        return {"model_name": self.model_name, "previous_version": prior,
                "promoted_version": loaded.model_version, "activation_performed": False}

    def rollback(self, version: str) -> dict[str, Any]:
        """Move the registry pointer to an explicit compatible qualified version.

        Worker activation is separate. The deployment store records its own
        prior active version and can exercise shadow rollback independently.
        """
        return self.promote(version)
