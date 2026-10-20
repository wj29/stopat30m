"""Read-only forward adapter for the unchanged A5 annual H5 native model.

This adapter never fits, updates a model, extends historical membership, or
places orders. The paper runner owns current membership, legal identities,
strict incremental-source evidence, and the separately declared holding rule.
"""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from stopat30m import ashare_forward as forward
from stopat30m import ashare_rolling_research as rolling
from stopat30m import ashare_stability_model_reuse as reuse
from stopat30m.ashare_union_research import REQUIRED_FIELDS
from stopat30m.data.qlib_snapshot import load_qlib_snapshot, read_qlib_field
from stopat30m.data import qlib_snapshot
from stopat30m.model import trainer
from stopat30m.model.trainer import FixedQlibLGBSpec, load_fixed_qlib_model


POLICY = "a9-existing-a5-h5-2026-native-forward-adapter-v1"
SELECTED_YEAR = 2026


def _read(path):
    value = json.loads(Path(path).read_bytes())
    if not isinstance(value, dict):
        raise ValueError("A frozen model JSON object is required")
    return value


def _binding(path, digest):
    path = Path(path).resolve()
    if not re.fullmatch(r"[0-9a-f]{64}", str(digest)):
        raise ValueError("An exact frozen SHA256 is required")
    return {"path": str(path), "sha256": digest}


class FrozenLowTurnoverCandidate:
    """A5 LGB_2026 inference with its actual original fitted statistics."""

    def __init__(self, root, manifest, protocol, model, processor, proofs):
        self.path = root
        self._manifest, self._protocol = manifest, protocol
        self._model, self._processor = model, processor
        self._proofs = proofs
        self.model_sha256 = manifest["selected_native_model_sha256"]

    @property
    def manifest(self):
        # The hash identifies the unchanged source whole-package manifest.
        return deepcopy(self._manifest)

    @property
    def protocol(self):
        return deepcopy(self._protocol)

    @property
    def source_proofs(self):
        return self.proofs()

    def proofs(self):
        """Compact original artifacts/dependencies, not a cached audit approval.

        The original market audit and source inventory recursively bind the
        larger source set. Its full verification runs once at load; callers
        may perform that complete verification again at an explicit boundary.
        """
        return deepcopy(self._proofs)

    def assert_unchanged(self):
        reuse._verify_proofs(self._proofs)

    def predict(self, snapshot_path, signal_date, instruments):
        """Predict explicit current instruments using only this close's features.

        No labels, future endpoints, future index membership, or new fitting
        are used. Input selection and current identity evidence belong to the
        provider; incomplete requested signal bars fail instead of being filled.
        """
        self.assert_unchanged()
        day = forward._day(signal_date)
        requested = list(instruments)
        if (not requested or len(set(requested)) != len(requested)
                or any(not isinstance(inst, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", inst)
                       for inst in requested)):
            raise ValueError("Explicit unique canonical A-share signal instruments are required")
        snapshot = load_qlib_snapshot(snapshot_path, verify=True)
        if (snapshot.manifest["source"] != self._protocol["source_identity"]
                or snapshot.manifest["price_semantics"] != self._protocol["price_semantics"]):
            raise ValueError("Forward inference source/basis differs from the original A5 model")
        calendar_path = snapshot.dataset_dir / "calendars/day.txt"
        calendar = (calendar_path).read_text().splitlines()
        if day not in calendar or len(calendar) != len(set(calendar)) or calendar != sorted(calendar):
            raise ValueError("The prediction close must be a unique actual source session")
        position = calendar.index(day)
        used = {"snapshot_manifest": reuse._proof(snapshot.path / "manifest.json"),
                "calendar": reuse._proof(calendar_path)}
        for inst in sorted(requested):
            for field in REQUIRED_FIELDS:
                name = f"features/{inst.lower()}/{field}.day.bin"
                info = snapshot.manifest["files"].get(name)
                if info is None:
                    raise ValueError("Requested signal instrument lacks an original required field")
                path = snapshot.dataset_dir / name
                used[name] = _binding(path, info["sha256"])
                value = read_qlib_field(path, calendar)[position]
                if not np.isfinite(value) or value <= 0:
                    raise ValueError("Requested signal close has incomplete observed source fields")
        features = forward._prediction_features(snapshot, day, requested)
        if (not isinstance(features.index, pd.MultiIndex)
                or features.index.names != ["datetime", "instrument"] or features.index.has_duplicates
                or set(features.index.get_level_values("datetime")) != {pd.Timestamp(day)}
                or set(features.index.get_level_values("instrument")) != set(requested)
                or len(features) != len(requested)):
            raise ValueError("Raw Alpha158 output differs from the exact requested close panel")
        scores = rolling.apply_frozen_processors(features, self._model, self._processor["processors"])
        if scores.index.has_duplicates or not np.isfinite(scores.to_numpy()).all():
            raise ValueError("Frozen A5 model produced invalid forward scores")
        # Rehash bytes actually consumed, and all compact model/declaration
        # proofs, before exposing results. No mtime cache authorizes inference.
        reuse._verify_proofs(used)
        self.assert_unchanged()
        return scores.sort_index()


def load_candidate(models_directory, protocol_path, market_audit_path):
    """Verify the reused A5 package once and bridge its original H5 2026 fold.

    The returned manifest hash is the SHA of the original package manifest
    bytes. Native metadata, fitted timestamps and processors remain unchanged;
    loading this object is not a new training or latest-data-refit claim.
    """
    root = Path(models_directory).resolve()
    protocol_path, market_audit_path = Path(protocol_path).resolve(), Path(market_audit_path).resolve()
    receipt = reuse.verify_reused_stability_models(root, protocol_path, market_audit_path)
    if receipt.get("status") != "passed" or receipt.get("training_reperformed") is not False:
        raise ValueError("An unchanged, fully verified A5 reused model package is required")
    manifest = _read(root / "manifest.json")
    if (manifest.get("kind") != "a5_union_stability_fixed_predictions"
            or manifest.get("models") != ["LGB"]
            or manifest.get("inputs", {}).get("declaration") != reuse._proof(protocol_path)
            or manifest.get("inputs", {}).get("market_audit") != reuse._proof(market_audit_path)):
        raise ValueError("Only the exact original A5 H5 package/declaration/audit may be bridged")
    folder = root / f"LGB_{SELECTED_YEAR}"
    record, processor, native = [_read(folder / name) for name in
                                ("protocol.json", "processors.json", "model/manifest.json")]
    model = load_fixed_qlib_model(folder / "model")
    source = load_qlib_snapshot(manifest["source_snapshot"], verify=False)
    # The complete source and recursive source-audit proofs were verified by
    # verify_reused_stability_models above; avoid hashing that dataset twice.
    if (source.manifest_sha256 != manifest["source_manifest_sha256"]
            or native.get("numpy_version") != np.__version__
            or native.get("pandas_version") != pd.__version__
            or model.spec != FixedQlibLGBSpec() or len(model.feature_names) != 158
            or model.booster.num_feature() != 158 or model.booster.current_iteration() != 100
            or processor.get("feature_names") != model.feature_names
            or processor.get("processors") != model.metadata.get("fitted_inference_processors")
            or record.get("execution_year") != SELECTED_YEAR
            or record.get("data_cutoff") != "2025-12-31"
            or record.get("train_signal_end") != "2025-12-23"
            or record.get("model_sha256") != native["model_sha256"]
            or native["model_sha256"] != reuse._sha(folder / "model/model.txt")):
        raise ValueError("Selected original annual native model/runtime/processor/chronology differs")
    training = _read(protocol_path)["training"]
    if training.get("label") != "Ref($open,-6)/Ref($open,-1)-1" or training.get("purge_future_sessions") != 6:
        raise ValueError("The original fixed mature five-session target is required")
    proofs = {**receipt["bound_inputs"], "source_models_manifest": reuse._proof(root / "manifest.json"),
              "candidate_adapter": reuse._proof(__file__),
              "alpha158_inference_implementation": reuse._proof(forward.__file__),
              "feature_processors_implementation": reuse._proof(rolling.__file__),
              "native_loader_implementation": reuse._proof(trainer.__file__),
              "snapshot_reader_implementation": reuse._proof(qlib_snapshot.__file__)}
    for name, digest in manifest["files"].items():
        proofs["source_artifact:" + name] = _binding(reuse._relative(root, name), digest)
    original_manifest_path = Path(receipt["bound_inputs"]["original_manifest"]["path"])
    original_manifest = _read(original_manifest_path)
    for name, digest in original_manifest["files"].items():
        proofs["original_training_artifact:" + name] = _binding(
            reuse._relative(original_manifest_path.parent, name), digest)
    protocol = {"policy": POLICY, "training_role": "existing_A5_2026_H5_reused_without_refitting",
        "label": training["label"], "label_future_sessions": 6,
        "source_identity": deepcopy(source.manifest["source"]),
        "price_semantics": deepcopy(source.manifest["price_semantics"]),
        "source_snapshot": str(source.path), "source_snapshot_manifest_sha256": source.manifest_sha256,
        "source_model_package": str(root), "source_model_package_manifest_sha256": reuse._sha(root / "manifest.json"),
        "source_protocol": str(protocol_path), "source_market_audit": str(market_audit_path),
        "train_start": record["train_start"], "data_cutoff": record["data_cutoff"],
        "train_signal_end": record["train_signal_end"], "original_fitted_at": record["fitted_at"],
        "rebalance_sessions": 5, "top_k": 20, "keep_rank": 40,
        "historical_oos_claim": False, "latest_data_refit": False,
        "future_scope_owned_by_provider": "dated_current_CSI300_CSI1000_union_plus_separate_held_targets"}
    exposed = deepcopy(manifest)
    exposed.update(manifest_sha256=protocol["source_model_package_manifest_sha256"],
                   selected_native_model_sha256=native["model_sha256"], adapter_policy=POLICY)
    candidate = FrozenLowTurnoverCandidate(root, exposed, protocol, model, processor, proofs)
    candidate.assert_unchanged()
    return candidate
