"""Forward adapter checks using frozen synthetic bytes; no model fitting."""
import json
import shutil
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_low_turnover_candidate as candidate


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True))


@pytest.fixture
def case(tmp_path, monkeypatch):
    root, source = tmp_path / "models", tmp_path / "source"
    days = pd.bdate_range("2026-06-01", periods=100).strftime("%Y-%m-%d").tolist()
    source.joinpath("dataset/calendars").mkdir(parents=True)
    source.joinpath("dataset/calendars/day.txt").write_text("\n".join(days)+"\n")
    files = {}
    for inst in ("SH600000", "SZ000001"):
        for field in candidate.REQUIRED_FIELDS:
            name = f"features/{inst.lower()}/{field}.day.bin"
            path = source / "dataset" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"immutable original synthetic source field")
            files[name] = {"sha256": candidate.reuse._sha(path)}
    source_manifest = {"source": {"kind": "community", "identity": "fixed-A5"},
                       "price_semantics": {"prices": "normalized"}, "files": files}
    _write(source / "manifest.json", source_manifest)
    snapshot = SimpleNamespace(path=source, dataset_dir=source / "dataset", manifest=source_manifest,
                               manifest_sha256=candidate.reuse._sha(source / "manifest.json"))
    names = [f"FEATURE_{n}" for n in range(158)]
    processors = [{"class": "RobustZScoreNorm", "fields_group": "feature", "clip_outlier": True,
        "mean_train": [0.1]*158, "std_train": [0.3]*158}, {"class": "Fillna", "fields_group": "feature"}]
    processor = {"schema_version": 1, "feature_names": names, "processors": processors,
                 "fillna_value": 0.0, "clip_min": -3.0, "clip_max": 3.0}
    root.mkdir()
    protocol, audit, original = tmp_path / "protocol.json", tmp_path / "audit.json", tmp_path / "original-models/manifest.json"
    _write(protocol, {"training": {"label": "Ref($open,-6)/Ref($open,-1)-1", "purge_future_sessions": 6}})
    _write(audit, {"status": "passed"})
    for year in (2025, 2026):
        folder = root / f"LGB_{year}"
        folder.joinpath("model").mkdir(parents=True)
        folder.joinpath("model/model.txt").write_text(f"immutable native model {year}; never fit here")
        _write(folder / "processors.json", processor)
        _write(folder / "model/manifest.json", {"numpy_version": np.__version__, "pandas_version": pd.__version__,
               "model_sha256": candidate.reuse._sha(folder / "model/model.txt")})
        _write(folder / "protocol.json", {"execution_year": year, "data_cutoff": "2025-12-31",
               "train_start": "2021-01-01", "train_signal_end": "2025-12-23",
               "fitted_at": "2026-10-20T07:51:41.958351+00:00",
               "model_sha256": candidate.reuse._sha(folder / "model/model.txt")})
    manifest = {"kind": "a5_union_stability_fixed_predictions", "models": ["LGB"],
        "source_snapshot": str(source), "source_manifest_sha256": snapshot.manifest_sha256,
        "files": {str(p.relative_to(root)): candidate.reuse._sha(p) for p in root.rglob("*") if p.is_file()},
        "inputs": {"declaration": candidate.reuse._proof(protocol), "market_audit": candidate.reuse._proof(audit)}}
    shutil.copytree(root, original.parent)
    _write(original, manifest)
    _write(root / "manifest.json", manifest)
    calls = {"verify": 0, "snapshot": []}
    def verify(folder, declaration, market_audit):
        calls["verify"] += 1
        candidate.reuse._verify_files(folder, manifest["files"])
        candidate.reuse._verify_proofs(manifest["inputs"])
        return {"status": "passed", "training_reperformed": False,
                "bound_inputs": {"original_manifest": candidate.reuse._proof(original),
                                 "source_snapshot_manifest": candidate.reuse._proof(source / "manifest.json")}}
    def load_source(path, *, verify=True):
        calls["snapshot"].append(verify)
        return snapshot
    class Booster:
        def num_feature(self): return 158
        def current_iteration(self): return 100
        def predict(self, values, *, num_threads):
            assert num_threads == 1
            return values.iloc[:, 0].to_numpy()*.1 + values.iloc[:, 1].to_numpy()
    model = SimpleNamespace(spec=candidate.FixedQlibLGBSpec(), feature_names=names, booster=Booster(),
                            metadata={"fitted_inference_processors": processors})
    monkeypatch.setattr(candidate.reuse, "verify_reused_stability_models", verify)
    monkeypatch.setattr(candidate, "load_fixed_qlib_model", lambda path: model)
    monkeypatch.setattr(candidate, "load_qlib_snapshot", load_source)
    monkeypatch.setattr(candidate, "read_qlib_field", lambda path, calendar: np.ones(len(calendar), dtype=np.float32))
    def features(snapshot, day, instruments):
        index = pd.MultiIndex.from_product([[pd.Timestamp(day)], sorted(instruments)], names=["datetime", "instrument"])
        return pd.DataFrame(np.arange(len(index)*158, dtype=np.float32).reshape(-1, 158)/1000,
                            index=index, columns=names)
    monkeypatch.setattr(candidate.forward, "_prediction_features", features)
    return SimpleNamespace(root=root, original=original.parent, source=source, snapshot=snapshot, protocol=protocol, audit=audit,
                           calls=calls, model=model, processor=processor, day=days[-1], instruments=["SZ000001", "SH600000"])


def _load(case):
    return candidate.load_candidate(case.root, case.protocol, case.audit)


def test_bridges_original_whole_package_hash_and_native_without_refitting(case):
    loaded = _load(case)
    assert loaded.manifest["manifest_sha256"] == candidate.reuse._sha(case.root / "manifest.json")
    assert loaded.model_sha256 == candidate.reuse._sha(case.root / "LGB_2026/model/model.txt")
    assert loaded.protocol["rebalance_sessions"] == 5 and loaded.protocol["top_k"] == 20
    assert loaded.protocol["keep_rank"] == 40 and loaded.protocol["latest_data_refit"] is False
    assert loaded.protocol["data_cutoff"] == "2025-12-31"
    assert "LGB_2025/model/model.txt" in " ".join(loaded.proofs())
    assert "LGB_2026/processors.json" in " ".join(loaded.proofs())
    altered = loaded.protocol
    altered["rebalance_sessions"] = 20
    assert loaded.protocol["rebalance_sessions"] == 5


def test_original_float32_processor_order_and_full_terminal_prediction_panel(case):
    loaded = _load(case)
    raw = candidate.forward._prediction_features(case.snapshot, case.day, case.instruments)
    expected = raw.copy()
    expected -= np.asarray(case.processor["processors"][0]["mean_train"], dtype=np.float32)
    expected /= np.asarray(case.processor["processors"][0]["std_train"], dtype=np.float32)
    expected = expected.clip(-3, 3).fillna(0)
    expected_scores = pd.Series(case.model.booster.predict(expected, num_threads=1), index=raw.index, name="score")
    actual = loaded.predict(case.source, case.day, case.instruments)
    pd.testing.assert_series_equal(actual, expected_scores)
    assert len(actual) == 2 and actual.index.names == ["datetime", "instrument"]
    loaded.predict(case.source, case.day, case.instruments)
    assert case.calls["verify"] == 1
    assert case.calls["snapshot"] == [False, True, True]


@pytest.mark.parametrize("relative", ["LGB_2026/model/model.txt", "LGB_2026/processors.json", "LGB_2025/protocol.json"])
def test_mutating_any_bound_original_artifact_after_load_rejects_inference(case, relative):
    loaded = _load(case)
    path = case.root / relative
    path.write_bytes(path.read_bytes()+b" ")
    with pytest.raises(ValueError, match="source bytes changed"):
        loaded.predict(case.source, case.day, case.instruments)


def test_original_generation_native_mutation_is_not_hidden_by_the_unchanged_reuse_copy(case):
    loaded = _load(case)
    path = case.original / "LGB_2026/model/model.txt"
    path.write_bytes(b"changed original training generation")
    with pytest.raises(ValueError, match="source bytes changed"):
        loaded.predict(case.source, case.day, case.instruments)


def test_source_basis_change_and_incomplete_signal_fields_fail_closed(case, monkeypatch):
    loaded = _load(case)
    case.snapshot.manifest["price_semantics"] = {"prices": "unadjusted"}
    with pytest.raises(ValueError, match="source/basis"):
        loaded.predict(case.source, case.day, case.instruments)
    case.snapshot.manifest["price_semantics"] = loaded.protocol["price_semantics"]
    monkeypatch.setattr(candidate, "read_qlib_field", lambda path, calendar: np.full(len(calendar), np.nan))
    with pytest.raises(ValueError, match="incomplete observed"):
        loaded.predict(case.source, case.day, case.instruments)


def test_used_source_mutation_during_prediction_is_detected_before_scores_escape(case, monkeypatch):
    loaded = _load(case)
    original = candidate.forward._prediction_features
    def mutated(snapshot, day, instruments):
        frame = original(snapshot, day, instruments)
        path = snapshot.dataset_dir / "features/sh600000/open.day.bin"
        path.write_bytes(b"changed during source consumption")
        return frame
    monkeypatch.setattr(candidate.forward, "_prediction_features", mutated)
    with pytest.raises(ValueError, match="source bytes changed"):
        loaded.predict(case.source, case.day, case.instruments)


@pytest.mark.parametrize("instruments", [[], ["SH600000", "SH600000"], ["BJ430001"], ["../SH600000"]])
def test_explicit_valid_unique_signal_universe_is_required(case, instruments):
    with pytest.raises(ValueError, match="unique canonical"):
        _load(case).predict(case.source, case.day, instruments)


def test_extra_missing_or_wrong_date_features_cannot_change_requested_panel(case, monkeypatch):
    loaded = _load(case)
    original = candidate.forward._prediction_features
    monkeypatch.setattr(candidate.forward, "_prediction_features",
                        lambda *args: original(*args).iloc[:1])
    with pytest.raises(ValueError, match="exact requested close panel"):
        loaded.predict(case.source, case.day, case.instruments)


def test_missing_or_tampered_original_package_is_rejected_before_loading(case):
    (case.root / "LGB_2026/model/model.txt").write_bytes(b"different model")
    with pytest.raises(ValueError, match="artifact bytes changed"):
        _load(case)
