from copy import deepcopy
from dataclasses import asdict
import hashlib
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_horizon_research as horizon
from stopat30m.ashare_union_research import ObservedTrainingEligibility
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm


def index(days, names=("SH600000",)):
    return pd.MultiIndex.from_product([pd.to_datetime(days), names], names=["datetime", "instrument"])


def eligibility(days, *, missing=(), ipo="2010-01-01", out=""):
    valid=np.ones(len(days),dtype=bool);valid[list(missing)]=False
    return ObservedTrainingEligibility(days,{"SH600000":valid},
        {"SH600000":{"type":"1","ipoDate":ipo,"outDate":out}})


def labels(i):
    return pd.DataFrame({"H5":np.ones(len(i)),"H20":-np.ones(len(i))},index=i)


def test_common_maturity_is21_actual_sessions_with_gap_not20_or_calendar_days():
    days=pd.bdate_range("2024-01-01",periods=45).delete(7)
    assert horizon.common_mature_signal_end(days,days[35])==days[14].date().isoformat()
    source=eligibility(days);i=index(days[:20]);pool={"SH600000":[(days[0],days[-1])]}
    keep,audit=horizon.common_training_mask(source,i,pool,cutoff=days[35],labels=labels(i))
    assert keep.tolist()==[True]*15+[False]*5
    assert audit["common_exclusions_in_application_order"]["immature_common_label"]==5
    assert audit["common_future_sessions"]==21


@pytest.mark.parametrize("endpoint",[1,6,21])
def test_common_endpoint_missing_excludes_both_arms_without_forward_fill(endpoint):
    days=pd.bdate_range("2024-01-01",periods=45);i=index(days[:1]);pool={"SH600000":[(days[0],days[-1])]}
    keep,audit=horizon.common_training_mask(eligibility(days,missing=[endpoint]),i,pool,cutoff=days[-1],labels=labels(i))
    assert not keep[0]
    assert audit["common_exclusions_in_application_order"]["unobserved_or_illegal_common_endpoints"]==1


def test_intermediate_halt_does_not_impose_future_trading_or_membership_survival():
    days=pd.bdate_range("2024-01-01",periods=45);i=index(days[:1])
    pool={"SH600000":[(days[0],days[0])]}  # Leaves immediately after signal.
    keep,audit=horizon.common_training_mask(eligibility(days,missing=[9]),i,pool,cutoff=days[-1],labels=labels(i))
    assert keep[0] and audit["future_membership_conditioned"] is False


def test_legal_identity_at_t_plus21_and_both_labels_are_required():
    days=pd.bdate_range("2024-01-01",periods=45);i=index(days[:1]);pool={"SH600000":[(days[0],days[-1])]}
    keep,_=horizon.common_training_mask(eligibility(days,out=days[21].date().isoformat()),i,pool,cutoff=days[-1],labels=labels(i))
    assert not keep[0]
    y=labels(i);y.loc[:,"H20"]=np.nan
    keep,audit=horizon.common_training_mask(eligibility(days),i,pool,cutoff=days[-1],labels=y)
    assert not keep[0] and audit["common_exclusions_in_application_order"]["missing_either_label"]==1


def training_panel():
    rng=np.random.default_rng(20261020)
    days=pd.bdate_range("2023-01-02",periods=70)
    names=tuple(f"SH{600000+n}" for n in range(20));i=index(days[:25],names)
    features=pd.DataFrame(rng.normal(size=(len(i),158)).astype(np.float32),index=i,columns=[f"f{n}" for n in range(158)])
    # Opposite labels with ties prove the native trainer gets different targets.
    first=np.tile(np.repeat(np.arange(10),2),25).astype(np.float32)
    raw_labels=pd.DataFrame({"H5":first,"H20":-first},index=i)
    features["f0"]=first
    frame=pd.concat({"feature":features,"label":raw_labels},axis=1)
    fold={"train_start":days[0].date().isoformat(),"train_signal_end":days[24].date().isoformat(),
        "data_cutoff":days[45].date().isoformat()}
    return frame,fold,days


def test_single_shared_processor_independent_rank_targets_and_native_consumption(tmp_path):
    pytest.importorskip("qlib");pytest.importorskip("lightgbm")
    frame,fold,calendar=training_panel()
    raw_hash=horizon.feature_digest(frame["feature"])
    learned,processors,digest=horizon.prepare_shared_training(frame,fold,calendar)
    np.testing.assert_array_equal(processors["processors"][0]["mean_train"],frame["feature"].median().to_numpy())
    expected=(frame["label"].groupby("datetime").rank(pct=True)-.5)*3.46
    pd.testing.assert_frame_equal(learned["label"],expected,check_dtype=False)
    a=horizon.SharedArmDataset(learned,"H5",digest);b=horizon.SharedArmDataset(learned,"H20",digest)
    left=a.prepare("train",col_set=["feature","label"],data_key="learn")
    right=b.prepare("train",col_set=["feature","label"],data_key="learn")
    pd.testing.assert_frame_equal(left["feature"],right["feature"])
    assert list(left["label"].columns)==list(right["label"].columns)==["LABEL0"]
    np.testing.assert_array_equal(left["label"].iloc[:,0],expected.H5)
    np.testing.assert_array_equal(right["label"].iloc[:,0],expected.H20)
    assert not left["label"].equals(right["label"])
    first=fit_fixed_qlib_lightgbm(a,spec=FixedQlibLGBSpec())
    second=fit_fixed_qlib_lightgbm(b,spec=FixedQlibLGBSpec())
    assert a.consumed_feature_sha256==b.consumed_feature_sha256==digest
    assert a.consumed_label_sha256!=b.consumed_label_sha256
    assert first.booster.model_to_string()!=second.booster.model_to_string()
    assert horizon.feature_digest(frame["feature"])==raw_hash
    assert horizon.feature_digest(learned["feature"])==digest
    assert first.training_rows==second.training_rows==len(frame)


def test_later_feature_and_label_rows_do_not_affect_fitted_shared_training():
    frame,fold,calendar=training_panel()
    learned,payload,digest=horizon.prepare_shared_training(frame,fold,calendar)
    later=frame.iloc[:20].copy()*1000000
    later.index=index(calendar[[26]],tuple(f"SH{600000+n}" for n in range(20)))
    # Out-of-panel rows must be rejected before a processor could see them.
    with pytest.raises(ValueError,match="crosses"):
        horizon.prepare_shared_training(pd.concat([frame,later]),fold,calendar)
    again,payload2,digest2=horizon.prepare_shared_training(frame,fold,calendar)
    assert payload==payload2 and digest==digest2
    pd.testing.assert_frame_equal(learned,again)


def test_shared_feature_tamper_is_rejected_at_actual_native_prepare():
    frame,fold,calendar=training_panel();learned,_,digest=horizon.prepare_shared_training(frame,fold,calendar)
    view=horizon.SharedArmDataset(learned,"H20",digest)
    learned.loc[learned.index[0],("feature","f0")]+=1
    with pytest.raises(ValueError,match="feature bytes"):
        fit_fixed_qlib_lightgbm(view,spec=FixedQlibLGBSpec())


def test_feature_provenance_binds_index_order_columns_dtypes_and_bytes():
    frame,_,_=training_panel();features=frame["feature"]
    digest=horizon.feature_digest(features)
    assert digest!=horizon.feature_digest(features.iloc[::-1])
    assert digest!=horizon.feature_digest(features.astype(np.float64))
    changed=features.copy();changed.iloc[0,0]+=1
    assert digest!=horizon.feature_digest(changed)
    assert hashlib.sha256(horizon._index_bytes(features.index)).hexdigest()!=hashlib.sha256(horizon._index_bytes(features.index[::-1])).hexdigest()


def test_inference_signal_eligibility_does_not_require_unknown_future_labels():
    days=pd.bdate_range("2024-01-01",periods=45);source=eligibility(days,missing=[1,6,21]);pool={"SH600000":[(days[0],days[-1])]}
    i=index(days[[0,-1]])
    keep,audit=source.mask(i,pool)
    assert keep.tolist()==[True,True] and audit["label_endpoint_checks"] is False


def test_training_row_validation_requires21_not_six_future_sessions():
    frame,fold,calendar=training_panel()
    fold["data_cutoff"]=calendar[40].date().isoformat()  #+6 fits, +21 does not.
    with pytest.raises(ValueError,match="21-session"):
        horizon.validate_common_training_rows(frame,fold,calendar)


def test_official_loader_explicitly_requests_both_targets_without_changing_old_label(monkeypatch):
    import qlib
    from qlib.contrib.data import handler
    from stopat30m.ashare_forward import LABEL
    frame,fold,calendar=training_panel();calendar=pd.bdate_range("2020-01-01",periods=160)
    # Reindex the synthetic frame into the requested loader span.
    frame=frame.iloc[:20].copy();frame.index=index(calendar[[90]],tuple(f"SH{600000+n}" for n in range(20)))
    captured={}
    class FakeAlpha158:
        def __init__(self,**kwargs):captured.update(kwargs)
        def fetch(self,**kwargs):return frame
    monkeypatch.setattr(qlib,"init",lambda **kwargs:None)
    monkeypatch.setattr(handler,"Alpha158",FakeAlpha158)
    result=horizon._raw_features(SimpleNamespace(dataset_dir=Path("/synthetic")),calendar,
        {"SH600000":[(calendar[90],calendar[100])]},str(calendar[90].date()),str(calendar[100].date()))
    assert captured["label"]==(list(horizon.LABELS.values()),list(horizon.MODELS))
    assert result is frame and LABEL=="Ref($open,-6)/Ref($open,-1)-1"


def test_real_official_qlib_labels_use_t_plus6_and21_even_after_index_exit(tmp_path):
    pytest.importorskip("qlib")
    calendar=pd.bdate_range("2020-01-01",periods=180)
    (tmp_path/"calendars").mkdir();(tmp_path/"calendars/day.txt").write_text("\n".join(calendar.strftime("%Y-%m-%d"))+"\n")
    (tmp_path/"instruments").mkdir();(tmp_path/"instruments/all.txt").write_text(f"SH600000\t{calendar[0].date()}\t{calendar[-1].date()}\n")
    folder=tmp_path/"features/sh600000";folder.mkdir(parents=True)
    opening=(10+np.arange(len(calendar))*.1).astype(np.float32)
    for field,values in {"open":opening,"close":opening,"vwap":opening,"high":opening+.1,
        "low":opening-.1,"volume":np.full(len(calendar),1000,np.float32),"factor":np.ones(len(calendar),np.float32)}.items():
        np.r_[np.float32(0),values].astype("<f4").tofile(folder/(field+".day.bin"))
    intervals={"SH600000":[(calendar[100],calendar[103])]}
    raw=horizon._raw_features(SimpleNamespace(dataset_dir=tmp_path),calendar,intervals,
        calendar[100].date().isoformat(),calendar[103].date().isoformat())
    assert len(raw)==4 and list(raw["label"].columns)==["H5","H20"]
    expected5=opening[np.arange(100,104)+6]/opening[np.arange(100,104)+1]-1
    expected20=opening[np.arange(100,104)+21]/opening[np.arange(100,104)+1]-1
    np.testing.assert_array_equal(raw["label"]["H5"].to_numpy(),expected5)
    np.testing.assert_array_equal(raw["label"]["H20"].to_numpy(),expected20)
    horizon._verify_raw_labels(raw,SimpleNamespace(dataset_dir=tmp_path),calendar)
