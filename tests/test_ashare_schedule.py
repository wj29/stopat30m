"""A successful-looking result cannot bypass immutable activation evidence."""
import hashlib
import json

import pytest

from stopat30m.ashare_schedule import verify_activation


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def comparison(root,*,total_return=.1,drawdown=-.1,incomplete=0,source_change=False):
    root.mkdir()
    source=root/"source.txt";source.write_text("original")
    train=root/"training";train.mkdir();(train/"manifest.json").write_text(json.dumps({"files":{}}))
    report={"comparison_complete":True,"activation_passed":True,"spec":{"parameter_search":False,
        "activation_gates":{"model_absolute_max_drawdown_at_most":"0.20"}},
        "metrics":{"lightgbm":{"total_return":total_return,"max_drawdown":drawdown,"incomplete_valuation_days":incomplete,"trade_count":1},"momentum":{},"equal_weight":{}},
        "training_origin":{"path":str(train/"manifest.json"),"sha256":digest(train/"manifest.json")}}
    (root/"report.json").write_text(json.dumps(report))
    (root/"protocol.json").write_text(json.dumps({"repair_audit":{"count":36}}))
    (root/"reference_price_audit.json").write_text(json.dumps({"status":"passed"}))
    receipt={"path":str(source),"sha256":digest(source)}
    (root/"manifest.json").write_text(json.dumps({"activation_passed":True,"files":{p.name:digest(p) for p in root.glob('*.json')},"bound_inputs":{"source":receipt}}))
    if source_change:source.write_text("revised")
    return root


def test_activation_checks_actual_files_and_chronological_model_receipt(tmp_path):
    root=comparison(tmp_path/"valid")
    assert verify_activation(root)["activation_passed"]
    (root/"report.json").write_text("tampered")
    with pytest.raises(ValueError,match="file changed"):
        verify_activation(root)


@pytest.mark.parametrize("kwargs",[{"total_return":-.01},{"drawdown":-.21},{"incomplete":1}])
def test_positive_gate_boolean_cannot_override_failed_actual_metrics(tmp_path,kwargs):
    root=comparison(tmp_path/"failed",**kwargs)
    with pytest.raises(ValueError,match="gates did not pass"):
        verify_activation(root)


def test_activation_refuses_changed_underlying_source(tmp_path):
    root=comparison(tmp_path/"revised",source_change=True)
    with pytest.raises(ValueError,match="implementation changed"):
        verify_activation(root)
