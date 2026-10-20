from copy import deepcopy

import pytest

from stopat30m.ashare_union_cost_diagnostics import (
    SCENARIOS, assert_baseline_reproduced, scenario_context,
)
from stopat30m.ashare_union_validation import EXECUTION


def test_sensitivities_do_not_mutate_verified_context_or_change_other_terms():
    original = {"protocol": {"execution": deepcopy(EXECUTION), "training": {"fixed": True}},
                "raw": object(), "scores": object(), "score_coverage": {"old": "cache"}}
    for name, differences in SCENARIOS.items():
        changed = scenario_context(original, name)
        assert changed["raw"] is original["raw"]
        assert changed["scores"] is original["scores"]
        assert "score_coverage" not in changed
        assert changed["protocol"]["training"] == {"fixed": True}
        assert {key: value for key, value in changed["protocol"]["execution"].items()
                if key not in differences} == {key: value for key, value in EXECUTION.items()
                                              if key not in differences}
        changed["protocol"]["training"]["fixed"] = False
    assert original["protocol"] == {"execution": EXECUTION, "training": {"fixed": True}}
    with pytest.raises(ValueError, match="Undeclared"):
        scenario_context(original, "choose_best_net_return")


@pytest.mark.parametrize("field", ["metrics", "performance_valid", "final_state"])
def test_baseline_mismatch_stops_before_interpreting_counterfactuals(field):
    result = {"metrics": {"nav": "100"}, "performance_valid": True, "final_state": {"cash": "100"}}
    reference = deepcopy(result)
    assert_baseline_reproduced(result, reference, reference["final_state"])
    result[field] = {"nav": "101"} if field == "metrics" else (False if field == "performance_valid" else {"cash": "101"})
    with pytest.raises(ValueError, match="does not reproduce"):
        assert_baseline_reproduced(result, reference, reference["final_state"])
