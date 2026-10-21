"""A13 frozen orchestration and qualification tests on synthetic records only."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from stopat30m import ashare_sparse_exposure_experiment as x
from stopat30m import ashare_exposure_matching_experiment as a12
from test_ashare_exposure_matching_experiment import (
    declaration_case as a12_case, _diagnostics, _write,
)

ARMS=(*x.CONTROL_ARMS,*x.CANDIDATES)
PERIODS=x.PERIODS


@pytest.fixture
def case(a12_case,monkeypatch):
    c=a12_case
    a12.freeze_exposure_matching_declaration(**c.arguments)
    parent=c.arguments['output'];reference=c.tmp/'a12-complete'
    _write(reference/'declaration.json',{})
    (reference/'declaration.json').write_bytes(parent.read_bytes())
    controls={}
    for arm in x.CONTROL_ARMS:
        for period in PERIODS:
            result=deepcopy(c.references['hold10_cap2' if arm=='matched_original' else arm,period])
            if arm=='matched_original':
                result['records']['exposure_matching']=[{'session':'2026-10-19','target':'.8'}]
                for index,row in enumerate(result['records']['trades']):row['order_purpose']='core' if index==0 else 'cash_control'
            controls[arm,period]=result
            a12._write_path(result,reference/arm/period,arm,_diagnostics([.8],start='2026-10-19'))
    _write(reference/'report.json',{'kind':'a12_lagged_exposure_matching_results'})
    _write(reference/'status.json',{'phase':'completed'})
    _write(reference/'manifest.json',{'kind':'a12_lagged_exposure_matching_results','files':{
        str(p.relative_to(reference)):x._proof(p)['sha256'] for p in reference.rglob('*') if p.is_file()}})
    design=c.tmp/'a13-design.json';_write(design,json.loads((Path(__file__).parents[1]/'docs/research/a13_sparse_exposure_design_v1.json').read_bytes()))
    codes={}
    for key in ('a13_experiment','a13_validation','a13_execution'):
        p=c.tmp/(key+'.py');p.write_text('# synthetic implementation\n');codes[key]=p
    monkeypatch.setattr(x,'implementation_proofs',lambda:{k:x._proof(p) for k,p in codes.items()})
    monkeypatch.setattr(x,'verify_reused_stability_models',lambda *args:c.calls.append(('verify13',args)) or {'training_reperformed':False})
    monkeypatch.setattr(x,'load_stability_context',lambda *args:pytest.fail('No historical sources allowed in synthetic test'))
    c.arguments={'parent_declaration':parent,'reference_directory':reference,'design_path':design,'output':c.tmp/'a13-declaration.json'}
    c.reference,c.controls,c.codes=reference,controls,codes
    return c


def freeze(c):return x.freeze_sparse_exposure_declaration(**c.arguments)


def test_complete_design_frozen_before_any_candidate_return():
    design=json.loads((Path(__file__).parents[1]/'docs/research/a13_sparse_exposure_design_v1.json').read_bytes())
    x._check_design(design)
    assert design['any_new_historical_candidate_returns_computed'] is False
    assert design['budget']['reported_ledgers']==21 and design['budget']['old_full_reproductions_before_any_new_arms']==9
    assert design['automatic_promotion'] is False


@pytest.mark.parametrize('section,key,value',[
    (None,'automatic_promotion',True),(None,'initial_cash_cny','200000'),(None,'evaluation_end','2026-10-16'),
    (None,'candidates',[]),('cash_controller','no_trade_band_absolute_fraction','.02'),
    ('cash_controller','cadence_anchor','after-last-success'),('cash_controller','minimum_filter','redistribute'),
    ('matching_gate','mean_absolute_gap_max','.03'),('budget','native_fits',1),('budget','reported_ledgers',24),
])
def test_design_cannot_be_changed_post_result(section,key,value):
    d=json.loads((Path(__file__).parents[1]/'docs/research/a13_sparse_exposure_design_v1.json').read_bytes())
    (d if section is None else d[section])[key]=value
    with pytest.raises(ValueError):x._check_design(d)


def test_freeze_binds_every_parent_and_reference_and_preserves_active(case):
    c=case;before={p:p.read_bytes() for p in c.paper.iterdir()};s=freeze(c)
    parent=json.loads(c.arguments['parent_declaration'].read_bytes())
    assert all(s['bound_inputs'][k]==v for k,v in parent['bound_inputs'].items())
    assert s['source']==parent['source'] and s['active_paper']==parent['active_paper']
    assert s['fits_performed']==0 and s['predictions_regenerated'] is False
    assert s['active_state_is_observation_not_frozen_input'] is True
    assert all(p.read_bytes()==b for p,b in before.items())
    assert {p['path'] for p in s['bound_inputs'].values()} >= {str(p.resolve()) for p in c.reference.rglob('*') if p.is_file()}
    with pytest.raises(ValueError,match='Preserve'):freeze(c)


@pytest.mark.parametrize('fault',['a13_execution','a13_validation','a13_experiment','parent','design','reference','active_code'])
def test_changed_bound_bytes_stop_before_loading_or_output(case,fault):
    c=case;freeze(c)
    p=c.codes[fault] if fault in c.codes else c.arguments['parent_declaration'] if fault=='parent' else c.arguments['design_path'] if fault=='design' else c.reference/'matched_original/2026/exposure_matching.json' if fault=='reference' else c.active_code
    p.write_text('changed after freeze')
    with pytest.raises(ValueError):x.run_sparse_exposure_comparison(c.arguments['output'],c.tmp/'results')
    assert not (c.tmp/'results').exists()


@pytest.mark.parametrize('field',['source','active_paper','reference_directory','inherited_proof'])
def test_declaration_cannot_redirect_inherited_sources(case,field):
    c=case;freeze(c);p=c.arguments['output'];s=json.loads(p.read_bytes())
    if field=='source':s['source']['snapshot']=str(c.tmp/'other-snapshot')
    elif field=='inherited_proof':
        other=c.tmp/'other-protocol';other.write_text('{}');s['bound_inputs']['protocol']=x._proof(other)
    else:s[field]=str(c.tmp/('other-'+field))
    _write(p,s)
    with pytest.raises(ValueError):x.run_sparse_exposure_comparison(p,c.tmp/'results')
    assert not (c.tmp/'results').exists()


def test_active_output_tree_isolation(case):
    c=case;freeze(c)
    for target in (c.paper/'offline',c.paper.parent):
        with pytest.raises(ValueError):x.run_sparse_exposure_comparison(c.arguments['output'],target)
    with pytest.raises(ValueError):x.freeze_sparse_exposure_declaration(**{**c.arguments,'output':c.paper/'declaration-new.json'})


class Verified:
    @contextmanager
    def verified_batch(self):yield self
    def assert_unchanged(self):pass


@pytest.fixture
def run_case(case,monkeypatch):
    c=case;freeze(c)
    context={'bound':{},'pool':Verified(),'actions':Verified(),'score_coverage':{'untouched':True}}
    monkeypatch.setattr(x,'load_stability_context',lambda *args:context)
    def simulate(context,*,arm_key,period,reference_exposures):
        assert 'score_coverage' not in context
        assert reference_exposures=={'2026-10-19':_diagnostics([.8],start='2026-10-19')['daily_exposures'][0]}
        c.calls.append(('simulate13',arm_key,period))
        return deepcopy(c.controls[arm_key if arm_key in x.CONTROL_ARMS else 'matched_original',period])
    monkeypatch.setattr(x,'simulate_sparse_exposure',simulate)
    from stopat30m import ashare_position_count_metrics as metrics
    monkeypatch.setattr(metrics,'position_count_diagnostics',lambda result:_diagnostics([.8],start='2026-10-19'))
    original=x.assert_sparse_reference_reproduced
    def reproduce(result,folder):
        r=original(result,folder);c.calls.append(('reproduced13',Path(folder).parent.name,result['period']));return r
    monkeypatch.setattr(x,'assert_sparse_reference_reproduced',reproduce)
    c.simulate,c.context=simulate,context
    return c


def test_nine_exact_controls_precede_all_twelve_candidates_and_preserve_every_record(run_case):
    c=run_case;before={p:p.read_bytes() for p in c.paper.iterdir()};progress=[]
    r=x.run_sparse_exposure_comparison(c.arguments['output'],c.tmp/'results',progress=progress.append)
    assert [v[1:] for v in c.calls if v[0]=='simulate13']==[(a,p) for a in ARMS for p in PERIODS]
    first=c.calls.index(('simulate13',next(iter(x.CANDIDATES)),'2025'))
    assert len([v for v in c.calls[:first] if v[0]=='reproduced13'])==9
    assert len(r['results'])==21 and len(r['baseline_full_reproduction'])==9
    assert set(r['matching_by_arm'])=={'matched_original',*x.CANDIDATES}
    assert r['all_performance_valid'] is False and r['decision']['status']=='INCONCLUSIVE_INVALID_REFERENCE'
    assert r['native_fits']==r['live_broker_orders']==0
    assert r['predictions_regenerated'] is r['active_paper_changed_by_experiment'] is False
    assert all(p.read_bytes()==b for p,b in before.items()) and c.context['score_coverage']=={'untouched':True}
    out=c.tmp/'results';m=json.loads((out/'manifest.json').read_bytes())
    assert set(m['files'])=={str(p.relative_to(out)) for p in out.rglob('*') if p.is_file() and p.name!='manifest.json'}
    assert all(hashlib.sha256((out/n).read_bytes()).hexdigest()==digest for n,digest in m['files'].items())
    assert len([v for v in progress if v.get('completed')])==21
    for arm in ARMS:
        for period in PERIODS:
            folder=out/arm/period;s=json.loads((folder/'summary.json').read_bytes())
            assert s['sparse_policy']==x.CANDIDATES.get(arm)
            assert s['purpose_costs']['cash_control']['fill_count']==(1 if arm=='matched_original' or arm in x.CANDIDATES else 0)


@pytest.mark.parametrize('arm,record',[(a,n) for a in x.CONTROL_ARMS for n in ('curves','trades','positions','corporate_actions','holding_age','turnover_plans')]+[('cap7p5pct','capital_turnover'),('matched_original','exposure_matching')])
def test_any_original_record_change_blocks_all_candidates(run_case,monkeypatch,arm,record):
    c=run_case
    def bad(*args,**kwargs):
        r=c.simulate(*args,**kwargs)
        if kwargs['arm_key']==arm and kwargs['period']=='2026':r['records'][record][0]['mutated']=True
        return r
    monkeypatch.setattr(x,'simulate_sparse_exposure',bad)
    with pytest.raises(ValueError,match='Full original record'):x.run_sparse_exposure_comparison(c.arguments['output'],c.tmp/'results')
    assert not any(v[0]=='simulate13' and v[1] in x.CANDIDATES for v in c.calls)
    assert not (c.tmp/'results/report.json').exists()
    assert json.loads((c.tmp/'results/failure.json').read_bytes())['no_valid_comparison_claim'] is True


def decision_inputs():
    results=[]
    for a in ARMS:
        for p in PERIODS:
            results.append({'arm_key':a,'period':p,'performance_valid':True,
                'metrics':{'total_return_lower_exact':'.03' if a in x.CANDIDATES else '.02','max_drawdown_conservative_exact':'-.04' if a in x.CANDIDATES else '-.05','total_fee_cny':'50' if a in x.CANDIDATES else '100','slippage_cost_cny':'10'},
                'purpose_costs':{'cash_control':{'fill_count':5 if a in x.CANDIDATES else 10}}})
    matching={a:{p:{'status':'MATCHED','passed':True} for p in PERIODS} for a in ('matched_original',*x.CANDIDATES)}
    return results,matching


def test_matching_financial_and_all_cost_checks_required_without_auto_promotion():
    rows,matching=decision_inputs();r=x.decide_sparse_exposure(rows,matching)
    assert all(c['isolated_period_diagnostic_passed'] for c in r['comparisons'])
    assert r['strategy_promoted'] is r['model_retrained'] is r['new_future_OOS_claim'] is False
    for c in r['comparisons']:
        b=c['periods']['2025']['descriptive_net_bridge'];assert b['exact_additive_net_bridge'] and b['pure_cash_or_turnover_causal_claim'] is False
        assert not c['qualified_for_automatic_promotion']


@pytest.mark.parametrize('status',['MATCHING_FAILED','UNKNOWN_EXPOSURE_COVERAGE','INVALID_PATH'])
def test_unmatched_profit_does_not_count_as_improvement(status):
    rows,matching=decision_inputs();key=next(iter(x.CANDIDATES));matching[key]['2025']={'status':status,'passed':False}
    c=x.decide_sparse_exposure(rows,matching)['comparisons'][0]
    assert not c['isolated_period_diagnostic_passed']
    assert c['periods']['2025']['descriptive_net_bridge'] is None
    assert all(v is None for v in c['periods']['2025']['checks'].values())


@pytest.mark.parametrize('bad_arm',ARMS)
def test_any_invalid_path_blocks_that_period_only(bad_arm):
    rows,matching=decision_inputs();next(r for r in rows if r['arm_key']==bad_arm and r['period']=='continuous')['performance_valid']=False
    r=x.decide_sparse_exposure(rows,matching);assert r['status']=='INCONCLUSIVE_INVALID_REFERENCE'
    for c in r['comparisons']:
        assert c['periods']['2025']['eligible_for_descriptive_comparison']
        if bad_arm in x.CONTROL_ARMS or c['arm_key']==bad_arm:
            assert c['periods']['continuous']['descriptive_net_bridge'] is None


@pytest.mark.parametrize('fault',['missing_path','duplicate','unknown_arm','missing_matching','truthy_valid','truthy_gate','conflicting_gate'])
def test_fixed_unique_paths_and_explicit_flags_required(fault):
    rows,m=decision_inputs()
    if fault=='missing_path':rows.pop()
    elif fault=='duplicate':rows.append(deepcopy(rows[0]))
    elif fault=='unknown_arm':rows[-1]['arm_key']='posthoc'
    elif fault=='missing_matching':m.pop(next(iter(x.CANDIDATES)))
    elif fault=='truthy_valid':rows[-1]['performance_valid']=1
    elif fault=='truthy_gate':m['matched_original']['2025']['passed']=1
    else:m['matched_original']['2025']['passed']=False
    with pytest.raises(ValueError):x.decide_sparse_exposure(rows,m)


def test_net_bridge_preserves_long_serialized_decimals_exactly():
    from decimal import Decimal, localcontext
    rows,matching=decision_inputs()
    values={'hold10_cap2':'0.1234567890123456789012345678','cap7p5pct':'100.1234567890123456789012345678','matched_original':'0.0000000000000000000000000001'}
    for row in rows:
        row['metrics']['total_return_lower_exact']=values.get(row['arm_key'],'0.9876543210987654321098765432')
    r=x.decide_sparse_exposure(rows,matching)
    for c in r['comparisons']:
        b=c['periods']['2025']['descriptive_net_bridge']
        with localcontext() as precision:
            precision.prec=80
            assert Decimal(b['candidate_cash_control_related_net_change_including_cost_and_path'])+Decimal(b['remaining_cap75_minus_candidate_net_gap'])==Decimal(b['raw_cap75_minus_original_net'])
