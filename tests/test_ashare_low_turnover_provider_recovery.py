"""Offline provider selection/contract boundaries for explicit source revisions.

These tests do not migrate a paper. Recovery-event authorization and the full
factor-revision loader have separate suites; here a returned event proof still
has to pass the provider's original refresh/snapshot loader.
"""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stopat30m import ashare_low_turnover_provider as provider
from stopat30m import ashare_low_turnover_recovery as recovery
from stopat30m.data import qlib_factor_revision as factor
from stopat30m.data.qlib_snapshot import load_qlib_snapshot
from test_ashare_low_turnover_provider import case as provider_case, refresh, small
from test_qlib_incremental import make_fixture


def saved_refresh(case, session, *, revised=False):
    value = {"kind":"low_turnover_union_close_increment","session":session,
        "bound_inputs":[case.evidence],"snapshot_path":str(case.parent.path),
        "snapshot_manifest_sha256":case.parent.manifest_sha256,
        "current_index_instruments":case.pool,"fixture_revision":revised}
    value["result_sha256"] = provider._digest(value)
    path = (case.tmp/'explicit-correction.json' if revised else
            case.provider.output_dir/'daily'/session/'refresh_result.json')
    return value, path, provider._save(path,value)


def test_without_recovery_event_original_latest_result_and_files_are_unchanged(provider_case):
    case=provider_case
    old,path,_=saved_refresh(case,'2026-10-20')
    before={p:p.read_bytes() for p in [case.paper/'state.json',case.paper/'manifest.json',path]}
    actual,chosen=case.provider._latest_refresh('2026-10-21')
    assert actual==old and chosen==path
    assert all(p.read_bytes()==value for p,value in before.items())
    assert not case.sources  # Reading saved parents must not instantiate any source client.


def test_without_event_and_without_refresh_original_initial_parent_is_used(provider_case):
    case=provider_case
    value,path=case.provider._latest_refresh('2026-10-20',before=True)
    assert path is None and value['snapshot_path']==str(case.parent.path)
    assert value['session']=='2026-10-19'
    assert not case.sources


def test_explicit_verified_revision_wins_only_same_session_tie(provider_case,monkeypatch):
    case=provider_case
    _,old_path,_=saved_refresh(case,'2026-10-20')
    revised,new_path,proof=saved_refresh(case,'2026-10-20',revised=True)
    monkeypatch.setattr(recovery,'source_repair',lambda state:proof)
    value,path=case.provider._latest_refresh('2026-10-21')
    assert value==revised and path==new_path and path!=old_path
    newer,newer_path,_=saved_refresh(case,'2026-10-21')
    value,path=case.provider._latest_refresh('2026-10-22')
    assert value==newer and path==newer_path
    assert not case.sources


@pytest.mark.parametrize('before',[False,True])
def test_before_flag_applies_equally_to_normal_and_explicit_repaired_parent(provider_case,monkeypatch,before):
    case=provider_case
    saved_refresh(case,'2026-10-20')
    revised,path,proof=saved_refresh(case,'2026-10-20',revised=True)
    monkeypatch.setattr(recovery,'source_repair',lambda state:proof)
    value,actual=case.provider._latest_refresh('2026-10-20',before=before)
    if before:
        assert actual is None and value['session']=='2026-10-19'
    else:
        assert actual==path and value==revised
    assert not case.sources


def test_future_dated_explicit_revision_cannot_enter_an_earlier_query(provider_case,monkeypatch):
    case=provider_case
    previous,path,_=saved_refresh(case,'2026-10-20')
    _,_,proof=saved_refresh(case,'2026-10-22',revised=True)
    monkeypatch.setattr(recovery,'source_repair',lambda state:proof)
    value,actual=case.provider._latest_refresh('2026-10-21')
    assert value==previous and actual==path


@pytest.mark.parametrize('defect',['proof','digest','snapshot','pool','bound_original'])
def test_event_revision_still_passes_original_source_and_snapshot_checks(provider_case,monkeypatch,defect):
    case=provider_case
    value,path,proof=saved_refresh(case,'2026-10-20',revised=True)
    if defect=='proof':proof={**proof,'sha256':'a'*64}
    else:
        if defect=='digest':value['result_sha256']='b'*64
        elif defect=='snapshot':value['snapshot_manifest_sha256']='c'*64
        elif defect=='pool':value['current_index_instruments']=list(reversed(case.pool))
        else:value['bound_inputs']=[{**case.evidence,'sha256':'d'*64}]
        if defect!='digest':value['result_sha256']=provider._digest({k:v for k,v in value.items() if k!='result_sha256'})
        proof=provider._save(path,value)
    monkeypatch.setattr(recovery,'source_repair',lambda state:proof)
    with pytest.raises(ValueError):case.provider._latest_refresh('2026-10-21')
    assert not case.sources


def test_invalid_event_is_not_silently_ignored_for_a_normal_saved_parent(provider_case,monkeypatch):
    case=provider_case;saved_refresh(case,'2026-10-21')
    def invalid(state):raise ValueError('recovery event bytes differ')
    monkeypatch.setattr(recovery,'source_repair',invalid)
    with pytest.raises(ValueError,match='recovery event bytes'):case.provider._latest_refresh('2026-10-22')
    assert not case.sources


def test_absence_of_increment_manifest_requires_no_revision_loader(tmp_path,monkeypatch):
    def unused(*args):raise AssertionError('initial parent has no prior source unit')
    monkeypatch.setattr(factor,'load_verified_revision_batch',unused)
    parent=SimpleNamespace(dataset_dir=tmp_path)
    assert provider.Provider._normalization_continuation(parent)==(None,[])


@pytest.fixture
def derived_parent(tmp_path,monkeypatch):
    _,batch,_,_=make_fixture(tmp_path)
    contract=provider._save(tmp_path/'factor-contract.json',{'synthetic_verified_contract':True})
    source=provider._save(tmp_path/'source-original.json',{'original':True})
    evidence={'verified_factor_revision':contract,'bound_inputs':[source],'derived_effective_factor_revision':True}
    previous=replace(batch,source='baostock',generation='explicit-derived-generation',evidence=evidence)
    path=tmp_path/'parent/incremental_manifest.json'
    increment={'source':previous.source,'generation':previous.generation,'source_evidence':deepcopy(evidence)}
    provider._save(path,increment)
    calls=[]
    def load(path):
        calls.append(str(path))
        return previous,{'revision_contract':contract,'saved_original':source}
    monkeypatch.setattr(factor,'load_verified_revision_batch',load)
    return SimpleNamespace(parent=SimpleNamespace(dataset_dir=path.parent),path=path,previous=previous,
        contract=contract,source=source,increment=increment,calls=calls)


def test_verified_derived_parent_uses_exact_reconstructed_batch_and_parent_increment_bytes(derived_parent):
    case=derived_parent;before=case.path.read_bytes()
    continuation,proofs=provider.Provider._normalization_continuation(case.parent)
    assert continuation.previous_batch is case.previous
    assert continuation.parent_increment_sha256==provider._proof(case.path)['sha256']
    assert case.calls==[case.contract['path']]
    assert {(p['path'],p['sha256']) for p in proofs}=={
        (case.contract['path'],case.contract['sha256']),(case.source['path'],case.source['sha256']),
        (str(case.path),provider._proof(case.path)['sha256'])}
    assert case.path.read_bytes()==before


@pytest.mark.parametrize('field',['source','generation','source_evidence'])
def test_verified_factor_contract_cannot_be_routed_to_another_parent_identity(derived_parent,field):
    case=derived_parent
    increment=deepcopy(case.increment)
    if field=='source_evidence':increment[field]['different_parent_fact']=True
    else:increment[field]='another-source-or-generation'
    provider._save(case.path,increment)
    with pytest.raises(ValueError,match='Derived parent source differs'):
        provider.Provider._normalization_continuation(case.parent)
    assert len(case.calls)==1


def test_bad_factor_contract_proof_fails_before_loader(derived_parent):
    case=derived_parent
    case.increment['source_evidence']['verified_factor_revision']['sha256']='f'*64
    provider._save(case.path,case.increment)
    with pytest.raises(ValueError):provider.Provider._normalization_continuation(case.parent)
    assert not case.calls


def test_factor_loader_rejection_does_not_fallback_to_unverified_capture(derived_parent,monkeypatch):
    case=derived_parent
    def invalid(path):raise ValueError('official factor source guards differ')
    monkeypatch.setattr(factor,'load_verified_revision_batch',invalid)
    with pytest.raises(ValueError,match='official factor source guards'):
        provider.Provider._normalization_continuation(case.parent)


def test_original_capture_continuation_branch_remains_readonly_and_uses_saved_guards(provider_case,monkeypatch):
    case=provider_case;small(case,monkeypatch)
    result,_=refresh(case)
    parent=load_qlib_snapshot(result['snapshot_path'])
    prior_calls=len(case.sources)
    incremental=parent.dataset_dir/'incremental_manifest.json'
    saved=json.loads(incremental.read_text())
    assert 'verified_factor_revision' not in saved['source_evidence']
    continuation,proofs=provider.Provider._normalization_continuation(parent)
    previous=continuation.previous_batch
    assert previous.source==saved['source'] and previous.generation==saved['generation']
    assert previous.evidence==saved['source_evidence']
    assert continuation.parent_increment_sha256==provider._proof(incremental)['sha256']
    assert any(Path(p['path']).name=='capture.json' for p in proofs)
    assert len(case.sources)==prior_calls
