"""Plot guards use explicit synthetic diagnostics, not model performance data."""
import hashlib
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

spec=importlib.util.spec_from_file_location('signal_plot',Path(__file__).parents[1]/'scripts/analyze_model_signal_diagnostics.py')
plot=importlib.util.module_from_spec(spec);spec.loader.exec_module(plot)


def source(tmp_path):
    root=tmp_path/'source';root.mkdir();rows=[];original={};comparisons={}
    for year in (2025,2026):
        day=f'{year}-01-02';end=f'{year}-01-10'
        original[str(year)]={'daily':[{'signal_date':day,'label_end':end,'pairs':100,'rank_IC':.05}]}
        comparisons[str(year)]={'original_full_dictionary_exactly_reproduced':True,'rank_IC_mean':.05,
            'tail_metrics_on_exact_original_dates':{g:{'complete_top20_days':1,'top20_gross_label_excess_mean':-.002} for g in ('union','signal_close_affordable_nonST_trading')}}
        for group in ('union','signal_close_affordable_nonST_trading'):
            rows.append({'signal_date':day,'execution_year':year,'month':day[:7],'horizon':5,'label_end':end,'group':group,'labelled_pairs':100,'rank_IC':.05,'top20_complete':True,'top20_gross_label_excess':-.002})
    pd.DataFrame(rows).to_csv(root/'signal_daily.csv',index=False)
    (root/'signal_original_quality_reproduction.json').write_text(json.dumps({'original_full_dictionaries':original,'comparisons':comparisons}))
    seal(root);return root


def seal(root):
    (root/'signal_completion_manifest.json').write_text(json.dumps({'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file() and p.name!='signal_completion_manifest.json'}}))


def test_matched_year_dates_and_complete_head_reconcile(tmp_path):
    root=source(tmp_path);frame,_,_=plot.load_original_matched_diagnostics(root)
    assert len(frame)==4 and set(frame.execution_year)=={2025,2026}


def test_changed_csv_rejected_before_plotting(tmp_path):
    root=source(tmp_path);(root/'signal_daily.csv').write_text('changed')
    with pytest.raises(ValueError,match='changed'):plot.load_original_matched_diagnostics(root)


@pytest.mark.parametrize('fault',['label_end','missing_day','rank_ic','head_mean'])
def test_resealed_but_inconsistent_diagnostics_rejected(tmp_path,fault):
    root=source(tmp_path);p=root/'signal_daily.csv';frame=pd.read_csv(p)
    if fault=='label_end':frame.loc[0,'label_end']='2025-02-01'
    elif fault=='missing_day':frame=frame.drop(index=0)
    elif fault=='rank_ic':frame.loc[0,'rank_IC']=.6
    else:frame.loc[0,'top20_gross_label_excess']=.1
    frame.to_csv(p,index=False);seal(root)
    with pytest.raises(ValueError):plot.load_original_matched_diagnostics(root)


def test_render_preserves_all_source_bytes_and_refuses_overwrite_and_forward(tmp_path):
    root=source(tmp_path);before={p.name:p.read_bytes() for p in root.iterdir()};out=tmp_path/'plots'
    plot.render(root,out)
    assert len(list(out.glob('*.png')))==len(list(out.glob('*.svg')))==2
    assert all((root/name).read_bytes()==data for name,data in before.items())
    m=json.loads((out/'manifest.json').read_bytes());assert m['no_fit_or_strategy_simulation']
    assert all(plot._sha(out/name)==digest for name,digest in m['files'].items())
    with pytest.raises(ValueError):plot.render(root,out)
    with pytest.raises(ValueError):plot.render(root,tmp_path/'forward'/'paper')
