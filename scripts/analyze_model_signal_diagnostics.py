"""Render source-bound model ranking diagnostics; never fit or simulate."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def load_original_matched_diagnostics(directory):
    directory=Path(directory).resolve()
    manifest=json.loads((directory/'signal_completion_manifest.json').read_bytes())
    for name,digest in manifest['files'].items():
        p=(directory/name).resolve()
        if not p.is_relative_to(directory) or _sha(p)!=digest:
            raise ValueError('Diagnostic original changed or escapes directory')
    r=json.loads((directory/'signal_original_quality_reproduction.json').read_bytes())
    if any(v.get('original_full_dictionary_exactly_reproduced') is not True for v in r['comparisons'].values()):
        raise ValueError('All original quality dictionaries must reproduce first')
    raw=pd.read_csv(directory/'signal_daily.csv')
    rows=[]
    for year in (2025,2026):
        original=r['original_full_dictionaries'][str(year)]['daily']
        dates={v['signal_date'] for v in original}
        subset=raw.loc[(raw.execution_year==year)&(raw.horizon==5)&raw.signal_date.isin(dates)].copy()
        union=subset.loc[subset.group=='union'].set_index('signal_date')
        if set(union.index)!=dates or not union.index.is_unique:
            raise ValueError('Same original daily dates must be complete and unique')
        for v in original:
            row=union.loc[v['signal_date']]
            if row.label_end!=v['label_end'] or row.labelled_pairs!=v['pairs'] or abs(row.rank_IC-v['rank_IC'])>2e-15:
                raise ValueError('Original dates, pairs, label maturity or IC differs')
        for group in ('union','signal_close_affordable_nonST_trading'):
            part=subset.loc[subset.group==group]
            if len(part)!=len(original):raise ValueError('Every slice retains all original dates')
            known=part.loc[part.top20_complete.eq(True)]
            expected=r['comparisons'][str(year)]['tail_metrics_on_exact_original_dates'][group]
            if len(known)!=expected['complete_top20_days'] or abs(known.top20_gross_label_excess.mean()-expected['top20_gross_label_excess_mean'])>1e-14:
                raise ValueError('Complete-head diagnostics do not reconcile')
        rows.append(subset)
    return pd.concat(rows,ignore_index=True),r,manifest


def render(directory,output):
    directory,output=Path(directory).resolve(),Path(output).resolve()
    if output.exists():raise ValueError('Preserve prior plots; use a new output directory')
    if output.is_relative_to(directory) or directory.is_relative_to(output):raise ValueError('Keep plot output separate from source')
    if 'forward' in output.parts:raise ValueError('Offline plots cannot target forward paper directories')
    rows,r,manifest=load_original_matched_diagnostics(directory)
    before={name:_sha(directory/name) for name in manifest['files']}
    output.mkdir(parents=True)
    fig,axes=plt.subplots(1,2,figsize=(12,4.5),layout='constrained')
    years=(2025,2026);xs=np.arange(2)
    axes[0].bar(xs,[r['comparisons'][str(y)]['rank_IC_mean'] for y in years],color='#3565a3')
    axes[0].set(xticks=xs,xticklabels=['2025','2026 to Oct19'],ylabel='Daily mean Spearman correlation',title='Global rank IC: original mature dates')
    for j,group in enumerate(('union','signal_close_affordable_nonST_trading')):
        values=[r['comparisons'][str(y)]['tail_metrics_on_exact_original_dates'][group]['top20_gross_label_excess_mean']*10000 for y in years]
        axes[1].bar(xs+(j-.5)*.32,values,width=.32,label=('Historical union','Signal-close affordable / non-ST / trading')[j])
    axes[1].set(xticks=xs,xticklabels=['2025','2026 to Oct19'],ylabel='Mean next-open 5-session label excess (bp)',title='Top20 minus same-date universe mean')
    axes[1].legend(fontsize=8);axes[1].axhline(0,color='black',lw=.6)
    for ax in axes:ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle('A14 | Positive global ranking does not prove a profitable top20\nOriginal A5 predictions; same A6 mature dates; overlapping gross labels, not net portfolio returns',fontsize=11)
    for ext in ('png','svg'):fig.savefig(output/('model_global_ic_vs_top20.'+ext),dpi=175)
    plt.close(fig)
    monthly=rows.loc[rows.group=='union'].groupby('month').agg(rank_IC=('rank_IC','mean'),top20_excess=('top20_gross_label_excess','mean'),complete_days=('top20_gross_label_excess','count'))
    fig,axes=plt.subplots(2,1,figsize=(13,6.5),sharex=True,layout='constrained')
    xs=np.arange(len(monthly));axes[0].bar(xs,monthly.rank_IC,color='#3565a3');axes[0].set(ylabel='Mean daily Spearman IC',title='Global ranking by month')
    axes[1].bar(xs,monthly.top20_excess*10000,color='#56755b');axes[1].set(ylabel='Mean overlapping 5-session label excess (bp)',xlabel='Signal month; final 2026 month is partial',title='Top20 advantage by month; incomplete heads remain unknown',xticks=xs,xticklabels=monthly.index.tolist())
    axes[1].tick_params(axis='x',rotation=55,labelsize=8)
    for ax in axes:ax.axhline(0,color='black',lw=.6);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle('A14 | Original annual maturity windows; no labels selected before ranking; no compounded-profit claim',fontsize=11)
    for ext in ('png','svg'):fig.savefig(output/('model_monthly_signal_stability.'+ext),dpi=175)
    plt.close(fig)
    rows.to_csv(output/'plot_input.csv',index=False)
    for name in ('signal_completion_manifest.json','signal_original_quality_reproduction.json'):
        shutil.copyfile(directory/name,output/name)
    if before!={name:_sha(directory/name) for name in manifest['files']}:raise ValueError('Source changed during rendering')
    shutil.copyfile(__file__,output/Path(__file__).name)
    (output/'manifest.json').write_text(json.dumps({'kind':'a14_original_model_signal_plots','source':str(directory),'source_files_sha256':before,'no_fit_or_strategy_simulation':True,'files':{p.name:_sha(p) for p in output.iterdir() if p.is_file()}},indent=2)+'\n')
    return output


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--diagnostic-dir',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True);a=p.parse_args();print(render(a.diagnostic_dir,a.output_dir))
