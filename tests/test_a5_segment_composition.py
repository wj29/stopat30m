"""Synthetic capture-identity tests; no provider downloads or portfolio PnL."""
from datetime import date
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from stopat30m.data import a5_segment_composition as c

A, B = "SH600000", "SZ300001"
DAYS = ["2017-01-03", "2017-12-29", "2018-12-28", "2019-12-31", "2020-01-02", "2021-12-31",
        "2022-12-30", "2023-01-03", "2025-06-02", "2026-10-19"]
WINDOWS = [("2017-01-03", "2019-12-31"), ("2020-01-01", "2022-12-31"), ("2023-01-01", "2026-10-19")]


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def collection(root, guards, targets, received, start, end, *, partial=False, empty_before=None):
    root.mkdir();generation=root.name
    manifest = dict(schema_version=1, source="baostock", generation=generation, as_of_date="2026-10-20",
        instruments=targets, request_start=start, request_end=end, target_selection={"kind":"explicit_download_targets"},
        responses={}, failures={"SZ999999":{"error":"source stopped"}} if partial else {},
        status="partial_not_publishable" if partial else "complete")
    for symbol in received:
        for mode, flag in c.MODES.items():
            request=dict(code=symbol[:2].lower()+"."+symbol[2:],fields=",".join(c.FIELDS),start_date=start,
                end_date=end,frequency="d",adjustflag=flag)
            rows=[]
            for day in DAYS:
                if start <= day <= end and (empty_before is None or day >= empty_before):
                    rows.append([day,request["code"],*( ["10","11","9","10"] if mode=="raw" else ["20","22","18","20"]),
                        "100","1000",flag,"1","0"])
            fetched="2026-10-20T01:00:02+00:00"
            payload=dict(request=request,fields=c.FIELDS,rows=rows,generation=generation,fetched_at=fetched)
            key=symbol+"_"+mode;proof=save(root/(key+".json"),payload)
            manifest["responses"][key]=dict(file=key+".json",sha256=proof["sha256"],rows=len(rows),fetched_at=fetched)
            echo={**request,"adjustflag":""}
            guard=dict(kind="baostock_daily_server_echo_guard",request=request,server_echo=echo,server_error_code="0",
                status="verified",rows_verified=len(rows),complete_row_iteration_observed=True,
                row_mode_flags_repaired=False,row_mode_anomalies_observed=[],mode_server_echo_present=False,
                started_at="2026-10-20T01:00:00+00:00",finished_at="2026-10-20T01:00:01+00:00")
            save(guards/(root.name+"_"+key+".json"),guard)
    save(root/"collection.json",manifest)
    if not partial:return root
    snapshot=save(root/"stopped_manifest_snapshot.json",manifest)
    stop=dict(kind="a5_stability_test_transport_stop_receipt",status="stopped_after3_consecutive_failures_no_automatic_reconnect",
        original_manifest_snapshot=snapshot,source_generation=generation,success_count=len(manifest["responses"]),
        raw_originals_modified=False,old_snapshots_modified=False,failures=manifest["failures"],
        original_successful_responses=[dict(key=key,record=record,original_response=dict(path=str((root/record["file"]).resolve()),sha256=record["sha256"]))
            for key,record in manifest["responses"].items()])
    save(root/"stop_receipt.json",stop)
    return {"directory":root,"stop_receipt_path":root/"stop_receipt.json"}


def fixture(tmp_path, *, late_ipo=False):
    guards=tmp_path/"guards";guards.mkdir()
    old=collection(tmp_path/"old",guards,[A,B],[A],"2017-01-01",DAYS[-1],partial=True)
    segments=[collection(tmp_path/("segment"+str(i)),guards,[B],[B],start,end,
        empty_before="2023-01-03" if late_ipo else None) for i,(start,end) in enumerate(WINDOWS)]
    calendar=tmp_path/"calendar.txt";calendar.write_text("\n".join(DAYS)+"\n")
    basic=tmp_path/"basic.csv";basic.write_text("code,ipoDate,outDate\nsh.600000,1999-11-10,\nsz.300001,"+
        ("2023-01-03" if late_ipo else "2009-10-30")+",\n")
    return dict(completed_collections=[old],segment_collections=segments,expected_instruments=[A,B],calendar=DAYS,
        parent_request_start=DAYS[0],parent_request_end=DAYS[-1],guard_directories=[guards],
        calendar_path=calendar,stock_basic_path=basic,receipt_dir=tmp_path/"result")


def rewrite_response(args, root, symbol, mode, edit):
    manifest_path=root/"collection.json";manifest=json.loads(manifest_path.read_text());key=symbol+"_"+mode
    p=root/manifest["responses"][key]["file"];payload=json.loads(p.read_text());edit(payload)
    manifest["responses"][key]["sha256"]=save(p,payload)["sha256"];manifest["responses"][key]["rows"]=len(payload["rows"])
    save(manifest_path,manifest)


def test_archive_and_segments_preserve_values_requests_vintages_and_per_row_provenance(tmp_path):
    args=fixture(tmp_path);old_path=args["completed_collections"][0]["directory"]/"collection.json";old=old_path.read_bytes()
    raw,hfq,receipt=c.compose_segmented_rebuild_sources(**args)
    assert len(raw)==len(hfq)==2*len(DAYS)
    assert raw.loc[raw.instrument.eq(B),"close"].tolist()==["10"]*len(DAYS)
    assert hfq.loc[hfq.instrument.eq(B),"close"].tolist()==["20"]*len(DAYS)
    assert old_path.read_bytes()==old and json.loads(old)["status"]=="partial_not_publishable"
    assert receipt["securities"][A]["kind"]=="immutable_original_whole_request_pair"
    assert len(receipt["securities"][B]["sources"])==3
    assert receipt["request_start"]==DAYS[0] and not receipt["new_vendor_whole_request_claimed"]
    assert not receipt["atomic_supplier_vintage_verified"] and not receipt["supplier_generation_claimed"]
    rows=[json.loads(row) for row in Path(receipt["row_provenance"]["path"]).read_text().splitlines()]
    assert len(rows)==len(raw)+len(hfq) and all(len(row)==9 for row in rows)
    assert all(row[6] == row[7] and row[8] is False for row in rows)
    b=[row for row in rows if row[0]==B and row[1]=="raw"]
    assert [row[3] for row in b]==list(range(len(DAYS)))
    assert [row[5] for row in b]==[0,1,2,3,0,1,2,0,1,2]
    for security in receipt["securities"].values():
        for source in security["sources"]:
            for mode in source["modes"].values():
                assert mode["terminal_capture_guard"]["mode_server_echo_present"] is False


def test_verified_empty_preipo_segments_are_explicit_and_need_identity(tmp_path):
    args=fixture(tmp_path,late_ipo=True);raw,_,receipt=c.compose_segmented_rebuild_sources(**args)
    assert len(raw.loc[raw.instrument.eq(B)])==3
    assert receipt["securities"][B]["sources"][0]["empty_segment_identity_evidence"]["reason"].endswith("verified_ipo")


@pytest.mark.parametrize("damage",["sha","request","generation","mode","field","nonprice","date","terminal","guardtime","echo","guardrows"])
def test_bad_original_identity_or_cross_mode_terms_block_publication(tmp_path,damage):
    args=fixture(tmp_path);root=args["segment_collections"][0]
    if damage=="sha":(root/(B+"_raw.json")).write_text("{}");match="SHA"
    elif damage in {"request","generation","mode","field","nonprice","date"}:
        def edit(payload):
            if damage=="request":payload["request"]["end_date"]="2019-12-30"
            if damage=="generation":payload["generation"]="different"
            if damage=="mode":payload["rows"][0][8]="3"
            if damage=="field":payload["fields"]=payload["fields"][:-1]
            if damage=="nonprice":payload["rows"][0][6]="101"
            if damage=="date":payload["rows"][0][0]="2017-01-04"
        rewrite_response(args,root,B,"hfq",edit);match="differs|disagree"
    else:
        path=args["guard_directories"][0]/(root.name+"_"+B+"_raw.json");guard=json.loads(path.read_text())
        if damage=="terminal":guard["complete_row_iteration_observed"]=False
        if damage=="guardtime":guard["finished_at"]="2026-10-20T01:00:03+00:00"
        if damage=="echo":guard["server_echo"]["code"]="sz.999999"
        if damage=="guardrows":guard["rows_verified"]+=1
        save(path,guard);match="guard is not verified"
    with pytest.raises(ValueError,match=match):c.compose_segmented_rebuild_sources(**args)
    assert not args["receipt_dir"].exists()


def test_empty_inside_listing_is_not_a_suspension_or_zero_return(tmp_path):
    args=fixture(tmp_path,late_ipo=True)
    args["stock_basic_path"].write_text("code,ipoDate,outDate\nsh.600000,1999-11-10,\nsz.300001,2009-10-30,\n")
    with pytest.raises(ValueError,match="overlaps"):c.compose_segmented_rebuild_sources(**args)
    assert not args["receipt_dir"].exists()


def test_stop_archive_cannot_be_rewritten_as_complete_or_lose_a_success(tmp_path):
    args=fixture(tmp_path);root=args["completed_collections"][0]["directory"]
    path=root/"collection.json";manifest=json.loads(path.read_text());manifest["status"]="complete";save(path,manifest)
    with pytest.raises(ValueError,match="Stopped archive"):c.compose_segmented_rebuild_sources(**args)


def test_exact_inventory_not_count_and_no_cross_source_target_duplicates(tmp_path):
    args=fixture(tmp_path);args["expected_instruments"]=[A,"SZ300002"]
    with pytest.raises(ValueError,match="exact full-window target inventory"):c.compose_segmented_rebuild_sources(**args)


def test_source_mutation_during_composition_is_rejected_before_receipt(tmp_path,monkeypatch):
    args=fixture(tmp_path);original=c._response;changed=False
    def mutate(*a,**kw):
        nonlocal changed
        result=original(*a,**kw)
        if not changed:
            p=Path(result[1]["original_response"]["path"]);p.write_text(p.read_text()+" ");changed=True
        return result
    monkeypatch.setattr(c,"_response",mutate)
    with pytest.raises(ValueError,match="changed before local composition"):c.compose_segmented_rebuild_sources(**args)
    assert not args["receipt_dir"].exists()


def declare_fixture(args, tmp_path):
    old=args['completed_collections'][0];oldproof=save(tmp_path/'selection.json',{'instruments':[A,B],'selection_uses_returns_or_model_metrics':False})
    protocol=dict(kind='fixed42_three_nonoverlapping_segment_hard30_capture_protocol',financial_policy_relaxed=False,
        synthetic_vendor_whole_request_claimed=False,automatic_reconnect=False,stop_after_consecutive_failures=3,
        hard_timeout_seconds_per_sdk_call=30,actual_min_interval_seconds_including_lazy_pages=2,
        created_at='2026-10-20T00:00:00+00:00',batch_generation='explicit_synthetic_batch',
        original_full109_selection=oldproof,preserved_old_completed_pair_instruments=[A],remaining_instruments=[B],
        calendar={'path':str(args['calendar_path'].resolve()),'sha256':hashlib.sha256(args['calendar_path'].read_bytes()).hexdigest()},
        completed_old_collection_manifests=[{'path':str((old['directory']/'collection.json').resolve()),
            'sha256':hashlib.sha256((old['directory']/'collection.json').read_bytes()).hexdigest()}],segments=[])
    manifests=[];entries=[]
    for root in args['segment_collections']:
        path=root/'collection.json';manifest=json.loads(path.read_text());snapshot={**manifest,'responses':{},'failures':{},'status':'collecting'}
        snapshotproof=save(tmp_path/(root.name+'_predeclared.json'),snapshot)
        protocol['segments'].append(dict(name=root.name,directory=str(root.resolve()),start_date=manifest['request_start'],
            end_date=manifest['request_end'],source_generation=manifest['generation'],
            actual_calendar_sessions=sum(manifest['request_start']<=day<=manifest['request_end'] for day in DAYS),
            predeclared_empty_generation_manifest=snapshotproof))
        manifests.append({'path':str(path.resolve()),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        for key,record in manifest['responses'].items():
            response=root/record['file'];payload=json.loads(response.read_text());guard=args['guard_directories'][0]/(root.name+'_'+key+'.json')
            entries.append(dict(segment=root.name,key=key,source_generation=manifest['generation'],
                original_response={'path':str(response.resolve()),'sha256':record['sha256']},
                original_guard={'path':str(guard.resolve()),'sha256':hashlib.sha256(guard.read_bytes()).hexdigest()},
                actual_request=payload['request'],rows=len(payload['rows']),rows_json_sha256=c._sha(c._json(payload['rows'])),
                row_hash_algorithm='UTF8_JSON_ensure_ascii_false_compact_no_sort_arrays',fetched_at=payload['fetched_at'],original_mode_anomalies_preserved=[]))
    args['capture_protocol_path']=tmp_path/'protocol.json';p=save(args['capture_protocol_path'],protocol)
    args['capture_receipt_path']=tmp_path/'capture_receipt.json'
    receipt=dict(kind='fixed42_three_segment_original_capture',status='source_capture_complete_pending_exact109_composition_and_strict_whole_audit',
        capture_protocol=p,batch_generation=protocol['batch_generation'],instruments=[B],source_manifests=manifests,
        original_source_bytes_modified=False,source_snapshot_atomic_supplier_vintage_proven=False,synthetic_vendor_whole_request_claimed=False,
        responses=len(entries),rows_both_modes=sum(e['rows'] for e in entries),verified_empty_responses=sum(e['rows']==0 for e in entries),
        entries=entries,finished_at='2026-10-20T02:00:00+00:00')
    save(args['capture_receipt_path'],receipt)
    return protocol,receipt


def test_actual_predeclared_generations_targets_and_final_row_hash_links_are_verified(tmp_path):
    args=fixture(tmp_path);declare_fixture(args,tmp_path)
    _,_,receipt=c.compose_segmented_rebuild_sources(**args)
    assert receipt['capture_protocol_bound'] is True
    assert str(args['capture_protocol_path'].resolve()) in {p['path'] for p in receipt['bound_inputs']}
    assert str(args['capture_receipt_path'].resolve()) in {p['path'] for p in receipt['bound_inputs']}


@pytest.mark.parametrize('damage',['targets','generation','start_clock','sidecar_rows','sidecar_source'])
def test_declaration_and_capture_sidecar_semantics_cannot_be_only_hash_checked(tmp_path,damage):
    args=fixture(tmp_path);protocol,receipt=declare_fixture(args,tmp_path)
    if damage=='targets':protocol['remaining_instruments']=[A]
    if damage=='generation':protocol['segments'][0]['source_generation']='wrong'
    if damage=='start_clock':protocol['created_at']='2026-10-20T01:00:00.5+00:00'
    if damage in ['targets','generation','start_clock']:
        receipt['capture_protocol']=save(args['capture_protocol_path'],protocol)
    if damage=='sidecar_rows':receipt['entries'][0]['rows_json_sha256']='f'*64
    if damage=='sidecar_source':receipt['entries'][0]['original_response']['path']=receipt['entries'][1]['original_response']['path']
    save(args['capture_receipt_path'],receipt)
    with pytest.raises(ValueError,match='inventory differs|empty generation|before its source declaration|sidecar|source SHA differs'):
        c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_receipt_cannot_be_created_inside_an_original_capture_archive(tmp_path):
    args=fixture(tmp_path);args['receipt_dir']=args['segment_collections'][0]/'derived'
    with pytest.raises(ValueError,match='must not alter an original source archive'):
        c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_partial_archive_orphan_success_cannot_be_silently_skipped(tmp_path):
    args=fixture(tmp_path);root=args['completed_collections'][0]['directory'];path=root/'collection.json'
    manifest=json.loads(path.read_text());manifest['responses'].pop(A+'_hfq');save(path,manifest)
    stop_path=root/'stop_receipt.json';stop=json.loads(stop_path.read_text())
    stop['original_manifest_snapshot']=save(root/'stopped_manifest_snapshot.json',manifest)
    stop['original_successful_responses']=[r for r in stop['original_successful_responses'] if r['key']!=A+'_hfq']
    stop['success_count']=1;save(stop_path,stop)
    with pytest.raises(ValueError,match='orphan mode'):c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_nonoverlapping_segments_cannot_have_a_calendar_request_gap(tmp_path):
    args=fixture(tmp_path);root=args['segment_collections'][1];path=root/'collection.json'
    manifest=json.loads(path.read_text());manifest['request_start']='2020-01-02';save(path,manifest)
    with pytest.raises(ValueError,match='partition the whole window'):c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_published_receipt_archives_exact_consumed_runtime_sources(tmp_path):
    args=fixture(tmp_path);_,_,receipt=c.compose_segmented_rebuild_sources(**args)
    assert 'a5_segment_composition.py' in receipt['runtime_source_archive']
    for archived in receipt['runtime_source_archive'].values():
        assert Path(archived['path']).read_bytes()==Path(archived['original_path']).read_bytes()
        assert hashlib.sha256(Path(archived['path']).read_bytes()).hexdigest()==archived['sha256']


def code_clock_fixture(args,tmp_path,effective='2023-01-03'):
    folder=tmp_path/'official_scope';folder.mkdir()
    issuer=tmp_path/'issuer_code_change.pdf';issuer.write_bytes(b'%PDF synthetic issuer identity fixture')
    issuerproof={'path':str(issuer.resolve()),'sha256':hashlib.sha256(issuer.read_bytes()).hexdigest()}
    event=dict(event_type='same_issuer_security_code_change',old_instrument='SZ300000',new_instrument=B,
        same_legal_issuer=True,share_quantity_changes=False,price_mapping_allowed=False,
        effective_on=effective,first_effective_session=effective,known_at='2022-12-30T23:59:59+08:00',evidence=[issuerproof])
    proof=save(folder/'events.json',{'kind':'ashare_official_index_adjustment_events','price_mapping_allowed':False,'events':[event]})
    save(folder/'manifest.json',{'kind':'ashare_official_event_membership_reconstruction','status':'passed','files':{'events.json':proof['sha256']}})
    args['official_code_validity_events_path']=folder/'events.json'
    for root in args['segment_collections'][:2]:
        rewrite_response(args,root,B,'hfq',lambda payload:[row.__setitem__(8,'3') for row in payload['rows']])
        path=args['guard_directories'][0]/(root.name+'_'+B+'_hfq.json');guard=json.loads(path.read_text())
        payload=json.loads((root/(B+'_hfq.json')).read_text())
        guard['row_mode_anomalies_observed']=[dict(date=row[0],requested_mode='1',returned_row_mode='3') for row in payload['rows']]
        save(path,guard)
    return event


def test_official_code_alias_rows_are_archival_before_mode_gate_not_fake_ipo_or_halts(tmp_path):
    args=fixture(tmp_path);code_clock_fixture(args,tmp_path)
    raw,hfq,receipt=c.compose_segmented_rebuild_sources(**args)
    assert raw.loc[raw.instrument.eq(B),'date'].tolist()==DAYS[-3:]
    assert hfq.loc[hfq.instrument.eq(B),'adjustflag'].tolist()==['1']*3
    assert receipt['excluded_code_alias_row_count_both_modes']==14
    originals=[json.loads(row) for row in Path(receipt['excluded_code_alias_original_rows']['path']).read_text().splitlines()]
    assert len(originals)==14 and {r['reason'] for r in originals}=={'code_invalid_archive_only'}
    assert all(r['old_code_price_mapping_allowed'] is False for r in originals)
    assert {r['original_row_values'][8] for r in originals if r['mode']=='hfq'}=={'3'}
    assert receipt['securities'][B]['sources'][0]['consumption_kind']=='code_invalid_archive_only'
    mapping=[json.loads(row) for row in Path(receipt['row_provenance']['path']).read_text().splitlines()]
    assert [row[5] for row in mapping if row[0]==B and row[1]=='hfq']==[0,1,2]


def test_valid_date_wrong_mode_still_blocks_even_with_official_code_clock(tmp_path):
    args=fixture(tmp_path);code_clock_fixture(args,tmp_path)
    rewrite_response(args,args['segment_collections'][2],B,'hfq',lambda payload:payload['rows'][0].__setitem__(8,'3'))
    with pytest.raises(ValueError,match='identity/mode/calendar differs'):
        c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_alias_archival_guard_anomalies_must_still_match_every_original_row(tmp_path):
    args=fixture(tmp_path);code_clock_fixture(args,tmp_path)
    path=args['guard_directories'][0]/('segment0_'+B+'_hfq.json');guard=json.loads(path.read_text())
    guard['row_mode_anomalies_observed']=[];save(path,guard)
    with pytest.raises(ValueError,match='mode guard is not verified'):
        c.compose_segmented_rebuild_sources(**args)
    assert not args['receipt_dir'].exists()


def test_mid_segment_code_clock_retains_original_row_indices_not_rebased_alias_positions(tmp_path):
    args=fixture(tmp_path);code_clock_fixture(args,tmp_path,effective='2025-06-02')
    raw,_,receipt=c.compose_segmented_rebuild_sources(**args)
    assert raw.loc[raw.instrument.eq(B),'date'].tolist()==DAYS[-2:]
    rows=[json.loads(row) for row in Path(receipt['row_provenance']['path']).read_text().splitlines()]
    consumed=[row for row in rows if row[0]==B and row[1]=='raw']
    assert [row[3] for row in consumed]==[0,1]
    assert [row[5] for row in consumed]==[1,2]
    excluded=[json.loads(row) for row in Path(receipt['excluded_code_alias_original_rows']['path']).read_text().splitlines()]
    assert len(excluded)==16 and any(row['date']=='2023-01-03' and row['original_response_row_index_zero_based']==0 for row in excluded)
