"""Strict evidence-based price corrections: no fitted scale or tolerance route."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import pandas as pd

from stopat30m.data import qlib_factor_revision as repair


def save(path, payload):
    raw = json.dumps(payload, sort_keys=True).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return {"path":str(path.resolve()),"sha256":hashlib.sha256(raw).hexdigest()}


def frames():
    rows = []
    for day in ("2026-10-16", repair.PREVIOUS_SESSION, repair.SESSION, repair.CONFIRMATION_END):
        rows.append([day,"sh.601898","10.00","11.00","9.00","10.50","10.25","10000","102000","3","1","0"])
    raw = pd.DataFrame(rows,columns=repair.EXECUTION_FIELDS.split(","))
    old = raw.iloc[:-1].copy(deep=True)
    old.adjustflag = "1"
    new = raw.iloc[1:].copy(deep=True)
    new.adjustflag = "1"
    for field in repair.PRICE_FIELDS:
        old[field] = old[field].map(lambda value:str(repair.Decimal(value)*2))
        new[field] = [str(repair.Decimal(value)*(2 if day < repair.SESSION else 3)) for value,day in zip(new[field],new.date)]
    factors = pd.DataFrame([["sh.601898","2008-02-01","0.5","1","1"],
        ["sh.601898","2026-08-26","0.6","2","2"],
        ["sh.601898",repair.SESSION,"0.7","3","3"]],columns=repair.FACTOR_FIELDS)
    return raw.iloc[:-1].copy(),old,raw.iloc[1:].copy(),new,factors


class VerifiedReplacementTest(unittest.TestCase):
    def apply(self, values=None):
        return repair._verified_replacement("SH601898",*(values or frames()))

    def test_only_new_effective_date_price_fields_change(self):
        values=frames();before=[value.copy(deep=True) for value in values]
        revised,evidence=self.apply(values)
        target=revised.date.eq(repair.SESSION)
        pd.testing.assert_frame_equal(revised.loc[~target],before[1].loc[~target])
        for field in repair.PRICE_FIELDS:
            self.assertEqual(revised.loc[target,field].iloc[0],before[3].set_index("date").loc[repair.SESSION,field])
        for field in set(revised)-set(repair.PRICE_FIELDS):
            pd.testing.assert_series_equal(revised[field],before[1][field])
        for original,unchanged in zip(values,before):
            pd.testing.assert_frame_equal(original,unchanged)
        self.assertTrue(evidence["exact_decimal_reconstruction"])
        self.assertTrue(evidence["raw_and_non_price_fields_unchanged"])

    def test_raw_price_changes_are_rejected(self):
        values=list(frames());values[2].loc[values[2].date.eq(repair.SESSION),"open"]="10.001"
        with self.assertRaisesRegex(ValueError,"Raw prices"):
            self.apply(values)

    def test_raw_non_price_changes_are_rejected(self):
        for field,value in (("volume","10001"),("amount","102001"),("tradestatus","0"),("isST","1"),("preclose","10.26")):
            with self.subTest(field=field):
                values=list(frames());values[2].loc[values[2].date.eq(repair.SESSION),field]=value
                with self.assertRaisesRegex(ValueError,"Raw prices"):
                    self.apply(values)

    def test_historical_adjusted_change_is_rejected(self):
        values=list(frames());values[3].loc[values[3].date.eq(repair.PREVIOUS_SESSION),"close"]="21.01"
        with self.assertRaisesRegex(ValueError,"prior historical"):
            self.apply(values)

    def test_adjusted_non_price_change_is_rejected(self):
        for field,value in (("volume","10001"),("amount","102001"),("tradestatus","0"),("isST","1"),("adjustflag","3")):
            with self.subTest(field=field):
                values=list(frames());values[3].loc[values[3].date.eq(repair.SESSION),field]=value
                with self.assertRaisesRegex(ValueError,"non-price"):
                    self.apply(values)

    def test_adjusted_rounding_or_fitted_factor_is_not_allowed(self):
        values=list(frames());values[3].loc[values[3].date.eq(repair.SESSION),"open"]="30.000000001"
        with self.assertRaisesRegex(ValueError,"exactly"):
            self.apply(values)

    def test_old_prices_must_have_exact_previous_factor(self):
        values=list(frames());values[1].loc[values[1].date.eq(repair.SESSION),"close"]="20.999999999"
        with self.assertRaisesRegex(ValueError,"previous-factor"):
            self.apply(values)

    def test_event_on_another_day_cannot_justify_repair(self):
        values=list(frames());values[4].loc[2,"dividOperateDate"]="2026-10-21"
        with self.assertRaisesRegex(ValueError,"explicit factor event"):
            self.apply(values)

    def test_no_prior_factor_is_rejected(self):
        values=list(frames());values[4]=values[4].iloc[2:]
        with self.assertRaisesRegex(ValueError,"predecessor"):
            self.apply(values)

    def test_unchanged_factor_is_rejected(self):
        values=list(frames());values[4].loc[2,"backAdjustFactor"]="2"
        with self.assertRaisesRegex(ValueError,"non-changing"):
            self.apply(values)

    def test_confirmation_next_day_must_also_reconstruct(self):
        values=list(frames());values[3].loc[values[3].date.eq(repair.CONFIRMATION_END),"close"]="31.6"
        with self.assertRaisesRegex(ValueError,"exactly"):
            self.apply(values)

    def test_preclose_is_verified_as_well_as_ohlc(self):
        values=list(frames());values[3].loc[values[3].date.eq(repair.SESSION),"preclose"]="30.76"
        with self.assertRaisesRegex(ValueError,"exactly"):
            self.apply(values)

    def test_missing_original_session_is_rejected(self):
        values=list(frames());values[0]=values[0].loc[values[0].date.ne(repair.SESSION)]
        with self.assertRaisesRegex(ValueError,"incomplete"):
            self.apply(values)

    def test_nonfinite_and_nonpositive_factor_or_price_are_rejected(self):
        for value in ("NaN","Infinity","-1","0"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    repair._positive(value)


class ContractAndGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)

    def contract(self):
        return {"kind":repair.KIND,"schema_version":1,"created_at":"2026-10-22T04:00:00+00:00",
            "session":repair.SESSION,"revised_instruments":list(repair.REVISED_INSTRUMENTS),
            "derived_generation":"derived_effective_factor_revision:"+uuid4().hex,
            **{key:{"path":"/source/"+key,"sha256":"a"*64} for key in ("previous_capture","confirmation_capture",
                "factor_verification_report","initial_snapshot_manifest","previous_snapshot_manifest","previous_refresh_result","calendar")}}

    def confirmation(self, *, factor=False):
        code="sh.601898";fields=list(repair.FACTOR_FIELDS) if factor else repair.EXECUTION_FIELDS.split(",")
        request={"code":code,"start_date":"2008-02-01","end_date":"2026-10-22"} if factor else {
            "code":code,"fields":repair.EXECUTION_FIELDS,"start_date":repair.PREVIOUS_SESSION,
            "end_date":repair.CONFIRMATION_END,"frequency":"d","adjustflag":"3"}
        rows=frames()[4].values.tolist() if factor else frames()[2].values.tolist()
        payload={"request":request,"fields":fields,"generation":"b"*32,
            "fetched_at":"2026-10-22T03:47:05+00:00","rows":rows,
            "query":"query_adjust_factor" if factor else "query_history_k_data_plus"}
        guard={"request":request,"server_echo":request,"status":"verified","server_error_code":"0",
            "complete_row_iteration_observed":True,"rows_verified":len(rows),
            "started_at":"2026-10-22T03:47:03+00:00","finished_at":"2026-10-22T03:47:04+00:00",
            "kind":"baostock_metadata_server_echo_guard" if factor else "baostock_daily_server_echo_guard",
            "row_mode_flags_repaired":False,"row_mode_anomalies_observed":[]}
        if factor:guard["query"]="query_adjust_factor"
        guardproof=save(self.root/"guard.json",guard);proof=save(self.root/"response.json",payload)
        capture={"generation":"b"*32,"actual_capture_day":"2026-10-22","responses":{
            "test":{**proof,"rows":len(rows),"fetched_at":payload["fetched_at"],"guard":guardproof}}}
        return capture,request,fields,payload,guard

    def rewrite(self,capture,payload=None,guard=None):
        record=capture["responses"]["test"]
        if payload is not None:record.update(save(self.root/"response.json",payload))
        if guard is not None:record["guard"]=save(self.root/"guard.json",guard)

    def test_valid_contract_is_narrow_and_explicitly_derived(self):
        repair._validate_contract(self.contract())

    def test_contract_scope_generation_schema_and_time_cannot_expand(self):
        for key,value in (("revised_instruments",["SH601668"]),("session","2026-10-21"),("schema_version",2),
                ("derived_generation","baostock:"+uuid4().hex),("created_at","2026-10-20T12:00:00+00:00"),
                ("created_at","2026-10-22T12:00:00")):
            with self.subTest(key=key):
                c=self.contract();c[key]=value
                with self.assertRaises(ValueError):repair._validate_contract(c)
        c=self.contract();c["fallback_scale"]=1
        with self.assertRaises(ValueError):repair._validate_contract(c)

    def test_proof_detects_tampered_bytes_and_symlinks(self):
        proof=save(self.root/"proof.json",{"value":1});repair._proof(proof)
        Path(proof["path"]).write_text("changed")
        with self.assertRaises(ValueError):repair._proof(proof)
        proof=save(self.root/"proof.json",{"value":1});link=self.root/"link.json";link.symlink_to(proof["path"])
        with self.assertRaises(ValueError):repair._proof({**proof,"path":str(link)})

    def test_guarded_independent_daily_and_factor_are_accepted(self):
        for factor in (False,True):
            with self.subTest(factor=factor):
                cap,req,fields,_,_=self.confirmation(factor=factor)
                frame,bound=repair._read_confirmation(cap,"test",req,fields,daily=not factor)
                self.assertEqual(len(frame),3);self.assertEqual(len(bound),2)

    def test_terminal_echo_rows_and_repaired_modes_cannot_be_bypassed(self):
        for key,value in (("complete_row_iteration_observed",False),("server_error_code","1"),
                ("rows_verified",2),("row_mode_flags_repaired",True),("row_mode_anomalies_observed",["wrongmode"])):
            with self.subTest(key=key):
                cap,req,fields,payload,guard=self.confirmation();guard[key]=value;self.rewrite(cap,guard=guard)
                with self.assertRaises(ValueError):repair._read_confirmation(cap,"test",req,fields,daily=True)

    def test_schema_generation_request_query_and_source_day_cannot_change(self):
        for key,value in (("generation","other"),("fields",["code"]),("query","query_other"),
                ("request",{"code":"sz.000563"}),("fetched_at","2026-10-21T03:47:05+00:00")):
            with self.subTest(key=key):
                cap,req,fields,payload,guard=self.confirmation();payload[key]=value;self.rewrite(cap,payload=payload)
                with self.assertRaises(ValueError):repair._read_confirmation(cap,"test",req,fields,daily=True)

    def test_daily_flags_and_missing_session_are_rejected(self):
        for change in ("mode","missing","status"):
            cap,req,fields,payload,guard=self.confirmation()
            if change=="mode":payload["rows"][1][9]="1"
            elif change=="status":payload["rows"][1][10]="0"
            else:payload["rows"][1][0]="2026-10-18"
            self.rewrite(cap,payload=payload)
            with self.assertRaises(ValueError):repair._read_confirmation(cap,"test",req,fields,daily=True)

    def test_factor_must_have_original_specific_query_guard(self):
        cap,req,fields,payload,guard=self.confirmation(factor=True);guard["query"]="query_dividend_data";self.rewrite(cap,guard=guard)
        with self.assertRaisesRegex(ValueError,"adjust-factor"):
            repair._read_confirmation(cap,"test",req,fields,daily=False)

    def test_factor_dates_must_be_complete_ordered_and_positive(self):
        for change in ("order","ipo","zero","future"):
            cap,req,fields,payload,guard=self.confirmation(factor=True)
            if change=="order":payload["rows"]=list(reversed(payload["rows"]))
            elif change=="ipo":payload["rows"][0][1]="2008-02-02"
            elif change=="zero":payload["rows"][2][3]="0"
            else:payload["rows"][2][1]="2026-10-23"
            self.rewrite(cap,payload=payload)
            with self.assertRaises(ValueError):repair._read_confirmation(cap,"test",req,fields,daily=False)


if __name__ == "__main__":
    unittest.main()
