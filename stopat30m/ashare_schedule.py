"""Native paper-only scheduling, conditional on the actual immutable A2 audit."""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import plistlib


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_activation(comparison_path):
    root=Path(comparison_path).resolve()
    manifest=json.loads((root/"manifest.json").read_text())
    for name, digest in manifest["files"].items():
        if Path(name).is_absolute() or ".." in Path(name).parts or _sha(root/name)!=digest:
            raise ValueError("A2 comparison file changed")
    for receipt in manifest["bound_inputs"].values():
        if _sha(receipt["path"])!=receipt["sha256"]:
            raise ValueError("A2 source, evidence or accounting implementation changed")
    report=json.loads((root/"report.json").read_text())
    protocol=json.loads((root/"protocol.json").read_text())
    references=json.loads((root/"reference_price_audit.json").read_text())
    model=report.get("metrics",{}).get("lightgbm",{})
    if (not report.get("comparison_complete") or not report.get("activation_passed")
            or not manifest.get("activation_passed") or set(report["metrics"])!={"lightgbm","momentum","equal_weight"}
            or protocol["repair_audit"]["count"]!=36 or references["status"]!="passed"
            or report["spec"].get("parameter_search") is not False
            or model.get("total_return",0)<=0 or abs(model.get("max_drawdown",1))>float(report["spec"]["activation_gates"]["model_absolute_max_drawdown_at_most"])
            or model.get("incomplete_valuation_days")!=0 or model.get("trade_count",0)<=0):
        raise ValueError("The declared data/accounting/performance activation gates did not pass")
    training=report["training_origin"]
    if _sha(training["path"])!=training["sha256"]:
        raise ValueError("Historical validation training receipt changed")
    origin=Path(training["path"]).parent
    for name,digest in json.loads(Path(training["path"]).read_text())["files"].items():
        if Path(name).is_absolute() or ".." in Path(name).parts or _sha(origin/name)!=digest:
            raise ValueError("Historical validation frozen model changed")
    return {"path":str(root),"manifest_sha256":_sha(root/"manifest.json"),"activation_passed":True}


def launch_agent_definition(project_root,paper_dir):
    root,paper=Path(project_root).resolve(),Path(paper_dir).resolve()
    paper.relative_to(root/"output/research/ashare/forward")
    if datetime.now().astimezone().utcoffset()!=timedelta(hours=8):
        raise ValueError("The native schedule requires the host timezone to be UTC+8")
    adapter=json.loads((paper/"adapter.json").read_text())
    validated=verify_activation(adapter["validation_path"])
    if validated["manifest_sha256"]!=adapter["validation_manifest_sha256"]:
        raise ValueError("Paper adapter belongs to another validation")
    from stopat30m.ashare_forward import ashare_forward_status
    ashare_forward_status(paper)
    logs=paper/"runner";logs.mkdir(exist_ok=True)
    label="com.stopat30m.asharepaper."+hashlib.sha256(str(paper).encode()).hexdigest()[:12]
    return {"Label":label,"ProgramArguments":[str(root/".venv/bin/python"),str(root/"main.py"),"research","ashare-paper-run","--paper-dir",str(paper),"--phase","auto"],
        "WorkingDirectory":str(root),"EnvironmentVariables":{"PYTHONUNBUFFERED":"1","TZ":"Asia/Shanghai"},
        "RunAtLoad":True,"StartCalendarInterval":[{"Weekday":day,"Hour":hour,"Minute":minute}
            for day in range(1,6) for hour,minute in ((9,35),(9,40),(9,44),(16,45),(17,15),(17,45))],
        "StandardOutPath":str(logs/"scheduler.log"),"StandardErrorPath":str(logs/"scheduler-error.log"),
        "ProcessType":"Background","ThrottleInterval":60}


def write_launch_agent(project_root,paper_dir,output):
    definition=launch_agent_definition(project_root,paper_dir)
    path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
    content=plistlib.dumps(definition,sort_keys=True)
    if path.exists() and path.read_bytes()!=content:
        raise ValueError("Existing job differs; do not overwrite another schedule")
    path.write_bytes(content)
    return definition
