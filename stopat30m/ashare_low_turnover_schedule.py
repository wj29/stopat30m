"""A single macOS paper-only job; the runner owns calendars and idempotence."""
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import plistlib


def launch_agent_definition(project_root,paper_dir,report_dir):
    from stopat30m.ashare_low_turnover_forward import low_turnover_status
    root,paper,report=map(lambda p:Path(p).resolve(),(project_root,paper_dir,report_dir))
    paper.relative_to(root/'output/research/ashare/forward')
    if report==paper or paper in report.parents:
        raise ValueError('Reports must be outside the immutable paper ledger')
    if datetime.now().astimezone().utcoffset()!=timedelta(hours=8):
        raise ValueError('Native schedule requires host UTC+8 timezone')
    low_turnover_status(paper)
    logs=paper/'runner';logs.mkdir(exist_ok=True)
    label='com.stopat30m.lowturnover.'+hashlib.sha256(str(paper).encode()).hexdigest()[:12]
    return {'Label':label,'ProgramArguments':[str(root/'.venv/bin/python'),'-m',
        'stopat30m.ashare_low_turnover_forward','run','--paper-dir',str(paper),
        '--phase','auto','--report-dir',str(report)],'WorkingDirectory':str(root),
        'EnvironmentVariables':{'PYTHONUNBUFFERED':'1','TZ':'Asia/Shanghai'},
        'RunAtLoad':True,'StartCalendarInterval':[{'Weekday':d,'Hour':h,'Minute':m}
            for d in range(1,6) for h,m in ((9,35),(9,40),(9,44),(19,0),(19,45),(20,30))],
        'StandardOutPath':str(logs/'scheduler.log'),'StandardErrorPath':str(logs/'scheduler-error.log'),
        'ProcessType':'Background','ThrottleInterval':60}


def write_launch_agent(project_root,paper_dir,report_dir,output):
    value=launch_agent_definition(project_root,paper_dir,report_dir)
    path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
    data=plistlib.dumps(value,sort_keys=True)
    if path.exists() and path.read_bytes()!=data:
        raise ValueError('Preserve any different existing schedule')
    path.write_bytes(data)
    return value
