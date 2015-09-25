import time
import datetime
import requests

import sunblock.util as util
requests.packages.urllib3.disable_warnings()

# Try to get last timestamp
last_ts = util.get_sunblock_conf()["last"]

if last_ts == 0:
    last_ts = None
else:
    last_ts = int(last_ts)

curr_time = (str(int(time.time())))

import sunblock.util as util
from acct_parse import Account
acct = Account(util.get_sunblock_conf()["acct_path"], False, last_ts=last_ts)
host = util.get_sunblock_conf()["sunblock_host"]

for id, job in sorted(acct.jobs.items()):

    jid = int(job["jobnumber"])
    tid = int(job["taskid"])

    payload = {
        "api_key": util.get_sunblock_conf()["api_key"],
        "jid": jid,
        "tid": tid,

        "job": {},
        "subjob": {
            "mem_used": job["mem"]["mem_used"] * 1024000000,
            "end_dt": str(job["time"]["end_dt"]),
            "duration": job["time"]["time_taken_td"].total_seconds(),
            "exit_code": job["exit_status"],
            "failed": job["failed"],
        }
    }
    requests.post(host+"/jobs/update/", json=payload, verify=False)

    if job["exit_status"] > 0 or job["failed"] > 0:
        payload = {
            "api_key": util.get_sunblock_conf()["api_key"],
            "jid": jid,
            "tid": tid,
        }
        import json
        r = requests.post(host+"/jobs/get_log/", json=payload, verify=False)
        try:
            rj = json.loads(r.text)
        except ValueError:
            continue

        log_data = ""
        try:
            log_f = open(rj["log_path"])
            log_data = "".join(log_f.readlines()[-100:])
            log_f.close()
        except:
            log_data = "LOG COULD NOT BE OPENED."

        payload = {
            "api_key": util.get_sunblock_conf()["api_key"],
            "jid": jid,
            "tid": tid,

            "job": {},
            "subjob": {
                "log_text": log_data
            }
        }
        requests.post(host+"/jobs/update/", json=payload, verify=False)


util.update_sunblock_conf("last", curr_time)

