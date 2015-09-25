import requests
import sys
import datetime
requests.packages.urllib3.disable_warnings()

import sunblock.util as util

payload = {
    "api_key": util.get_sunblock_conf()["api_key"],
    "jid": sys.argv[1],
    "tid": sys.argv[2],

    "job": {},
    "subjob": {
        "end_dt": str(datetime.datetime.now()),
        "tentative_completed": True
    }
}
host = util.get_sunblock_conf()["sunblock_host"]
requests.post(host+"/jobs/update/", json=payload, verify=False)

