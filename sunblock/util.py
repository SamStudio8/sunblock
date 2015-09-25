from datetime import datetime
import json
import os
import sys

def get_job_list():
    record_fh = get_record_fh()
    if record_fh is not None:
        records = []
        with open(record_fh, "r") as json_file:
            for line in json_file:
                records.append(json.loads(line))
        return records
    else:
        return None

def get_record_fh():
    home_path = os.path.expanduser('~')
    sbdb_path = os.path.join(home_path, "sunblock", "sunblock.db.json")
    if not os.path.isfile(sbdb_path):
        print("[WARN] Creating new sunblock database at '%s'" % sbdb_path)
    return sbdb_path

def get_cache_acct_fh():
    home_path = os.path.expanduser('~')
    sbdb_path = os.path.join(home_path, "sunblock", "sunblock.acct.json")
    if not os.path.isfile(sbdb_path):
        print("[WARN] Creating new sunblock cache at '%s'" % sbdb_path)
    return sbdb_path

def get_sunblock_path():
    home_path = os.path.expanduser('~')
    sunblock_path = os.path.join(home_path, "sunblock")
    if not os.path.isdir(sunblock_path):
        print("[WARN] Creating new sunblock environment at '%s'" % sunblock_path)
        try:
            os.makedir(sunblock_path)
        except:
            print("[FAIL] Failed to create new sunblock environment at '%s'" % sunblock_path)
            sys.exit(1)
    return sunblock_path

def append_job_list(new_record):
    record_fh = get_record_fh()
    with open(record_fh, "a") as json_file:
        json_file.write("{}\n".format(json.dumps(new_record)))
    return True

def append_cache_account(new_lines):
    record_fh = get_cache_acct_fh()
    with open(record_fh, "a") as _file:
        _file.write(new_lines.strip() + "\n")
    return True

def get_sunblock_conf():
    home_path = os.path.expanduser('~')
    conf_path = os.path.join(home_path, "sunblock", "sunblock.conf.json")
    if not os.path.isfile(conf_path):
        with open(conf_path, "w") as json_file:
            json_file.write(json.dumps({
                "api_key": "",
                "sunblock_host": "",
                "acct_path": "/cm/shared/apps/sge/6.2u5p2/default/common/accounting",
                "last": 0,
                "engine": "SGE",
                "sunblock_venv": "",
                "farm": "",
                "lsf-group": ""
            }))
    with open(conf_path, "r") as json_file:
        return json.loads("\n".join(json_file.readlines()))

def update_sunblock_conf(k, v):
    home_path = os.path.expanduser('~')
    conf_path = os.path.join(home_path, "sunblock", "sunblock.conf.json")
    with open(conf_path, "r") as json_file:
        conf = json.loads("\n".join(json_file.readlines()))
    with open(conf_path, "w") as json_file:
        conf[k] = v
        json_file.write(json.dumps(conf))

