from datetime import datetime
import json
import os
import sys

#TODO I'd like to dynamically load modules to avoid users needing to edit this qq
from sunblock.jobs import (
        blast,
        blast2gff,
        filtergff,
        addgffinfo,
        helloworld,
        rapsearch,
)

def get_template_list():
    return {
        "blast": blast.BLAST,
        "blast2gff": blast2gff.BLAST2GFF,
        "filtergff": filtergff.FilterGFF,
        "addgffinfo": addgffinfo.AddGFFInfo,
        "helloworld": helloworld.HelloWorld,
        "rapsearch": rapsearch.RAPSearch,
    }

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
