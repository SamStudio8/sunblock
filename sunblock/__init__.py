from datetime import datetime
import json
import sys
import time

import click
from zenlog import log

import sunblock.util as util

@click.group()
def cli():
    pass

@cli.command(help="Generate a job configuration")
@click.argument('template')
def init(template):

    # Does template exist?
    if template not in util.get_template_list():
        log.error("No template for job type: '%s'" % template)
        sys.exit(1)

    job = util.get_template_list()[template]()
    log.debug("Found template for job type: '%s' with %d required keys" % (template, len(job.config)))
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        log.debug("\t%s: %s" % (key, conf["desc"]))

    # Prompt
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        job.set_key(key, click.prompt(conf["prompt"], type=conf["type"]))

    # Write
    to_write = {"template": template}
    for key, conf in job.config.items():
        to_write[key] = conf["value"]

    out_fh = open("job.out", "w")
    out_fh.write(json.dumps(to_write))
    out_fh.close()

@cli.command(help="Execute a configured job")
@click.argument('config', type=click.Path(exists=True, readable=True))
def execute(config):

    # Open
    in_fh = open(config)
    config = json.loads(in_fh.read())
    in_fh.close()

    # Load
    job = util.get_template_list()[config["template"]]()
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        job.set_key(key, config[key])

    # Check config
    #TODO More helpful output -- what key is missing or incorrect?
    if not job.check():
        log.error("Invalid job configuration. Aborting.")
        sys.exit(1)

    script_names = job.execute()
    if len(script_names) == 0:
        print("[WARN] No script names returned by config.")
    else:
        new_record = {
            "njobs": 0,
            "tjobs": 0,
            "jobs": [],
            "timestamp": int(time.mktime(datetime.now().timetuple())),
            "template": job.template_name
        }

        from subprocess import check_output
        for name in script_names:
            p = check_output(['qsub', '-terse', name])

            try:
                jid, trange = p.strip().split(".")
            except ValueError:
                jid = p.strip()
                trange = None
            ts = 1
            te = 1

            if trange:
                ts, te = trange.split(":")[0].split("-")

            new_record["jobs"].append({
                "jid": int(jid),
                "t_start": int(ts),
                "t_end": int(te),
                "script_path": name
            })
            new_record["njobs"] += 1
            new_record["tjobs"] += int(te)

        if not util.append_job_list(new_record):
            print("Error appending to sunblock record file.")
        else:
            print("Submitted %d job arrays, totalling %d jobs." % (new_record["njobs"], new_record["tjobs"]))

@cli.command(help="Check the status of a submitted job")
@click.argument("job_id", type=int)
@click.option("--acct_path", default="/cm/shared/apps/sge/6.2u5p2/default/common/accounting", type=click.Path(exists=True, readable=True))
@click.option("--format", default="jobnumber:hostname:jobname:exit_status")
@click.option("--failed", is_flag=True)
@click.option("--quiet", is_flag=True)
def check(job_id, acct_path, format, failed, quiet):
    jobs = util.get_job_list()
    if not jobs:
        print("No jobs found.")
    else:
        from acct_parse import Account
        try:
            job = jobs[job_id]
        except IndexError:
            print("Invalid job id.")
            sys.exit(1)

        to_search = []
        to_search_d = {}
        # Collect jids
        for subjob in job["jobs"]:
            to_search.append(subjob["jid"])
            to_search_d[subjob["jid"]] = {
                "found": 0,
                "nonzero": 0,
                "records": [],
                "expecting": subjob["t_end"]
            }

        acct = Account(acct_path, quiet)
        for id, job in sorted(acct.jobs.items()):
            jid = int(id.split(":")[0])
            try:
                tid = id.split(":")[1]
            except IndexError:
                tid = ""

            if jid in to_search:
                to_search_d[jid]["found"] += 1

                if job["exit_status"] == 0:
                    if failed:
                        continue
                else:
                    to_search_d[jid]["nonzero"] += 1

                to_search_d[jid]["records"].append(acct.print_job(jid, tid, format))

        for jid in to_search_d:
            pc = float(to_search_d[jid]["found"]) / to_search_d[jid]["expecting"]
            pc_75 = int(pc*75)
            print("%d\t[%s%s] %.2f%% (%d of %d, %d failed)" % (jid, pc_75*'=', (75-pc_75)*' ', pc*100, to_search_d[jid]["found"], to_search_d[jid]["expecting"], to_search_d[jid]["nonzero"]))
#            print("%d\tWaiting on %d records...
            for record in to_search_d[jid]["records"]:
                print("\t%s" % record)

@cli.command(help="List available [templates|jobs]")
@click.argument('what')
@click.option("--expand", is_flag=True)
def list(what, expand):
    if what.lower() == "templates":
        print("\n".join(util.get_template_list()))
    elif what.lower() == "jobs":
        jobs = util.get_job_list()
        if not jobs:
            print("No jobs found.")
        else:
            for i, job in enumerate(jobs):
                print("%d\t%s\t%s\t%d (%d)" % (i, datetime.fromtimestamp(job["timestamp"]).strftime("%Y-%m-%d_%H%M"), job["template"], job["njobs"], job["tjobs"]))
                if expand:
                    for subjob in job["jobs"]:
                        print("\t\t%d\t%s (%d)" % (subjob["jid"], subjob["script_path"], subjob["t_end"]))
    else:
        print("Nothing to list for '%s'" % what)

if __name__ == "__main__":
    cli()
