from datetime import datetime
import json
import sys
import time

import click
from zenlog import log

import sunblock.util as util

#TODO Name job.out something less stupid
#TODO View SGE file
#TODO View output directory
#TODO Check .md5 file for output files
#TODO Email
#TODO Manifest files?
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

@cli.command(help="List available templates")
def templates():
    print("\n".join(util.get_template_list()))

@cli.command(help="Summarise job information")
@click.pass_context
def summary(ctx):
    ctx.forward(jobs)
    ctx.invoke(jobs, job_id="all", type="summary")

@cli.command(help="Report specific job information")
@click.pass_context
def summary(ctx):
    ctx.invoke(jobs, job_id="all", type="summary")

@cli.command(help="Summarise job information")
@click.argument('job_id')
@click.argument('type')
@click.option("--acct_path", default="/cm/shared/apps/sge/6.2u5p2/default/common/accounting", type=click.Path(exists=True, readable=True))
@click.option("--format", default="jobnumber:taskid:hostname:jobname:exit_status:time.time_req_td:time.time_taken_td:mem.mem_req_tot:mem.mem_used")
@click.option("--expand", is_flag=True)
@click.option("--noisy", is_flag=True)
@click.option("--failed", is_flag=True)
def jobs(job_id, type, acct_path, format, expand, noisy, failed):
    ACCEPTABLE_TYPES = ["summary", "table"]
    if type not in ACCEPTABLE_TYPES:
        print("Invalid report type '%s'." % type)
        sys.exit(1)

    jobs = util.get_job_list()
    if not jobs:
        print("No jobs found.")
        sys.exit(0)

    if job_id.lower() != "all":
        try:
            job_id = int(job_id)
            job = jobs[job_id]
        except TypeError:
            print("Invalid job id.")
            sys.exit(1)
        except IndexError:
            print("Job with that id does not exist.")
            sys.exit(1)
    else:
        job_id = None

    from acct_parse import Account
    job_statii = {}
    jid_to_i = {}

    # Collect jids to check in qacct
    for i, job in enumerate(jobs):
        if job_id:
            if i != job_id:
                continue

        job_statii[i] = {
            "subjobs": {},
            "total_found": 0,
            "total_nonzero": 0
        }

        for subjob in job["jobs"]:
            job_statii[i]["subjobs"][subjob["jid"]] = {
                "jid": subjob["jid"],
                "found": 0,
                "nonzero": 0,
                "array_jobs": [] # Store tids
            }
            jid_to_i[subjob["jid"]] = i


    acct = Account(acct_path, noisy)
    for id, job in sorted(acct.jobs.items()):
        jid = job["jobnumber"]
        tid = job["taskid"]

        if jid in jid_to_i:
            curr_jid_i = jid_to_i[jid]
            job_statii[curr_jid_i]["total_found"] += 1
            job_statii[curr_jid_i]["subjobs"][jid]["found"] += 1
            job_statii[curr_jid_i]["subjobs"][jid]["tid"] = tid

            if job["exit_status"] != 0:
                job_statii[curr_jid_i]["total_nonzero"] += 1
                job_statii[curr_jid_i]["subjobs"][jid]["nonzero"] += 1
            else:
                if failed:
                    continue

            job_statii[curr_jid_i]["subjobs"][jid]["array_jobs"].append(tid)

    for i in job_statii:
        if type.lower() == "summary":
            print("%d\t%s" % (i, jobs[i]["template"]))
            if expand:
                for j, jid in enumerate(job_statii[i]["subjobs"]):
                    pc = float(job_statii[i]["subjobs"][jid]["found"]) / jobs[i]["jobs"][j]["t_end"]
                    pc_75 = int(pc*75)
                    print("\t%d [%s%s] %.2f%% (%d of %d, %d waiting, %d failed)" % (jid, pc_75*'=', (75-pc_75)*' ', pc*100, job_statii[i]["subjobs"][jid]["found"], jobs[i]["jobs"][j]["t_end"], jobs[i]["jobs"][j]["t_end"] - job_statii[i]["subjobs"][jid]["found"], job_statii[i]["subjobs"][jid]["nonzero"]))
            else:
                pc = float(job_statii[i]["total_found"]) / jobs[i]["tjobs"]
                pc_75 = int(pc*75)
                print("[%s%s] %.2f%% (%d of %d, %d waiting, %d failed)" % (pc_75*'=', (75-pc_75)*' ', pc*100, job_statii[i]["total_found"], jobs[i]["tjobs"], jobs[i]["tjobs"] - job_statii[i]["total_found"], job_statii[i]["total_nonzero"]))
        elif type.lower() == "table":
            for j, jid in enumerate(job_statii[i]["subjobs"]):
                for tid in sorted(job_statii[i]["subjobs"][jid]["array_jobs"]):
                    print(acct.print_job(jid, tid, format))

if __name__ == "__main__":
    cli()
