from datetime import datetime
from datetime import timedelta
import glob
import json
import os
import re
import sys
import time

import click
import requests
requests.packages.urllib3.disable_warnings()

from zenlog import log

import sunblock.util
import sunblock.templates as _templates

#TODO For the love of god make RSUB less awful
#TODO Make pipelines repeatable? Job X follows Y or something. Be able to generate reports...
#     "add-labbook limpet job 7"
#TODO Name job.out something less stupid
#     ..put them somewhere useful, a staging area of ~/sunblock?
#TODO View SGE file
#TODO View output directory
#TODO Check .md5 file for output files
#TODO Email
#TODO Delete/hide jobs
#TODO Add basepath and job prefix to sdb
#TODO Cancel jobs (ie. kill all SGE jobs related to a sunblock id)
#     * Offer to remove output dir
#TODO Store job meta
#TODO Remove manifest name from SGE logs, makes it harder to grep - or make sunblock look for you?
#TODO We're going to need to start caching SGE data:
#   a) To stop wasting time fetching it
#   b) Because it goes away
#   - Update cache on every command?
#   - Update cache via CRON (and force update when calling jobs/summary/execute
#   - Force jobs to report back their status somehow -- probably unworkable in the long run
#   * We should store a map of sge+jid>sun+jid
#TODO How to pass job-specific requirements to a job-class (ie. #threads == multicores)
#TODO Mark RSUB jobs as RSUB, when RSUBbing an RSUB, cascade back to origin
#TODO Allow optional --file-extension argument to set queries
#TODO More useful manifest builder, accept *multiple* n-tuples of files
#TODO File/dir handling of input should be performed by the add-queries code, not in each job
@click.group()
def cli():
    pass

@cli.command(help="Notify sunblock-server of a job start")
@click.argument("jid")
@click.argument("tid")
@click.argument("hostname")
def report_job_start(jid, tid, hostname):
    payload = {
        "api_key": util.get_sunblock_conf()["api_key"],
        "jid": jid,
        "tid": tid,

        "job": {},
        "subjob": {
            "start_dt": str(datetime.now()),
            "hostname": hostname
        }
    }

    host = util.get_sunblock_conf()["sunblock_host"]
    requests.post(host+"/jobs/update/", json=payload, verify=False)

@cli.command(help="Notify sunblock-server of a tentative job end")
@click.argument("jid")
@click.argument("tid")
def report_job_end(jid, tid):
    payload = {
        "api_key": util.get_sunblock_conf()["api_key"],
        "jid": jid,
        "tid": tid,

        "job": {},
        "subjob": {
            "end_dt": str(datetime.now()),
            "tentative_completed": True
        }
    }
    host = util.get_sunblock_conf()["sunblock_host"]
    requests.post(host+"/jobs/update/", json=payload, verify=False)

@cli.command(help="Resubmit jobs (or a subset of their tasks)")
@click.argument("tasks")
@click.option("--dry")
def resub(tasks, dry):

    jobs = util.get_job_list()
    if not jobs:
        print("No jobs found.")
        sys.exit(0)

    # Parse task string
    to_resub = {}
    ranges = tasks.strip().split(",")
    n = 0
    for r in ranges:
        if ":" in r :
            jid = int(r.split(":")[0])
            tids = r.split(":")[1]

            for tid_range in tids.split(","):
                if "-" in tid_range:
                    start = int(tid_range.split("-")[0])
                    end = int(tid_range.split("-")[1])
                else:
                    start = end = int(tid_range)

                if jid not in to_resub:
                    to_resub[jid] = []
                to_resub[jid].extend(range(start, end + 1))
                print to_resub
                n += (end - start + 1)
        else:
            to_resub[int(r)] = []
            start = end = int(r)
            n += (end - start + 1)

    job_working_dir = click.prompt("Working Dir [default: .]", type=click.Path(exists=True, writable=True))
    if job_working_dir == ".":
        job_working_dir = None
    queue_list = [q.strip() for q in click.prompt("Queue List [comma delimited]", default="large.q,amd.q,intel.q").strip().split(",")]
    mem_gb = click.prompt("Memory (GB)", type=int)
    time_hours = click.prompt("Time (Hours)", type=int)
    cores = click.prompt("Slots (PE Cores)", type=int)
    script_paths = []
    now = datetime.now()

    # Collect jids
    for i, job in enumerate(jobs):
        for j, subjob in enumerate(job["jobs"]):

            if subjob["jid"] in to_resub:
                # Get manifest
                #TODO This is quite a horrible hack to get the manifest file from the script
                manifest = []
                try:
                    with open(subjob["script_path"][:-4] + ".manifest") as manifest_fh:
                        for i, line in enumerate(manifest_fh):
                            tid = i + 1
                            fpath = line.strip()
                            if len(to_resub[subjob["jid"]]) == 0:
                                manifest.append(fpath)
                            else:
                                if tid in to_resub[subjob["jid"]]:
                                    manifest.append(fpath)
                except:
                    # No manifest, probably.
                    pass

                #TODO Also pretty terrible
                job_prefix = job["prefix"]
                job_basepath = subjob["script_path"].split("/script/")[0]
                job_stdeo_path = os.path.join(job_basepath, "stdeo")
                job_config_path = os.path.join(job_basepath, "config")
                job_script_path = os.path.join(job_basepath, "script")
                job_log_path = os.path.join(job_basepath, "log")
                job_conf = subjob["script_path"].split("/script/")[0] + "/config/" + job_prefix + ".conf"
                with open(job_conf) as conf_fh:
                    config = json.loads(conf_fh.read())

                    re_job = _templates.get_template_list()[job["template"]]()
                    for key, conf in sorted(re_job.config.items(), key=lambda x: x[1]["order"]):
                        re_job.set_key(key, config[key])

                    #TODO THIS IS THE WORST OF ALL
                    shard = re_job.get_shards()[j]
                    re_job.shard = shard
                    re_job.define(shard=shard)
                    re_job.WORKING_DIR = job_working_dir

                    #TODO Are we going to just store conf/manifest of rsub seperately?
                    re_job_prefix = "%s__RSUB__%s" % (job_prefix, now.strftime("%Y-%m-%d_%H%M"))
                    re_job_dt = "RSUB__%s" % (now.strftime("%Y-%m-%d_%H%M"))

                    # Now override the array
                    re_job.array["values"] = manifest
                    re_job.array["n"] = len(manifest)

                    # Grim
                    # This *works* because SGEJob inherits from Job anyway, so there's no danger
                    # in monkey patching the class. In future this will be much less terrible.
                    if util.get_sunblock_conf()["engine"] == "SGE":
                        from jobs.job import SGEJob
                        job.__class__.__bases__ = (SGEJob, )
                    elif util.get_sunblock_conf()["engine"] == "LSF":
                        from jobs.job import LSFJob
                        job.__class__.__bases__ = (LSFJob, )
                    else:
                        print "Sunblock currently only supports SGE and attempts to support LSF.\nBravely selecting neither, you're probably about to have a bad time."
                    job_script = (re_job.prepare(job_prefix, job_basepath, queue_list, mem_gb, time_hours, cores))


                    # New config
                    job_config_config_path = os.path.join(job_config_path, re_job_prefix + ".conf")
                    to_write = {"template": config["template"]}
                    for key, conf in re_job.config.items():
                        to_write[key] = conf["value"]
                    out_fh = open(job_config_config_path, "w")
                    out_fh.write(json.dumps(to_write))
                    out_fh.close()

                    # New Manifest
                    #TODO ew :(
                    manifest_shard = ""
                    if re_job.array:
                        if re_job.shard:
                            #TODO Assumes name key...
                            manifest_shard = "." + re_job.shard["name"]
                        job_manifest_script_path = os.path.join(job_script_path, job_prefix + "%s.%s.manifest" % (manifest_shard, re_job_dt))
                        fo = open(job_manifest_script_path, "w")
                        fo.writelines("\n".join(v.strip() for v in re_job.array["values"]))
                        fo.close()

                    # Write script
                    manifest_shard = ""
                    if re_job.array:
                        if re_job.shard:
                            #TODO Assumes name key...
                            try:
                                manifest_shard = "." + re_job.shard["name"]
                            except:
                                pass

                    job_script_script_path = os.path.join(job_script_path, job_prefix + "%s.%s.sge" % (manifest_shard, re_job_dt))

                    fo = open(job_script_script_path, "w")
                    fo.writelines(job_script)
                    fo.close()

                    script_paths.append(job_script_script_path)

    if not dry:
        if len(script_paths) == 0:
            print("[WARN] No script names returned by config.")
        else:
            new_record = {
                "njobs": 0,
                "tjobs": 0,
                "jobs": [],
                "timestamp": int(time.mktime(datetime.now().timetuple())),
                "template": re_job.template_name,
                "prefix": re_job_prefix,
                "working_dir": re_job.WORKING_DIR,
            }

            from subprocess import check_output
            for name in script_paths:
                p = check_output([SUBMIT_COMMAND, '-terse', name])

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
                new_record["jid"] = int(jid)

            if not util.append_job_list(new_record):
                print("Error appending to sunblock record file.")
            else:
                print("Submitted %d job arrays, totalling %d jobs." % (new_record["njobs"], new_record["tjobs"]))

@cli.command(help="Execute a configured job")
@click.argument('template')
@click.option("--dry", is_flag=True)
def execute(template, dry):

    # Does template exist?
    if template not in _templates.get_template_list():
        log.error("No template for job type: '%s'" % template)
        sys.exit(1)

    job = _templates.get_template_list()[template]()
    log.debug("Found template for job type: '%s' with %d required keys" % (template, len(job.config)))
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        log.debug("\t%s: %s" % (key, conf["desc"]))

    # Prompt
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        job.set_key(key, click.prompt(conf["prompt"], default=conf["default"], type=conf["type"]))

    # Check config
    #TODO More helpful output -- what key is missing or incorrect?
    if not job.check():
        log.error("Invalid job configuration. Aborting.")
        sys.exit(1)

    # Name the job...
    #TODO Make it filepath friendly...
    #TODO Force resolving of working dir
    user_job_name = click.prompt("Job Name")
    job_name = "%s-%s-%s" % (job.template_name, datetime.now().strftime("%Y-%m-%d_%H%M"), user_job_name)
    job_working_dir = click.prompt("Working Dir [default: .]", type=click.Path(exists=True, writable=True))
    if job_working_dir == ".":
        job_working_dir = None

    #FUTURE Abstract SGE specific behaviour...
    #       Job types will specify a "prompt_user" to capture additional
    #       metadata required by the particular scheduler of choice.
    #       We'll do that here for now because ffs I just want to do my job.
    queue_list = [q.strip() for q in click.prompt("Queue List [comma delimited]", default="large.q,amd.q,intel.q").strip().split(",")]
    mem_gb = click.prompt("Memory (GB)", type=int)
    time_hours = click.prompt("Time (Hours)", type=int)
    cores = click.prompt("Slots (PE Cores)", type=int)

    # Get sunblock_id and create a directory
    #FUTURE Create soft links to output directory?
    job_basepath = os.path.join(util.get_sunblock_path(), job_name)

    job_queue = []
    job_scripts = []

    config = job.config
    config["template"] = template
    for shard in job.get_shards():
        #TODO gross.
        # Re-load
        job = _templates.get_template_list()[config["template"]]()
        for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
            job.set_key(key, config[key]["value"])
        job.name = job_name
        job.shard = shard
        job.WORKING_DIR = job_working_dir

        job_prefix = job_name
        job.prefix = job_prefix

        job.define(shard=shard)

        job_queue.append(job)

        # Grim
        # This *works* because SGEJob inherits from Job anyway, so there's no danger
        # in monkey patching the class. In future this will be much less terrible.
        if util.get_sunblock_conf()["engine"] == "SGE":
            from jobs.job import SGEJob
            job.__class__.__bases__ = (SGEJob, )
        elif util.get_sunblock_conf()["engine"] == "LSF":
            from jobs.job import LSFJob
            job.__class__.__bases__ = (LSFJob, )
        else:
            print "Sunblock currently only supports SGE and attempts to support LSF.\nBravely selecting neither, you're probably about to have a bad time."
        job_scripts.append(job.prepare(job_prefix, job_basepath, queue_list, mem_gb, time_hours, cores))

    # All jobs prepared ok, write the files
    #TODO Check dir doesn't already exist...
    os.mkdir(job_basepath)
    job_stdeo_path = os.path.join(job_basepath, "stdeo")
    job_config_path = os.path.join(job_basepath, "config")
    job_script_path = os.path.join(job_basepath, "script")
    job_log_path = os.path.join(job_basepath, "log")
    os.makedirs(job_stdeo_path)
    os.makedirs(job_config_path)
    os.makedirs(job_script_path)
    os.makedirs(job_log_path)

    # "Move" config
    job_config_config_path = os.path.join(job_config_path, job_prefix + ".conf")
    to_write = {"template": config["template"]}
    for key, conf in job.config.items():
        to_write[key] = conf["value"]
    out_fh = open(job_config_config_path, "w")
    out_fh.write(json.dumps(to_write))
    out_fh.close()

    # Write manifests
    for job in job_queue:
        #TODO ew :(
        manifest_shard = ""
        if job.array:
            if job.shard:
                #TODO Assumes name key...
                manifest_shard = "." + job.shard["name"]
            job_manifest_script_path = os.path.join(job_script_path, job_prefix + "%s.manifest" % manifest_shard)

            fo = open(job_manifest_script_path, "w")
            fo.writelines("\n".join(v.strip() for v in job.array["values"]))
            fo.close()

    # Write scripts
    script_paths = []
    if not dry:
        for i, job in enumerate(job_queue):
            manifest_shard = ""
            if job.array:
                if job.shard:
                    #TODO Assumes name key...
                    try:
                        manifest_shard = "." + job.shard["name"]
                    except:
                        pass


            if util.get_sunblock_conf()["engine"] == "SGE":
                ext = "sge"
            elif util.get_sunblock_conf()["engine"] == "LSF":
                ext = "lsf"
            else:
                ext = "script"
            job_script_script_path = os.path.join(job_script_path, job_prefix + "%s.%s" % (manifest_shard, ext))

            fo = open(job_script_script_path, "w")
            fo.writelines(job_scripts[i])
            fo.close()

            script_paths.append(job_script_script_path)

    # QSUB
    if not dry:
        if len(script_paths) == 0:
            print("[WARN] No script names returned by config.")
        else:
            new_record = {
                "njobs": 0,
                "tjobs": 0,
                "jobs": [],
                "timestamp": int(time.mktime(datetime.now().timetuple())),
                "template": job.template_name,
                "prefix": job.prefix,
                "working_dir": job.WORKING_DIR,
                "name": job_name
            }
            new_record_s = {
                "api_key": util.get_sunblock_conf()["api_key"],
                "farm": util.get_sunblock_conf()["farm"],
                "jobs": [],
                "working_dir": job.WORKING_DIR,
            }

            for l, name in enumerate(script_paths):
                ts = 1
                te = 1

                if util.get_sunblock_conf()["engine"] == "SGE":
                    from subprocess import check_output
                    p = check_output(['qsub', '-terse', name])

                    try:
                        jid, trange = p.strip().split(".")
                    except ValueError:
                        jid = p.strip()
                        trange = None
                    if trange:
                        ts, te = trange.split(":")[0].split("-")

                elif util.get_sunblock_conf()["engine"] == "LSF":
                    from subprocess import Popen, PIPE
                    p = Popen(['bsub'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    # Spoonfeed LSF because ffs what
                    p = p.communicate("\n".join(open(name).readlines()))

                    # Get JID and hack trange
                    try:
                        jid = re.search("\d+", p[0]).group(0)
                    except:
                        raise Exception("Could not get job id from bsub?")

                    trange = job_queue[l].N
                    ts = 1
                    te = trange
                else:
                    raise Exception()

                subjobs = []
                for k in range(int(ts), int(te) + 1):
                    subjobs.append({
                        "tid": k,
                        "manifest_line": job.array["values"][k-1],
                        "log_path": os.path.join(job_stdeo_path, "%d.o" % k)
                    })

                new_record["jobs"].append({
                    "jid": int(jid),
                    "t_start": int(ts),
                    "t_end": int(te),
                    "script_path": name
                })
                new_record_s["jobs"].append({
                    "jid": int(jid),
                    "name": user_job_name,
                    "owner": "sunblock",
                    "subjobs": subjobs,

                    "slots": cores,
                    "mem_req": int(mem_gb * 1000000000 * 1.073741824),
                    "time_req": time_hours,

                    "script_text": job_scripts[l],
                    "template": job.template_name,
                })
                new_record["njobs"] += 1
                new_record["tjobs"] += int(te)
                new_record["jid"] = int(jid)

            host = util.get_sunblock_conf()["sunblock_host"]
            r = requests.post(host+"/jobs/submit/", json=new_record_s, verify=False)
            if not util.append_job_list(new_record):
                print("Error appending to sunblock record file.")
            else:
                print("Submitted %d job arrays, totalling %d jobs." % (new_record["njobs"], new_record["tjobs"]))

@cli.command(help="List available templates")
def templates():
    print("\n".join(_templates.get_template_list()))

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

    # Try to get last timestamp
    last_ts = None
    try:
        last = util.get_job_list()[-1]
        last_ts = last["timestamp"]
    except IndexError:
        pass

    # Parse old cached entries
    seen_jids = []
    acct = Account(util.get_cache_acct_fh(), noisy)
    for id, job in sorted(acct.jobs.items()):
        jid = job["jobnumber"]
        tid = job["taskid"]

        if jid in jid_to_i:
            curr_jid_i = jid_to_i[jid]
            seen_jids.append(jid)

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

    seen_jids = set(seen_jids)
    for seen in seen_jids:
        del jid_to_i[seen]

    # Parse new entries
    acct = Account(acct_path, noisy, last_ts=last_ts)
    count = -1
    for id, job in sorted(acct.jobs.items()):
        count += 1
        jid = job["jobnumber"]
        tid = job["taskid"]

        if jid in jid_to_i:
            # Write new entry
            util.append_cache_account(acct.lines[count])

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
            print("%d\t%s: %s" % (i, jobs[i]["template"], jobs[i].get("name", "untitled")))
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
