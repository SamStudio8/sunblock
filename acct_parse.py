"""
Provides functions for the purpose of parsing a Sun Grid Engine (SGE)
accounting file for job metadata.
"""

__author__ = "Sam Nicholls <msn@aber.ac.uk>"
__copyright__ = "Copyright (c) Sam Nicholls"
__version__ = "0.0.32"
__maintainer__ = "Sam Nicholls <msn@aber.ac.uk>"

import sys

from datetime import datetime, timedelta

class Account(object):

    def __init__(self, acct_path, noisy, parse_extra=None, annot=True, last_ts=None, only_jid=None):

        try:
            self.fh = open(acct_path)
        except:
            self.fh = []

        self.jobs = {}
        self.lines = []
        self.parse_extra = parse_extra
        self.noisy = noisy
        self.last_ts = last_ts
        self.only_jid = only_jid
        self.annot=annot

        self.fields_encountered = []
        self.parse()

    def parse(self):

        for line in self.fh:
            if line[0] == "#":
                # Skip comments
                continue

            j = self.parse_job(line)

            if j is not None:
                if self.last_ts:
                    if j["end_time"] < self.last_ts:
                        continue

                if self.only_jid:
                    if j["jobnumber"] != self.only_jid:
                        continue

                self.lines.append(line)

                if self.annot:
                    ja = self.annotate_job(j)
                    j.update(ja)

                job_str = str(j["jobnumber"])
                if j["taskid"] > 0:
                    job_str += (":%d" % j["taskid"])

                self.jobs[job_str] = j
                self.jobs[job_str]["extra"] = {}

                if self.parse_extra:
                    self.jobs[job_str]["extra"] = self.parse_extra(j, ja)

        #self.fh.close()


    def parse_job(self, line):
        """
        Given a valid line from an SGE accounting file, return a dictionary
        of each of the fields as a key. Values are parsed as described in
        http://manpages.ubuntu.com/manpages/lucid/man5/sge_accounting.5.html
        """

        fields = line.strip().split(":")

        try:
            return {
                "qname": fields[0],
                "hostname": fields[1],
                "group": fields[2],
                "owner": fields[3],
                "jobname": fields[4],
                "jobnumber": int(fields[5]),
                "account": fields[6],
                "priority": float(fields[7]),
                "qsub_time": int(fields[8]),
                "start_time": int(fields[9]),
                "end_time": int(fields[10]),
                "failed": int(fields[11]),
                "exit_status": int(fields[12]),
                "ru_wallclock": float(fields[13]),
                "ru_utime": float(fields[14]),
                "ru_stime": float(fields[15]),
                "ru_maxrss": float(fields[16]),
                "ru_ixrss": float(fields[17]),
                "ru_ismrss": float(fields[18]),
                "ru_idrss": float(fields[19]),
                "ru_isrss": float(fields[20]),
                "ru_minflt": float(fields[21]),
                "ru_majflt": float(fields[22]),
                "ru_nswap": float(fields[23]),
                "ru_inblock": float(fields[24]),
                "ru_oublock": float(fields[25]),
                "ru_msgsnd": float(fields[26]),
                "ru_msgrcv": float(fields[27]),
                "ru_nsignals": float(fields[28]),
                "ru_nvcsw": float(fields[29]),
                "ru_nivcsw": float(fields[30]),
                "project": fields[31],
                "department": fields[32],
                "granted_pe": fields[33],
                "slots": int(fields[34]),
                "taskid": int(fields[35]),
                "cpu": float(fields[36]),
                "mem": float(fields[37]),
                "io": float(fields[38]),
                "category": self.parse_category(fields[39], self.noisy),
                "iow": float(fields[40]),
                "pe_taskid": int(fields[41]) if fields[41] != "NONE" else None,
                "maxvmem": float(fields[42]),
                "arid": fields[43],
                "ar_submission_time": fields[44]
            }
        except IndexError:
            if self.noisy:
                sys.stderr.write("[WARN] Seemingly invalid job line encountered. Skipping.\n")
            return None

    @staticmethod
    def parse_category(category_str, noisy):
        """
        Parse the queue 'category' field as found in a job line, typically
        containing additional options specified to the queue on submission,
        including runtime and memory resource requests.
        """

        # NOTE I'm not overly happy with the parsing here but wanted to avoid
        #      importing the re library and doing a bunch of regular expressions.
        fields = category_str.split("-")

        req_usergroup = "default"

        # h_rt defaults to 1800000 seconds (500 hours)
        req_l = {"h_vmem": None, "h_rt": 1800000, "h_stack": None}
        req_queues = []

        for field in fields:
            if len(field) == 0:
                # First field will always be empty as line (should) begin with delimiter
                continue

            if field[0:2] == "pe":
                # Expected format: "-pe multithread <int>"
                # NOTE Must appear before a check on field[0] == 'p'
                # NOTE key:value not stored as job_dict['slots'] already has the same data
                #      but block here suppresses "unknown string data" error in else.
                pass
            elif field[0] == "U":
                # Expected format: "-U <str>"
                req_usergroup = field.split(" ")[1]
            elif field[0] == "l":
                # Expected format: "-l h_vmem=<float>[Mm|Gg],h_stack=<float>[Mm|Gg],h_rt=<int>"
                for sub_field in field[1:].strip().split(","):
                    key, value = sub_field.strip().split("=")

                    if key.lower() == "h_stack" or key.lower() == "h_vmem":
                        # Convert value to MB
                        if value[-1].upper() == "G":
                            #TODO Multiply by 1000 or 1024?
                            value = float(value[:-1]) * 1024
                        elif value[-1].upper() == "M":
                            value = float(value[:-1])
                        elif value[-1].isdigit() is True:
                            # Convert bytes to MB
                            value = float(value) / 1000000
                        else:
                            if noisy:
                                sys.stderr.write("[WARN] Unknown unit of memory encountered parsing -l subfield in queue category field: %s\n" % value[-1].upper())
                                sys.stderr.write("       %s\n" % field.strip())
                    elif key.lower() == "h_rt":
                        #NOTE Is in seconds
                        value = int(value)
                    else:
                        if noisy:
                            sys.stderr.write("[WARN] Unknown subfield encountered parsing queue category field: %s\n" % key)
                            sys.stderr.write("       %s\n" % field.strip())

                    # Insert the new key:value pair to the req_l dict
                    req_l[key] = value
            elif field[0] == "q":
                # Expected format: "-q <str1>,<str2>,...,<strN>"
                req_queues = [f.strip() for f in field[1:].split(",")[0:]]
            elif field[0] == "u":
                # Expected format: "-u <str>"
                #NOTE No action required
                pass
            else:
                if noisy:
                    sys.stderr.write("[WARN] Unknown queue category string field: %s\n" % field)
                    sys.stderr.write("       %s\n" % category_str)

        return {
            "req_usergroup": req_usergroup,
            "req_l": req_l,
            "req_queues": req_queues,
        }

    @staticmethod
    def annotate_job(job_dict):
        """
        Given a job dict as created by `parse_job`, return a dict of potentially
        useful additional metadata.

        mem
            mem_req         gibibytes of memory requested per slot
            mem_req_tot     gibibytes of memory requested across all slots
            mem_used        maximum memory used in gibibytes
            mem_diff        mem_req_tot - mem_used
            mem_pct         percentage of memory requested in respect to amount needed

        time
            qsub_dt         qsub_time as datetime
            start_dt        start_time as datetime
            end_dt          end_time as datetime
            time_taken      end_time - start_time (as unix timestamp)
            time_taken_td   time_taken as datetime timedelta
            time_req        seconds requested
            time_req_td     time_req as timedelta
        """

        mem_req_tot = (job_dict["category"]["req_l"]["h_vmem"]/1000)
        #mem_req_tot = mem_req * job_dict["slots"]
        mem_req = float(mem_req_tot) / job_dict["slots"]
        mem_used = (job_dict["maxvmem"]/1000000000)*0.931323
        mem_diff = mem_req_tot - mem_used

        try:
            mem_pct = (mem_req_tot / mem_used) * 100
        except ZeroDivisionError:
            mem_pct = 0

        start_dt = datetime.fromtimestamp(job_dict["start_time"])
        end_dt = datetime.fromtimestamp(job_dict["end_time"])
        time_taken_td = end_dt - start_dt
        time_taken = job_dict["end_time"] - job_dict["start_time"]

        time_req = job_dict["category"]["req_l"]["h_rt"]
        time_req_td = timedelta(seconds=time_req)
        time_req = time_req/(60*60)

        return {
            "mem": {
                "mem_req": mem_req,
                "mem_req_tot": mem_req_tot,
                "mem_used": mem_used,
                "mem_diff": mem_diff,
                "mem_pct": mem_pct
            },
            "time": {
                "qsub_dt": datetime.fromtimestamp(job_dict["qsub_time"]),
                "start_dt": datetime.fromtimestamp(job_dict["start_time"]),
                "end_dt": datetime.fromtimestamp(job_dict["end_time"]),
                "time_taken": time_taken,
                "time_taken_td": time_taken_td,
                "time_req": time_req,
                "time_req_td": time_req_td,
            }
        }

    def print_job(self, jid, tid, fmt_str):
        fields = fmt_str.split(":")

        job_str = str(jid)
        if tid:
            job_str += ":%s" % tid

        job = self.jobs[job_str]
        out_str = []

        for field in fields:
            if "." in field:
                field, subfield = field.split(".")
                value = job.get(field, {}).get(subfield, None)
            else:
                value = job.get(field, None)

            if value is not None:
                #TODO Will ignore fields which genuinely are allowed to be None
                if type(value) is float:
                    out_str.append("{0:.2f}".format(value))
                else:
                    out_str.append(str(value))
            else:
                if field not in self.fields_encountered:
                    self.fields_encountered.append(field)

                    if self.noisy:
                        sys.stderr.write("[WARN] Field '%s' not found in job object.\n" % field)

        return "\t".join(out_str)

