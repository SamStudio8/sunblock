from datetime import datetime
import os

import click
from zenlog import log

#TODO Should probably have an SGEJob class inheriting from Job to
#     modularise SGE specific functionality from generic job handling
class Job(object):

    def __init__(self):
        #TODO Could keep job-meta such as wait/execution time
        self.order = 0
        self.config = {}
        self.template_name = "sunblockjob"
        self.name = "sunblockjob"

        self.modules = []
        self.venv = None
        self.array = None

        self.pre_commands = []
        self.commands = []
        self.pre_log = []
        self.post_log = []
        self.post_checksum = []

        self.LIMIT = []
        self.WORKING_DIR = None

        self.shard = None

    #TODO Switch to OrderedDict
    def add_key(self, name, desc, prompt, type, validate=None):
        self.config[name] = {
            "key": name,
            "desc": desc,
            "prompt": prompt,
            "type": type,
            "value": None,
            "order": self.order,
            "validate": validate
        }
        self.order += 1

    def check(self):
        for key, conf in self.config.items():
            if conf["value"] is None:
                return False
        return True

    def set_key(self, key, value):
        if key not in self.config:
            log.warn("Attempted to set invalid key: '%s'" % key)
            return

        if self.config[key]["validate"] is not None:
            if not self.config[key]["validate"](value):
                log.error("'%s' is not a valid value for key '%s'. Aborting." % (str(value), key))
                sys.exit(1)
        self.config[key]["value"] = value

    def get_shards(self):
        return [{
        }]

    def prepare(self, job_prefix, job_basepath, queue_list, mem_gb, time_hours, cores):
        job_stdeo_path = os.path.join(job_basepath, "stdeo")
        job_config_path = os.path.join(job_basepath, "config")
        job_script_path = os.path.join(job_basepath, "script")
        job_log_path = os.path.join(job_basepath, "log")

        job_md5_log_path = os.path.join(job_log_path, job_prefix + ".md5")
        job_log_log_path = os.path.join(job_log_path, job_prefix + ".log")

        # Determine whether the job will be array-ed
        n = 1
        if self.array is not None:
            n = self.array["n"]

        # Build header
        sge_lines = [
            "#$ -S /bin/sh",
            "#$ -q %s" % ",".join(queue_list),
            "#$ -l h_vmem=%dG" % mem_gb,
            "#$ -l h_rt=%d:0:0" % time_hours,
            "#$ -R y",
            "#$ -t 1-%d" % n,
            "",
            "# Set log path (-j y causes -e to be ignored but we'll set it anyway)",
            "#$ -e %s/$JOB_NAME.$JOB_ID.$TASK_ID.e" % job_stdeo_path,
            "#$ -o %s/$JOB_NAME.$JOB_ID.$TASK_ID.o" % job_stdeo_path,
            "#$ -j y",
        ]

        if self.WORKING_DIR:
            sge_lines.append("#$ -wd %s" % self.WORKING_DIR)
            sge_lines.append("OUTDIR=%s" % self.WORKING_DIR)
        else:
            sge_lines.append("#$ -cwd")

        sge_lines.append("\n")

        if cores > 1:
            sge_lines.append("#$ -pe multithread %d" % cores)

        sge_lines.append("CURR_i=$(expr $SGE_TASK_ID - 1)")

        # Add array if defined
        if self.array is not None:
            #TODO throw error for empty array
            for i, f in enumerate(self.array["values"]):
                if i == 0:
                    # Open array definition
                    sge_lines.append("\n%s=(%s" % (self.array["name"], f.strip()))
                else:
                    sge_lines.append("\t%s" % f.strip())
            sge_lines.append(")")
            sge_lines.append("%s=${%s[$CURR_i]}" % (self.array["var"], self.array["name"]))

        # Append modules
        sge_lines.append("")
        sge_lines += ["module add %s" % name for name in self.modules]

        # Start venv if needed
        if self.venv is not None:
            sge_lines.append("source %s" % self.venv)

        sge_lines.append("\n#Pre Commands")
        # Pre Commands
        for line in self.pre_commands:
            sge_lines.append(line)

        sge_lines.append("\n#Pre Log")
        # Pre Logs
        for line in self.pre_log:
            sge_lines.append("echo \"[$(date)][$JOB_ID][$SGE_TASK_ID]: $(%s)\" >> %s" % (line, job_log_log_path))

        sge_lines.append("\n#Commands")
        # Commands
        for line in self.commands:
            sge_lines.append(line)

        sge_lines.append("\n#Post Log")
        # Post Log
        for line in self.post_log:
            sge_lines.append("echo \"[$(date)][$JOB_ID][$SGE_TASK_ID]: $(%s)\" >> %s" % (line, job_log_log_path))

        if len(self.post_checksum) > 0:
            sge_lines.append("\n#Checksum Output")
            for path in self.post_checksum:
                sge_lines.append("echo \"[$(date)][$JOB_ID][$SGE_TASK_ID]: $(md5sum `echo %s`)\" >> %s" % (path, job_md5_log_path))

        # Shut down the venv
        if self.venv is not None:
            sge_lines.append("\ndeactivate")

        return "\n".join(sge_lines)

    def define(self, shard=None):
        raise NotImplementedError()

    def use_module(self, module_name):
        #FUTURE Check module list
        self.modules.append(module_name)

    def use_venv(self, venv_path):
        #TODO Check path
        self.venv = venv_path

    def add_array(self, array_name, array_val, array_var):
        new_array_val = []
        if self.LIMIT:
            for i, value in array_val:
                if i in self.LIMIT:
                    new_array_val.append(value)
            array_val = new_array_val

        self.array = {
            "name": array_name,
            "values": array_val,
            "var": array_var,
            "n": len(array_val)
        }

    def set_pre_commands(self, pre_commands):
        self.pre_commands = pre_commands

    def set_commands(self, commands):
        self.commands = commands

    def add_pre_log_line(self, line):
        self.pre_log.append(line)

    def add_post_log_line(self, line):
        self.post_log.append(line)

    def add_post_checksum(self, path):
        self.post_checksum.append(path)

