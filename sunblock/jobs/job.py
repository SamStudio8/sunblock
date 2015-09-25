from datetime import datetime
import os

import click
from zenlog import log

from .. import util

class Job(object):

    def __init__(self):
        #TODO Could keep job-meta such as wait/execution time
        self.order = 0
        self.config = {}
        self.template_name = "sunblockjob"
        self.name = "sunblockjob"
        self.engine = "sunblock"

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
        self.FORWARD_ENV = False
        self.IGNORE_UNSET = False

        self.shard = None
        self.prefix = None

    #TODO Switch to OrderedDict
    def add_key(self, name, desc, prompt, type, validate=None, default=None):
        self.config[name] = {
            "key": name,
            "desc": desc,
            "prompt": prompt,
            "type": type,
            "value": None,
            "order": self.order,
            "validate": validate,
            "default": default
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
        JOB_PATHS = self.prepare_paths(job_prefix, job_basepath)

        # Determine whether the job will be array-ed
        n = 1
        if self.array is not None:
            n = self.array["n"]
            self.N = n

        lines = self.prepare_header(queue_list, mem_gb, time_hours, cores, JOB_PATHS, n=n)

        # Append modules
        lines.append("# Modules and venvs")
        lines.append("")
        lines += ["module add %s" % name for name in self.modules]

        # Open sunblock venv
        lines.append("source %s/bin/activate" % util.get_sunblock_conf()["sunblock_venv"])
        lines.append("sunblock report_job_start $SUNBLOCK_JOB_ID $SUNBLOCK_TASK_ID $HOSTNAME")

        # Start venv if needed
        if self.venv is not None:
            lines.append("source %s" % self.venv)

        # Housekeeping
        #TODO Flag for enabling core dumps
        lines.append("# Housekeeping")
        if not self.IGNORE_UNSET:
            lines.append("# * Abort on unset variable")
            lines.append("set -u")
        lines.append("# * Abort on non-zero")
        lines.append("set -e")
        lines.append("# * Disable core dumps")
        lines.append("ulimit -c 0")
        lines.append("")

        lines.append("CURR_i=$(($SUNBLOCK_TASK_ID - 1))")

        # Add array if defined
        if self.array is not None:
            #TODO throw error for empty array
            for i, f in enumerate(self.array["values"]):
                if i == 0:
                    # Open array definition
                    lines.append("\n%s=(%s" % (self.array["name"], f.strip()))
                else:
                    lines.append("\t%s" % f.strip())
            lines.append(")")
            lines.append("%s=${%s[$CURR_i]}" % (self.array["var"], self.array["name"]))

        lines.append("\n#Pre Commands")
        # Pre Commands
        for line in self.pre_commands:
            lines.append(line)

        lines.append("\n#Pre Log")
        # Pre Logs
        for line in self.pre_log:
            lines.append("echo \"[$(date)][$SUNBLOCK_JOB_ID][$SUNBLOCK_TASK_ID]: $(%s)\" >> %s" % (line, JOB_PATHS["log_log_path"]))

        lines.append("\n#Commands")
        # Commands
        for line in self.commands:
            lines.append(line)

        lines.append("\n#Post Log")
        # Post Log
        for line in self.post_log:
            lines.append("echo \"[$(date)][$SUNBLOCK_JOB_ID][$SUNBLOCK_TASK_ID]: $(%s)\" >> %s" % (line, JOB_PATHS["log_log_path"]))

        if len(self.post_checksum) > 0:
            lines.append("\n#Checksum Output")
            for path in self.post_checksum:
                lines.append("echo \"[$(date)][$SUNBLOCK_JOB_ID][$SUNBLOCK_TASK_ID]: $(md5sum `echo %s`)\" >> %s" % (path, JOB_PATHS["md5_log_path"]))

        lines.append("sunblock report_job_end $SUNBLOCK_JOB_ID $SUNBLOCK_TASK_ID")
        # Shut down the venv
        if self.venv is not None:
            lines.append("\ndeactivate")

        return "\n".join(lines)

    def prepare_header(self, queue_list, mem_gb, time_hours, cores, PATHS, n=1):
        raise NotImplementedError("prepare_header")

    def prepare_paths(self, job_prefix, job_basepath):
        paths = {
            "stdeo_path": os.path.join(job_basepath, "stdeo"),
            "config_path": os.path.join(job_basepath, "config"),
            "script_path": os.path.join(job_basepath, "script"),
            "log_path": os.path.join(job_basepath, "log"),
        }

        paths["md5_log_path"] = os.path.join(paths["log_path"], job_prefix + ".md5")
        paths["log_log_path"] = os.path.join(paths["log_path"], job_prefix + ".log")

        return paths

    def define(self, shard=None):
        raise NotImplementedError("define")

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


class LSFJob(Job):

    def __init__(self):
        super(LSFJob, self).__init__()
        self.template_name = "sunblockjob-lsf"
        self.name = "sunblockjob-lsf"
        self.engine = "LSF"
        self.N = 1

    def prepare_header(self, queue_list, mem_gb, time_hours, cores, JOB_PATHS, n=1):
        # Build header
        lines = [
            "#BSUB -L /bin/sh",
            "#BSUB -q %s" % ",".join(queue_list),
            "#BSUB -M %d" % (mem_gb * 1000),
            "#BSUB -R \"select[mem>%d] rusage[mem=%d]\"" % (mem_gb * 1000, mem_gb * 1000),
            #"#BSUB -W %d:0" % time_hours, # Banned at Sanger # TODO Need more generic way of specifying these rules
            "#BSUB -J job[1-%d]" % n,
            "#BSUB -G %s" % util.get_sunblock_conf()["lsf-group"], # Required for submission
            "",
            "#BSUB -o %s/$LSB_JOBINDEX.eo" % JOB_PATHS["stdeo_path"],
        ]

        if not self.FORWARD_ENV:
            print "WARNING: LSF automatically forwards environment variables!"

        if cores > 1:
            # Not sure if this is the correct way to multithread (though guess it
            # depends on whether user wants OpenMP or not)
            lines.append("#BSUB -n %d" % cores)

        lines.append("")

        if self.WORKING_DIR:
            lines.append("#BSUB -cwd %s" % self.WORKING_DIR)
            lines.append("OUTDIR=%s" % self.WORKING_DIR)
        else:
            #lines.append("#BSUB -cwd")
            lines.append("OUTDIR=`pwd -P`")

        lines.append("\nSUNBLOCK_JOB_ID=$LSB_JOBID")
        lines.append("SUNBLOCK_TASK_ID=$LSB_JOBINDEX\n")

        return lines

class SGEJob(Job):

    def __init__(self):
        super(SGEJob, self).__init__()
        self.template_name = "sunblockjob-sge"
        self.name = "sunblockjob-sge"
        self.engine = "SGE"

    def prepare_header(self, queue_list, mem_gb, time_hours, cores, JOB_PATHS, n=1):
        # Build header
        lines = [
            "#$ -S /bin/sh",
            "#$ -q %s" % ",".join(queue_list),
            "#$ -l h_vmem=%dG" % mem_gb,
            "#$ -l h_rt=%d:0:0" % time_hours,
            "#$ -R y",
            "#$ -t 1-%d" % n,
            "",
            "# Set log path (-j y causes -e to be ignored but we'll set it anyway)",
            "#$ -e %s/$TASK_ID.e" % JOB_PATHS["stdeo_path"],
            "#$ -o %s/$TASK_ID.o" % JOB_PATHS["stdeo_path"],
            "#$ -j y",
        ]

        if self.FORWARD_ENV:
            lines.append("#$ -V")

        if cores > 1:
            lines.append("#$ -pe multithread %d" % cores)

        lines.append("")

        if self.WORKING_DIR:
            lines.append("#$ -wd %s" % self.WORKING_DIR)
            lines.append("OUTDIR=%s" % self.WORKING_DIR)
        else:
            lines.append("#$ -cwd")
            lines.append("OUTDIR=`pwd -P`")

        lines.append("\nSUNBLOCK_JOB_ID=$JOB_ID")
        lines.append("SUNBLOCK_TASK_ID=$SGE_TASK_ID\n")

        return lines
