from datetime import datetime

from zenlog import log

#TODO Should probably have an SGEJob class inheriting from Job to
#     modularise SGE specific functionality from generic job handling
class Job(object):

    def __init__(self):
        self.order = 0
        self.config = {}
        self.template_name = "sunblockjob"

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

        self.modules = []
        self.venv = None
        self.array = None

        self.pre_commands = []
        self.commands = []
        self.pre_log = []
        self.post_log = []

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

    def execute(self):
        raise NotImplementedError()

    def use_module(self, module_name):
        #FUTURE Check module list
        self.modules.append(module_name)

    def use_venv(self, venv_path):
        #TODO Check path
        self.venv = venv_path

    def add_array(self, array_name, array_val, array_var):
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

    def generate_sge(self, queue_list, mem_gb, time_hours, cores):

        # Generate log name
        log_path = "%s.%s.sunblock.log" % (self.template_name, datetime.now().strftime("%Y-%m-%d_%H%M"))

        # Determine whether the job will be array-ed
        n = 1
        if self.array is not None:
            n = self.array["n"]

        # Build header
        sge_lines = [
            "#$ -S /bin/sh",
            "#$ -j y",
            "#$ -cwd",
            "#$ -q %s" % ",".join(queue_list),
            "#$ -l h_vmem=%dG" % mem_gb,
            "#$ -l h_rt=%d:0:0" % time_hours,
            "#$ -V",
            "#$ -R y",
            "#$ -t 1-%d" % n,
        ]
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
            sge_lines.append("echo \"[$(date)][$JOB_ID][$SGE_TASK_ID]: $(%s)\" >> %s" % (line, log_path))

        sge_lines.append("\n#Commands")
        # Commands
        for line in self.commands:
            sge_lines.append(line)

        sge_lines.append("\n#Post Log")
        # Post Log
        for line in self.post_log:
            sge_lines.append("echo \"[$(date)][$JOB_ID][$SGE_TASK_ID]: $(%s)\" >> %s" % (line, log_path))

        # Shut down the venv
        if self.venv is not None:
            sge_lines.append("\ndeactivate")

        return "\n".join(sge_lines)

