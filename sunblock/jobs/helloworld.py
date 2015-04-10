from datetime import datetime

from sunblock.jobs import job

class HelloWorld(job.Job):

    def __init__(self):
        super(HelloWorld, self).__init__()
        self.template_name = "helloworld"

    def execute(self):
        self.add_array("messages", [
            "Hello",
            "World"
        ], "MESSAGE")

        self.set_commands([
            "echo $MESSAGE",
        ])
        names = []
        fname = "%s.%s.sunblock.sge" % (self.template_name, datetime.now().strftime("%Y-%m-%d_%H%M"))
        fo = open(fname, "w")
        fo.writelines(self.generate_sge(["amd.q", "intel.q"], 1, 1, 1))
        fo.close()
        names.append(fname)
        return names

