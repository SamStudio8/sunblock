from datetime import datetime

from sunblock.jobs import job

class HelloWorld(job.Job):

    def __init__(self):
        super(HelloWorld, self).__init__()
        self.template_name = "helloworld"

    def execute(self):
        self.add_array("messages", [
            "Hello",
            "World",
            "Hoot",
            "Meow"
        ], "MESSAGE")

        self.set_commands([
            "echo $MESSAGE",
            "echo $MESSAGE > message.out",
            "if [ $(($SGE_TASK_ID % 2)) -eq 0 ]; then exit 1; fi",
        ])


        self.add_post_checksum("message.out")

        names = []
        fname = "%s.%s.sunblock.sge" % (self.template_name, datetime.now().strftime("%Y-%m-%d_%H%M"))
        fo = open(fname, "w")
        fo.writelines(self.generate_sge(["amd.q", "intel.q"], 1, 1, 1, manifest="helloworld"))
        fo.close()
        names.append(fname)
        return names

