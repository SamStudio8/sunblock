from datetime import datetime

from sunblock.jobs import job

class HelloWorld(job.Job):

    def __init__(self):
        super(HelloWorld, self).__init__()
        self.template_name = "helloworld"

    def define(self, shard=None):
        self.add_array("messages", [
            "Hello",
            "World",
            "Hoot",
            "Meow"
        ], "MESSAGE")

        self.set_commands([
            "echo $MESSAGE",
            "echo $MESSAGE > $OUTDIR/message.out",
            "if [ $(($SGE_TASK_ID % 2)) -eq 0 ]; then exit 1; fi",
        ])

        self.add_pre_log_line("echo $MESSAGE")
        self.add_post_checksum("$OUTDIR/message.out")

