import glob
import os
import sys

from click import Path, BOOL

from sunblock.jobs import job

class SAMToolsIndex(job.Job):

    def __init__(self):
        super(SAMToolsIndex, self).__init__()
        self.template_name = "samtools-index"

        self.add_key("in", "Input", "Input", Path(exists=True, readable=True, writable=True, resolve_path=True))

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.bam")), "QUERY")

        self.set_commands([
            "/ibers/ernie/home/msn/git/samtools/samtools index $QUERY"
        ])
        self.add_post_checksum("$QUERY.bai")


