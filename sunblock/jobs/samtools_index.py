import glob
import os
import sys

from click import Path, BOOL

from sunblock.jobs import job

class SAMToolsIndex(job.Job):

    def __init__(self):
        super(SAMToolsIndex, self).__init__()
        self.template_name = "samtools-index"

        self.add_key("file", "Path to file to index", "File to index", Path(exists=True, readable=True, writable=True, resolve_path=True))

    def define(self, shard=None):
        self.set_commands([
            "/ibers/ernie/home/msn/git/samtools/samtools index %s" % (self.config["file"]["value"]),
        ])
        self.add_post_checksum(self.config["file"]["value"] + ".bai")


