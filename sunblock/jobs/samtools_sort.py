import glob
import os
import sys

from click import Path, BOOL

from sunblock.jobs import job

class SAMToolsSort(job.Job):

    def __init__(self):
        super(SAMToolsSort, self).__init__()
        self.template_name = "samtools-sort"

        self.add_key("file", "Path to file to sort", "File to sort", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("out", "Path to file to sorted output", "Output file", Path(writable=True, resolve_path=True))
        self.add_key("threads", "numberof sorting threads", "num sort threads")
        #self.add_key("readname", "Sort by readname", "Readname flag", BOOL)

    def define(self, shard=None):
        self.set_commands([
            ("/ibers/ernie/home/msn/git/samtools/samtools sort -@ %d -T %s -Obam -o %s %s" % (self.config["threads"]["value"], self.config["file"]["value"], self.config["out"]["value"], self.config["file"]["value"]))
        ])
        self.add_post_checksum(self.config["out"]["value"])


