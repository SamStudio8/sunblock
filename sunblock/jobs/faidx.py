import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class IndexFA(job.Job):

    def __init__(self):
        super(IndexFA, self).__init__()
        self.template_name = "indexfa"

        self.add_key("directory", "Path to directory of FA files", "FA Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))

    def define(self, shard=None):
        self.use_module("samtools")
        self.add_array("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*.fa")), "QUERY")

        self.set_commands([
            "samtools faidx $QUERY"
        ])

        self.add_pre_log_line("echo $QUERY.fai")
        self.add_post_checksum("$QUERY.fai")


