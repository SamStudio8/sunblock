import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class PicardSeqDict(job.Job):

    def __init__(self):
        super(PicardSeqDict, self).__init__()
        self.template_name = "picard-seqdict"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.fa")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .fa`.dict",
            "java -jar /ibers/ernie/home/msn/git/picard-tools-1.138/picard.jar CreateSequenceDictionary R=$QUERY O=$OUTFILE",
        ])
        self.add_post_checksum("$OUTFILE")



