import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class PicardReorder(job.Job):

    def __init__(self):
        super(PicardReorder, self).__init__()
        self.template_name = "picard-reorder"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("reference", "Reference FASTA", "Reference (FASTA, a FAI and DICT must exist too.)", Path(exists=True, readable=True, writable=True, resolve_path=True))

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.bam")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .bam`.reorder.bam",
            "java -jar /ibers/ernie/home/msn/git/picard-tools-1.138/picard.jar ReorderSam I=$QUERY R=%s O=$OUTFILE ALLOW_INCOMPLETE_DICT_CONCORDANCE=true" % (self.config["reference"]["value"]),
        ])
        self.add_post_checksum("$OUTFILE")



