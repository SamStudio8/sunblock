import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class GATKRealignTargets(job.Job):

    def __init__(self):
        super(GATKRealignTargets, self).__init__()
        self.template_name = "gatk-realigntargets"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("reference", "Reference FASTA", "Reference (FASTA, a FAI and DICT must exist too.)", Path(exists=True, readable=True, writable=True, resolve_path=True))

        self.use_module("java/jdk1.7.0_03")

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.targets.list")), "QUERY")

        self.set_commands([
            "DUPFILE=$OUTDIR/`basename $QUERY .targets.list`.bam",
            "OUTFILE=$OUTDIR/`basename $QUERY .targets.list`.realign.bam",
            "java -jar /ibers/ernie/home/msn/git/gatk-3.4.46/GenomeAnalysisTK.jar -T IndelRealigner -R %s -I $QUERY -targetIntervals %s -o $OUTFILE" % (self.config["reference"]["value"])
        ])
        self.add_post_checksum("$OUTFILE")


