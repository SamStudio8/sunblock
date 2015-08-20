import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class GATKGetTargets(job.Job):

    def __init__(self):
        super(GATKGetTargets, self).__init__()
        self.template_name = "gatk-gettargets"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("reference", "Reference FASTA", "Reference (FASTA, a FAI and DICT must exist too.)", Path(exists=True, readable=True, writable=True, resolve_path=True))

        self.use_module("java/jdk1.7.0_03")

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.markdup.bam")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .bam`.targets.list",
            "java -jar /ibers/ernie/home/msn/git/gatk-3.4.46/GenomeAnalysisTK.jar -T RealignerTargetCreator -R %s -I $QUERY -o $OUTFILE" % (self.config["reference"]["value"])
        ])
        self.add_post_checksum("$OUTFILE")


