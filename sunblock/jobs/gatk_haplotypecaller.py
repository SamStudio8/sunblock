import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class GATKHaplocall(job.Job):

    def __init__(self):
        super(GATKHaplocall, self).__init__()
        self.template_name = "gatk-haplocall"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("reference", "Reference FASTA", "Reference (FASTA, a FAI and DICT must exist too.)", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("emit_conf", "Emission Confidence", "Minimum confidence threshold (phred-scaled) at which the program should emit sites that appear to be possibly variant", int, default=10)
        self.add_key("call_conf", "Call Confidence", "Mminimum confidence threshold (phred-scaled) at which the program should emit variant sites as called.", int, default=30)

        self.use_module("java/jdk1.7.0_03")

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.bam")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .bam`.vcf",
            "java -Djava.io.tmpdir=$OUTDIR -jar /ibers/ernie/home/msn/git/gatk-3.4.46/GenomeAnalysisTK.jar -T HaplotypeCaller -R %s -I $QUERY --genotyping_mode DISCOVERY -stand_emit_conf %d -stand_call_conf %d -o $OUTFILE" % (self.config["reference"]["value"], self.config["emit_conf"]["value"], self.config["call_conf"]["value"])
        ])
        self.add_post_checksum("$OUTFILE")

