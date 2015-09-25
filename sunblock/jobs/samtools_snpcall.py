import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class SAMToolsSNPCall(job.Job):

    def __init__(self):
        super(SAMToolsSNPCall, self).__init__()
        self.template_name = "samtools-pileup-snp"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("reference", "Reference FASTA", "Reference (FASTA, a FAI and DICT must exist too.)", Path(exists=True, readable=True, writable=True, resolve_path=True))

        self.use_module("samtools/1.2")

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.bam")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .bam`.vcf",
            "samtools mpileup -Iuf %s $QUERY | /ibers/ernie/home/msn/git/bcftools-1.2/bcftools view -v snps - > $OUTFILE" % self.config["reference"]["value"],
        ])
        self.add_post_checksum("$OUTFILE")


