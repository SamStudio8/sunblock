import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class PicardMarkDup(job.Job):

    def __init__(self):
        super(PicardMarkDup, self).__init__()
        self.template_name = "picard-markdup"

        self.add_key("in", "Input File/Dir", "Input File/Dir", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("metric", "Metric File", "Metric File", Path(writable=True, resolve_path=True))

    def define(self, shard=None):
        if os.path.isfile(self.config["in"]["value"]):
            self.add_array("queries", [self.config["in"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["in"]["value"] + "/*.bam")), "QUERY")

        self.set_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .bam`.markdup.bam",
            "java -Djava.io.tmpdir=$OUTDIR -jar /ibers/ernie/home/msn/git/picard-tools-1.138/picard.jar MarkDuplicates INPUT=$QUERY OUTPUT=$OUTFILE METRICS_FILE=%s MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=1000" % (self.config["metric"]["value"]),
        ])
        self.add_post_checksum("$OUTFILE")



