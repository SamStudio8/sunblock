import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class FilterGFF(job.Job):

    def __init__(self):
        super(FilterGFF, self).__init__()
        self.template_name = "filtergff"
        self.FORWARD_ENV = True
        self.IGNORE_UNSET = True

        self.add_key("directory", "Path to directory of GFF files", "GFF Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("overlap_size", "Overlap Size", "Overlap Size [100]", int)

    def define(self, shard=None):
        self.use_venv("/ibers/ernie/groups/rumenISPG/mgkit/venv/bin/activate")
        if os.path.isfile(self.config["directory"]["value"]):
            self.add_array("queries", [self.config["directory"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*.gff")), "QUERY")

        self.set_pre_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .gff`.filtered.gff.wip",
            "SORTFILE=`basename $QUERY .gff`.sorted"
        ])

        self.set_commands([
            "sort -s -k1,1 -k7,7 $QUERY > $SORTFILE",
            "filter-gff overlap -s %d -t -v $SORTFILE $OUTFILE" % self.config["overlap_size"]["value"],
            "rm $SORTFILE",
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY `echo $OUTFILE | sed 's/.wip$//'`")
        self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

