import glob
import sys

from click import Path

from sunblock.jobs import job

class BLAST2GFF(job.Job):

    def __init__(self):
        super(BLAST2GFF, self).__init__()
        self.template_name = "blast2gff"

        #TODO Output dir
        self.add_key("directory", "Path to directory of blast6 files", "BLAST6 Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("database", "Database Type", "Database Type (UNIPROT-[SP|TR])", str)
        self.add_key("bit_score", "Bit Score Threshold", "Bit Score Threshold (40)", int)
        self.add_key("db_qual", "Database Quality Threshold", "Database Quality Threshold (SP10, TR8)", int)

    def execute(self):
        self.use_module("python")
        self.use_venv("/ibers/ernie/groups/rumenISPG/mgkit/venv/bin/activate")
        self.add_array("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*.blast6")), "QUERY")

        self.set_pre_commands([
            "OUTFILE=`dirname $QUERY`/`basename $QUERY .blast6`.gff.wip",
        ])

        self.set_commands([
            "blast2gff uniprot -b %d -dbq %d -db %s $QUERY $OUTFILE" % (self.config["bit_score"]["value"], self.config["db_qual"]["value"], self.config["database"]["value"]),
            #"rename 's/\.wip$//' $OUTFILE",
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY `echo $OUTFILE | sed 's/.wip$//'`")
        self.add_post_log_line("md5sum `echo $OUTFILE | sed 's/.wip$//'`")

        print self.generate_sge(["amd.q", "intel.q"], 1, 6, 1)
