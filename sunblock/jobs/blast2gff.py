import glob
import sys

from click import Path

from sunblock.jobs import job

class BLAST2GFF(job.Job):

    def __init__(self):
        super(BLAST2GFF, self).__init__()
        self.template_name = "blast2gff"
        self.FORWARD_ENV = True
        self.IGNORE_UNSET = True

        #TODO Output dir
        self.add_key("directory", "Path to directory of R6 files", "6 Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("ext", "R6 File Extension", "R6 File Extension", str)

        self.add_key("database", "Database Type", "Database Type (UNIPROT-[SP|TR])", str)
        self.add_key("bit_score", "Bit Score Threshold", "Bit Score Threshold (40)", int)
        self.add_key("db_qual", "Database Quality Threshold", "Database Quality Threshold (SP10, TR8)", int)

    def define(self, shard=None):
        #self.use_module("python")
        self.use_venv("/ibers/ernie/groups/rumenISPG/mgkit/venv/bin/activate")
        self.add_array("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*." + self.config["ext"]["value"])), "QUERY")

        self.set_pre_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .%s`.gff.wip" % self.config["ext"]["value"],
        ])

        self.set_commands([
            "blast2gff uniprot -b %d -dbq %d -db %s $QUERY $OUTFILE" % (self.config["bit_score"]["value"], self.config["db_qual"]["value"], self.config["database"]["value"]),
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY `echo $OUTFILE | sed 's/.wip$//'`")
        self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

