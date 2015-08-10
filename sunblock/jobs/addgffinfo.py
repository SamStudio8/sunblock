import glob
import os
import sys

from click import Path, BOOL

from sunblock.jobs import job

#TODO Split file to prevent troubles when connection errors out?
class AddGFFInfo(job.Job):

    def __init__(self):
        super(AddGFFInfo, self).__init__()
        self.template_name = "addgffinfo"
        self.FORWARD_ENV = True
        self.IGNORE_UNSET = True

        self.add_key("directory", "Path to directory of GFF files", "GFF Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("payload", "payload [-e -ec -ko]", "payload [-e -ec -ko]", str)

    def define(self, shard=None):
        self.use_venv("/ibers/ernie/groups/rumenISPG/mgkit/venv/bin/activate")
        #TODO filtered.gff assumption
        if os.path.isfile(self.config["directory"]["value"]):
            self.add_array("queries", [self.config["directory"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*.filtered.gff")), "QUERY")

        self.set_pre_commands([
            "OUTFILE=$OUTDIR/`basename $QUERY .gff`.annotated.gff.wip",
        ])

        self.set_commands([
            "add-gff-info uniprot -t %s $QUERY $OUTFILE" % self.config["payload"]["value"],
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY `echo $OUTFILE | sed 's/.wip$//'`")
        self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

