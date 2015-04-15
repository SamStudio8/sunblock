from datetime import datetime
import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class BLAST(job.Job):

    def __init__(self):
        super(BLAST, self).__init__()
        self.template_name = "blast"

        self.add_key("command", "BLAST command", "BLAST command", str)
        self.add_key("queries_dir", "QDIR", "QDIR", Path(exists=True, readable=True, dir_okay=True, resolve_path=True))
        self.add_key("queries_ext", "QEXT", "QEXT", str)
        self.add_key("outdir", "outdir", "outdir", Path(exists=True, dir_okay=True, writable=True, resolve_path=True))

        #TODO Check database
        self.add_key("database", "DBASE_ROOT", "DBASE_ROOT", str)

        self.add_key("shards", "SHARDS", "SHARDS", int)
        self.add_key("delimiter", "delimiter [.]", "delimiter [.]", str)
        self.add_key("start", "shard start [0]", "shard start [0]", int)
        self.add_key("padding", "shard pad [0]", "shard pad [0]", int)

        self.add_key("payload", "payload [-evalue 0.00001]", "payload [-evalue 0.00001]", str)

    def get_shards(self):
        shards = []

        def pad_shard(i, padding):
            return ("{0:0%dd}" % padding).format(i)

        database = clean_database(self.config["database"]["value"])
        if self.config["shards"]["value"] > 1:
            for i in range(self.config["start"]["value"], self.config["start"]["value"] + self.config["shards"]["value"]):
                curr_shard = {
                    "database": database + self.config["delimiter"]["value"] + pad_shard(i, self.config["padding"]["value"]),
                    "name": database + self.config["delimiter"]["value"] + pad_shard(i, self.config["padding"]["value"])
                }
                shards.append(curr_shard)
        else:
            curr_shard = {
                "database": database,
                "name": database
            }
            shards.append(curr_shard)

        return shards

    def define(self, shard=None):
        # Remove file extension if necessary
        database = self.clean_database(self.config["database"]["value"])
        if database != self.config["database"]["value"]:
            sys.stderr.write("[WARN] Invalid database path: '%s'\n" % args.database)
            sys.stderr.write("[WARN] Continuing assuming you want to remove the file extension.\n")

        # Check database (probably) exists
        # TODO Fairly naive check that just ensures at least one file exists on the
        #      given database path and could easily be confused.
        if os.path.isdir(database):
            sys.stderr.write("[FAIL] Database '%s' appears to be a directory.\n" % database)
            sys.exit(1)
        if len(glob.glob(database+"*")) == 0:
            sys.stderr.write("[FAIL] Database '%s' does not appear to exist.\n" % database)
            sys.exit(1)

        self.use_module("BLAST/blast-2.2.28")
        self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*." + self.config["queries_ext"]["value"])), "QUERY")
        #self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*" + os.path.basename(curr_shard) + "."+ self.config["queries_ext"]["value"])), "QUERY")

        self.set_pre_commands([
            "DB=`basename %s`" % shard["database"],
            "OUTFILE=%s/`basename $QUERY .%s`.$DB.blast6.wip" % (self.config["outdir"]["value"], self.config["queries_ext"]["value"]),
        ])

        self.set_commands([
            self.config["command"]["value"] + " -query $QUERY -db " + shard["database"] + " -out $OUTFILE -outfmt 6 -num_threads $NSLOTS " + self.config["payload"]["value"],
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY \"%s\" `echo $OUTFILE | sed 's/.wip$//'`" % shard["database"])
        self.add_post_log_line("md5sum `echo $OUTFILE | sed 's/.wip$//'`")
        self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

    def clean_database(path):
        EXTENSIONS = [
            "phr",
            "pin",
            "pog",
            "psd",
            "psi",
            "psq",
            "pal",
            "udb",
        ]
        for ext in EXTENSIONS:
            if path.endswith(ext):
                path = path.replace(ext, "")[0:-1]
                break
        return path

