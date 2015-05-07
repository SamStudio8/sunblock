from datetime import datetime
import glob
import os
import sys

from click import Path

from sunblock.jobs import job

class RAPSearch(job.Job):

    def __init__(self):
        super(RAPSearch, self).__init__()
        self.template_name = "rapsearch"

        self.add_key("queries_dir", "QDIR", "QDIR", Path(exists=True, readable=True, file_okay=True, dir_okay=True, resolve_path=True))
        self.add_key("queries_ext", "QEXT", "QEXT", str)
        self.add_key("outdir", "outdir", "outdir", Path(exists=True, dir_okay=True, writable=True, resolve_path=True))

        #TODO Check database
        self.add_key("database", "DBASE_ROOT", "DBASE_ROOT", str)

        self.add_key("shards", "SHARDS", "SHARDS", int)
        self.add_key("delimiter", "delimiter [.]", "delimiter [.]", str)
        self.add_key("start", "shard start [0]", "shard start [0]", int)
        self.add_key("padding", "shard pad [0]", "shard pad [0]", int)

        #self.add_key("threads", "threads [1]", "threads [1]", int)

        self.add_key("payload", "payload [-evalue 0.00001]", "payload [-e 0.00001]", str)

    def get_shards(self):
        shards = []

        def pad_shard(i, padding):
            return ("{0:0%dd}" % padding).format(i)

        database = self.config["database"]["value"]
        if self.config["shards"]["value"] > 1:
            for i in range(self.config["start"]["value"], self.config["start"]["value"] + self.config["shards"]["value"]):
                curr_shard = {
                    "database": database + self.config["delimiter"]["value"] + pad_shard(i, self.config["padding"]["value"]),
                    "name": os.path.basename(database + self.config["delimiter"]["value"] + pad_shard(i, self.config["padding"]["value"]))
                }
                shards.append(curr_shard)
        else:
            curr_shard = {
                "database": database,
                "name": os.path.basename(database)
            }
            shards.append(curr_shard)

        return shards

    def define(self, shard=None):
        database = self.config["database"]["value"]

        # Check database (probably) exists
        # TODO Fairly naive check that just ensures at least one file exists on the
        #      given database path and could easily be confused.
        if os.path.isdir(database):
            sys.stderr.write("[FAIL] Database '%s' appears to be a directory.\n" % database)
            sys.exit(1)
        if len(glob.glob(database+"*")) == 0:
            sys.stderr.write("[FAIL] Database '%s' does not appear to exist.\n" % database)
            sys.exit(1)

        self.use_module("RAPSearch/2.22")

        if os.path.isfile(self.config["queries_dir"]["value"]):
            self.add_array("queries", [self.config["queries_dir"]["value"]], "QUERY")
        else:
            self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*." + self.config["queries_ext"]["value"])), "QUERY")
            #self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*" + os.path.basename(curr_shard) + "."+ self.config["queries_ext"]["value"])), "QUERY")

        self.set_pre_commands([
            "DB=`basename %s`" % shard["database"],
            "OUTFILE=%s/`basename $QUERY .%s`.$DB.rap6.wip" % (self.config["outdir"]["value"], self.config["queries_ext"]["value"]),
        ])

        self.set_commands([
            #"rapsearch -q $QUERY -d " + shard["database"] + " -u 1 -z " + self.config["threads"]["value"] + " " + self.config["payload"]["value"] + " > $OUTFILE",
            #"rapsearch -q $QUERY -d " + shard["database"] + " -u 1 -z 8 " + self.config["payload"]["value"] + " > $OUTFILE",
            "rapsearch -q $QUERY -d " + shard["database"] + " -z 8 -b 0" + self.config["payload"]["value"] + " -o $OUTFILE",
            "mv $OUTFILE.m8 `echo $OUTFILE | sed 's/.wip$//'`",
        ])

        self.add_pre_log_line("echo $QUERY \"%s\" `echo $OUTFILE | sed 's/.wip$//'`" % shard["database"])
        self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

