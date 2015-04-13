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

        self.add_key("queries_dir", "QDIR", "QDIR", Path(exists=True, readable=True, dir_okay=True, resolve_path=True))
        self.add_key("queries_ext", "QEXT", "QEXT", str)
        self.add_key("outdir", "outdir", "outdir", Path(exists=True, dir_okay=True, writable=True, resolve_path=True))

        #TODO Check database
        self.add_key("database", "DBASE_ROOT", "DBASE_ROOT", str)

        self.add_key("shards", "SHARDS", "SHARDS", int)
        self.add_key("delimiter", "delimiter [.]", "delimiter [.]", str)
        self.add_key("start", "shard start [0]", "shard start [0]", int)
        self.add_key("padding", "shard pad [0]", "shard pad [0]", int)

        self.add_key("payload", "payload [-evalue 0.00001]", "payload [-e 0.00001]", str)

    def execute(self):
        def clean_database(path):
            EXTENSIONS = [
            ]
            for ext in EXTENSIONS:
                if path.endswith(ext):
                    path = path.replace(ext, "")[0:-1]
                    break
            return path

        def pad_shard(i, padding):
            return ("{0:0%dd}" % padding).format(i)

        # Remove file extension if necessary
        database = clean_database(self.config["database"]["value"])
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

        sharded = False
        if self.config["shards"]["value"] > 1:
            sharded = True

        names = []
        if sharded:
            # Submit job
            #TODO Check database exists
            for i in range(self.config["start"]["value"], self.config["start"]["value"]+self.config["shards"]["value"]):
                curr_shard = database + self.config["delimiter"]["value"] + pad_shard(i, self.config["padding"]["value"])

                #TODO gross.
                old_config = self.config.copy()
                self = RAPSearch()
                for key, conf in sorted(self.config.items(), key=lambda x: x[1]["order"]):
                    self.set_key(key, old_config[key]["value"])

                self.use_module("RAPSearch/2.22")
                self.WORKING_DIR = self.config["outdir"]["value"]
                self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*." + self.config["queries_ext"]["value"])), "QUERY")
                #self.add_array("queries", sorted(glob.glob(self.config["queries_dir"]["value"] + "/*" + os.path.basename(curr_shard) + "."+ self.config["queries_ext"]["value"])), "QUERY")


                self.set_pre_commands([
                    "DB=`basename %s`" % curr_shard,
                    "OUTFILE=%s/`basename $QUERY .%s`.$DB.rap6.wip" % (self.config["outdir"]["value"], self.config["queries_ext"]["value"]),
                ])

                self.set_commands([
                    "rapsearch -q $QUERY -d " + curr_shard + " -o $OUTFILE -z 5 " + self.config["payload"]["value"],
                    "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
                ])

                self.add_pre_log_line("echo $QUERY \"%s\" `echo $OUTFILE | sed 's/.wip$//'`" % curr_shard)
                self.add_post_log_line("md5sum `echo $OUTFILE | sed 's/.wip$//'`")
                self.add_post_checksum("$OUTFILE | sed 's/.wip$//'")

                desc = "%s-%s" % (os.path.basename(self.config["queries_dir"]["value"]), os.path.basename(curr_shard))
                fname = "%s.%s.%s.sunblock.sge" % (self.template_name, desc, datetime.now().strftime("%Y-%m-%d_%H%M"))
                fo = open(fname, "w")
                fo.writelines(self.generate_sge(["large.q", "amd.q", "intel.q"], 5, 24, 1, manifest=desc))
                fo.close()
                names.append(fname)
        else:
            pass
        return names
