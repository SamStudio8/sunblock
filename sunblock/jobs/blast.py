from click import Path

from sunblock.jobs import job

class BLAST(job.Job):

    def __init__(self):
        super(BLAST, self).__init__()
        self.add_key("database", "Location of the BLAST database", "Where is the database?", Path(exists=True, readable=True))
        self.add_key("query", "Location of the Query list", "Where is the query list?", Path(exists=True, readable=True))

    def execute():
        # Remove file extension if necessary
        database = clean_database(args.database)
        if database != args.database:
            sys.stderr.write("[WARN] Invalid database path: '%s'\n" % args.database)
            sys.stderr.write("[WARN] Continuing assuming you want to remove the file extension.\n")

        # Check queries file exists and load queries
        queries_path = args.queries
        if not os.path.isfile(args.queries):
            sys.stderr.write("[FAIL] Queries file '%s' does not exist. Check permissions.\n" % args.queries)
            sys.exit(1)
        sys.stderr.write("[_QS_] %s\n" % queries_path)
        fh_queries = open(queries_path)
        queries = fh_queries.readlines()
        fh_queries.close()

        # Check database (probably) exists
        # TODO Fairly naive check that just ensures at least one file exists on the
        #      given database path and could easily be confused.
        if os.path.isdir(database):
            sys.stderr.write("[FAIL] Database '%s' appears to be a directory.\n" % database)
            sys.exit(1)
        if len(glob.glob(database+"*")) == 0:
            sys.stderr.write("[FAIL] Database '%s' does not appear to exist.\n" % database)
            sys.exit(1)
        sys.stderr.write("[_DB_] %s\n" % database)

        sharded = False
        if args.shards > 1:
            sharded = True
            sys.stderr.write("[SHRD] %d shards, beginning from %s\n" % (args.shards, pad_shard(args.start, args.padding)))

        # Create submission script for SGE (or use given script)
        if args.script:
            if not os.path.isfile(args.script):
                sys.stderr.write("[FAIL] Script file '%s' does not exist. Check permissions.\n" % args.script)
                sys.exit(1)
            script_path = args.script
        else:
            script_path = "split-blast." + datetime.now().strftime("%Y-%m-%d_%H%M") + ".sge"

        #TODO Check all query files exist?
        sge_lines = generate_sge_script(script_path, args.command, args.module, args.payload, queries, "fa", "blast6", args.outdir)

        if not args.dry:
            fh = open(script_path, 'w')
            fh.write("\n".join(sge_lines))
            fh.close()
        if args.verbose:
            sys.stderr.writelines("\n[SGE_]\t".join(sge_lines) + "\n")

        # Create output directory if required
        if args.outdir:
            if not os.path.exists(args.outdir):
                os.makedirs(args.outdir)

        if sharded:
            # Submit job
            #TODO Check database exists
            for i in range(args.start, args.start+args.shards):
                curr_shard = database + args.delimiter + pad_shard(i, args.padding)
                qsub_cmd = "qsub %s %s" % (script_path, curr_shard)
                sys.stderr.write("[QSUB] %s\n" % qsub_cmd)

                if not args.dry:
                    #TODO A bit naughty, should probably be using subprocess, but w/e
                    os.system(qsub_cmd)
        else:
            # Submit single job
            for query in queries:
                #TODO Check query file exists
                #TODO Check database exists
                query = query.strip()
                outfile = os.path.basename(query) + "." + os.path.basename(database) + ".blast6"
                if args.outdir:
                    outfile = os.path.join(args.outdir, outfile)

                qsub_cmd = "qsub %s %s %s %s" % (script_path, query, database, outfile)
                sys.stderr.write("[QSUB] %s\n" % qsub_cmd)

                #TODO A bit naughty, should probably be using subprocess, but w/e
                if not args.dry:
                    os.system(qsub_cmd)
