import glob
import sys

from click import Path

from sunblock.jobs import job

class BLAST2GFF(job.Job):

    def __init__(self):
        super(BLAST2GFF, self).__init__()

        #TODO Output dir
        self.add_key("directory", "Path to directory of blast6 files", "BLAST6 Directory", Path(exists=True, readable=True, writable=True, resolve_path=True))
        self.add_key("database", "Database Type", "Database Type (UNIPROT-[SP|TR])", str)
        self.add_key("bit_score", "Bit Score Threshold", "Bit Score Threshold (40)", int)
        self.add_key("db_qual", "Database Quality Threshold", "Database Quality Threshold (SP10, TR8)", int)

    def execute(self):
        def generate_array_lines(name, l):
            ret = []
            for i, f in enumerate(l):
                if i == 0:
                    ret.append(name + "=(" + f.strip())
                else:
                    ret.append("\t" + f.strip())
            ret.append(")")
            return ret

        # Generate SGE
        sge_lines = ["",
            "#$ -S /bin/sh",
            "#$ -j y",
            "#$ -cwd",
            "#$ -q large.q",
            "#$ -l h_vmem=1G",
            "#$ -l h_rt=6:0:0",
            "#$ -V",
            "#$ -t 1-" + str(len(glob.glob(self.config["directory"]["value"] + "/*.blast6"))),
            "",
            "module add python",
            "source /ibers/ernie/groups/rumenISPG/mgkit/venv/bin/activate",
            ""] + generate_array_lines("queries", sorted(glob.glob(self.config["directory"]["value"] + "/*.blast6"))) + [
            "",
            "CURR_i=$(expr $SGE_TASK_ID - 1)",
            "QUERY=${queries[$CURR_i]}",
            "OUTFILE=`dirname $QUERY`/`basename $QUERY .blast6`.gff.wip",
            "",
            "echo $QUERY $OUTFILE",
            "blast2gff uniprot -b %d -dbq %d -db %s $QUERY $OUTFILE" % (self.config["bit_score"]["value"], self.config["db_qual"]["value"], self.config["database"]["value"]),
            #"rename 's/\.wip$//' $OUTFILE",
            "mv $OUTFILE `echo $OUTFILE | sed 's/.wip$//'`",
            "deactivate",
        ]
        sys.stdout.writelines("\n".join(sge_lines) + "\n")

        # Submit
        pass
