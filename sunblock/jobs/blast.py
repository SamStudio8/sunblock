from click import Path

from sunblock.jobs import job

PATH_EXIST_AND_READ = Path(exists=True, readable=True)

class BLAST(job.Job):

    def __init__(self):
        super(BLAST, self).__init__()
        self.add_key("database", "Location of the BLAST database", "Where is the database?", PATH_EXIST_AND_READ)
        self.add_key("query", "Location of the Query list", "Where is the query list?", PATH_EXIST_AND_READ)
