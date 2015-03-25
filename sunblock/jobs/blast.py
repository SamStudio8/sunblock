from sunblock.jobs import job

class BLAST(job.Job):

    def __init__(self):
        super(BLAST, self).__init__()
        self.add_key("database", "Location of the BLAST database", "Where is the database?", str)
        self.add_key("query", "Location of the Query list", "Where is the query list?", str)
