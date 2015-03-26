#TODO I'd like to dynamically load modules to avoid users needing to edit this qq
from sunblock.jobs import blast
from sunblock.jobs import blast2gff

def get_template_list():
    return {
        "blast": blast.BLAST,
        "blast2gff": blast2gff.BLAST2GFF
    }
