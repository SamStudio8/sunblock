#TODO I'd like to dynamically load modules to avoid users needing to edit this qq
from sunblock.jobs import (
        blast,
        blast2gff,
        filtergff,
        addgffinfo,
        helloworld,
        rapsearch,
        faidx,
        samtools_sort,
        samtools_index,
        picard_markdup,
        picard_seqdict,
        gatk_get_targets,
        gatk_realign_targets,
        picard_reorder,
        gatk_haplotypecaller,
        samtools_snpcall,
)

def get_template_list():
    return {
        "blast": blast.BLAST,
        "blast2gff": blast2gff.BLAST2GFF,
        "filtergff": filtergff.FilterGFF,
        "addgffinfo": addgffinfo.AddGFFInfo,
        "helloworld": helloworld.HelloWorld,
        "rapsearch": rapsearch.RAPSearch,
        "indexfa": faidx.IndexFA,
        "samtools-sort": samtools_sort.SAMToolsSort,
        "samtools-index": samtools_index.SAMToolsIndex,
        "picard-markdup": picard_markdup.PicardMarkDup,
        "picard-seqdict": picard_seqdict.PicardSeqDict,
        "gatk-gettargets": gatk_get_targets.GATKGetTargets,
        "gatk-realigntargets": gatk_realign_targets.GATKRealignTargets,
        "picard-reorder": picard_reorder.PicardReorder,
        "gatk-haplocall": gatk_haplotypecaller.GATKHaplocall,
        "samtools-snpcall": samtools_snpcall.SAMToolsSNPCall,
    }


