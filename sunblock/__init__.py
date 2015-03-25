import sys

import click
from zenlog import log

import sunblock.util as util

@click.group()
def cli():
    pass

@cli.command(help="Generate a job configuration")
@click.argument('template')
def init(template):
    # Does template exist?
    if template not in util.get_template_list():
        log.error("No template for job type: '%s'" % template)
        sys.exit(1)

    job = util.get_template_list()[template]()
    log.debug("Found template for job type: '%s' with %d required keys" % (template, len(job.config)))
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        log.debug("\t%s: %s" % (key, conf["desc"]))

    # Prompt
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        conf["value"] = click.prompt(conf["prompt"], type=conf["type"])

    # Check config
    if not job.check():
        log.error("Invalid job configuration. Aborting.")
        sys.exit(1)


@cli.command(help="List available job configuration templates")
def list():
    print("\n".join(util.get_template_list()))

if __name__ == "__main__":
    cli()
