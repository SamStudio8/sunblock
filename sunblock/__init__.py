import json
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
        job.set_key(key, click.prompt(conf["prompt"], type=conf["type"]))

    # Write
    to_write = {"template": template}
    for key, conf in job.config.items():
        to_write[key] = conf["value"]

    out_fh = open("job.out", "w")
    out_fh.write(json.dumps(to_write))
    out_fh.close()

@cli.command(help="Execute a configured job")
@click.argument('config', type=click.Path(exists=True, readable=True))
def execute(config):

    # Open
    in_fh = open(config)
    config = json.loads(in_fh.read())
    in_fh.close()

    # Load
    job = util.get_template_list()[config["template"]]()
    for key, conf in sorted(job.config.items(), key=lambda x: x[1]["order"]):
        job.set_key(key, config[key])

    # Check config
    #TODO More helpful output -- what key is missing or incorrect?
    if not job.check():
        log.error("Invalid job configuration. Aborting.")
        sys.exit(1)

    job.execute()


@cli.command(help="List available job configuration templates")
def list():
    print("\n".join(util.get_template_list()))

if __name__ == "__main__":
    cli()
