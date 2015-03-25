import sys

import click

import sunblock.util as util

@click.group()
def cli():
    pass

@cli.command()
@click.argument('template')
def init(template):
    # Does template exist?
    if template not in util.get_template_list():
        print("[FAIL] No template for job type: '%s'" % template)
        sys.exit(1)

@cli.command()
def list():
    print("\n".join(util.get_template_list()))

if __name__ == "__main__":
    cli()
