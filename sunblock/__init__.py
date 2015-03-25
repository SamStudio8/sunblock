import sys

import click

import sunblock.util as util

@click.group()
def cli():
    pass

@cli.command(help="Generate a job configuration")
@click.argument('template')
def init(template):
    # Does template exist?
    if template not in util.get_template_list():
        print("[FAIL] No template for job type: '%s'" % template)
        sys.exit(1)

    print("[NOTE] Found template for job type: '%s'" % template)

@cli.command(help="List available job configuration templates")
def list():
    print("\n".join(util.get_template_list()))

if __name__ == "__main__":
    cli()
