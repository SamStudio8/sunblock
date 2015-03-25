import click

@click.group()
def cli():
    pass

@cli.command()
@click.argument('template')
def init(template):
    pass

if __name__ == "__main__":
    cli()
