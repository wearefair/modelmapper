import click
from modelmapper import Mapper


@click.group()
def cli():
    """A simple command line tool."""
    pass


@cli.command()
@click.option('--path', default='', help='Max number of files to fetch.')
def analyze(path):
    click.echo(f'Analyzing{path}')
    mapper = Mapper(path)
    mapper.analyze()
