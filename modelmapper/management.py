import sys
import click
from modelmapper import Mapper


@click.group()
def cli():
    """A simple command line tool."""
    pass


@cli.command()
@click.option('--path', default='', help='Max number of files to fetch.')
def analyze(path):
    if not path.startswith('/'):
        sys.exit('Error: Please enter the full path to the setup toml file, NOT relative path.')
    click.echo(f'Analyzing {path}')
    mapper = Mapper(path)
    mapper.analyze()


@cli.command()
@click.option('--debug', is_flag=True)
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def run(path, debug):
    click.echo(f'Running {path}')
    mapper = Mapper(path, debug=debug)
    mapper.run()
