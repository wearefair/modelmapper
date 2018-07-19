import click
from modelmapper import Mapper, initialize
from .excel import excel_file_to_csv_files


@click.group()
def cli():
    """A simple command line tool."""
    pass


@cli.command()
@click.option('--debug', is_flag=True)
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def analyze(path, debug):
    """
    Only analyze the files based on the setup_toml settings and write the analyzed toml files.
    """
    click.echo(f'Analyzing {path}')
    mapper = Mapper(path, debug=debug)
    mapper.analyze()


@cli.command()
@click.option('--debug', is_flag=True)
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def write_orm_model(path, debug):
    """
    Only write model the files based on the setup_toml if the combined python module is already written.
    """
    click.echo(f'Writrng model files')
    mapper = Mapper(path, debug=debug)
    mapper.write_orm_model()


@cli.command()
@click.option('--debug', is_flag=True)
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def run(path, debug):
    """
    In addition to analyzing the files based on the setup_toml, go ahead and generate the ORM models and related files.
    """
    click.echo(f'Running {path}')
    mapper = Mapper(path, debug=debug)
    mapper.run()


@cli.command()
@click.argument('path', type=click.Path(resolve_path=True))
def init(path):
    """
    Initializing ModelMapper for a model
    """
    click.echo(f'Initializing')
    initialize(path)


@cli.command()
@click.option('--sheet-names', '-s', multiple=True,
              help='Sheets from the excel file to be converted. If none provided, all sheets will be converted.')
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def excel_to_csv(path, sheet_names):
    """
    In addition to analyzing the files based on the setup_toml, go ahead and generate the ORM models and related files.
    """
    click.echo(f'Converting {path} to csvs.')
    sheet_names = sheet_names if sheet_names else None
    excel_file_to_csv_files(path, sheet_names=sheet_names)
