# flake8: noqa
__version__ = '0.4.4'
import sys
pyversion = float(sys.version[:3])
if pyversion < 3.6:
    sys.exit('ModelMapper requires Python 3.6 or later.')

from modelmapper.mapper import Mapper, SqlalchemyFieldType
from modelmapper.ui import get_user_choice, get_user_input
from modelmapper.cleaner import Cleaner
from modelmapper.initialize import initialize
from modelmapper.etl import ETL
from modelmapper.loader import PostgresBulkLoaderMixin, PostgresSnapshotLoaderMixin

