# flake8: noqa
import sys
pyversion = float(sys.version[:3])
if pyversion < 3.6:
    sys.exit('ModelMapper requires Python 3.6 or later.')

from modelmapper.mapper import Mapper
from modelmapper.ui import get_user_choice, get_user_input


