import os
import pytest

from modelmapper import Mapper

current_dir = os.path.dirname(os.path.abspath(__file__))
template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.yml')


class TestMapper:

    def test_mapper_setup(self):
        mapper = Mapper(template_setup_path)
        assert mapper.field_name_full_conversion == []
        assert isinstance(mapper.field_name_part_conversion, list)
        assert ['#', 'num'] == mapper.field_name_part_conversion[0]
