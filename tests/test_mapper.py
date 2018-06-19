import os
import pytest

from modelmapper import Mapper

current_dir = os.path.dirname(os.path.abspath(__file__))
template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.yml')


@pytest.fixture
def mapper():
    return Mapper(template_setup_path)


class TestMapper:

    def test_mapper_setup(self, mapper):
        assert mapper.field_name_full_conversion == []
        assert isinstance(mapper.field_name_part_conversion, list)
        assert ['#', 'num'] == mapper.field_name_part_conversion[0]

    @pytest.mark.parametrize("data, expected", [
        (("Burrito's CAN Fly!", "Really?", "keep it <= 99"),
         {"Burrito's CAN Fly!": 'burritos_can_fly', 'Really?': 'really', 'keep it <= 99': 'keep_it_less_or_equal_99'}),
        (('The other lines__stuff', ),
         {'The other lines__stuff': 'the_other_lines_stuff'}),
    ])
    def test_get_all_clean_field_names(self, data, expected, mapper):
        result = mapper._get_all_clean_field_names(data)
        assert expected == result
