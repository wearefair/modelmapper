import os
import pytest

from modelmapper import Mapper
from fixtures.training_fixture1_all_values import all_fixture1_values

current_dir = os.path.dirname(os.path.abspath(__file__))
template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.yml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')


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
        (('The other  lines__stuff', ),
         {'The other  lines__stuff': 'the_other_lines_stuff'}),
    ])
    def test_get_all_clean_field_names(self, data, expected, mapper):
        result = mapper._get_all_clean_field_names_mapping(data)
        assert expected == result

    @pytest.mark.parametrize("names_mapping", [
        {'Name 1': 'name_1', 'HELLO': 'hello', 'NAME  1_': 'name_1'},
        {'Name 1': 'name_1', 'NAME 1?': 'name_1'}
    ])
    def test_verify_no_duplicate_clean_names(self, names_mapping, mapper):
        with pytest.raises(ValueError) as exc_info:
            mapper._verify_no_duplicate_clean_names(names_mapping)
        assert str(exc_info.value).endswith("field has a collision with 'Name 1'")

    def test_get_all_values_per_clean_name(self, all_fixture1_values, mapper):
        result = mapper._get_all_values_per_clean_name(training_fixture1_path)
        assert all_fixture1_values == result
