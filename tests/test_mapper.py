import os
import pytest
from deepdiff import DeepDiff

from modelmapper import Mapper
from modelmapper.mapper import FieldStats, HasNull, HasDecimal, HasInt, HasDollar, HasPercent, HasString, HasDateTime, HasBoolean
from fixtures.training_fixture1_all_values import all_fixture1_values
from collections import Counter

current_dir = os.path.dirname(os.path.abspath(__file__))
template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')


@pytest.fixture
def mapper():
    return Mapper(template_setup_path)


class TestMapper:

    def test_mapper_setup(self, mapper):
        assert [['carrot', 'cheese'], ['bread', 'salted_butter'], ['model_year', 'year']] == mapper.settings.field_name_full_conversion
        assert isinstance(mapper.settings.field_name_part_conversion, list)
        assert ['#', 'num'] == mapper.settings.field_name_part_conversion[0]

    @pytest.mark.parametrize("data, expected", [
        (("Burrito's CAN Fly!", "Really?", "keep it <= 99"),
         {"Burrito's CAN Fly!": 'burritos_can_fly', 'Really?': 'really', 'keep it <= 99': 'keep_it_less_or_equal_99'}),
        (('The other  lines__stuff', ),
         {'The other  lines__stuff': 'the_other_lines_stuff'}),
    ])
    def test_get_all_clean_field_names(self, data, expected, mapper):
        result = mapper._get_all_clean_field_names_mapping(data)
        assert expected == result

    @pytest.mark.parametrize("name, expected", [
        ('brEAD ', 'salted_butter')
    ])
    def test_get_clean_field_name(self, name, expected, mapper):
        result = mapper._get_clean_field_name(name)
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

    @pytest.mark.parametrize("values, expected", [
        (['1', '3', '4', ''],
         FieldStats(counter=Counter(HasNull=1, HasInt=3, HasBoolean=1),
                    max_int=4)
         ),
        (['y', 'y', 'y', 'n', '', 'y'],
         FieldStats(counter=Counter(HasNull=1, HasBoolean=5))
         ),
        (['1', '1', '0', 'null', '0', ''],
         FieldStats(counter=Counter(HasNull=2, HasBoolean=4, HasInt=4),
                    max_int=1)
         ),
        (['$1.92', '$33.6', '$0', 'null', '$130.22'],
         FieldStats(counter=Counter(HasNull=1, HasDecimal=3, HasInt=1, HasDollar=4),
                    max_decimal_precision=9, max_decimal_scale=4)
         ),
        (['apple', 'orange', 'what is going on here?', 'aha!'],
         FieldStats(counter=Counter(HasString=4),
                    max_string_len=54)
         ),
    ])
    def test_get_stats(self, values, expected, mapper):
        result = mapper._get_stats(values, field_name='blah')
        diff = DeepDiff(expected, result)
        assert not diff
