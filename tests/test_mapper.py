import os
import pytest
from unittest import mock
from collections import Counter
from deepdiff import DeepDiff

from modelmapper import Mapper
from modelmapper.mapper import FieldResult, SqlalchemyFieldType, get_field_result_from_dict
from modelmapper.stats import FieldStats
from tests.fixtures.training_fixture1_mapping import all_fixture1_values, all_field_results_fixture1, all_field_sqlalchemy_str_fixture1  # NOQA
from tests.fixtures.analysis_fixtures import (analysis_fixture_a, analysis_fixture_b, override_fixture1,
                                              analysis_fixture_a_only_combined, analysis_fixture_a_and_b_combined,
                                              analysis_fixture_a_and_b_combined_with_override)

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')


@pytest.fixture
def mapper():
    return Mapper(example_setup_path)


class TestMapper:

    def test_mapper_setup(self, mapper):
        expected = [['carrot', 'cheese'], ['bread', 'salted_butter'], ['model_year', 'year']]
        assert expected == mapper.settings.field_name_full_conversion
        assert isinstance(mapper.settings.field_name_part_conversion, list)
        assert [['\n', '_'], ['#', 'num']] == mapper.settings.field_name_part_conversion[:2]

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
        assert str(exc_info.value).endswith("field has a collision with 'Name 1'. They both produce 'name_1'")


    @pytest.mark.parametrize("names, expected_duplicates", [
        (['this', 'or', 'this'], ['this']),
        (['this', 'or', 'this', 'or', 'what'], ['this', 'or']),
    ])
    def test_verify_no_duplicate_names(self, names, expected_duplicates, mapper):
        with pytest.raises(ValueError) as exc_info:
            mapper._verify_no_duplicate_names(names)
        for item in expected_duplicates:
            assert item in str(exc_info.value)

    def test_get_all_values_per_clean_name(self, all_fixture1_values, mapper):
        result = mapper._get_all_values_per_clean_name(training_fixture1_path)
        assert all_fixture1_values == result

    @pytest.mark.parametrize("stats, expected_field_result", [
        (FieldStats(counter=Counter(HasNull=1, HasInt=3, HasBoolean=1),
                    max_int=4, len=4),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.SmallInteger, field_db_str='SmallInteger',
                     is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasNull=1, HasBoolean=5, HasString=5),
                    max_string_len=1, len=6),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasNull=2, HasBoolean=4, HasInt=4),
                    max_int=1, len=6),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasBoolean=4, HasNull=1, HasString=2, HasInt=2),
                    max_int=1, max_string_len=4, len=5),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasBoolean=4, HasNull=1, HasString=3, HasInt=2),
                    max_int=1, max_string_len=4, len=6),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasBoolean=4, HasInt=5),
                    max_int=20, len=5),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.SmallInteger, field_db_str='SmallInteger',
                     is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasNull=1, HasDecimal=3, HasInt=1, HasDollar=4),
                    max_pre_decimal=5, max_decimal_scale=2, len=5),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Integer, field_db_str='Integer',
                     is_nullable=True, is_dollar=True)
         ),
        (FieldStats(counter=Counter(HasString=4),
                    max_string_len=22, len=4),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.String, field_db_str='String(54)', is_nullable=False)
         ),
        (FieldStats(counter=Counter(HasString=4, HasNull=1),
                    max_string_len=2, len=5),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.String, field_db_str='String(34)', is_nullable=False)
         ),
        (FieldStats(counter=Counter(HasDateTime=3, HasNull=1),
                    datetime_formats={'%m/%d/%y'}, len=4),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.DateTime, field_db_str='DateTime',
                     is_nullable=True, datetime_formats={'%m/%d/%y'})
         ),
        (FieldStats(counter=Counter(HasString=1, HasDateTime=3, HasNull=2),
                    datetime_formats={'%m/%d/%y'}, max_string_len=13, len=6),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.String, is_nullable=False, field_db_str='String(45)')
         ),
        (FieldStats(counter=Counter(HasPercent=5, HasInt=5),
                    max_int=20, len=5),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, field_db_str='DECIMAL(6, 2)',
                     is_nullable=True, is_percent=True)
         ),
        (FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1),
                    max_int=0, len=3, max_decimal_scale=6, max_pre_decimal=3),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, field_db_str='DECIMAL(13, 8)',
                     is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasPercent=3, HasInt=1, HasDecimal=2),
                    max_int=0, len=3, max_decimal_scale=6, max_pre_decimal=3),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, field_db_str='DECIMAL(13, 10)',
                     is_nullable=True, is_percent=True)
         ),
        (FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1),
                    max_int=0, len=3, max_decimal_scale=2, max_pre_decimal=4),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, field_db_str='DECIMAL(10, 4)',
                     is_nullable=True)
         ),
        (FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1, HasDollar=2),
                    max_int=0, len=3, max_decimal_scale=2, max_pre_decimal=4),
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Integer, field_db_str='Integer',
                     is_nullable=True, is_dollar=True)
         ),
    ])
    def test_get_stats_and_get_field_result_from_stats(self, stats, expected_field_result, mapper):
        field_result = mapper._get_field_result_from_stats(field_name='blah', stats=stats)
        diff = DeepDiff(expected_field_result, field_result)
        assert not diff

    def test_get_field_results_from_csv(self, all_field_results_fixture1, mapper):
        for field_name, field_result in mapper._get_field_results_from_csv(training_fixture1_path):
            diff = DeepDiff(all_field_results_fixture1[field_name], field_result)
            assert not diff

    def test_get_field_orm_string(self, all_field_results_fixture1, all_field_sqlalchemy_str_fixture1, mapper):
        for field_name, field_result in all_field_results_fixture1.items():
            assert all_field_sqlalchemy_str_fixture1[field_name] == mapper._get_field_orm_string(
                field_name=field_name,
                field_result=field_result
            )

    @mock.patch('modelmapper.mapper.write_toml')
    def test_analyze(self, mock_write_toml, mapper):
        expected_results = mapper._read_analyzed_csv_results()
        results = mapper.analyze()
        diff = DeepDiff(expected_results, results)
        assert not diff

    @pytest.mark.parametrize("item, expected", [
        ({'field_db_str': "Boolean", 'is_nullable': True},
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, is_nullable=True)),
        ({'field_db_str': "string(64)", 'is_nullable': True},
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.String, is_nullable=True, args=64)),
        ({'field_db_str': " decimal( 11, 12 )"},
         FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, args=(11, 12))),
    ])
    def test_get_field_result_from_dict(self, item, expected):
        result = get_field_result_from_dict(item)
        diff = DeepDiff(expected, result)
        assert not diff

    @pytest.mark.parametrize("values, overrides, expected", [
        ([analysis_fixture_a()],
         None,
         analysis_fixture_a_only_combined()),
        ([analysis_fixture_a(), analysis_fixture_b()],
         None,
         analysis_fixture_a_and_b_combined()),
        ([analysis_fixture_a(), analysis_fixture_b()],
         override_fixture1(),
         analysis_fixture_a_and_b_combined_with_override()),
    ])
    def test_combine_analyzed_csvs(self, values, overrides, expected, mapper):
        result = mapper._combine_analyzed_csvs(values, overrides)
        diff = DeepDiff(expected, result)
        assert not diff
