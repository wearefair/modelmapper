import os
import enum
import pytest
from unittest import mock
from deepdiff import DeepDiff
from modelmapper.misc import escape_word, get_combined_dict, load_toml, convert_dict_key, convert_dict_item_type, write_toml
from modelmapper.mapper import SqlalchemyFieldType
from fixtures.analysis_fixtures import analysis_fixture_c_in_dict
TOML_KEYS_THAT_ARE_SET = 'datetime_formats'


current_dir = os.path.dirname(os.path.abspath(__file__))


class TestMisc:

    @pytest.mark.parametrize("word, expected", [
        ('Are you Really reading this?',
         'are_you_really_reading_this'),
        ('What The #^*@*!',
         'what_the')
    ])
    def test_escape_word(self, word, expected):
        result = escape_word(word)
        assert expected == result

    @pytest.mark.parametrize("comparison_func, items, expected", [
        (lambda x: x['a'],
         [{'a': 10, 'b': 20}, {'a': 1, 'b': 25, 'c': 11}],
         {'a': 10, 'b': 20, 'c': 11}),
        (lambda x: x['a'],
         [{'a': 10, 'b': [11]}, {'a': 1, 'b': [25], 'c': 11}],
         {'a': 10, 'b': [25, 11], 'c': 11}),
    ])
    def test_get_combined_dict(self, comparison_func, items, expected):
        result = get_combined_dict(comparison_func, *items)
        assert expected == result

    def test_load_toml(self, analysis_fixture_c_in_dict):
        path = os.path.join(current_dir, 'fixtures/analysis_fixture_c.toml')
        result = load_toml(path, keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)
        diff = DeepDiff(analysis_fixture_c_in_dict, result)
        assert not diff

    @pytest.mark.parametrize("values, func, expected", [
        (
            {'a': [], 'b': [1, 2], 'c': {'a': [4, 5]}},
            set,
            {'a': [], 'b': {1, 2}, 'c': {'a': [4, 5]}},
        ),
        (
            {'a': [], 'b': {1, 2}, 'c': {'a': {4, 5}}},
            list,
            {'a': [], 'b': [1, 2], 'c': {'a': {4, 5}}},
        ),
    ])
    def test_convert_dict_key(self, values, func, expected):
        convert_dict_key(values, key='b', func=func)
        diff = DeepDiff(expected, values)
        assert not diff

    @pytest.mark.parametrize("values, func, expected", [
        (
            {'a': [], 'b': [1, SqlalchemyFieldType.Integer], 'c': {'a': SqlalchemyFieldType.String}},
            str,
            {'a': [], 'b': [1, 'SqlalchemyFieldType.Integer'], 'c': {'a': 'SqlalchemyFieldType.String'}},
        ),
    ])
    def test_convert_dict_item_type(self, values, func, expected):
        convert_dict_item_type(values, _type=enum.Enum, func=func)
        diff = DeepDiff(expected, values)
        assert not diff

    @mock.patch('modelmapper.misc.open')
    def test_write_toml(self, mock_open):
        item = {'a': [], 'b': [1, SqlalchemyFieldType.Integer], 'c': {'a': SqlalchemyFieldType.String}}
        result = write_toml('some path', item)
        assert result == 'a = []\nb = [1, "SqlalchemyFieldType.Integer"]\n\n[c]\na = "SqlalchemyFieldType.String"\n'
