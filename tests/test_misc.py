import csv
import io
import os
import enum
import pytest
from unittest import mock
from deepdiff import DeepDiff
from modelmapper.misc import (escape_word, get_combined_dict, load_toml, convert_dict_key,
                              convert_dict_item_type, write_toml, write_settings, read_csv_gen,
                              DefaultList, generator_chunker, generator_updater)
from modelmapper.mapper import SqlalchemyFieldType
from tests.fixtures.analysis_fixtures import analysis_fixture_c_in_dict  # NOQA
from tests.fixtures.excel_fixtures import xls_xml_contents_in_json2, csv_contents2, offset_header, corrected_header

TOML_KEYS_THAT_ARE_SET = 'datetime_formats'


current_dir = os.path.dirname(os.path.abspath(__file__))


def dummy_cleaning_func(x):
    return x.lower().strip()


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

    def test_write_settings(self):
        template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.toml')
        loaded_template = load_toml(template_setup_path)
        with open(template_setup_path, 'r') as the_file:
            template_settings_content = the_file.read()
        result = write_settings('/tmp/settings.toml', loaded_template)
        assert template_settings_content == result

    def test_default_list1(self):
        items = DefaultList()
        items.append(1)
        items[2] = 3
        assert [1, None, 3] == items

    def test_default_list_none(self):
        items = DefaultList([1, 2, 3])
        items[6] = 'Nice, I like it.'
        assert [1, 2, 3, None, None, None, 'Nice, I like it.'] == items

    def test_default_list_string(self):
        items = DefaultList([1, 2, 3], default='yes')
        items[5] = 'Nice, I like it.'
        assert [1, 2, 3, 'yes', 'yes', 'Nice, I like it.'] == items

    def test_default_list_dict(self):
        items = DefaultList([1, 2, 3], default=dict)
        items[5]['key'] = 'Nice, I like it.'
        assert [1, 2, 3, {}, {}, {'key': 'Nice, I like it.'}] == items

    @pytest.mark.parametrize("gen, chunk_size, expected", [
        (
            (i for i in range(10)),
            3,
            [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        ),
        (
            (i for i in range(5)),
            6,
            [[0, 1, 2, 3, 4]]
        ),
    ])
    def test_generator_chunker(self, gen, chunk_size, expected):
        result = generator_chunker(gen, chunk_size)
        result_list = list(result)
        assert expected == result_list

    @pytest.mark.parametrize("gen, update, expected", [
        (
            ({i: i} for i in range(2)),
            {'a': 'b'},
            [{0: 0, 'a': 'b'}, {1: 1, 'a': 'b'}]
        ),
    ])
    def test_generator_updater(self, gen, update, expected):
        result = generator_updater(gen, **update)
        result_list = list(result)
        assert expected == result_list

    @pytest.mark.parametrize('contents, expected, raw_headers', [
        (csv_contents2(), xls_xml_contents_in_json2()['Sheet1'], {}),
    ])
    def test_read_csv_gen(self, contents, expected, raw_headers):  # NOQA
        item = io.StringIO(contents)
        item = list(read_csv_gen(item, raw_headers_include=raw_headers))
        assert item == expected

    def test_read_csv_gen_offset_header(self):
        offset_io = io.StringIO(offset_header())
        raw_headers = {dummy_cleaning_func(i) for i in {'Account Number', 'Fees'}}
        csv_gen = read_csv_gen(offset_io, raw_headers_include=raw_headers, cleaning_func=dummy_cleaning_func)
        for corrected, expected in zip(list(csv_gen), corrected_header().split('\n')):
            assert corrected == expected.split(',')

    def test_read_csv_gen_errors(self):
        offset_io = io.StringIO(offset_header())
        with pytest.raises(csv.Error):
            list(read_csv_gen(offset_io))
