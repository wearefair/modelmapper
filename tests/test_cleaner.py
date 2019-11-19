import io
import os
import pytest

from deepdiff import DeepDiff
from modelmapper import Cleaner
from modelmapper.cleaner import ErrorRegistry
from modelmapper.mapper import SqlalchemyFieldType
from tests.fixtures.training_fixture1_cleaned_for_import import cleaned_csv_for_import_fixture  # NOQA
from tests.fixtures.training_fixture1_with_2_sheets_cleaned_for_import import cleaned_csv_with_2_sheets_combined_for_import_fixture  # NOQA
from tests.fixtures.cleaning_err_registry_fixtures import (
    add_errs1, expected_report_str1, expected_report_dict1,
    add_errs2, expected_report_str2, expected_report_dict2,
    add_errs3, expected_report_str3, expected_report_dict3,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
tsv_setup_path = os.path.join(current_dir, '../modelmapper/example/tsv_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')
training_fixture1_xls_xml_path = training_fixture1_path.replace('.csv', '.xml')
training_fixture1_xls_path = training_fixture1_path.replace('.csv', '.xls')
training_fixture1_xlsx_path = training_fixture1_path.replace('.csv', '.xlsx')
training_fixture1_tab_path = training_fixture1_path.replace('.csv', '.tsv')
training_fixture1_with_2_sheets_path = os.path.join(current_dir, 'fixtures/training_fixture1_with_2_sheets.xml')
training_fixture1_xlsx_with_2_sheets_path = training_fixture1_with_2_sheets_path.replace('xml', 'xlsx')
new_field_fixture_path = os.path.join(current_dir, 'fixtures/new_field_fixture.csv')

with open(training_fixture1_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_content_str = the_file.read()

with open(new_field_fixture_path, 'r', encoding='utf-8-sig') as the_file:
    new_field_fixture_str = the_file.read()

with open(training_fixture1_xls_xml_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_xls_xml_content_str = the_file.read()

with open(training_fixture1_tab_path, 'r', encoding='utf-8-sig') as ostream:
    training_fixture1_tab_content_str = ostream.read()


@pytest.fixture
def cleaner():
    return Cleaner(example_setup_path)


@pytest.fixture
def tsv_cleaner():
    return Cleaner(tsv_setup_path)


class TestCleaner:

    def test_get_csv_data_cleaned(self, cleaner, cleaned_csv_for_import_fixture):  # NOQA
        result = list(cleaner.get_csv_data_cleaned(training_fixture1_path))
        assert result == cleaned_csv_for_import_fixture

    @pytest.mark.parametrize("line, is_parsable", [
        (["1", "2", ""], True),
        (["", "", "a"], True),
        (["", "", ""], False),
        (["---", "-", "--"], False),
    ])
    def test_does_line_include_data(self, cleaner, line, is_parsable):
        result = cleaner._does_line_include_data(line)
        assert is_parsable is result

    @pytest.mark.parametrize("content_type, path, content, sheet_names, _cleaner, missing_field", [  # NOQA
        ('csv', None, new_field_fixture_str, None, cleaner(), ['new_column']),
        ('csv', None, io.StringIO(training_fixture1_content_str), None, cleaner(), []),
        ('csv', None, training_fixture1_content_str, None, cleaner(), []),
        ('csv', None, training_fixture1_content_str.encode('utf-8'), None, cleaner(), []),
        ('csv', None, io.BytesIO(training_fixture1_content_str.encode('utf-8')), None, cleaner(), []),
        ('csv', training_fixture1_path, None, None, cleaner(), []),
        ('tsv', None, io.StringIO(training_fixture1_tab_content_str), None, tsv_cleaner(), []),
        ('tsv', None, training_fixture1_tab_content_str, None, tsv_cleaner(), []),
        ('tsv', None, training_fixture1_tab_content_str.encode('utf-8'), None, tsv_cleaner(), []),
        ('tsv', None, io.BytesIO(training_fixture1_tab_content_str.encode('utf-8')), None, tsv_cleaner(), []),
        ('tsv', training_fixture1_tab_path, None, None, tsv_cleaner(), []),
        ('xls_xml', training_fixture1_xls_xml_path, None, None, cleaner(), []),
        ('xls_xml', None, training_fixture1_xls_xml_content_str, None, cleaner(), []),
        ('xls_xml', None, training_fixture1_xls_xml_content_str.encode('utf-8'), None, cleaner(), []),
        ('xls_xml', None, io.BytesIO(training_fixture1_xls_xml_content_str.encode('utf-8')), None, cleaner(), []),
        ('xls_xml', None, io.StringIO(training_fixture1_xls_xml_content_str), None, cleaner(), []),
    ])
    def test_clean_csv_and_xls_xml(self, cleaned_csv_for_import_fixture, content_type,
                                   path, content, sheet_names, _cleaner, missing_field):

        result_gen = _cleaner.clean(content_type=content_type, path=path,
                                    content=content, sheet_names=sheet_names)
        result = list(result_gen)
        diff = DeepDiff(cleaned_csv_for_import_fixture, result)
        assert not diff
        assert list(_cleaner._missing_fields) == missing_field

    @pytest.mark.parametrize("content_type, path, content, sheet_names", [  # NOQA
        ('xls', training_fixture1_xls_path, None, None),
        ('xlsx', training_fixture1_xlsx_path, None, None),
    ])
    def test_clean_excel(self, cleaner, cleaned_csv_for_import_fixture, content_type,
                         path, content, sheet_names):

        result_gen = cleaner.clean(content_type=content_type, path=path,
                                   content=content, sheet_names=sheet_names)
        result = list(result_gen)
        diff = DeepDiff(cleaned_csv_for_import_fixture, result)
        for item in diff['values_changed'].keys():
            assert item.endswith("year']")

    @pytest.mark.parametrize("content_type, path, content, sheet_names", [  # NOQA
        ('xlsx', training_fixture1_xlsx_with_2_sheets_path, None, None),
    ])
    def test_clean_xlsx_multiple_sheets(self, cleaner, cleaned_csv_with_2_sheets_combined_for_import_fixture,
                                        content_type, path, content, sheet_names):

        result_gen = cleaner.clean(content_type=content_type, path=path,
                                   content=content, sheet_names=sheet_names)
        results = list(result_gen)
        diff = DeepDiff(cleaned_csv_with_2_sheets_combined_for_import_fixture, results)
        for item in diff['values_changed'].keys():
            assert item.endswith("year']")

    def test_clean_xlsx_string_date_values(self, cleaner):
        field_info = {
            'is_nullable': True,
            'field_db_sqlalchemy_type': SqlalchemyFieldType.DateTime,
            'datetime_formats': ['%Y/%m/%d']
        }
        field_values = ['2018/02/24']

        assert field_values == cleaner._get_field_values_cleaned_for_importing(
            'test_field', field_info, field_values, 'xlsx')


class TestErrorRegistry:

    @pytest.mark.parametrize('test_num, errs, expected_report_str, expected_report_dict', [
        (1, add_errs1, expected_report_str1, expected_report_dict1),
        (2, add_errs2, expected_report_str2, expected_report_dict2),
        (3, add_errs3, expected_report_str3, expected_report_dict3),
    ])
    def test_error_registry(self, test_num, errs, expected_report_str, expected_report_dict):
        err_reg = ErrorRegistry(total_item_count_per_field=20)
        for (msg, field_name, item) in errs:
            err_reg.add_err(msg=msg, field_name=field_name, item=item)
        result = err_reg.get_report_str()
        result_dict = err_reg.get_report_dict()
        # TO DEBUG:
        # Run this test with pdb and once it fails here, copy paste the following line into terminal:
        # from pprint import pprint; nn = list(map((lambda x: x + '\n'), result.split('\n'))); nn[-1]=nn[-1][:-1]; pprint(nn)  # NOQA
        assert expected_report_str == result
        assert expected_report_dict == result_dict
