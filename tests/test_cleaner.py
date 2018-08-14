import io
import os
import pytest

from deepdiff import DeepDiff
from modelmapper import Cleaner
from tests.fixtures.training_fixture1_cleaned_for_import import cleaned_csv_for_import_fixture  # NOQA
from tests.fixtures.training_fixture1_with_2_sheets_cleaned_for_import import cleaned_csv_with_2_sheets_combined_for_import_fixture  # NOQA

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')
training_fixture1_xls_xml_path = training_fixture1_path.replace('.csv', '.xml')
training_fixture1_xls_path = training_fixture1_path.replace('.csv', '.xls')
training_fixture1_xlsx_path = training_fixture1_path.replace('.csv', '.xlsx')
training_fixture1_tab_path = training_fixture1_path.replace('.csv', '.txt')
training_fixture1_with_2_sheets_path = os.path.join(current_dir, 'fixtures/training_fixture1_with_2_sheets.xml')
training_fixture1_xlsx_with_2_sheets_path = training_fixture1_with_2_sheets_path.replace('xml', 'xlsx')

with open(training_fixture1_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_content_str = the_file.read()

with open(training_fixture1_xls_xml_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_xls_xml_content_str = the_file.read()

with open(training_fixture1_tab_path, 'r', encoding='utf-8-sig') as ostream:
    training_fixture1_tab_content_str = ostream.read()


@pytest.fixture
def cleaner():
    return Cleaner(example_setup_path)


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

    @pytest.mark.parametrize("content_type, path, content, sheet_names", [  # NOQA
        ('csv', None, io.StringIO(training_fixture1_content_str), None),
        ('csv', None, training_fixture1_content_str, None),
        ('csv', None, training_fixture1_content_str.encode('utf-8'), None),
        ('csv', None, io.BytesIO(training_fixture1_content_str.encode('utf-8')), None),
        ('csv', training_fixture1_path, None, None),
        ('txt', None, io.StringIO(training_fixture1_tab_content_str), None),
        ('txt', None, training_fixture1_tab_content_str, None),
        ('txt', None, training_fixture1_tab_content_str.encode('utf-8'), None),
        ('txt', None, io.BytesIO(training_fixture1_tab_content_str.encode('utf-8')), None),
        ('txt', training_fixture1_tab_path, None, None),
        ('xls_xml', training_fixture1_xls_xml_path, None, None),
        ('xls_xml', None, training_fixture1_xls_xml_content_str, None),
        ('xls_xml', None, training_fixture1_xls_xml_content_str.encode('utf-8'), None),
        ('xls_xml', None, io.BytesIO(training_fixture1_xls_xml_content_str.encode('utf-8')), None),
        ('xls_xml', None, io.StringIO(training_fixture1_xls_xml_content_str), None),
    ])
    def test_clean_csv_and_xls_xml(self, cleaner, cleaned_csv_for_import_fixture, content_type,
                                   path, content, sheet_names):

        result_gen = cleaner.clean(content_type=content_type, path=path,
                                   content=content, sheet_names=sheet_names)
        result = list(result_gen)
        diff = DeepDiff(cleaned_csv_for_import_fixture, result)
        assert not diff

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
