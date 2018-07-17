import os
import pytest

from deepdiff import DeepDiff
from modelmapper import Cleaner
from fixtures.training_fixture1_cleaned_for_import import cleaned_csv_for_import_fixture  # NOQA

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')
training_fixture1_xls_xml_path = training_fixture1_path.replace('.csv', '.xml')
training_fixture1_xls_path = training_fixture1_path.replace('.csv', '.xls')


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
        ('csv', training_fixture1_path, None, None),
        ('xls_xml', training_fixture1_xls_xml_path, None, None),
    ])
    def test_clean_csv_and_xls_xml(self, cleaner, cleaned_csv_for_import_fixture, content_type, path, content, sheet_names):

        result_gen = cleaner.clean(content_type=content_type, path=path,
                                   content=content, sheet_names=sheet_names)
        result = list(result_gen)
        diff = DeepDiff(cleaned_csv_for_import_fixture, result)
        assert not diff

    @pytest.mark.parametrize("content_type, path, content, sheet_names", [  # NOQA
        ('xls', training_fixture1_xls_path, None, None),
    ])
    def test_clean_xls(self, cleaner, cleaned_csv_for_import_fixture, content_type, path, content, sheet_names):

        result_gen = cleaner.clean(content_type=content_type, path=path,
                                   content=content, sheet_names=sheet_names)
        result = list(result_gen)
        diff = DeepDiff(cleaned_csv_for_import_fixture, result)
        for item in diff['values_changed'].keys():
            assert item.endswith("year']")
