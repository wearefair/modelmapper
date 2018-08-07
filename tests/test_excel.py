import os
import pytest
from deepdiff import DeepDiff
from modelmapper.excel import (_xls_contents_to_csvs, _xls_xml_contents_to_dict,
                               _xls_xml_contents_to_csvs, excel_contents_to_csvs,
                               _xlsx_contents_to_csvs, excel_file_to_csv_files)

from tests.fixtures.excel_fixtures import (xls_contents2, xls_xml_contents1, xls_xml_contents2,  # NOQA
                                           xls_xml_contents_in_json1, xls_xml_contents_in_json2, csv_contents2,
                                           xls_xml_contents1_with_2_sheets, csv_contents1_other_sheet,
                                           csv_contents1, csv_contents1_reformatted,
                                           xlsx_contents1_with_2_sheets, xlsx_contents2)


current_dir = os.path.dirname(os.path.abspath(__file__))


class TestExcel:

    @pytest.mark.parametrize('contents, content_transform', [ # NOQA
        (xls_contents2(), _xls_contents_to_csvs),
        (xls_xml_contents2(), _xls_xml_contents_to_csvs),
        (xlsx_contents2(), _xlsx_contents_to_csvs),
    ])
    def test_contents_csvs(self, contents, content_transform, csv_contents2):
        results = content_transform(contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents2

    @pytest.mark.parametrize('contents, expected', [
        (xls_xml_contents1(), xls_xml_contents_in_json1()),
        (xls_xml_contents2(), xls_xml_contents_in_json2()),
    ])
    def test_xls_xml_contents_to_list(self, contents, expected):
        results = _xls_xml_contents_to_dict(contents)
        assert results == expected

    @pytest.mark.parametrize("contents", [  # NOQA
        xls_contents2(),
        xls_xml_contents2(),
        xlsx_contents2(),
    ])
    def test_excel_contents_to_csvs(self, contents, csv_contents2):
        results = excel_contents_to_csvs(contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        diff = DeepDiff(result_content, csv_contents2)
        assert not diff

    @pytest.mark.parametrize("contents", [  # NOQA
        xls_xml_contents1_with_2_sheets(),
    ])
    def test_excel_contents_to_csvs_multiple_sheets(self, contents, csv_contents1_reformatted,
                                                    csv_contents1_other_sheet):
        results = excel_contents_to_csvs(contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents1_other_sheet
        result_content = results['training_fixture1'].read()
        assert result_content == csv_contents1_reformatted

    @pytest.mark.parametrize('file_type', [
        'xls',
        'xlsx'
    ])
    def test_excel_file_to_csv_files(self, file_type):
        output_csv_path = None
        try:
            path = os.path.join(current_dir, 'fixtures/training_fixture2.{}'.format(file_type))
            excel_file_to_csv_files(path=path)
            output_csv_path = os.path.join(current_dir, 'fixtures/training_fixture2__Sheet1.csv')
            assert os.path.exists(output_csv_path)
        finally:
            if output_csv_path:
                os.remove(output_csv_path)
