import os
import pytest
from deepdiff import DeepDiff
from modelmapper.excel import (_xls_contents_to_csvs, _xls_xml_contents_to_dict,
                               _xls_xml_contents_to_csvs, excel_contents_to_csvs,
                               excel_file_to_csv_files)

from fixtures.excel_fixtures import (xls_contents2, xls_xml_contents1, xls_xml_contents2,  # NOQA
                                     xls_xml_contents_in_json1, xls_xml_contents_in_json2, csv_contents2,
                                     xls_xml_contents1_with_2_sheets, csv_contents1_other_sheet,
                                     csv_contents1, csv_contents1_reformatted)


current_dir = os.path.dirname(os.path.abspath(__file__))


class TestExcel:

    def test_xls_contents_to_csvs(self, xls_contents2, csv_contents2):  # NOQA
        results = _xls_contents_to_csvs(xls_contents2)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents2

    def test_xls_xml_contents_to_csvs(self, xls_xml_contents2, csv_contents2):  # NOQA
        results = _xls_xml_contents_to_csvs(xls_xml_contents2)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents2

    def test_xls_xml_contents_to_lists1(self, xls_xml_contents1, xls_xml_contents_in_json1):  # NOQA
        results = _xls_xml_contents_to_dict(xls_xml_contents1)
        assert results == xls_xml_contents_in_json1

    def test_xls_xml_contents_to_lists2(self, xls_xml_contents2, xls_xml_contents_in_json2):  # NOQA
        results = _xls_xml_contents_to_dict(xls_xml_contents2)
        assert results == xls_xml_contents_in_json2

    @pytest.mark.parametrize("contents", [  # NOQA
        xls_contents2(),
        xls_xml_contents2(),
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

    def test_excel_file_to_csv_files(self):
        output_csv_path = None
        try:
            path = os.path.join(current_dir, 'fixtures/training_fixture2.xls')
            excel_file_to_csv_files(path=path)
            output_csv_path = os.path.join(current_dir, 'fixtures/training_fixture2__Sheet1.csv')
            assert os.path.exists(output_csv_path)
        finally:
            if output_csv_path:
                os.remove(output_csv_path)
