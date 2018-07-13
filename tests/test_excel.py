import os
import pytest
from modelmapper.excel import (_xls_contents_to_csvs, _xls_xml_contents_to_dict,
                               _xls_xml_contents_to_csvs, excel_contents_to_csvs,
                               excel_file_to_csv_files)

from fixtures.excel_fixtures import xls_contents, xls_xml_contents, xls_xml_contents_in_json, csv_contents  # NOQA


current_dir = os.path.dirname(os.path.abspath(__file__))


class TestExcel:

    def test_xls_contents_to_csvs(self, xls_contents, csv_contents):  # NOQA
        results = _xls_contents_to_csvs(xls_contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents

    def test_xls_xml_contents_to_csvs(self, xls_xml_contents, csv_contents):  # NOQA
        results = _xls_xml_contents_to_csvs(xls_xml_contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents

    def test_xls_xml_contents_to_lists(self, xls_xml_contents, xls_xml_contents_in_json):  # NOQA
        results = _xls_xml_contents_to_dict(xls_xml_contents)
        assert results == xls_xml_contents_in_json

    @pytest.mark.parametrize("contents", [  # NOQA
        xls_contents(),
        xls_xml_contents(),
    ])
    def test_excel_contents_to_csvs(self, contents, csv_contents):
        results = excel_contents_to_csvs(contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents

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
