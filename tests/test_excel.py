import os
import json
import pytest
from modelmapper.excel import (_xls_contents_to_csvs, _xls_xml_contents_to_dict,
                               _xls_xml_contents_to_csvs, excel_contents_to_csvs)


current_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope='session')
def xls_contents():
    with open(os.path.join(current_dir, 'fixtures/training_fixture2.xls'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents():
    with open(os.path.join(current_dir, 'fixtures/training_fixture2.xml'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents_in_json():
    with open(os.path.join(current_dir, 'fixtures/training_fixture2.json'), 'rb') as the_file:
        return json.loads(the_file.read())


@pytest.fixture(scope='session')
def csv_contents():
    with open(os.path.join(current_dir, 'fixtures/training_fixture2.csv'), 'r') as the_file:
        return the_file.read()


class TestExcel:

    def test_xls_contents_to_csvs(self, xls_contents, csv_contents):
        results = _xls_contents_to_csvs(xls_contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents

    def test_xls_xml_contents_to_csvs(self, xls_xml_contents, csv_contents):
        results = _xls_xml_contents_to_csvs(xls_xml_contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents

    def test_xls_xml_contents_to_lists(self, xls_xml_contents, xls_xml_contents_in_json):
        results = _xls_xml_contents_to_dict(xls_xml_contents)
        assert results == xls_xml_contents_in_json

    @pytest.mark.parametrize("contents", [
        xls_contents(),
        xls_xml_contents(),
    ])
    def test_excel_contents_to_csvs(self, contents, csv_contents):
        results = excel_contents_to_csvs(contents)
        assert 'Sheet1' in results
        result_content = results['Sheet1'].read()
        assert result_content == csv_contents
