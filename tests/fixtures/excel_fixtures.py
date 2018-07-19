import os
import json
import pytest

current_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope='session')
def xls_contents1():
    with open(os.path.join(current_dir, 'training_fixture1.xls'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents1():
    with open(os.path.join(current_dir, 'training_fixture1.xml'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents_in_json1():
    with open(os.path.join(current_dir, 'training_fixture1.json'), 'rb') as the_file:
        return json.loads(the_file.read())


@pytest.fixture(scope='session')
def csv_contents1():
    with open(os.path.join(current_dir, 'training_fixture1.csv'), 'r', encoding='utf-8-sig') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def csv_contents1_reformatted():
    with open(os.path.join(current_dir, 'training_fixture1_reformatted.csv'), 'r', encoding='utf-8-sig') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def csv_contents1_other_sheet():
    with open(os.path.join(current_dir, 'training_fixture1_with_2_sheets__Sheet1.csv'), 'r', encoding='utf-8-sig') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents1_with_2_sheets():
    with open(os.path.join(current_dir, 'training_fixture1_with_2_sheets.xml'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_contents2():
    with open(os.path.join(current_dir, 'training_fixture2.xls'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents2():
    with open(os.path.join(current_dir, 'training_fixture2.xml'), 'rb') as the_file:
        return the_file.read()


@pytest.fixture(scope='session')
def xls_xml_contents_in_json2():
    with open(os.path.join(current_dir, 'training_fixture2.json'), 'rb') as the_file:
        return json.loads(the_file.read())


@pytest.fixture(scope='session')
def csv_contents2():
    with open(os.path.join(current_dir, 'training_fixture2.csv'), 'r', encoding='utf-8-sig') as the_file:
        return the_file.read()
