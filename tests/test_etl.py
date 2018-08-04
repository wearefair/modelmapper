import os
from types import GeneratorType
from unittest import mock
from uuid import uuid4

import pytest

from modelmapper import ETL

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')

with open(training_fixture1_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_content_str = the_file.read()


@pytest.fixture(scope='module')
def job():
    return ETL(setup_path=example_setup_path)


class TestETL:

    @mock.patch('modelmapper.ETL.get_client_data')
    @mock.patch('modelmapper.ETL._create_raw_key')
    def test_extract_no_generator(self, mock_client_data, mock_create_raw_key, job):
        mock_client_data.return_value = training_fixture1_content_str
        mock_create_raw_key.return_value = uuid4()
        data = job._extract(None, backup_data=False, content_type='csv')
        assert not isinstance(data['content'], GeneratorType)

    @mock.patch('modelmapper.ETL.get_client_data')
    @mock.patch('modelmapper.ETL._create_raw_key')
    def test_extract_generator(self, mock_client_data, mock_create_raw_key, job):
        mock_client_data.return_value = yield training_fixture1_content_str
        mock_create_raw_key.return_value = uuid4()
        data = job._extract(None, backup_data=False, content_type='csv')
        assert isinstance(data['content]'], GeneratorType)
