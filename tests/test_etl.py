import os
from types import GeneratorType
from unittest import mock
from uuid import uuid4

import mmh3
import pytest

from modelmapper import ETL
from tests.fixtures.etl import BasicETL

current_dir = os.path.dirname(os.path.abspath(__file__))
example_setup_path = os.path.join(current_dir, '../modelmapper/example/some_model_setup.toml')
training_fixture1_path = os.path.join(current_dir, 'fixtures/training_fixture1.csv')

with open(training_fixture1_path, 'r', encoding='utf-8-sig') as the_file:
    training_fixture1_content_str = the_file.read()


@pytest.fixture(scope='module')
def job():
    return ETL(setup_path=example_setup_path)


@pytest.fixture(scope='module')
def basic():
    return BasicETL(setup_path=example_setup_path)


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
        assert isinstance(data['content'], GeneratorType)

    @pytest.mark.parametrize('fn_name, arg_count', [
        ('get_client_data', 0),
        ('report_exception', 1),
        ('encrypt_data', 1),
        ('backup_data', 3),
        ('get_session', 0),
        ('insert_chunk_of_data_to_db', 3),
        ('encrypt_row_fields', 1),
        ('verify_access_to_backup_source', 0),
    ])
    def test_raises_no_subclass_exceptions(self, fn_name, arg_count, job):
        with pytest.raises(NotImplementedError):
            # kinda hacky but it does the trick
            args = [None] * arg_count
            getattr(job, fn_name)(*args)

    @pytest.mark.parametrize('fn_name, arg_count', [
        ('get_client_data', 0),
        ('report_exception', 1),
        ('encrypt_data', 1),
        ('backup_data', 3),
        ('get_session', 0),
        ('insert_chunk_of_data_to_db', 3),
        ('encrypt_row_fields', 1),
        ('verify_access_to_backup_source', 0),
    ])
    def test_subclass_implemented(self, fn_name, arg_count, job):
        with pytest.raises(NotImplementedError):
            # kinda hacky but it does the trick
            args = [None] * arg_count
            getattr(job, fn_name)(*args)

    @pytest.mark.parametrize('content', [
        b'foo_bar',
        'foo_bar',
    ])
    def test_hash_of_bytes(self, job, content):
        assert job.get_hash_of_bytes(content) == mmh3.hash(content)

    def test_hash_of_bytes_converts_to_str(self, job):
        content = None
        assert job.get_hash_of_bytes(content) == mmh3.hash(str(content))
