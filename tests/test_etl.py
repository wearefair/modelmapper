import os
from unittest import mock
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import exc as core_exc

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


def content_generator():
    yield training_fixture1_content_str


class TestETL:
    @mock.patch('modelmapper.ETL._create_raw_key')
    @mock.patch('modelmapper.ETL.get_client_data')
    def test_extract_generator(self, mock_client_data, mock_create_raw_key, job):
        mock_client_data.return_value = content_generator()
        mock_create_raw_key.return_value = uuid4()
        data = job._extract(None, backup_data=False, content_type='csv')

        assert ''.join(list(data['content'])) == training_fixture1_content_str

    @mock.patch('modelmapper.ETL._create_raw_key')
    @mock.patch('modelmapper.ETL.get_client_data')
    def test_extract_no_generator(self, mock_client_data, mock_create_raw_key, job):
        mock_client_data.return_value = training_fixture1_content_str

        mock_create_raw_key.return_value = uuid4()

        data = job._extract(None, backup_data=False, content_type='csv')

        assert data['content'] == training_fixture1_content_str

    def test_reprocess_in_create_raw_key(self):
        # This mock simulates the case where the create raw key function
        # is called and received a duplicate key. It raises an
        # IntegrityError which triggers our reprocessing code if the
        # reprocessing feature is enabled.
        mock_session = Mock()
        mock_session.flush = Mock(side_effect=core_exc.IntegrityError("test", "test", "test"))

        mock_session.query.filter = Mock()

        raw_id = Mock()
        raw_id.id = "123"

        filter_ret_mock = Mock()
        filter_ret_mock.one = Mock(return_value=raw_id)

        filter_mock = Mock()
        filter_mock.filter = Mock(return_value=filter_ret_mock)

        mock_session.query = Mock(return_value=filter_mock)

        test_etl = ETL(setup_path=example_setup_path)
        test_etl.settings = test_etl.settings._replace(should_reprocess=True)  # creating a new settings namedtuple.
        test_etl.RAW_KEY_MODEL = Mock()
        actual_id = test_etl._create_raw_key(mock_session, "123", "123")

        assert actual_id == '123'

    @pytest.mark.parametrize('fn_name, arg_count', [
        ('get_client_data', 0),
        ('report_exception', 1),
        ('encrypt_raw_data', 1),
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
        ('encrypt_raw_data', 1),
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
