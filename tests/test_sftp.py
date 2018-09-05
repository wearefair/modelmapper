from unittest import mock

import pytest

from modelmapper.client import SFTPClient, ClientException


class SFTPClientStub:
    def __enter__(*args):
        return {}

    def __exit__(*args):
        return None


@pytest.fixture(scope='session')
def sftp_client():
    sftp_kwargs = {
        'raw_key_model': {},
        'session': {},
        'hostname': 'localhost',
        'username': 'skamdart',
        'password': 'puppies'
    }
    return SFTPClient(**sftp_kwargs)


@pytest.fixture(scope='session')
def client():
    return SFTPClient()


class TestSFTPClient:

    def test_constructor(self):
        with pytest.raises(ClientException):
            SFTPClient()

    def test_default_callback(self, sftp_client):
        assert callable(sftp_client.default_callback)

    @mock.patch('modelmapper.client.SFTPClient.get_sftp')
    def test_contents(self, mock, sftp_client):
        pass

    def test_getfo(self):
        pass

    def test_get(self):
        pass
