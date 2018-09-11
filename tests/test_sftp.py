from unittest import mock

import pytest

from modelmapper.client import SFTPClient, ClientException


HOSTNAME = 'localhost'
USERNAME = 'skamdart'
PASS = 'puppies'
REMOTE_PATH = '/tmp/file.txt'
LOCAL_PATH = '/tmp/local.txt'
sftp_kwargs = {'raw_key_model': {},
               'session': {},
               'hostname': HOSTNAME,
               'username': USERNAME,
               'password': PASS}


@pytest.fixture(scope='session')
def client():
    return SFTPClient(**sftp_kwargs)


class TestSFTPClient:

    def test_constructor(self):
        with pytest.raises(ClientException):
            SFTPClient()

    def test_default_callback(self, client):
        assert callable(client.default_callback)

    @mock.patch('modelmapper.client.SFTPClient.get_sftp')
    def test_contents(self, mock, client):
        client.contents('/')
        assert mock.call_args[0][0] == HOSTNAME
        assert mock.call_args[1]['password'] == PASS
        assert mock.call_args[1]['username'] == USERNAME

    @mock.patch('modelmapper.client.SFTPClient.get_sftp')
    def test_getfo(self, mock, client):
        from io import BytesIO
        assert client.getfo(REMOTE_PATH, BytesIO())
        assert mock.call_args[0][0] == HOSTNAME
        assert mock.call_args[1]['password'] == PASS
        assert mock.call_args[1]['username'] == USERNAME

    @mock.patch('modelmapper.client.SFTPClient.get_sftp')
    def test_get(self, mock, client):
        assert client.get(REMOTE_PATH, LOCAL_PATH) == LOCAL_PATH
        assert mock.call_args[0][0] == HOSTNAME
        assert mock.call_args[1]['password'] == PASS
        assert mock.call_args[1]['username'] == USERNAME
