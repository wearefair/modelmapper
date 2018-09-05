import pytest

from modelmapper.client import SFTPClient


@pytest.fixture(scope='session')
def client():
    return SFTPClient()


class TestSFTPClient:

    def test_foo(self):
        pass
