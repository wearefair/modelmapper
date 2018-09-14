import pytest

from modelmapper.client import BaseClient


@pytest.fixture()
def client():
    return BaseClient()


class TestBaseClient:
    def test_extract_not_implemented():
        with pytest.raises(NotImplemented):
            BaseClient()
