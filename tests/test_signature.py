import datetime
from decimal import Decimal
import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, DECIMAL

from modelmapper.signature import (
    generate_row_signature,
    normalize_decimal_columns,
    drop_model_defaults,
    sort_row_values,
    get_byte_str_of_row,
    get_hash_of_bytes
)

Base = declarative_base()


class Model(Base):
    __tablename__ = "test-table"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    signature = Column(BigInteger, nullable=True, index=True)
    field_id = Column(String(64), nullable=False, default='', index=True)
    field_decimal = Column(DECIMAL(8, 2), nullable=True)


class TestSignatures:

    @pytest.mark.parametrize("value, hash_args, expected", [
        (b'abc', {'x64arch': False, 'bits': 64}, 'x5d4ff95a8a32392f'),
        (b'abc', {'x64arch': True, 'bits': 64}, 'x4b69c0c0c0528799'),
        (b'cba', {'bits': 32}, '63613144'),
        (b'zzz', {'bits': 128, 'x64arch': True}, '1bcc202d964665860f1690f05fe76db')
    ])
    def test_get_hash(self, value, hash_args, expected):
        assert get_hash_of_bytes(value, **hash_args) == expected

    @pytest.mark.parametrize("row, ignore_fields, expected", [
        ({'col_1': 1, 'col_2': 2}, [], b'col_1:1,col_2:2'),
        ({'col_1': 1, 'col_2': 2}, ['col_1'], b'col_2:2'),
        ([('col_1', 1), ('col_2', 2)], ['col_1'], b'col_2:2'),
    ])
    def test_get_byte_str_of_row(self, row, ignore_fields, expected):
        assert get_byte_str_of_row(row, ignore_fields) == expected

    @pytest.mark.parametrize("row, expected", [
        ({'b': 1, 'a': 2}, [('a', 2), ('b', 1)]),
        ({'b': 1, 'a': 2, 1: 3}, [(1, 3), ('a', 2), ('b', 1)])
    ])
    def test_sort_row_values(self, row, expected):
        assert sort_row_values(row) == expected

    @pytest.mark.parametrize("row, expected", [
        ({'field_id': '', 'field_decimal': Decimal(1.1)}, {'field_decimal': Decimal(1.1)}),
        ({'field_id': 'name', 'field_decimal': Decimal(1.1)}, {'field_id': 'name', 'field_decimal': Decimal(1.1)})
    ])
    def test_drop_model_defaults(self, row, expected):
        assert drop_model_defaults(row, Model) == expected

    @pytest.mark.parametrize("row, expected", [
        ({'field_decimal': Decimal('1.5000')}, {'field_decimal': Decimal(1.5)}),
        ({'field_decimal': Decimal('0.000000')}, {'field_decimal': Decimal(0)}),
        ({'field_decimal': Decimal(5), 'test': 'test'}, {'field_decimal': Decimal(5), 'test': 'test'})
    ])
    def test_normalize_decimal_columns(self, row, expected):
        assert normalize_decimal_columns(row) == expected

    @pytest.mark.parametrize("row, ignore_fields, expected", [
        ({'field_id': 'test', 'field_decimal': Decimal('1.5000'), 'id': 1}, ['id'], 'db5ab208f5b5c8bdc70ee2be1e72a37e')
    ])
    def test_generate_row_signature(self, row, ignore_fields, expected):
        assert generate_row_signature(row, Model, ignore_fields) == expected
