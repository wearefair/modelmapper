import os
import pytest
from modelmapper.misc import load_toml
from modelmapper.mapper import SqlalchemyFieldType
TOML_KEYS_THAT_ARE_SET = 'datetime_formats'


current_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def analysis_fixture_a():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_a.toml'), keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)


@pytest.fixture
def analysis_fixture_b():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_b.toml'), keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)


@pytest.fixture
def analysis_fixture_c():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_c.toml'), keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)


@pytest.fixture
def analysis_fixture_c_in_dict():
    return {
        'last_payment_date': {
            'datetime_formats': {'%d/%m/%y'},
            'field_db_str': 'DateTime'
        },
        'score': {
            'field_db_str': 'BigInteger',
            'is_nullable': True
        },
        'slope': {
            'field_db_str': 'DECIMAL(11, 2)'
        },
        'value_current': {
            'field_db_str': 'String(23)',
            'is_nullable': False
        }
    }


@pytest.fixture
def override_fixture1():
    return {'score': {'field_db_str': 'Decimal', 'args': (6, 2)}, }


@pytest.fixture
def analysis_fixture_a_only_combined():
    return {'casualty': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean),
            'last_payment_date': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.DateTime, is_nullable=True, datetime_formats={'%m/%d/%y'}),
            'slope': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.Integer),
            'value_current': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.String, is_nullable=False, args=38)}


@pytest.fixture
def analysis_fixture_a_and_b_combined():
    return {
        'casualty': {
            'field_db_sqlalchemy_type': SqlalchemyFieldType.Boolean,
            'is_nullable': True
        },
        'last_payment_date': {
            'datetime_formats': {'%m/%d/%y', '%d/%m/%y'},
            'field_db_sqlalchemy_type': SqlalchemyFieldType.DateTime,
            'is_nullable': True
        },
        'score': {
            'field_db_sqlalchemy_type': SqlalchemyFieldType.SmallInteger,
            'is_nullable': True
        },
        'slope': {
            'args': (5, 2),
            'field_db_sqlalchemy_type': SqlalchemyFieldType.Decimal,
            'is_nullable': True
        },
        'value_current': {
            'args': 64,
            'field_db_sqlalchemy_type': SqlalchemyFieldType.String,
            'is_nullable': False
        }
    }


@pytest.fixture
def analysis_fixture_a_and_b_combined_with_override():
    return {
        'casualty': {
            'field_db_sqlalchemy_type': SqlalchemyFieldType.Boolean,
            'is_nullable': True
        },
        'last_payment_date': {
            'datetime_formats': {'%m/%d/%y', '%d/%m/%y'},
            'field_db_sqlalchemy_type': SqlalchemyFieldType.DateTime,
            'is_nullable': True
        },
        'score': {
            'field_db_sqlalchemy_type': SqlalchemyFieldType.Decimal,
            'is_nullable': True,
            'args': (6, 2),
        },
        'slope': {
            'args': (5, 2),
            'field_db_sqlalchemy_type': SqlalchemyFieldType.Decimal,
            'is_nullable': True
        },
        'value_current': {
            'args': 64,
            'field_db_sqlalchemy_type': SqlalchemyFieldType.String,
            'is_nullable': False
        }
    }
