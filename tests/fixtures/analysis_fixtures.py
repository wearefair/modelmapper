import os
import pytest
from modelmapper.misc import load_toml
from modelmapper.mapper import SqlalchemyFieldType, FieldResult


current_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def analysis_fixture_a():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_a.toml'))


@pytest.fixture
def analysis_fixture_b():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_b.toml'))


@pytest.fixture
def analysis_fixture_c():
    return load_toml(os.path.join(current_dir, 'analysis_fixture_c.toml'))


@pytest.fixture
def analysis_fixture_a_only_combined():
    return {'casualty': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean),
            'last_payment_date': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.DateTime, is_nullable=True, datetime_formats={'%m/%d/%y', '%d/%m/%y'}),
            'slope': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.Integer),
            'value_current': dict(field_db_sqlalchemy_type=SqlalchemyFieldType.String, is_nullable=False, args=[38])}
