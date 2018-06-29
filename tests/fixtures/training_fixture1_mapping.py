import pytest

from modelmapper.mapper import FieldResult, SqlalchemyFieldType


@pytest.fixture
def all_fixture1_values():
    return {'available': ['FALSE', 'TRUE', '', '', ''],
            'casualty': ['N', 'N', 'N', '', 'N'],
            'last_payment_date': ['5/5/18', '5/5/18', '5/7/18', 'Nan', '5/7/18'],
            'make': ['Cadillac', 'Chrysler', 'Kia', 'Ford', 'BMW'],
            'score': ['233', '1000', '', '100', '200'],
            'slope': ['-2.14%', '-1.91%', '-2.48%', '-1.90%', '-1.92%'],
            'value_current': ['$15,688 ', '$12,785 ', '$8,655 ', '$13,700 ', '$16,313 '],
            'year': ['2015', '2014', '2017', '2017', '2014']}


@pytest.fixture
def all_field_results_fixture1():
    return {'available': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True),
            'casualty': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean, field_db_str='Boolean', is_nullable=True),
            'last_payment_date': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.DateTime, field_db_str='DateTime', is_nullable=True, datetime_formats={'%m/%d/%y'}),
            'make': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.String, field_db_str='String(40)', is_nullable=False),
            'score': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.SmallInteger, field_db_str='SmallInteger', is_nullable=True),
            'slope': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Decimal, field_db_str='DECIMAL(7, 6)', is_nullable=True, is_percent=True),
            'value_current': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.Integer, field_db_str='Integer', is_nullable=True, is_dollar=True),
            'year': FieldResult(field_db_sqlalchemy_type=SqlalchemyFieldType.SmallInteger, field_db_str='SmallInteger', is_nullable=True)}


@pytest.fixture
def all_field_sqlalchemy_str_fixture1():
    return {'available': '    available = Column(Boolean, nullable=True)\n',
            'casualty': '    casualty = Column(Boolean, nullable=True)\n',
            'last_payment_date': '    last_payment_date = Column(DateTime, '
                                 'nullable=True)\n',
            'make': "    make = Column(String(40), nullable=False, default='')\n",
            'score': '    score = Column(SmallInteger, nullable=True)\n',
            'slope': '    slope = Column(DECIMAL(7, 6), nullable=True)\n',
            'value_current': "    value_current = Column(Integer, nullable=True)\n",
            'year': '    year = Column(SmallInteger, nullable=True)\n'}
