# flake8: noqa
# NOTE: THIS FILE IS AUTO GENERATED BY MODEL MAPPER BASED ON CSV DATA. DO NOT MODIFY THE FILE.

from modelmapper import SqlalchemyFieldType

FIELDS = {   'available': {   'field_db_sqlalchemy_type': SqlalchemyFieldType.Boolean,
                     'is_nullable': True},
    'casualty': {   'field_db_sqlalchemy_type': SqlalchemyFieldType.Boolean,
                    'is_nullable': True},
    'last_payment_date': {   'datetime_formats': {'%m/%d/%y', '%Y-%m-%d'},
                             'field_db_sqlalchemy_type': SqlalchemyFieldType.DateTime,
                             'is_nullable': True},
    'make': {   'args': 40,
                'field_db_sqlalchemy_type': SqlalchemyFieldType.String,
                'is_nullable': False},
    'score': {   'field_db_sqlalchemy_type': SqlalchemyFieldType.SmallInteger,
                 'is_nullable': True},
    'slope': {   'args': (7, 6),
                 'field_db_sqlalchemy_type': SqlalchemyFieldType.Decimal,
                 'is_nullable': True,
                 'is_percent': True},
    'value_current': {   'field_db_sqlalchemy_type': SqlalchemyFieldType.Integer,
                         'is_dollar': True,
                         'is_nullable': True},
    'year': {   'args': 6,
                'field_db_sqlalchemy_type': SqlalchemyFieldType.String,
                'is_nullable': False}}
