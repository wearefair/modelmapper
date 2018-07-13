import pytest
import datetime
from decimal import Decimal


@pytest.fixture
def cleaned_csv_for_import_fixture():
    return [{
        'casualty': False,
        'value_current': 1568800,
        'last_payment_date': datetime.datetime(2018, 5, 5, 0, 0),
        'year': '2015',
        'score': 233,
        'slope': Decimal('-0.0214'),
        'available': False,
        'make': 'Cadillac'
    }, {
        'casualty': False,
        'value_current': 1278500,
        'last_payment_date': datetime.datetime(2018, 5, 5, 0, 0),
        'year': '2014',
        'score': 1000,
        'slope': Decimal('-0.0191'),
        'available': True,
        'make': 'Chrysler'
    }, {
        'casualty': False,
        'value_current': 865500,
        'last_payment_date': datetime.datetime(2018, 5, 7, 0, 0),
        'year': '2017',
        'score': None,
        'slope': Decimal('-0.0248'),
        'available': None,
        'make': 'Kia'
    }, {
        'casualty': None,
        'value_current': 1370000,
        'last_payment_date': None,
        'year': '2017',
        'score': 100,
        'slope': Decimal('-0.019'),
        'available': None,
        'make': 'Ford'
    }, {
        'casualty': False,
        'value_current': 1631300,
        'last_payment_date': datetime.datetime(2018, 5, 7, 0, 0),
        'year': '2014',
        'score': 200,
        'slope': Decimal('-0.0192'),
        'available': None,
        'make': 'BMW'
    }]
