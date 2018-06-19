import pytest


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
