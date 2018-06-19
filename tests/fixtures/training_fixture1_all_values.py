import pytest


@pytest.fixture
def all_fixture1_values():
    return {'available': {'', 'FALSE', 'TRUE'},
            'casualty': {'N', ''},
            'last_payment_date': {'Nan', '5/5/18', '5/7/18'},
            'make': {'BMW', 'Kia', 'Chrysler', 'Ford', 'Cadillac'},
            'model_year': {'2017', '2015', '2014'},
            'score': {'', '200', '233', '100', '1000'},
            'slope': {'-1.92%', '-2.48%', '-1.91%', '-2.14%', '-1.90%'},
            'value_current': {'$15,688 ', '$16,313 ', '$13,700 ', '$12,785 ', '$8,655 '}}
