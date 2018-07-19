import pytest

from modelmapper.normalization import normalize_numberic_values


class TestNormalization:

    @pytest.mark.parametrize('value, absolute, expected', [
        ('-10', True, '10'),
        ('-10', False, '-10'),
        ('10-1-1', True, '10-1-1'),  # mins (-) only at the beginning of the string should be removed.
        ('10-1-1', False, '10-1-1'),
    ])
    def test_normalize_numberic_values(self, value, absolute, expected):
        result = normalize_numberic_values(value, absolute=absolute)
        assert expected == result
