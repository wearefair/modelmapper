from unittest import mock
import pytest

from modelmapper.ui import get_user_input, TooManyFailures


def _is_divisible(user_input, item):
    try:
        item / int(user_input)
    except Exception:
        result = False
    else:
        result = True
    return result


class TestUI:

    @pytest.mark.parametrize("user_input, expected", [
        (['11'], '11'),
        (['b', '10'], '10'),
        # (['0'], False)
    ])
    @mock.patch('modelmapper.ui._get_input')
    def test_user_input(self, mock_get_input, user_input, expected):
        mock_get_input.side_effect = user_input
        result = get_user_input('enter to find out', validate_func=_is_divisible, item=10)
        assert expected == result

    @mock.patch('modelmapper.ui._get_input')
    def test_user_input_wrong(self, mock_get_input):
        mock_get_input.side_effect = [i for i in 'this is too long']
        with pytest.raises(TooManyFailures):
            get_user_input('enter to find out', validate_func=_is_divisible, item=10)
