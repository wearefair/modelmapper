import pytest
from modelmapper.misc import escape_word, get_combined_dict


class TestMisc:

    @pytest.mark.parametrize("word, expected", [
        ('Are you Really reading this?',
         'are_you_really_reading_this'),
        ('What The #^*@*!',
         'what_the')
    ])
    def test_escape_word(self, word, expected):
        result = escape_word(word)
        assert expected == result

    @pytest.mark.parametrize("comparison_func, items, expected", [
        # (lambda x: x['a'],
        #  [{'a': 10, 'b': 20}, {'a': 1, 'b': 25, 'c': 11}],
        #  {'a': 1, 'b': 25, 'c': 11}),
        (lambda x: x['a'],
         [{'a': 10, 'b': [11]}, {'a': 1, 'b': [25], 'c': 11}],
         {'a': 1, 'b': [11, 25], 'c': 11}),
    ])
    def test_get_combined_dict(self, comparison_func, items, expected):
        result = get_combined_dict(comparison_func, *items)
        assert expected == result
