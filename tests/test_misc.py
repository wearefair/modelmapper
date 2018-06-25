import pytest
from modelmapper.misc import escape_word


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
