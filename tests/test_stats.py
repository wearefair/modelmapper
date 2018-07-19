import pytest
from collections import Counter
from deepdiff import DeepDiff

from modelmapper.stats import StatsCollector, FieldStats, InconsistentData


@pytest.fixture(scope='function')
def default_collector():
    return StatsCollector()


@pytest.mark.parametrize("values, expected_stats", [
    # Test that boolean and int can overlap
    (['1', '3', '4', ''],
     FieldStats(counter=Counter(HasNull=1, HasInt=3, HasBoolean=1),
                max_int=4, len=4)
     ),
    # Test that boolean can be nullable
    (['y', 'y', 'y', 'n', '', 'y'],
     FieldStats(counter=Counter(HasNull=1, HasBoolean=5),
                len=6),
     ),
    # Test that boolean/int mix can be nullable
    (['1', '1', '0', 'null', '0', ''],
     FieldStats(counter=Counter(HasNull=2, HasBoolean=4, HasInt=4),
                max_int=1, len=6),
     ),
    # Test that boolean can be considered string
    (['1', '0', 'F', 'True', 'na'],
     FieldStats(counter=Counter(HasBoolean=4, HasNull=1, HasInt=2),
                max_int=1, len=5),
     ),
    # Test that boolean/null/string mix is possible
    (['1', '0', 'F', 'True', 'na', 'no!'],
     FieldStats(counter=Counter(HasBoolean=4, HasNull=1, HasString=1, HasInt=2),
                max_int=1, max_string_len=3, len=6),
     ),
    # Test that with mostly bools, we'll count more ints if one is included
    (['1', '1', '0', '0', '20'],
     FieldStats(counter=Counter(HasBoolean=4, HasInt=5),
                max_int=20, len=5),
     ),
    # Tests that dollar amounts and decimal amounts can overlap
    (['$1.92', '$33.6', '$0', 'null', '$13000.22'],
     FieldStats(counter=Counter(HasNull=1, HasDecimal=3, HasInt=1, HasDollar=4),
                max_pre_decimal=5, max_decimal_scale=2, len=5),
     ),
    # Tests string matcher
    (['apple', 'orange', 'what is going on here?', 'aha!'],
     FieldStats(counter=Counter(HasString=4), max_string_len=22, len=4),
     ),
    # Tests that strings/nulls mix is possible
    (['ca', 'wa', 'pe', '', 'be'],
     FieldStats(counter=Counter(HasString=4, HasNull=1),
                max_string_len=2, len=5),
     ),
    # Test datetime/nulls mix is possible
    (['8/8/18', '12/8/18', '12/22/18', ''],
     FieldStats(counter=Counter(HasDateTime=3, HasNull=1),
                datetime_formats={'%m/%d/%y'}, len=4),
     ),
    # Tests that string/datetime/null mix is possible
    (['random string', '8/8/18', '12/8/18', 'NONE', '12/22/18', ''],
     FieldStats(counter=Counter(HasString=1, HasDateTime=3, HasNull=2),
                datetime_formats={'%m/%d/%y'}, max_string_len=13, len=6),
     ),
    (['2018-05-05', '2018-05-05', 'Nan', '2018-05-07'],
     FieldStats(counter=Counter(HasDateTime=3, HasNull=1),
                datetime_formats={'%Y-%m-%d'}, len=4),
     ),
    (['20180505', '20180505', 'Nan', '20180507'],
     FieldStats(counter=Counter(HasDateTime=3, HasNull=1, HasInt=3),
                datetime_formats={'%Y%m%d'}, max_int=20180507, len=4),
     ),
    # Test percentage/int matcher
    (['1%', '2%', '0%', '0%', '20%'],
     FieldStats(counter=Counter(HasPercent=5, HasInt=5), max_int=20, len=5),
     ),
    # Test decimal/boolean/int matchers
    (['1.102933', '220.23', '0'],
     FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1),
                max_int=0, len=3, max_decimal_scale=6, max_pre_decimal=3),
     ),
    # Test decimal/percent/int matchers
    (['1.102933%', '220.23%', '0%'],
     FieldStats(counter=Counter(HasPercent=3, HasInt=1, HasDecimal=2),
                max_int=0, len=3, max_decimal_scale=6, max_pre_decimal=3),
     ),
    # Test decimal/int/boolean matchers
    (['1,001.10', '220.23', '0'],
     FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1),
                max_int=0, len=3, max_decimal_scale=2, max_pre_decimal=4),
     ),
    # Test dollar/int/boolean matchers
    (['$1,001.10', '$220.23', '0'],
     FieldStats(counter=Counter(HasInt=1, HasDecimal=2, HasBoolean=1, HasDollar=2),
                max_int=0, len=3, max_decimal_scale=2, max_pre_decimal=4),
     ),
    (['$0.00', '($12,000.00)', '$7,765.00'],
     FieldStats(counter=Counter({'HasDollar': 3, 'HasDecimal': 3}),
                max_pre_decimal=5, max_decimal_scale=2, len=3)
     ),
    (["$15,688 ", "$12,785 ", "$8,655 ", "$13,700 ", "$16,313 "],
     FieldStats(counter=Counter({'HasDollar': 5, 'HasInt': 5}),
                max_int=16313, len=5)
     ),
    (["15688 ", "12785 ", "8655 ", "13700 ", "16313 "],
     FieldStats(counter=Counter({'HasInt': 5}),
                max_int=16313, len=5)
     ),
    (['1', '1', '0', '0', '(20)'],
     FieldStats(counter=Counter(HasBoolean=4, HasInt=5),
                max_int=20, len=5),
     ),
])
def test_expected_stats_with_defaults(default_collector, values, expected_stats):
    """
    Tests that without any arguments, the stats collector uses the usual defaults
    """
    for item in values:
        default_collector.inspect_item('test-field', item)
    stats = default_collector.collect()
    assert DeepDiff(expected_stats, stats) == {}


@pytest.mark.parametrize("values, expected_error", [
    (['8/8/18', '12/8/18', '12/11/2018'],
     'field blah has inconsistent datetime data: 12/11/2018 had %m/%d/%Y but previous dates in this field had %m/%d/%y'
     ),
])
def test_inconsistent_datetime(default_collector, values, expected_error):
    """
    Tests that without any arguments, the stats collector uses the usual defaults
    """
    with pytest.raises(InconsistentData) as excinfo:
        for item in values:
            default_collector.inspect_item('blah', item)
        default_collector.collect()
    assert str(excinfo.value) == expected_error
