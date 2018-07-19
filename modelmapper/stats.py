import decimal
import datetime
from collections import Counter
from typing import Any, NamedTuple

from modelmapper.normalization import normalize_numberic_values
from modelmapper.types import (
    HasBoolean,
    HasDateTime,
    HasDecimal,
    HasDollar,
    HasInt,
    HasNull,
    HasPercent,
    HasString,
)


class FieldStats(NamedTuple):
    counter: Any
    max_int: 'FieldStats' = 0
    max_pre_decimal: 'FieldStats' = 0
    max_decimal_scale: 'FieldStats' = 0
    max_string_len: 'FieldStats' = 0
    datetime_formats: 'FieldStats' = None
    len: 'FieldStats' = 0


class UserInferenceRequired(ValueError):
    """Thrown when we need the prompt the user for more information"""
    def __init__(self, value_type, message):
        super().__init__(message)
        self.value_type = value_type


class NoMatchFound(ValueError):
    """Thrown when we can't match a datatype"""
    pass


class InconsistentData(ValueError):
    """Thrown when we detect inconsistent data within a field"""
    pass


class TypeMatcher:
    """
    Matches a value to a type
    """
    def match(self, item):
        return self._match(self.__normalize(item))

    def is_exclusive(self, item=None):
        """
        Return True if a match of this type should be the _only_ reported type for a field
        """
        return False

    def _match(self, item):
        """
        Subclasses should override this method
        """
        return False

    def __normalize(self, item):
        """
        Default normalization, lowercases and strips the field.
        """
        return item.lower().strip()


class TypeAccumulator:
    """
    Accumulates some value over every item it sees
    """
    def reset(self):
        """
        Resets the accumulator
        """
        pass

    def inspect(self, field_name, item):
        """
        Inspects the item
        """
        return False

    def collect(self):
        """
        Sets the value of a field on the FieldStats object
        """
        pass


class InMatcher(TypeMatcher):
    """
    Abstract Matacher -- This should be subclassed and not created directly.

    Matches a value by comparing it to values in
    the settings object.

    >>> InMatcher(['1', 'y', 'true']).match('test')
    False
    >>> InMatcher(['1', 'y', 'true']).match('1')
    True
    """
    def __init__(self, candidate_values):
        self.candidate_values = candidate_values

    def _match(self, item):
        return item in self.candidate_values


class ContainsMatcher(TypeMatcher):
    """
    Abstract Matacher -- This should be subclassed and not created directly.

    Matches when a value contains a certain character

    >>> ContainsMatcher('$').match('test')
    False
    >>> ContainsMatcher('$').match('$0.0')
    True
    """
    def __init__(self, contains):
        self.contains = contains

    def _match(self, item):
        return self.contains in item


class NullMatcher(InMatcher):
    """
    Matches values to the designated null value strings in the settings file

    >>> NullMatcher().match('test')
    False
    >>> NullMatcher().match('null')
    True
    """
    value_type = HasNull

    def __init__(self, null_values=None):
        """
        Constructs a new :class:`.NullMatcher`

        :param null_values:
            List of values that should be considered nulls
        """
        super().__init__(candidate_values=null_values)

    def is_exclusive(self, item=None):
        return True


class BooleanMatcher(InMatcher):
    """
    Matches values to the designated boolean value strings provided in the constructor

    >>> BooleanMatcher(['1', 'y', 'true']).match('test')
    False
    >>> BooleanMatcher(['1', 'y', 'true']).match('true')
    True
    """
    value_type = HasBoolean

    def __init__(self, boolean_values=None):
        """
        Constructs a new :class:`.BooleanMatcher`. This class is not an exclusive matcher
        as 0 and 1 may denote true/false, but also an integer.

        :param boolean_values:
            List of values that should be considered booleans
        """
        super().__init__(candidate_values=boolean_values)


class DollarMatcher(ContainsMatcher):
    """
    Matches values that contain the $ character

    >>> DollarMatcher().match('test')
    False
    >>> DollarMatcher().match('$1.50')
    True
    """
    value_type = HasDollar

    def __init__(self):
        super().__init__(contains='$')


class PercentMatcher(ContainsMatcher):
    """
    Matches values that contain the % character

    >>> PercentMatcher().match('test')
    False
    >>> PercentMatcher().match('10%')
    True
    """
    value_type = HasPercent

    def __init__(self):
        super().__init__(contains='%')


class PositiveIntMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as integers. This matcher
    also acts as an accumulator and records the max integer value that
    it saw.

    >>> PositiveIntMatcher().match('test')
    False
    >>> PositiveIntMatcher().match('10')
    True
    >>> PositiveIntMatcher().match('$10')
    True
    """
    value_type = HasInt

    def __init__(self, max_int=0):
        self.original_max = max_int
        self.reset()

    def reset(self):
        self.max_int = self.original_max

    def _match(self, item):
        return self._get_positive_int(item) is not False

    def inspect(self, field_name, item):
        self.max_int = max(self._get_positive_int(item), self.max_int)

    def collect(self):
        return {'max_int': self.max_int }

    def is_exclusive(self, item=None):
        """
        A number with less than 6 digits is definitely a number and does not need to be
        checked for matching with other data types.
        A number equal or above 6 digits could be datetime. For example 20201010 could be a date or integer.
        """
        return True if item is None else len(item) < 6

    def _get_positive_int(self, item):
        try:
            return int(normalize_numberic_values(item, absolute=True))
        except ValueError:
            return False


class PositiveDecimalMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as decimals. This matcher
    also acts as an accumulator and records the max scale and precision that
    it saw.

    >>> PositiveDecimalMatcher().match('test')
    False
    >>> PositiveDecimalMatcher().match('10.0')
    True
    >>> PositiveIntMatcher().match('$10.01')
    True
    """
    value_type = HasDecimal

    def __init__(self, max_scale=0, max_precision=0):
        self.original_scale = max_scale
        self.original_precision = max_precision
        self.reset()

    def reset(self):
        self.max_scale = self.original_scale
        self.max_precision = self.original_precision

    def _match(self, item):
        return self._get_positive_decimal(item) is not False

    def inspect(self, field_name, item):
        value = self._get_positive_decimal(item)
        precision, scale = self._get_decimal_places(value)
        self.max_precision = max(precision, self.max_precision)
        self.max_scale = max(scale, self.max_scale)

    def collect(self):
        return {
            'max_pre_decimal': self.max_precision,
            'max_decimal_scale': self.max_scale
        }

    def is_exclusive(self, item=None):
        return True

    def _get_decimal_places(self, value):
        item = str(value)
        if '.' in item:
            i, v = list(map(len, item.split('.')))
            return i, v
        else:
            return 0, 0

    def _get_positive_decimal(self, item):
        if '.' not in item:
            return False
        try:
            return decimal.Decimal(normalize_numberic_values(item, absolute=True))
        except decimal.InvalidOperation:
            return False


class DateTimeMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as datetimes. This matcher
    also acts as an accumulator and records the possible datetime formats
    that are used to represent this field.

    >>> DateTimeMatcher(datetime_formats=['%m/%d/%Y']).match('test', settings)
    False
    >>> DateTimeMatcher(datetime_formats=['%m/%d/%Y']).match('10/15/1992', settings)
    True
    """
    value_type = HasDateTime

    def __init__(self, datetime_formats=None):
        self.datetime_formats = datetime_formats
        self.reset()

    def reset(self):
        self.candidate_formats = self.datetime_formats.copy()
        self.has_matched_before = False

    def _match(self, item):
        for _format in self.datetime_formats:
            try:
                datetime.datetime.strptime(item, _format)
                self.has_matched_before = True
                return True
            except ValueError:
                continue

        if self.has_matched_before:
            raise UserInferenceRequired(
                self.value_type,
                "Item contained invalid datetime, prompting the user for a valid format."
            )
        return False

    def inspect(self, field_name, item):
        """
        Narrows down candidate datetime formats for a column. If there isn't any format that matches all
        the data, this will raise an InconsistentData exception.
        """
        matches, failures = self._get_format_data(item)
        # Subtract out any failing formats from our candidates
        new_candidates = self.candidate_formats - failures
        # If no possible matches are left, panic!
        if not new_candidates:
            matching = ", ".join(matches)
            old_formats = ", ".join(self.candidate_formats)
            raise InconsistentData(f"field {field_name} has inconsistent datetime data: {item} "
                                   f"had {matching} but previous dates in this field had {old_formats}")
        self.candidate_formats = new_candidates

    def collect(self):
        return {'datetime_formats': self.candidate_formats} if self.has_matched_before else {}

    def is_exclusive(self, item=None):
        return True

    def _get_format_data(self, item):
        matching_formats = set()
        failed_formats = set()
        for _format in self.datetime_formats:
            try:
                datetime.datetime.strptime(item, _format)
                matching_formats.add(_format)
            except ValueError:
                failed_formats.add(_format)
        return matching_formats, failed_formats


class StringMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches any string value, this is the default matcher if nothing else matches.

    This also acts as an accumulator and records the max string length that it found.

    >>> StringMatcher().match('test')
    True
    """
    value_type = HasString

    def __init__(self):
        self.reset()

    def reset(self):
        self.max_length = 0

    def inspect(self, field_name, item):
        self.max_length = max(len(item), self.max_length)

    def collect(self):
        return {'max_string_len': self.max_length}

    def _match(self, item):
        return True


def matchers_from_settings(settings=None):
    """
    Creates a default set of matchers from the settings object
    """
    datetime_formats = settings.datetime_formats if settings else {"%m/%d/%y", "%m/%d/%Y", "%Y%m%d", "%Y-%m-%d"}
    null_values = settings.null_values if settings else ["\\n", "", "na", "unk", "null", "none", "nan", "1/0/00", "1/0/1900", "-"]  # NOQA
    boolean_values = settings.booleans if settings else ["true", "t", "yes", "y", "1", "false", "f", "no", "n", "0"]
    return [
        NullMatcher(null_values=null_values),
        BooleanMatcher(boolean_values=boolean_values),
        DollarMatcher(),
        PercentMatcher(),
        PositiveIntMatcher(),
        PositiveDecimalMatcher(),
        DateTimeMatcher(datetime_formats=datetime_formats),
        StringMatcher(),
    ]


class StatsCollector(object):
    """
    This class builds a FieldStats object by inspecting each value of the field in the CSV.
    Callers may cached this class, but :meth:`.StatsCollector.reset` should be called between
    field inspections.

    Users may extend or override this by passing in their own matchers and stats class.
    """
    def __init__(self, matchers=None, stats_class=None):
        self.results = []
        self.inspected = 0
        self.stats_class = stats_class or FieldStats
        self.matchers = matchers or matchers_from_settings()

    def reset(self):
        """
        Resets the matchers and the collector to their default state.
        """
        self.inspected = 0
        self.results = []
        for matcher in self.matchers:
            if isinstance(matcher, TypeAccumulator):
                matcher.reset()

    def collect(self):
        """
        Constructs an instance of the stats_class provided in the constructor. By default this is
        the FieldStats object, but callers may override this behavior if required.

        :return:
            :class:`.FieldStats` object or user provided class instance.
        """
        data = {}
        for matcher in self.matchers:
            if isinstance(matcher, TypeAccumulator):
                data.update(matcher.collect())
        return self.stats_class(counter=Counter(self.results),
                                len=self.inspected, **data)

    def inspect_item(self, field_name, item):
        """
        Inspects a given field value against all the matchers and accumulators. This will raise an
        exception if no type was able to be determined.

        :param item:
            String value from the CSV
        :param settings:
            Settings object
        """
        item = item.strip()
        results = []
        already_matched = False
        for matcher in self.matchers:
            if already_matched and isinstance(matcher, StringMatcher):
                continue
            if matcher.match(item):
                already_matched = True
            else:
                continue
            if isinstance(matcher, TypeAccumulator):
                matcher.inspect(field_name, item)
            results.append(matcher.value_type)
            if matcher.is_exclusive(item):
                break

        if not results:
            raise NoMatchFound(f"Failed to find a matching value type for {item}.")

        self.results.extend(results)
        self.inspected += 1
        return results
