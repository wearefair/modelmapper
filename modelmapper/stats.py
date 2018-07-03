import decimal
import datetime
from collections import Counter
from typing import Any, NamedTuple

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


class TypeMatcher(object):
    """
    Matches a value to a type
    """
    def match(self, item, settings):
        return False

    def is_exclusive(self):
        """
        Return True if a match of this type should be the _only_ reported type for a field
        """
        return False


class TypeAccumulator(object):
    """
    Accumulates some value over every item it sees
    """
    def reset(self):
        """
        Resets the accumulator
        """
        pass

    def inspect(self, item, settings):
        """
        Inspects the item
        """
        return False

    def collect(self):
        """
        Sets the value of a field on the FieldStats object
        """
        pass


class InSettingsMatcher(TypeMatcher):
    """
    Abstract Matacher -- This should be subclasses not created directly.

    Matches a value by comparing it to values in
    the settings object.

    >>> settings.test = ['1', 'y', 'true']
    >>> InSettingsMatcher('test').match('test', settings)
    False
    >>> settings.test = ['1', 'y', 'true']
    >>> InSettingsMatcher('test').match('1', settings)
    True
    """
    def __init__(self, field):
        self.field = field

    def match(self, item, settings):
        if not hasattr(settings, self.field):
            # XXX: Raise exception
            raise ValueError('missing field')
            return False
        return item in getattr(settings, self.field)


class InMatcher(TypeMatcher):
    """
    Abstract Matacher -- This should be subclasses not created directly.

    Matches when a value contains a certain character

    >>> InMatcher('$').match('test', settings)
    False
    >>> InMatcher('$').match('$0.0', settings)
    True
    """
    def __init__(self, to_match):
        self.to_match = to_match

    def match(self, item, settings):
        return self.to_match in item


class NullMatcher(InSettingsMatcher):
    """
    Matches values to the designated null value strings in the settings file

    >>> settings.null_values = ['null', '', ' ']
    >>> NullMatcher().match('test', settings)
    False
    >>> settings.null_values = ['null', '', ' ']
    >>> NullMatcher().match('null', settings)
    True
    """
    value_type = HasNull

    def __init__(self):
        super().__init__('null_values')

    def is_exclusive(self):
        return True


class BooleanMatcher(InSettingsMatcher):
    """
    Matches values to the designated boolean value strings in the settings file

    >>> settings.booleans = ['1', 'y', 'true']
    >>> BooleanMatcher().match('test', settings)
    False
    >>> settings.booleans = ['1', 'y', 'true']
    >>> BooleanMatcher().match('true', settings)
    True
    """
    value_type = HasBoolean

    def __init__(self):
        super().__init__('booleans')


class DollarMatcher(InMatcher):
    """
    Matches values that contain the $ character

    >>> DollarMatcher().match('test', settings)
    False
    >>> DollarMatcher().match('$1.50', settings)
    True
    """
    value_type = HasDollar

    def __init__(self):
        super().__init__('$')


class PercentMatcher(InMatcher):
    """
    Matches values that contain the % character

    >>> PercentMatcher().match('test', settings)
    False
    >>> PercentMatcher().match('10%', settings)
    True
    """
    value_type = HasPercent

    def __init__(self):
        super().__init__('%')


class PositiveIntMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as integers. This matcher
    also acts as an accumulator and records the max integer value that
    it saw.

    >>> PositiveIntMatcher().match('test', settings)
    False
    >>> PositiveIntMatcher().match('10', settings)
    True
    >>> PositiveIntMatcher().match('$10', settings)
    True
    """
    value_type = HasInt

    def __init__(self, max_int=0):
        self.original_max = max_int
        self.max_int = max_int

    def reset(self):
        self.max_int = self.original_max

    def match(self, item, settings):
        return self._get_positive_int(item) is not False

    def inspect(self, item, settings):
        self.max_int = max(self._get_positive_int(item), self.max_int)

    def collect(self):
        return {'max_int': self.max_int }

    def _normalize(self, item):
        return item.replace('-', '').replace(',', '').replace('$', '').replace('%', '')

    def is_exclusive(self):
        return True

    def _get_positive_int(self, item):
        try:
            return int(self._normalize(item))
        except ValueError:
            return False


class PositiveDecimalMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as decimals. This matcher
    also acts as an accumulator and records the max scale and precision that
    it saw.

    >>> PositiveDecimalMatcher().match('test', settings)
    False
    >>> PositiveDecimalMatcher().match('10.0', settings)
    True
    >>> PositiveIntMatcher().match('$10.01', settings)
    True
    """
    value_type = HasDecimal

    def __init__(self, max_scale=0, max_precision=0):
        self.original_scale = max_scale
        self.max_scale = max_scale
        self.original_precision = max_precision
        self.max_precision = max_precision

    def reset(self):
        self.max_scale = self.original_scale
        self.max_precision = self.original_precision

    def match(self, item, settings):
        return self._get_positive_decimal(item) is not False

    def inspect(self, item, settings):
        value = self._get_positive_decimal(item)
        precision, scale = self._get_decimal_places(value)
        self.max_precision = max(precision, self.max_precision)
        self.max_scale = max(scale, self.max_scale)

    def collect(self):
        return {
            'max_pre_decimal': self.max_precision,
            'max_decimal_scale': self.max_scale
        }

    def is_exclusive(self):
        return True

    def _get_decimal_places(self, value):
        item = str(value)
        if '.' in item:
            i, v = list(map(len, item.split('.')))
            return i, v
        else:
            return 0, 0

    def _normalize(self, item):
        return item.replace('-', '').replace(',', '').replace('$', '').replace('%', '')

    def _get_positive_decimal(self, item):
        if '.' not in item:
            return False
        try:
            return decimal.Decimal(self._normalize(item))
        except decimal.InvalidOperation:
            return False


class DateTimeMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches values that could be represented as datetimes. This matcher
    also acts as an accumulator and records the possible datetime formats
    that are used to represent this field.

    >>> settings.datetime_formats = set(['%mm/%dd/%yyyy'])
    >>> DateTimeMatcher().match('test', settings)
    False
    >>> DateTimeMatcher().match('10/15/1992', settings)
    True
    """
    value_type = HasDateTime

    def __init__(self):
        self.reset()

    def reset(self):
        self.matched_formats = set()
        self.has_matched_before = False

    def match(self, item, settings):
        if set(item) <= settings.datetime_allowed_characters:
            matches = self._get_matching_formats(item, settings.datetime_formats)
            if not matches and not self.has_matched_before:
                return False
            elif not matches:
                # Inconsistent data exception instead?
                raise UserInferenceRequired(
                    self.value_type,
                    "Item contained datetime characters, but didn't match any expected format."
                )
            return True
        return False

    def inspect(self, item, settings):
        self.matched_formats |= self._get_matching_formats(item, settings.datetime_formats)

    def collect(self):
        return {'datetime_formats': self.matched_formats} if self.matched_formats else {}

    def is_exclusive(self):
        return True

    def _get_matching_formats(self, item, datetime_formats):
        matched_formats = set()
        for _format in datetime_formats:
            try:
                datetime.datetime.strptime(item, _format)
            except ValueError:
                continue
            else:
                matched_formats.add(_format)
        return matched_formats


class StringMatcher(TypeMatcher, TypeAccumulator):
    """
    Matches any string value, this is the default matcher if nothing else matches.

    This also acts as an accumulator and records the max string length that it found.

    >>> StringMatcher().match('test', settings)
    True
    """
    value_type = HasString

    def __init__(self):
        self.reset()

    def reset(self):
        self.max_length = 0

    def inspect(self, item, settings):
        self.max_length = max(len(item), self.max_length)

    def collect(self):
        return {'max_string_len': self.max_length}

    def match(self, item, settings):
        return True


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
        self.matchers = matchers or [
            NullMatcher(),
            BooleanMatcher(),
            DollarMatcher(),
            PercentMatcher(),
            PositiveIntMatcher(),
            PositiveDecimalMatcher(),
            DateTimeMatcher(),
            StringMatcher(),
        ]

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

    def inspect_item(self, item, settings):
        """
        Inspects a given field value against all the matchers and accumulators. This will raise an
        exception if no type was able to be determined.

        :param item:
            String value from the CSV
        :param settings:
            Settings object
        """
        results = []
        for matcher in self.matchers:
            if not matcher.match(item, settings):
                continue
            if isinstance(matcher, TypeAccumulator):
                matcher.inspect(item, settings)
            results.append(matcher.value_type)
            if matcher.is_exclusive():
                break

        if not results:
            raise NoMatchFound(f"Failed to find a matching value type for {item}.")

        self.results.extend(results)
        self.inspected += 1

    def _match(self, matcher, item, settings):
        return matcher.value_type
