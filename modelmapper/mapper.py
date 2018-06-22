import csv
import os
import sys
import pytoml
import decimal
import datetime
from copy import deepcopy
from collections import defaultdict, Counter
from decimal import Decimal
# We are using both the new style and old style of named tuple
from typing import Any, NamedTuple
from collections import namedtuple

from modelmapper.ui import get_user_choice, get_user_input


INVALID_DATETIME_USER_OPTIONS = {
    'y': {'help': 'to define the date format', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit}
}


class InconsistentData(ValueError):
    pass


class FieldStats(NamedTuple):
    counter: Any
    max_int: 'FieldStats' = 0
    max_decimal_precision: 'FieldStats' = 0
    max_decimal_scale: 'FieldStats' = 0
    max_string_len: 'FieldStats' = 0
    datetime_formats: 'FieldStats' = None


class FileNotFound(ValueError):
    pass


HasNull = 'HasNull'
HasDecimal = 'HasDecimal'
HasInt = 'HasInt'
HasDollar = 'HasDollar'
HasPercent = 'HasPercent'
HasString = 'HasString'
HasDateTime = 'HasDateTime'
HasBoolean = 'HasBoolean'


def get_positive_int(item):
    item = item.replace('-', '')
    try:
        result = int(item)
    except ValueError:
        result = False
    return result


def get_positive_decimal(item):
    item = item.replace('-', '')
    try:
        result = Decimal(item)
    except decimal.InvalidOperation:
        result = False
    return result


def _check_file_exists(path):
    if not os.path.exists(path):
        raise FileNotFound(f'{path} does not exist')


def load_toml(path):
    _check_file_exists(path)
    with open(path, 'r') as the_file:
        contents = the_file.read()
    return pytoml.loads(contents)


def write_toml(path, contents):
    with open(path, 'w') as the_file:
        the_file.write(pytoml.dumps(contents))


def _read_csv_gen(path, **kwargs):
    _check_file_exists(path)
    encoding = kwargs.pop('encoding', 'utf-8-sig')
    with open(path, 'r', encoding=encoding) as csvfile:
        for i in csv.reader(csvfile, **kwargs):
            yield i


def _is_valid_dateformat(user_input, item):
    try:
        datetime.datetime.strptime(item, user_input)
    except ValueError:
        result = False
    else:
        result = True
    return result


class Mapper:

    def __init__(self, setup_path):
        self.setup_path = setup_path
        clean_later = ['field_name_full_conversion']
        convert_to_set = ['null_values', 'boolean_true', 'boolean_false', 'datetime_formats']
        self._original_settings = load_toml(setup_path)['settings']
        self.settings = deepcopy(self._original_settings)
        for item in clean_later:
            self.settings[item] = [[self._clean_it(i), self._clean_it(j)] for i, j in self.settings[item]]
        for item in convert_to_set:
            self.settings[item] = set(self.settings.get(item, []))
        self.settings['booleans'] = self.settings['boolean_true'] | self.settings['boolean_false']
        self.settings['datetime_allowed_characters'] = set(self.settings['datetime_allowed_characters'])
        Settings = namedtuple('Settings', ' '.join(self.settings.keys()))
        self.settings = Settings(**self.settings)

    def _clean_it(self, name):
        conv = self.settings['field_name_part_conversion'] if isinstance(self.settings, dict) else self.settings.field_name_part_conversion
        item = name.lower().strip()
        for source, to_replace in conv:
            item = item.replace(source, to_replace)
        return item.strip('_')

    def _get_clean_field_name(self, name):
        item = self._clean_it(name)
        for source, to_replace in self.settings.field_name_full_conversion:
            if item == source:
                item = to_replace
                break
        return item

    def _get_all_clean_field_names_mapping(self, names):
        name_mapping = {}
        for name in names:
            name_mapping[name] = self._get_clean_field_name(name)

        return name_mapping

    def _verify_no_duplicate_clean_names(self, names_mapping):
        clean_names_mapping = {}
        for name, clean_name in names_mapping.items():
            if clean_name in clean_names_mapping:
                raise ValueError(f"'{name}' field has a collision with '{clean_names_mapping[clean_name]}'")
            else:
                clean_names_mapping[clean_name] = name

    def _get_all_values_per_clean_name(self, path):
        result = defaultdict(list)
        reader = _read_csv_gen(path)
        names = next(reader)
        name_mapping = self._get_all_clean_field_names_mapping(names)
        self._verify_no_duplicate_clean_names(name_mapping)
        clean_names = list(name_mapping.values())
        # transposing csv and turning into dictionary
        for line in reader:
            for i, v in enumerate(line):
                result[clean_names[i]].append(v)
        return result

    def _get_decimal_places(self, item):
        if '.' in item:
            i, v = list(map(len, item.split('.')))
            return i + v + 2 * self.settings.add_digits_to_decimal_field, v + self.settings.add_digits_to_decimal_field
        else:
            return 0, 0

    def _get_datetime_formats(self, field_name, item, datetime_formats, failed_datetime_formats):
        current_successful_formats = set()
        current_failed_formats = set()
        for _format in datetime_formats:
            try:
                datetime.datetime.strptime(item, _format)
            except ValueError:
                failed_datetime_formats.add(_format)
            else:
                current_successful_formats.add(_format)
        if not current_successful_formats:
            for _format in failed_datetime_formats:
                try:
                    datetime.datetime.strptime(item, _format)
                except ValueError:
                    pass
                else:
                    raise InconsistentData(f"field {field_name} has inconsistent datetime data: {item} had {_format} but previous dates in this field had {', '.join(datetime_formats)}")
        failed_datetime_formats |= current_failed_formats
        return current_successful_formats, failed_datetime_formats

    def _get_stats(self, items, field_name):
        max_int = 0
        max_decimal_precision = 0
        max_decimal_scale = 0
        max_string_len = 0
        datetime_formats = self.settings.datetime_formats.copy()
        failed_datetime_formats = set()
        datetime_detected_in_this_field = False
        result = []
        for item in items:
            item = item.lower().strip()
            if item in self.settings.null_values:
                result.append(HasNull)
                continue
            if item in self.settings.booleans:
                result.append(HasBoolean)
                if item not in {'0', '1'}:
                    continue
            if '$' in item:
                item = item.replace('$', '')
                result.append(HasDollar)
            if '%' in item:
                item = item.replace('%', '')
                result.append(HasPercent)
            positive_int = get_positive_int(item)
            if positive_int is not False:
                result.append(HasInt)
                max_int = max(positive_int, max_int)
                continue
            positive_decimal = get_positive_decimal(item)
            if positive_decimal is not False:
                result.append(HasDecimal)
                decimal_precision, decimal_scale = self._get_decimal_places(item)
                max_decimal_precision = max(max_decimal_precision, decimal_precision)
                max_decimal_scale = max(max_decimal_scale, decimal_scale)
                continue
            if set(item) <= self.settings.datetime_allowed_characters:
                datetime_formats, failed_datetime_formats = self._get_datetime_formats(field_name, item, datetime_formats, failed_datetime_formats)
                if datetime_formats:
                    result.append(HasDateTime)
                    datetime_detected_in_this_field = True
                    continue
                elif datetime_detected_in_this_field:
                    msg = f'field {field_name} has inconsistent datetime data: {item}.'
                    get_user_choice(msg, choices=INVALID_DATETIME_USER_OPTIONS)
                    msg = f'Please enter the datetime format for {item}'
                    new_format = get_user_input(msg, validate_func=_is_valid_dateformat, item=item)
                    datetime_formats.add(new_format)
                    result.append(HasDateTime)
                    if new_format in self.settings.datetime_formats:
                        raise InconsistentData(f'field {field_name} has inconsistent datetime data: {item}. {new_format} was already in your settings.')
                    else:
                        sys.stdout.write(f'Adding {new_format} to your settings.')
                        self.settings.datetime_formats.add(new_format)
                        self._original_settings['datetime_formats'].append(new_format)
                        write_toml(self.setup_path, self._original_settings)
                        continue
            result.append(HasString)
            if max_string_len < 255:
                max_string_len = max(max_string_len, len(item) + self.settings.add_to_string_legth)
                max_string_len = min(max_string_len, 255)

        return FieldStats(counter=Counter(result), max_int=max_int,
                          max_decimal_precision=max_decimal_precision,
                          max_decimal_scale=max_decimal_scale,
                          max_string_len=max_string_len,
                          datetime_formats=datetime_formats if datetime_detected_in_this_field else None)
