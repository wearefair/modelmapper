import enum
import csv
import os
import sys
import pytoml
import decimal
import datetime
import logging
from copy import deepcopy
from collections import defaultdict, Counter
from decimal import Decimal
# We are using both the new style and old style of named tuple
from typing import Any, NamedTuple
from collections import namedtuple

from modelmapper.ui import get_user_choice, get_user_input

logger = logging.getLogger(__name__)


INVALID_DATETIME_USER_OPTIONS = {
    'y': {'help': 'to define the date format', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit}
}


UNABLE_TO_INFER_TYPE_OPTIONS = {
    'y': {'help': 'to continue', 'func': lambda x: True},
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
    len: 'FieldStats' = 0


class FieldResult(NamedTuple):
    db_field_sqlalchemy_type: 'FieldResult' = None
    db_field_str: 'FieldResult' = None
    is_nullable: 'FieldResult' = None
    is_percent: 'FieldResult' = None
    is_dollar: 'FieldResult' = None
    datetime_formats: 'FieldResult' = None


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


class SqlalchemyFieldType(enum.Enum):
    String = 'String({})'
    SmallInteger = 'SmallInteger'
    Integer = 'Integer'
    Decimal = 'DECIMAL({}, {})'
    DateTime = 'DateTime'
    Boolean = 'Boolean'


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


# def _is_valid_db_field(user_input):
#     for i in {i.name.lower() for i in SqlalchemyFieldType}:
#         if i.startswith(user_input.lower()):
#             return i
#     return False


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
        _max_int = ((int(i), v) for i, v in self.settings['max_int'].items())
        self.settings['max_int'] = dict(sorted(_max_int, key=lambda x: x[0]))
        Settings = namedtuple('Settings', ' '.join(self.settings.keys()))
        self.settings = Settings(**self.settings)
        self.questionable_fields = defaultdict(set)

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
            # do not parse empty lines
            if filter(lambda x: bool(x.strip()), line):
                for i, v in enumerate(line):
                    result[clean_names[i]].append(v)
        return result

    def _get_decimal_places(self, item):
        if '.' in item:
            i, v = list(map(len, item.split('.')))
            return i + v, v
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
            if item.lower() in self.settings.null_values:
                result.append(HasNull)
                continue
            if item.lower() in self.settings.booleans:
                result.append(HasBoolean)
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
                datetime_formats, failed_datetime_formats = self._get_datetime_formats(
                    field_name, item, datetime_formats, failed_datetime_formats)
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
            max_string_len = max(max_string_len, len(item))

        return FieldStats(counter=Counter(result), max_int=max_int,
                          max_decimal_precision=max_decimal_precision,
                          max_decimal_scale=max_decimal_scale,
                          max_string_len=max_string_len,
                          datetime_formats=datetime_formats if datetime_detected_in_this_field else None,
                          len=len(items))

    def _get_integer_field(self, max_int):
        previous_key = 0
        for key, field_db_type in self.settings.max_int.items():
            if key < previous_key:
                raise ValueError('max_int keys are not properly sorted.')
            previous_key = key
            if max_int < key:
                return getattr(SqlalchemyFieldType, field_db_type)
        raise ValueError(f'{max_int} is bigger than the largest integer the database takes: {key}')

    def _get_field_type_from_stats(self, stats, field_name):
        counter = stats.counter.copy()

# FieldResult(NamedTuple):
#     db_field_sqlalchemy_type: 'FieldResult' = None
#     db_field_str: 'FieldResult' = None
#     is_nullable: 'FieldResult' = None
#     is_percent: 'FieldResult' = None
#     is_dollar: 'FieldResult' = None
    # datetime_formats: 'FieldResult' = None

        _type = _type_str = None
        null_count = counter.pop(HasNull, 0)
        non_string_nullable = self.settings.non_string_fields_are_all_nullable or null_count
        max_bool_word_size = max(map(len, self.settings.booleans))

        str_length = min(stats.max_string_len + self.settings.add_to_string_legth, 255)
        field_result_string = FieldResult(
            db_field_sqlalchemy_type=SqlalchemyFieldType.String,
            db_field_str=SqlalchemyFieldType.String.value.format(str_length),
            is_nullable=bool(null_count) and self.settings.string_fields_can_be_nullable)

        field_result_boolean = FieldResult(
            db_field_sqlalchemy_type=SqlalchemyFieldType.Boolean,
            is_nullable=non_string_nullable)

        field_result_datetime = FieldResult(
            db_field_sqlalchemy_type=SqlalchemyFieldType.DateTime,
            is_nullable=non_string_nullable,
            datetime_formats=stats.datetime_formats)

        if stats.max_string_len > max_bool_word_size:
            return field_result_string

        most_common = dict(counter.most_common(3))
        most_common_keys = set(most_common.keys())
        most_common_count = counter.most_common(1)[0][1]

        for _has, _field_result in (('HasBoolean', field_result_boolean), ('HasDateTime', field_result_datetime), ('HasString', field_result_string)):
            if counter[_has] == most_common_count:
                if counter[_has] + counter['HasNull'] != stats.len:
                    _field_type = _field_result.db_field_sqlalchemy_type.value
                    self.questionable_fields[field_name].add(f'Field is probably {_field_type}. There are values that are not {_field_type} and not Null though.')
                return _field_result

        is_percent = is_dollar = None
        if HasDecimal in counter:
            _type = SqlalchemyFieldType.Decimal
            max_decimal_precision = stats.max_decimal_precision
            max_decimal_scale = stats.max_decimal_scale
            if counter['HasInt']:
                max_int_precision = len(str(stats.max_int)) + max_decimal_scale
                max_decimal_precision = max(max_decimal_precision, max_int_precision)
            if counter['HasDollar'] and self.settings.dollar_to_cent:
                max_int = int('9' * max_decimal_precision)
                _type = self._get_integer_field(max_int)
                is_dollar = True
        elif HasInt in most_common_keys:
            if counter['HasPercent'] and self.settings.percent_to_decimal:
                max_decimal_scale = 2
                max_decimal_precision = len(str(stats.max_int))
                _type = SqlalchemyFieldType.Decimal
                is_percent = True
            else:
                _type = self._get_integer_field(stats.max_int)
        if _type is SqlalchemyFieldType.Decimal:
            max_decimal_precision += 2 * self.settings.add_digits_to_decimal_field
            max_decimal_scale += self.settings.add_digits_to_decimal_field
            _type_str = SqlalchemyFieldType.Decimal.value.format(max_decimal_precision, max_decimal_scale)

        if _type:
            return FieldResult(
                db_field_sqlalchemy_type=_type,
                db_field_str=_type_str,
                is_nullable=non_string_nullable,
                is_percent=is_percent,
                is_dollar=is_dollar)

        logger.error(f'Unable to understand the field type from the data in {field_name}')
        logger.error('Please train the system for that field with a different dataset or manually define an override in the output later.')

        return FieldResult(None)

        # info_['field_csv_name'] = field_csv_name
        # field_db_name = info_.pop('field_db_name')
        # field_db_type = info_['field_db_type']
        # example_value = info_['example_value']
        # nullable = not field_db_type.startswith('String')
        # info_['nullable'] = nullable
        # info_['boolean'] = field_db_type == 'Boolean'
        # info_['datetime'] = field_db_type in {'Date', 'DateTime'}
        # info_['is_percent'] = nullable and '%' in example_value

        # if field_db_type == 'Integer' and '%' not in example_value:
        #     field_db_name, is_cent = make_dollar_field_to_cents(field_db_name)
        #     if is_cent:
        #         info_['to_cent'] = True

        # default = "" if nullable else ", default=''"
        # nullable = str(nullable)
        # index = ", index=True" if field_db_name in {'make', 'model', 'vin', 'cut_off_date'} else ""
        # db_model.append(f"    {field_db_name} = Column({field_db_type}, nullable={nullable}{default}{index})\n")
        # fields_info[field_db_name] = info_
        # field_csv_name_to_db_name[field_csv_name] = field_db_name

        # return fields_info, field_csv_name_to_db_name, db_model

