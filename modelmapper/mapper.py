import enum
import os
import sys
import decimal
import datetime
import logging
from copy import deepcopy
from collections import defaultdict, Counter
from decimal import Decimal
# We are using both the new style and old style of named tuple
from typing import Any, NamedTuple
from collections import namedtuple
from tabulate import tabulate

from modelmapper.ui import get_user_choice, get_user_input
from modelmapper.misc import read_csv_gen, load_toml, write_toml, named_tuple_to_compact_dict, escape_word

logger = logging.getLogger(__name__)

SQLALCHEMY_ORM = 'SQLALCHEMY_ORM'

HasNull = 'HasNull'
HasDecimal = 'HasDecimal'
HasInt = 'HasInt'
HasDollar = 'HasDollar'
HasPercent = 'HasPercent'
HasString = 'HasString'
HasDateTime = 'HasDateTime'
HasBoolean = 'HasBoolean'


INVALID_DATETIME_USER_OPTIONS = {
    'y': {'help': 'to define the date format', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit}
}


CONTINUE_OR_ABORT_OPTIONS = {
    'y': {'help': 'to continue when done', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit}
}


class InconsistentData(ValueError):
    pass


class FieldStats(NamedTuple):
    counter: Any
    max_int: 'FieldStats' = 0
    max_pre_decimal: 'FieldStats' = 0
    max_decimal_scale: 'FieldStats' = 0
    max_string_len: 'FieldStats' = 0
    datetime_formats: 'FieldStats' = None
    len: 'FieldStats' = 0


class FieldResult(NamedTuple):
    field_db_sqlalchemy_type: 'FieldResult' = None
    field_db_str: 'FieldResult' = None
    is_nullable: 'FieldResult' = None
    is_percent: 'FieldResult' = None
    is_dollar: 'FieldResult' = None
    datetime_formats: 'FieldResult' = None
    to_index: 'FieldResult' = None
    args: 'FieldResult' = None


def get_field_result_from_dict(item):
    field_db_str = item.pop('field_db_str')
    field_db_str_low = field_db_str.lower().strip()
    field_db_sqlalchemy_type = {i.name.lower(): i for i in SqlalchemyFieldType}.get(field_db_str_low, None)
    args = None
    if field_db_sqlalchemy_type is None:
        if field_db_str_low.startswith('string'):
            field_db_sqlalchemy_type = SqlalchemyFieldType.String
            arg = field_db_str_low.replace('string(', '').rstrip(')')
            args = [int(arg)]
        if field_db_str_low.lower().startswith('decimal'):
            field_db_sqlalchemy_type = SqlalchemyFieldType.Decimal
            args = field_db_str_low.replace('decimal(', '').rstrip(')').split(',')
            args = list(map(int, args))
    return FieldResult(field_db_sqlalchemy_type=field_db_sqlalchemy_type, args=args, **item)


class SqlalchemyFieldType(enum.Enum):
    String = 'String({})'
    SmallInteger = 'SmallInteger'
    Integer = 'Integer'
    BigInteger = 'BigInteger'
    Decimal = 'DECIMAL({}, {})'
    DateTime = 'DateTime'
    Boolean = 'Boolean'


FIELD_RESULT_COMPARISON_NUMBERS = {
    SqlalchemyFieldType.Decimal: 10,
    SqlalchemyFieldType.BigInteger: 8,
    SqlalchemyFieldType.Integer: 6,
    SqlalchemyFieldType.SmallInteger: 4,
}


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


def _is_valid_dateformat(user_input, item):
    try:
        datetime.datetime.strptime(item, user_input)
    except ValueError:
        result = False
    else:
        result = True
    return result


class Mapper:

    def __init__(self, setup_path, debug=False):
        if not setup_path.endswith('_setup.toml'):
            raise ValueError('The path needs to end with _setup.toml')
        self.debug = debug
        self.setup_path = setup_path
        self.setup_dir = os.path.dirname(setup_path)
        clean_later = ['field_name_full_conversion']
        convert_to_set = ['null_values', 'boolean_true', 'boolean_false', 'datetime_formats']
        self._original_settings = load_toml(setup_path)['settings']
        self.settings = deepcopy(self._original_settings)
        for item in clean_later:
            self.settings[item] = [[self._clean_it(i), self._clean_it(j)] for i, j in self.settings[item]]
        for item in convert_to_set:
            self.settings[item] = set(self.settings.get(item, []))
        self.settings['identifier'] = os.path.basename(setup_path).replace('_setup.toml', '')
        self.settings['overrides_file_name'] = overrides_file_name = f"{self.settings['identifier']}_overrides.toml"
        self.settings['booleans'] = self.settings['boolean_true'] | self.settings['boolean_false']
        self.settings['datetime_allowed_characters'] = set(self.settings['datetime_allowed_characters'])
        self.settings['overrides_path'] = os.path.join(self.setup_dir, overrides_file_name)
        _max_int = ((int(i), v) for i, v in self.settings['max_int'].items())
        self.settings['max_int'] = dict(sorted(_max_int, key=lambda x: x[0]))
        Settings = namedtuple('Settings', ' '.join(self.settings.keys()))
        self.settings = Settings(**self.settings)
        self.questionable_fields = {}
        self.failed_to_infer_fields = []

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
                raise ValueError(f"'{name}' field has a collision with '{clean_names_mapping[clean_name]}'. They both produce '{clean_name}'")
            else:
                clean_names_mapping[clean_name] = name

    def _get_all_values_per_clean_name(self, path):
        result = defaultdict(list)
        reader = read_csv_gen(path)
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
            return i, v
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

    def _get_stats(self, field_name, items):
        max_int = 0
        max_pre_decimal = 0
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
                pre_decimal_precision, decimal_scale = self._get_decimal_places(item)
                max_pre_decimal = max(max_pre_decimal, pre_decimal_precision)
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
                        print(f'Adding {new_format} to your settings.')
                        self.settings.datetime_formats.add(new_format)
                        self._original_settings['datetime_formats'].append(new_format)
                        write_toml(self.setup_path, self._original_settings)
                        continue
            result.append(HasString)
            max_string_len = max(max_string_len, len(item))

        return FieldStats(counter=Counter(result), max_int=max_int,
                          max_pre_decimal=max_pre_decimal,
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

    def _get_field_result_from_stats(self, field_name, stats):
        counter = stats.counter.copy()

        _type = _type_str = None
        null_count = counter.pop(HasNull, 0)
        non_string_nullable = self.settings.non_string_fields_are_all_nullable or null_count
        max_bool_word_size = max(map(len, self.settings.booleans))

        str_length = min(stats.max_string_len + self.settings.add_to_string_legth, 255)
        field_result_string = FieldResult(
            field_db_sqlalchemy_type=SqlalchemyFieldType.String,
            field_db_str=SqlalchemyFieldType.String.value.format(str_length),
            is_nullable=bool(null_count) and self.settings.string_fields_can_be_nullable)

        field_result_boolean = FieldResult(
            field_db_sqlalchemy_type=SqlalchemyFieldType.Boolean,
            field_db_str=SqlalchemyFieldType.Boolean.value,
            is_nullable=non_string_nullable)

        field_result_datetime = FieldResult(
            field_db_sqlalchemy_type=SqlalchemyFieldType.DateTime,
            field_db_str=SqlalchemyFieldType.DateTime.value,
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
                    _field_type = _field_result.field_db_sqlalchemy_type.value
                    self.questionable_fields[field_name] = f'Field is probably {_field_type}. There are values that are not {_field_type} and not Null though.'
                return _field_result

        is_percent = is_dollar = None
        if HasDecimal in counter:
            _type = SqlalchemyFieldType.Decimal
            max_pre_decimal = stats.max_pre_decimal
            max_decimal_scale = stats.max_decimal_scale
            if counter['HasInt']:
                max_int_precision = len(str(stats.max_int))
                max_pre_decimal = max(max_pre_decimal, max_int_precision)
            if counter['HasDollar'] and self.settings.dollar_to_cent:
                max_int = int('9' * (max_pre_decimal + max_decimal_scale))
                _type = self._get_integer_field(max_int)
                is_dollar = True
            if counter['HasPercent'] and self.settings.percent_to_decimal:
                max_decimal_scale += 2
                max_pre_decimal -= 2
                is_percent = True
        elif HasInt in most_common_keys:
            if counter['HasPercent'] and self.settings.percent_to_decimal:
                max_decimal_scale = 0
                max_pre_decimal = len(str(stats.max_int))
                _type = SqlalchemyFieldType.Decimal
                is_percent = True
            else:
                _type = self._get_integer_field(stats.max_int)
        if _type is SqlalchemyFieldType.Decimal:
            max_pre_decimal += self.settings.add_digits_to_decimal_field
            max_decimal_scale += self.settings.add_digits_to_decimal_field
            _type_str = SqlalchemyFieldType.Decimal.value.format(max_pre_decimal + max_decimal_scale, max_decimal_scale)

        if _type:
            return FieldResult(
                field_db_sqlalchemy_type=_type,
                field_db_str=_type_str if _type_str else _type.value,
                is_nullable=non_string_nullable,
                is_percent=is_percent,
                is_dollar=is_dollar)

        logger.error(f'Unable to understand the field type from the data in {field_name}')
        logger.error('Please train the system for that field with a different dataset or manually define an override in the output later.')
        self.failed_to_infer_fields.append(field_name)
        return FieldResult(None)

    def _get_field_orm_string(self, field_name, field_result, orm=SQLALCHEMY_ORM):
        field_db_type = field_result.field_db_str if field_result.field_db_str else field_result.field_db_sqlalchemy_type.value
        default = "" if field_result.is_nullable else ", default=''"
        nullable = str(field_result.is_nullable)
        index = ", index=True" if field_result.to_index else ""
        if orm == SQLALCHEMY_ORM:
            result = f"    {field_name} = Column({field_db_type}, nullable={nullable}{default}{index})\n"
        else:
            raise NotImplementedError(f'_get_field_orm_string is not implemented for {orm} orm yet.')
        return result

    def _get_field_results_from_csv(self, path):
        all_items = self._get_all_values_per_clean_name(path)
        for field_name, field_values in all_items.items():
            stats = self._get_stats(field_name=field_name, items=field_values)
            yield field_name, self._get_field_result_from_stats(field_name=field_name, stats=stats)

    def _get_analyzed_file_path_from_csv_path(self, path):
        csv_name = os.path.basename(path)
        analyzed_file_name = f'{self.settings.identifier}_{escape_word(csv_name)}_analysis.toml'
        return os.path.join(self.setup_dir, analyzed_file_name)

    def _get_csv_full_path(self, path):
        if not path.startswith('/'):
            csv_path = os.path.join(self.setup_dir, path)
        return os.path.join(self.setup_dir, csv_path)

    def _get_overrides(self):
        return load_toml(self.settings.overrides_path)

    def _read_analyzed_csv_results(self):
        results = []
        for csv_path in self.settings.training_csvs:
            file_path = self._get_analyzed_file_path_from_csv_path(csv_path)
            result = load_toml(file_path)
            results.append(result)
        return results

    def analyze(self):
        results = []
        for csv_path in self.settings.training_csvs:
            csv_path = self._get_csv_full_path(csv_path)
            file_path = self._get_analyzed_file_path_from_csv_path(csv_path)
            result = {}
            for field_name, field_result in self._get_field_results_from_csv(csv_path):
                result[field_name] = named_tuple_to_compact_dict(field_result)
            write_toml(file_path, result, auto_generated_from=os.path.basename(csv_path))
            results.append(result)
            print(f'{file_path} updated.')

        return results

    def get_combined_field_results_from_analyzed_csvs(self, analyzed_results_all, overrides=None):
        results = {}
        for analyzed_results in analyzed_results_all:
            for field_name, field_result_dict in analyzed_results.items():
                field_result = get_field_result_from_dict(field_result_dict)
                if field_name not in results:
                    results[field_name] = field_result
                else:
                    old_field_result = results[field_name]
                    old_type = old_field_result.field_db_sqlalchemy_type
                    new_type = field_result.field_db_sqlalchemy_type
                    if old_type != new_type:
                        if {old_type, new_type} <= set(FIELD_RESULT_COMPARISON_NUMBERS.keys()):
                            bigger_field_result = max(field_result, old_field_result, key=lambda x: FIELD_RESULT_COMPARISON_NUMBERS[x.field_db_sqlalchemy_type])
                        else:
                            raise ValueError(f'Field types that are inferred have conflicts: {old_type.name} vs {new_type.name} for field name {field_name}')
                    else:
                        if new_type == SqlalchemyFieldType.String:
                            bigger_field_result = max(old_field_result, field_result, key=lambda x: x.args[0])
                        elif new_type == SqlalchemyFieldType.Decimal:
                            bigger_pre_decimal = max(old_field_result.args[0] - old_field_result.args[1], field_result.args[0] - field_result.args[1])
                            bigger_decimal_scale = max(old_field_result.args[1], field_result.args[1])
                            bigger_decimal_precision = bigger_pre_decimal + bigger_decimal_scale
                            field_result_dict = named_tuple_to_compact_dict(field_result)
                            field_result_dict['args'] = [bigger_decimal_precision, bigger_decimal_scale]
                            field_result_dict['field_db_sqlalchemy_type'] = new_type
                            bigger_field_result = FieldResult(**field_result_dict)
                        else:
                            continue
                    results[field_name] = bigger_field_result
        return results

    def run(self):
        try:
            self.analyze()
            if self.failed_to_infer_fields:
                print("Failed to process results for:")
                print('\n'.join(self.failed_to_infer_fields))
                msg = f'Please provide the overrides for these fields in {self.settings.overrides_file_name}'
                get_user_choice(msg, choices=CONTINUE_OR_ABORT_OPTIONS)

            if self.questionable_fields:
                print("The following fields had results that might need to be verified:")
                headers = ['field name', 'reason']
                print(tabulate(self.questionable_fields.items(), headers=headers))
                msg = f'Please verify the fields and provide the overrides if necessary in {self.settings.overrides_file_name}'
                get_user_choice(msg, choices=CONTINUE_OR_ABORT_OPTIONS)

            analyzed_results_all = self._read_analyzed_csv_results()
            overrides = self._get_overrides()
            combined_results = self.get_combined_field_results_from_analyzed_csvs(analyzed_results_all, overrides)
            print(combined_results)
        except Exception as e:
            if self.debug:
                raise
            else:
                print(e)
