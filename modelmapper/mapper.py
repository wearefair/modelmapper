import enum
import os
import sys
import datetime
import logging
import importlib
from copy import deepcopy
from collections import defaultdict
from decimal import Decimal
# We are using both the new style and old style of named tuple
from typing import NamedTuple
from collections import namedtuple, Counter
from tabulate import tabulate

from modelmapper.ui import get_user_choice, get_user_input, YES_NO_CHOICES
from modelmapper.normalization import normalize_numberic_values
from modelmapper.misc import (read_csv_gen, load_toml, write_toml, write_settings,
                              named_tuple_to_compact_dict, escape_word, get_combined_dict,
                              write_full_python_file, update_file_chunk_content)

from modelmapper.stats import (
    StatsCollector,
    UserInferenceRequired,
    InconsistentData,
    matchers_from_settings
)
from modelmapper.types import (
    HasNull,
    HasDecimal,
    HasInt,
    HasString,
    HasDateTime,
    HasBoolean,
)

logger = logging.getLogger(__name__)

SQLALCHEMY_ORM = 'SQLALCHEMY_ORM'

ONE_HUNDRED = Decimal('100')

TOML_KEYS_THAT_ARE_SET = {'datetime_formats'}


INVALID_DATETIME_USER_OPTIONS = {
    'y': {'help': 'to define the date format', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit()}
}


CONTINUE_OR_ABORT_OPTIONS = {
    'y': {'help': 'to continue when done', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit()}
}


class FieldResult(NamedTuple):
    field_db_sqlalchemy_type: 'FieldResult' = None
    field_db_str: 'FieldResult' = None
    is_nullable: 'FieldResult' = None
    is_percent: 'FieldResult' = None
    is_dollar: 'FieldResult' = None
    datetime_formats: 'FieldResult' = None
    to_index: 'FieldResult' = None
    args: 'FieldResult' = None


class FieldReport(NamedTuple):
    field_name: 'FieldReport' = None
    decision: 'FieldReport' = None
    item_count: 'FieldReport' = None
    stats: 'FieldReport' = None


def update_field_result_dict_metadata(item):
    if item.get('field_db_sqlalchemy_type'):
        return item
    field_db_str = item.pop('field_db_str')
    field_db_str_low = field_db_str.lower().strip()
    field_db_sqlalchemy_type = {i.name.lower(): i for i in SqlalchemyFieldType}.get(field_db_str_low)
    args = None
    if field_db_sqlalchemy_type is None:
        if field_db_str_low.startswith('string'):
            field_db_sqlalchemy_type = SqlalchemyFieldType.String
            arg = field_db_str_low.replace('string(', '').rstrip(')')
            args = int(arg)
        if field_db_str_low.lower().startswith('decimal'):
            field_db_sqlalchemy_type = SqlalchemyFieldType.Decimal
            args = field_db_str_low.replace('decimal(', '').rstrip(')').split(',')
            args = tuple(map(int, args))
    item['field_db_sqlalchemy_type'] = field_db_sqlalchemy_type
    if args:
        item['args'] = args
    return item


def get_field_result_from_dict(item):
    item = update_field_result_dict_metadata(item)
    return FieldResult(**item)


SqlalchemyFieldTypeToHas = dict(
    String=HasString,
    SmallInteger=HasInt,
    Integer=HasInt,
    BigInteger=HasInt,
    Decimal=HasDecimal,
    DateTime=HasDateTime,
    Boolean=HasBoolean,
)


class SqlalchemyFieldType(enum.Enum):
    String = 'String({})'
    SmallInteger = 'SmallInteger'
    Integer = 'Integer'
    BigInteger = 'BigInteger'
    Decimal = 'DECIMAL({}, {})'
    DateTime = 'DateTime'
    Boolean = 'Boolean'

    def __str__(self):
        return f"{self.__class__.__name__}.{self._name_}"

    __repr__ = __str__

    @property
    def hastype(self):
        return SqlalchemyFieldTypeToHas[self._name_]


FIELD_RESULT_COMPARISON_NUMBERS = {
    SqlalchemyFieldType.Decimal: 10,
    SqlalchemyFieldType.BigInteger: 8,
    SqlalchemyFieldType.Integer: 6,
    SqlalchemyFieldType.SmallInteger: 4,
}

INTEGER_SQLALCHEMY_TYPES = {
    SqlalchemyFieldType.SmallInteger,
    SqlalchemyFieldType.Integer,
    SqlalchemyFieldType.BigInteger
}
NUMERIC_REMOVE = (',', '$', '%')


def _is_valid_dateformat(user_input, item):
    try:
        datetime.datetime.strptime(item, user_input)
    except ValueError:
        result = False
    else:
        result = True
    return result


def _is_valid_path(user_input, setup_dir):
    full_path = os.path.join(setup_dir, user_input)
    return os.path.exists(full_path)


def _validate_file_has_start_and_end_lines(user_input, path, identifier):
    try:
        update_file_chunk_content(path=path, code=[], identifier=identifier, check_only=True)
    except ValueError as e:
        print(e)
        return False
    else:
        return True


OVERRIDES_FILE_NAME = "{}_overrides.toml"
COMBINED_FILE_NAME = "{}_combined.py"


class Mapper:

    def __init__(self, setup_path, debug=False):
        if not setup_path.endswith('_setup.toml'):
            raise ValueError('The path needs to end with _setup.toml')
        self.debug = debug
        self.setup_path = setup_path
        self.setup_dir = os.path.dirname(setup_path)
        sys.path.append(self.setup_dir)
        clean_later = ['field_name_full_conversion']
        convert_to_set = ['null_values', 'boolean_true', 'boolean_false', 'datetime_formats',
                          'ignore_lines_that_include_only_subset_of', ]
        self._original_settings = load_toml(setup_path)['settings']
        self.settings = deepcopy(self._original_settings)
        for item in clean_later:
            self.settings[item] = [[self._clean_it(i), self._clean_it(j)] for i, j in self.settings[item]]
        for item in convert_to_set:
            self.settings[item] = set(self.settings.get(item, []))
        self.settings['identifier'] = identifier = os.path.basename(setup_path).replace('_setup.toml', '')
        self.settings['overrides_file_name'] = OVERRIDES_FILE_NAME.format(identifier)
        self.settings['combined_file_name'] = COMBINED_FILE_NAME.format(identifier)
        self.settings['booleans'] = self.settings['boolean_true'] | self.settings['boolean_false']
        self.settings['datetime_allowed_characters'] = set(self.settings['datetime_allowed_characters'])
        for i, v in (('overrides_path', 'overrides_file_name'),
                     ('combined_path', 'combined_file_name'),
                     ('output_model_path', 'output_model_file')):
            self.settings[i] = os.path.join(self.setup_dir, self.settings[v])
        # Since we cleaning up the field_name_part_conversion, special characters
        # such as \n need to be added seperately.
        self.settings['field_name_part_conversion'].insert(0, ['\n', '_'])
        _max_int = ((int(i), v) for i, v in self.settings['max_int'].items())
        self.settings['max_int'] = dict(sorted(_max_int, key=lambda x: x[0]))
        Settings = namedtuple('Settings', ' '.join(self.settings.keys()))
        self.settings = Settings(**self.settings)
        self.questionable_fields = {}
        self.solid_decisions = {}
        self.failed_to_infer_fields = set()
        self.empty_fields = set()

    def _clean_it(self, name):
        conv = (self.settings['field_name_part_conversion'] if isinstance(self.settings, dict)
                else self.settings.field_name_part_conversion)
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
                raise ValueError(f"'{name}' field has a collision with '{clean_names_mapping[clean_name]}'. "
                                 f"They both produce '{clean_name}'")
            else:
                clean_names_mapping[clean_name] = name

    def _does_line_include_data(self, line):
        # whether line has any characters in it that are not in ignore_lines_that_include_only_subset_of
        return any(filter(lambda x: set(x.strip()) - self.settings.ignore_lines_that_include_only_subset_of, line))

    def _verify_no_duplicate_names(self, names):
        counter = Counter(names)
        duplicates = {i: v for i, v in counter.most_common(10) if v > 1}
        if duplicates:
            raise ValueError(f'The following fields were repeated in the csv: {duplicates}')

    def _get_clean_names_and_csv_data_gen(self, path):
        reader = read_csv_gen(path)
        names = next(reader)
        self._verify_no_duplicate_names(names)
        name_mapping = self._get_all_clean_field_names_mapping(names)
        self._verify_no_duplicate_clean_names(name_mapping)
        clean_names = list(name_mapping.values())
        return clean_names, reader

    def _get_all_values_per_clean_name(self, path):
        result = defaultdict(list)
        clean_names, reader = self._get_clean_names_and_csv_data_gen(path)
        # transposing csv and turning into dictionary
        for line in reader:
            if self._does_line_include_data(line):
                for i, v in enumerate(line):
                    try:
                        result[clean_names[i]].append(v)
                    except IndexError:
                        raise ValueError("Your csv might have new lines in the field names. "
                                         "Please fix that and try again.")
        return result

    def _get_stats(self, field_name, items):
        try:
            collector = StatsCollector(matchers=matchers_from_settings(self.settings))
            for item in items:
                collector.inspect_item(field_name, item)
            return collector.collect()
        except UserInferenceRequired as err:
            if err.value_type == HasDateTime:
                msg = f'field {field_name} has inconsistent datetime data: {item}.'
                get_user_choice(msg, choices=INVALID_DATETIME_USER_OPTIONS)
                msg = f'Please enter the datetime format for {item}'
                new_format = get_user_input(msg, validate_func=_is_valid_dateformat, item=item)
                if new_format in self.settings.datetime_formats:
                    raise InconsistentData(f"field {field_name} has inconsistent datetime data: "
                                           f"{item}. {new_format} was already in your settings.")
                print(f'Adding {new_format} to your settings.')
                self.settings.datetime_formats.add(new_format)
                self._original_settings['datetime_formats'].append(new_format)
                write_settings(self.setup_path, self._original_settings)
                return self._get_stats(field_name, items)

    def _get_integer_field(self, max_int):
        previous_key = 0
        for key, field_db_type in self.settings.max_int.items():
            if key < previous_key:
                raise ValueError('max_int keys are not properly sorted.')
            previous_key = key
            if max_int < key:
                return getattr(SqlalchemyFieldType, field_db_type)
        raise ValueError(f'{max_int} is bigger than the largest integer the database takes: {key}')

    def _validate_decision(self, field_name, field_result, stats):
        field_report = FieldReport(field_name=field_name,
                                   decision=field_result.field_db_sqlalchemy_type.name,
                                   item_count=stats.len, stats=stats.counter)
        field_stats = stats.counter[field_result.field_db_sqlalchemy_type.hastype]
        should_process_as_dollar = self.settings.dollar_to_cent and field_result.is_dollar
        if (field_stats + stats.counter[HasNull] == stats.len or
                (should_process_as_dollar and
                 stats.counter[HasInt] + stats.counter[HasDecimal] + stats.counter[HasNull] == stats.len)):
            if field_name in self.questionable_fields:
                del self.questionable_fields[field_name]
            self.solid_decisions[field_name] = field_report
        elif field_name not in self.solid_decisions:
            self.questionable_fields[field_name] = field_report

    def _is_dollar_field(self, field_name, counter):
        is_dollar = counter['HasDollar'] and self.settings.dollar_to_cent
        if not is_dollar:
            for item in self.settings.dollar_value_if_word_in_field_name:
                if item in field_name:
                    is_dollar = True
                    break
        return is_dollar

    def _get_field_result_from_stats(self, field_name, stats):
        counter = stats.counter.copy()

        _type = _type_str = None
        null_count = counter.pop(HasNull, 0)
        if not counter:
            self.empty_fields.add(field_name)
            return

        non_string_nullable = self.settings.non_string_fields_are_all_nullable or null_count
        max_bool_word_size = max(map(len, self.settings.booleans))

        str_length = min(stats.max_string_len + self.settings.add_to_string_length, 255)
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
            self._validate_decision(field_name, field_result=field_result_string, stats=stats)
            return field_result_string

        most_common = dict(counter.most_common(3))
        most_common_keys = set(most_common.keys())
        most_common_count = counter.most_common(1)[0][1]

        for _has, _field_result in (('HasBoolean', field_result_boolean),
                                    ('HasDateTime', field_result_datetime),
                                    ('HasString', field_result_string)):
            if counter[_has] == most_common_count:
                self._validate_decision(field_name, field_result=_field_result, stats=stats)
                return _field_result

        is_percent = is_dollar = None
        if HasDecimal in counter:
            _type = SqlalchemyFieldType.Decimal
            max_pre_decimal = stats.max_pre_decimal
            max_decimal_scale = stats.max_decimal_scale
            if counter['HasInt']:
                max_int_precision = len(str(stats.max_int))
                max_pre_decimal = max(max_pre_decimal, max_int_precision)
            if self._is_dollar_field(field_name, counter):
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
            elif self._is_dollar_field(field_name, counter):
                max_int = stats.max_int * 100
                _type = self._get_integer_field(max_int)
                is_dollar = True
            else:
                _type = self._get_integer_field(stats.max_int)
        if _type is SqlalchemyFieldType.Decimal:
            max_pre_decimal += self.settings.add_digits_to_decimal_field
            max_decimal_scale += self.settings.add_digits_to_decimal_field
            _type_str = SqlalchemyFieldType.Decimal.value.format(max_pre_decimal + max_decimal_scale, max_decimal_scale)

        if _type:
            field_result = FieldResult(
                field_db_sqlalchemy_type=_type,
                field_db_str=_type_str if _type_str else _type.value,
                is_nullable=non_string_nullable,
                is_percent=is_percent,
                is_dollar=is_dollar)
            self._validate_decision(field_name, field_result=field_result, stats=stats)
            return field_result

        logger.error(f'Unable to understand the field type from the data in {field_name}')
        logger.error("Please train the system for that field with a different dataset "
                     "or manually define an override in the output later.")
        self.failed_to_infer_fields.add(field_name)
        return None

    def _get_field_orm_string(self, field_name, field_result, orm=SQLALCHEMY_ORM):
        if field_result.field_db_str:
            field_db_type = field_result.field_db_str
        elif isinstance(field_result.args, (tuple, list)):
            field_db_type = field_result.field_db_sqlalchemy_type.value.format(*field_result.args)
        elif field_result.args is not None:
            field_db_type = field_result.field_db_sqlalchemy_type.value.format(field_result.args)
        else:
            field_db_type = field_result.field_db_sqlalchemy_type.value
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
            field_result = self._get_field_result_from_stats(field_name=field_name, stats=stats)
            if field_result:
                yield field_name, field_result

    def _get_analyzed_file_path_from_csv_path(self, path):
        csv_name = os.path.basename(path)
        analyzed_file_name = f'{self.settings.identifier}_{escape_word(csv_name)}_analysis.toml'
        return os.path.join(self.setup_dir, analyzed_file_name)

    def _get_csv_full_path(self, path):
        if not path.startswith('/'):
            path = os.path.join(self.setup_dir, path)
        return os.path.join(self.setup_dir, path)

    def _get_overrides(self):
        if os.path.exists(self.settings.overrides_path):
            return load_toml(self.settings.overrides_path, keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)

    def _read_analyzed_csv_results(self):
        results = []
        for csv_path in self.settings.training_csvs:
            file_path = self._get_analyzed_file_path_from_csv_path(csv_path)
            result = load_toml(file_path, keys_to_convert_to_set=TOML_KEYS_THAT_ARE_SET)
            results.append(result)
        return results

    def analyze(self):
        results = []
        if not self.settings.training_csvs:
            raise ValueError('The list of training_csvs in the settings file is empty.')
        for csv_path in self.settings.training_csvs:
            csv_path = self._get_csv_full_path(csv_path)
            file_path = self._get_analyzed_file_path_from_csv_path(csv_path)
            result = {}
            for field_name, field_result in self._get_field_results_from_csv(csv_path):
                result[field_name] = named_tuple_to_compact_dict(field_result)
            write_toml(file_path, result, auto_generated_from=os.path.basename(csv_path),
                       keys_to_convert_to_list=TOML_KEYS_THAT_ARE_SET)
            results.append(result)
            print(f'{file_path} updated.')
        overrides = self._get_overrides()
        if overrides:
            overrides_keys = {k for k, v in overrides.items() if 'field_db_str' in v}
        else:
            overrides_keys = set()
        self.empty_fields -= overrides_keys

        return results

    def _combine_analyzed_csvs(self, analyzed_results_all, overrides=None):
        results = {}
        for analyzed_results in analyzed_results_all:
            for field_name, field_result_dict in analyzed_results.items():
                update_field_result_dict_metadata(field_result_dict)
                if overrides and field_name in overrides:
                    update_field_result_dict_metadata(overrides[field_name])
                    field_result_dict.update(overrides[field_name])
                if field_name not in results:
                    results[field_name] = field_result_dict
                else:
                    old_field_result_dict = results[field_name]
                    old_type = old_field_result_dict['field_db_sqlalchemy_type']
                    _type = field_result_dict['field_db_sqlalchemy_type']
                    bigger_field_result_dict = None
                    if old_type == _type:
                        if _type == SqlalchemyFieldType.String:
                            bigger_field_result_dict = get_combined_dict(lambda x: x['args'], old_field_result_dict,
                                                                         field_result_dict)
                        elif _type == SqlalchemyFieldType.Decimal:
                            bigger_pre_decimal = max(old_field_result_dict['args'][0], field_result_dict['args'][0])
                            bigger_decimal_scale = max(old_field_result_dict['args'][1], field_result_dict['args'][1])
                            field_result_dict['args'] = (bigger_pre_decimal, bigger_decimal_scale)
                            bigger_field_result_dict = field_result_dict
                        else:
                            bigger_field_result_dict = get_combined_dict(None, old_field_result_dict, field_result_dict)
                    else:
                        if {old_type, _type} <= set(FIELD_RESULT_COMPARISON_NUMBERS.keys()):
                            bigger_field_result_dict = get_combined_dict(
                                lambda x: FIELD_RESULT_COMPARISON_NUMBERS[x['field_db_sqlalchemy_type']],
                                field_result_dict,
                                old_field_result_dict)
                        else:
                            raise ValueError("Field types that are inferred have conflicts: "
                                             f"{old_type.name} vs {_type.name} for field name {field_name}")
                    if bigger_field_result_dict is None:
                        raise ValueError('Bug: bigger_field_result_dict is not set when making the decision.')
                    results[field_name] = bigger_field_result_dict
        if overrides:
            for field_name, value in overrides.items():
                if field_name not in results:
                    update_field_result_dict_metadata(value)
                    results[field_name] = value
                    if field_name in self.empty_fields:
                        self.empty_fields.remove(field_name)
        return results

    def combine_results(self):
        analyzed_results_all = self._read_analyzed_csv_results()
        overrides = self._get_overrides()
        combined_results = self._combine_analyzed_csvs(analyzed_results_all, overrides)
        for field_name in self.empty_fields:
            if field_name not in combined_results:
                combined_results[field_name] = {'field_db_sqlalchemy_type': SqlalchemyFieldType.Boolean,
                                                'is_nullable': True}
        write_full_python_file(self.settings.combined_path, variable_name='FIELDS',
                               contents=combined_results, header='from modelmapper import SqlalchemyFieldType')
        print(f'{self.settings.combined_path} overwritten.')

    def _get_combined_module(self):
        combined_module_str = self.settings.combined_file_name[:-3]
        return importlib.import_module(combined_module_str)

    def write_orm_model(self):
        combined_module = self._get_combined_module()
        code = []
        for field_name, field_result_dict in combined_module.FIELDS.items():
            result = self._get_field_orm_string(field_name, field_result=FieldResult(**field_result_dict),
                                                orm=SQLALCHEMY_ORM)
            code.append(result)
        update_file_chunk_content(path=self.settings.output_model_path, code=code, identifier=self.settings.identifier)
        print(f'{self.settings.output_model_path} is updated.')

    def run(self):
        try:
            self.analyze()
            self.empty_fields = self.empty_fields - (
                self.failed_to_infer_fields |
                set(self.solid_decisions.keys()) |
                set(self.questionable_fields.keys()))

            if self.empty_fields:
                print("=" * 50)
                print("The following fields were empty in the csvs. Setting them to nullable boolean. "
                      "If you define overrides for them in "
                      f"{self.settings.overrides_file_name}, the override will be applied.")
                print("\n".join(self.empty_fields))
                print("")

            if self.failed_to_infer_fields:
                print("=" * 50)
                print("The following fields failed:")
                print('\n'.join(self.failed_to_infer_fields))
                msg = f'Please provide the overrides for these fields in {self.settings.overrides_file_name}\n'
                get_user_choice(msg, choices=CONTINUE_OR_ABORT_OPTIONS)
                print("")

            if self.questionable_fields:
                print("The following fields had results that might need to be verified:")
                headers = FieldReport._fields
                print(tabulate(self.questionable_fields.values(), headers=headers))
                msg = ("Please verify the fields and provide the overrides if "
                       f"necessary in {self.settings.overrides_file_name}")
                get_user_choice(msg, choices=CONTINUE_OR_ABORT_OPTIONS)
                print("")

            self.combine_results()
            self.write_orm_model()
        except Exception as e:
            if self.debug:
                raise
            else:
                print(f'{e.__class__}: {e}')

    def get_csv_data_cleaned(self, path):
        combined_module = self._get_combined_module()
        model_info = combined_module.FIELDS

        all_items = self._get_all_values_per_clean_name(path)
        for field_name, field_values in all_items.items():
            field_info = model_info[field_name]
            self.get_field_values_cleaned_for_importing(field_name, field_info, field_values)

        # transposing
        all_lines_cleaned = zip(*all_items.values())

        for i in all_lines_cleaned:
            yield dict(zip(all_items.keys(), i))

    def get_field_values_cleaned_for_importing(self, field_name, field_info, field_values):
        is_nullable = field_info.get('is_nullable', False)
        is_decimal = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.Decimal
        is_dollar = field_info.get('is_dollar', False)
        is_integer = field_info['field_db_sqlalchemy_type'] in INTEGER_SQLALCHEMY_TYPES
        is_percent = field_info.get('is_percent', False)
        is_boolean = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.Boolean
        is_datetime = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.DateTime
        is_string = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.String
        datetime_formats = list(field_info.get('datetime_formats', []))

        max_string_len = field_info.get('args', 255) if is_string else 0

        def _mark_nulls(item):
            return None if item in self.settings.null_values else item

        def _mark_booleans(item):
            if item in self.settings.boolean_true:
                result = True
            elif item in self.settings.boolean_false:
                result = False
            else:
                raise ValueError(f"There is a value of {item} in {field_name} "
                                 "which is not a recognized Boolean or Null value.")
            return result

        for i, item in enumerate(field_values):
            original_item = item
            item = item.strip().lower()
            if is_string:
                if len(item) > max_string_len:
                    raise ValueError(f'There is a value that is longer than {max_string_len} for {field_name}: {item}')

            if is_integer or is_decimal:
                item = normalize_numberic_values(item)

            if is_nullable:
                item = _mark_nulls(item)

            if item is not None:
                if is_boolean:
                    item = _mark_booleans(item)

                if is_integer or is_decimal or is_dollar or is_percent:
                    try:
                        item = Decimal(item)
                    except Exception as e:
                        raise TypeError(f'Unable to convert {item} into decimal: {e}') from None

                if is_dollar:
                    item = item * ONE_HUNDRED
                if is_percent:
                    item = item / ONE_HUNDRED
                if is_integer:
                    item = int(item)
                if is_datetime:
                    if not set(item) <= self.settings.datetime_allowed_characters:
                        raise ValueError(f"Datetime value of {item} in {field_name} has characters "
                                         "that are NOT defined in datetime_allowed_characters")
                    msg = (f"{field_name} has invalid datetime format for {item} "
                           f"that is not in {field_info.get('datetime_formats')}")
                    try:
                        _format = datetime_formats[-1]
                        datetime.datetime.strptime(item, _format)
                    except IndexError:
                        raise ValueError(msg) from None
                    except ValueError:
                        if datetime_formats:
                            datetime_formats.pop()
                        else:
                            raise ValueError(msg) from None
                if is_string:
                    item = original_item

            field_values[i] = item

        if is_datetime:
            field_values[:] = map(lambda x: None if x is None else datetime.datetime.strptime(x, _format), field_values)
        return field_values


def initialize(path):
    """
    Initialize a ModelMapper setup for a model
    """
    identifier = os.path.basename(path)
    setup_dir = os.path.dirname(path)
    setup_path = os.path.join(setup_dir, f'{identifier}_setup.toml')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    template_setup_path = os.path.join(current_dir, '../modelmapper/templates/setup_template.toml')
    settings = load_toml(template_setup_path)['settings']
    overrides_file_name = OVERRIDES_FILE_NAME.format(identifier)
    overrides_path = os.path.join(setup_dir, overrides_file_name)
    if os.path.exists(overrides_path):
        get_user_choice(f'{overrides_path} already exists. Do you want to overwrite it?', choices=YES_NO_CHOICES)
    with open(overrides_path, 'w') as the_file:
        the_file.write('# Overrides filse. You can add your overrides for any fields here.')
    output_model_file = get_user_input('Please provide the relative path to the existing ORM model file.',
                                       validate_func=_is_valid_path, setup_dir=setup_dir)
    settings['output_model_file'] = output_model_file
    output_model_path = os.path.join(setup_dir, output_model_file)
    if not _validate_file_has_start_and_end_lines(user_input=None, path=output_model_path, identifier=identifier):
        get_user_input(f'Please add the lines in a proper place in {output_model_file} code and enter continue',
                       _validate_file_has_start_and_end_lines, path=output_model_path, identifier=identifier)

    if os.path.exists(setup_path):
        get_user_choice(f'{setup_path} already exists. Do you want to overwrite it?', choices=YES_NO_CHOICES)

    write_settings(setup_path, settings)
    print(f'{setup_path} is written. Please add "the relative path to the training CSV files"'
          'in your settings and run modelmapper')
    print('Please verify the generated settings and provide a list of relative paths for training'
          'csvs in the settings file.')
