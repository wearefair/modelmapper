import enum
import os

import sys
import datetime

from decimal import Decimal
from typing import NamedTuple
from tabulate import tabulate

from modelmapper.base import Base
from modelmapper.ui import get_user_choice, get_user_input
from modelmapper.misc import (load_toml, write_toml, write_settings,
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


class Mapper(Base):

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

        self.logger.error(f'Unable to understand the field type from the data in {field_name}')
        self.logger.error("Please train the system for that field with a different dataset "
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
