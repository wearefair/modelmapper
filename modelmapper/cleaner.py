"""
Functionality for cleaning the data for importing into tables that mapper has created.
"""
import io
import datetime

from itertools import chain
from functools import partial
from decimal import Decimal
from string import digits
from xlrd import xldate_as_datetime
from modelmapper.base import Base
from modelmapper.normalization import normalize_numberic_values
from modelmapper.mapper import ONE_HUNDRED, SqlalchemyFieldType, INTEGER_SQLALCHEMY_TYPES
from modelmapper.excel import _xls_contents_to_csvs, _xls_xml_contents_to_csvs, _xlsx_contents_to_csvs

strptime = datetime.datetime.strptime

FLOAT_ACCEPTABLE = frozenset('.' + digits)

FIELD_NAME_NOT_FOUND_MSG = ('{} is not found in the combined model file.'
                            'Either there are new columns that the model needs to be trained with'
                            'or you are running the setup for another model.')


class ParsingError(ValueError):
    pass


def get_file_content_bytes(path):
    with open(path, 'rb') as the_file:
        return the_file.read()


def get_file_content_string(path):
    with open(path, 'r') as the_file:
        return the_file.read()


class Cleaner(Base):

    def __init__(self, *args, **kwargs):
        # setting the XLS date mode which is only used when parsing old Excel XLS files.
        # https://github.com/python-excel/xlrd/blob/master/xlrd/xldate.py
        # 0: 1900-based, 1: 1904-based.
        self.xls_date_mode = kwargs.pop('xls_date_mode', 0)
        self._missing_fields = set()
        self.publicized = False

        super().__init__(*args, **kwargs)

    def get_csv_data_cleaned(self, path_or_content, original_content_type=None, ignore_missing_fields=True):
        """
        Gets csv data cleaned. Use it only if you know you have a CSV path or stringIO with CSV content.
        Otherwise use the clean method in this class.
        """
        combined_module = self._get_combined_module()
        model_info = combined_module.FIELDS

        all_items = self._get_all_values_per_clean_name(path_or_content)
        for field_name, field_values in all_items.items():
            try:
                field_info = model_info[field_name]
            except KeyError:
                if ignore_missing_fields:
                    self._missing_fields.add(field_name)
                    continue
                else:
                    raise KeyError(FIELD_NAME_NOT_FOUND_MSG.format(field_name))
            self._get_field_values_cleaned_for_importing(
                field_name, field_info, field_values, original_content_type
            )
        if self._missing_fields:
            for field in self._missing_fields:
                try:
                    del all_items[field]
                except KeyError:
                    pass

            if not self.publicized:
                error_msg = (
                    f'There were fields found in the source data that were not defined in the given Model.\n'
                    f'Fields Missing: {self._missing_fields}\n'
                    f'Model: {self.settings.combined_file_name[:-3]}'
                )
                self.logger.error(error_msg)
                self.slack(error_msg)
                self.publicized = True

        all_lines_cleaned = zip(*all_items.values())

        for i in all_lines_cleaned:
            yield dict(zip(all_items.keys(), i))

    def _get_field_values_cleaned_for_importing(self, field_name, field_info, field_values, original_content_type):
        """Prepares source data for insertion into database.

        Arguments:
            field_name (str) - The name of the field to be inserted into the model.
            field_info (dict) - Information about the filed pulled from the model's TOML file.
            field_values (list) - All values to be inserted into the model from the source data.
            original_content_type (str) - The file type of the source data (i.e. xls or csv)

        Returns:
            field_values (list) - The prepared values to insert into the given model.

        Raises:
            ValueError, TypeError - Indicates something is wrong with the incoming data, refer to error message.
        """

        is_nullable = field_info.get('is_nullable', False)
        is_decimal = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.Decimal
        is_dollar = field_info.get('is_dollar', False)
        is_integer = field_info['field_db_sqlalchemy_type'] in INTEGER_SQLALCHEMY_TYPES
        is_percent = field_info.get('is_percent', False)
        is_boolean = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.Boolean
        is_datetime = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.DateTime
        is_string = field_info['field_db_sqlalchemy_type'] == SqlalchemyFieldType.String
        is_excel = original_content_type == 'xlsx' or original_content_type == 'xls'
        datetime_formats = list(field_info.get('datetime_formats', []))

        max_string_len = field_info.get('args', 255) if is_string else 0
        max_string_len_padded = min(max_string_len + self.settings.add_to_string_length, 255)

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
                if len(item) > max_string_len_padded:
                    msg = f'There is a value that is longer than {max_string_len_padded} for {field_name}: {item}'
                    raise ValueError(msg)

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
                if is_percent and not is_excel:  # xls already has it divided by 100
                    item = item / ONE_HUNDRED
                if is_integer:
                    item = int(item)
                if is_datetime:
                    item_chars = set(item)
                    if not item_chars <= self.settings.datetime_allowed_characters:
                        raise ValueError(f"Datetime value of {item} in {field_name} has characters "
                                         "that are NOT defined in datetime_allowed_characters")
                    msg = (f"{field_name} has invalid datetime format for {item} "
                           f"that is not in {field_info.get('datetime_formats')}")
                    try:
                        _format = datetime_formats[-1]
                        strptime(item, _format)
                    except IndexError:
                        if is_excel and item_chars <= FLOAT_ACCEPTABLE:
                            pass
                        else:
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
            def convert_dates(x):
                if x is None:
                    return None
                try:
                    return strptime(x, _format)
                except ValueError:
                    if is_excel:
                        return xldate_as_datetime(float(x), self.xls_date_mode)

            field_values[:] = map(convert_dates, field_values)

        return field_values

    def clean(self, content_type, path=None, content=None, sheet_names=None, ignore_missing_fields=True):
        """
        Clean the data for importing into database.
        content_type: Options: csv, xls, xls_xml, xlsx
        path: (optional) The path to the file to open
        content: (optional) The content to be read. The content can be bytes, string, BytesIO or StringIO
        sheet_names: (optional) The sheet names from the Excel file to be considered.
                                If none provided, all sheets will be considered.
        ignore_missing_fields: (optional) If true: fields not found in the model will be ignored
        """
        def _excel_contents_cleaned(content, func, sheet_names):
            results = func(content, sheet_names=sheet_names)
            csvs_chained = results.values()
            csvs_cleaned = map(
                lambda x: self.get_csv_data_cleaned(
                    x, content_type, ignore_missing_fields=ignore_missing_fields
                ), csvs_chained
            )
            return chain.from_iterable(csvs_cleaned)

        get_csv_data_cleaned = partial(
            self.get_csv_data_cleaned, ignore_missing_fields=ignore_missing_fields
        )
        xls_contents_cleaned = partial(_excel_contents_cleaned, func=_xls_contents_to_csvs,
                                       sheet_names=sheet_names)
        xls_xml_contents_cleaned = partial(_excel_contents_cleaned, func=_xls_xml_contents_to_csvs,
                                           sheet_names=sheet_names)
        xlsx_contents_cleaned = partial(_excel_contents_cleaned, func=_xlsx_contents_to_csvs,
                                        sheet_names=sheet_names)
        solutions = {
            'csv': {'path': [get_csv_data_cleaned],
                    'content_str': [io.StringIO, get_csv_data_cleaned],
                    'content_bytes': [lambda x: x.decode('utf-8'), io.StringIO, get_csv_data_cleaned],
                    'content_stringio': [get_csv_data_cleaned],
                    'content_bytesio': [lambda x: x.getvalue().decode('utf-8'),
                                        io.StringIO, get_csv_data_cleaned],
                    },
            'xls': {'path': [get_file_content_bytes, xls_contents_cleaned],
                    'content_str': [lambda x: x.encode('utf-8'), xls_contents_cleaned],
                    'content_bytes': [xls_contents_cleaned],
                    'content_bytesio': [lambda x: x.getvalue(), xls_contents_cleaned],
                    'content_stringio': [lambda x: x.getvalue().encode('utf-8'), xls_contents_cleaned],
                    },
            'xls_xml': {'path': [get_file_content_bytes, xls_xml_contents_cleaned],
                        'content_str': [lambda x: x.encode('utf-8'), xls_xml_contents_cleaned],
                        'content_bytes': [xls_xml_contents_cleaned],
                        'content_bytesio': [lambda x: x.getvalue(), xls_xml_contents_cleaned],
                        'content_stringio': [lambda x: x.getvalue().encode('utf-8'), xls_xml_contents_cleaned],
                        },
            'xlsx': {'path': [get_file_content_bytes, xls_contents_cleaned],
                     'content_str': [lambda x: x.encode('utf-8'), xlsx_contents_cleaned],
                     'content_bytes': [xlsx_contents_cleaned],
                     'content_bytesio': [lambda x: x.getvalue(), xlsx_contents_cleaned],
                     'content_stringio': [lambda x: x.getvalue().encode('utf-8'), xlsx_contents_cleaned],
                     },
        }
        solutions['tsv'] = solutions['csv']

        self.publicized = False
        self._missing_fields = set()

        content_type = content_type.lower()
        try:
            content_type_solution = solutions[content_type]
        except KeyError as e:
            raise KeyError(f"content_type of {e} is invalid. Options are: {', '.join(solutions.keys())}")

        if path:
            key = 'path'
            value = path
        elif content:
            value = content
            key = f'content_{content.__class__.__name__.lower()}'
        else:
            raise ValueError('Either path or content need to be passed.')

        try:
            funcs = content_type_solution[key]
        except KeyError as e:
            raise ValueError('Unrecognized content. It has to be either bytes, string, BytesIO or StringIO')
        try:
            for function in funcs:
                value = function(value)
        except Exception as e:
            raise ParsingError(f'Error parsing for content type of {content_type}: {e}')
        return value
