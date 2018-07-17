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
from modelmapper.normalization import normalize_numberic_values
from modelmapper.mapper import Mapper, ONE_HUNDRED, SqlalchemyFieldType, INTEGER_SQLALCHEMY_TYPES
from modelmapper.excel import _xls_contents_to_csvs, _xls_xml_contents_to_csvs

strptime = datetime.datetime.strptime

FLOAT_ACCEPTABLE = frozenset('.' + digits)


def get_file_content_bytes(path):
    with open(path, 'rb') as the_file:
        return the_file.read()


def get_file_content_string(path):
    with open(path, 'r') as the_file:
        return the_file.read()


class Cleaner(Mapper):

    def __init__(self, *args, **kwargs):
        # setting the XLS date mode which is only used when parsing old Excel XLS files.
        # https://github.com/python-excel/xlrd/blob/master/xlrd/xldate.py
        # 0: 1900-based, 1: 1904-based.
        self.xls_date_mode = kwargs.pop('xls_date_mode', 0)

        super().__init__(*args, **kwargs)

    def get_csv_data_cleaned(self, path, original_content_type=None):
        """
        Gets csv data cleaned. Use it only if you know you have a CSV path or stringIO with CSV content.
        Otherwise use the clean method in this class.
        """
        combined_module = self._get_combined_module()
        model_info = combined_module.FIELDS

        all_items = self._get_all_values_per_clean_name(path)
        for field_name, field_values in all_items.items():
            field_info = model_info[field_name]
            self._get_field_values_cleaned_for_importing(field_name, field_info, field_values, original_content_type)

        # transposing
        all_lines_cleaned = zip(*all_items.values())

        for i in all_lines_cleaned:
            yield dict(zip(all_items.keys(), i))

    def _get_field_values_cleaned_for_importing(self, field_name, field_info, field_values, original_content_type):
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
                    item_chars = set(item)
                    if not item_chars <= self.settings.datetime_allowed_characters:
                        if original_content_type == 'xls' and item_chars <= FLOAT_ACCEPTABLE:
                            _format = original_content_type
                            continue
                        else:
                            raise ValueError(f"Datetime value of {item} in {field_name} has characters "
                                             "that are NOT defined in datetime_allowed_characters")
                    msg = (f"{field_name} has invalid datetime format for {item} "
                           f"that is not in {field_info.get('datetime_formats')}")
                    try:
                        _format = datetime_formats[-1]
                        strptime(item, _format)
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
            if _format == 'xls':
                def xls_date(x):
                    return xldate_as_datetime(float(x), self.xls_date_mode)
                field_values[:] = map(lambda x: None if x is None else xls_date(x), field_values)
            else:
                field_values[:] = map(lambda x: None if x is None else strptime(x, _format), field_values)
        return field_values

    def clean(self, content_type, path=None, content=None, sheet_names=None):
        """
        Clean the data for importing into database.
        content_type: Options: csv, xls, xls_xml
        path: (optional) The path to the file to open
        content: (optional) The content to be read. The content can be bytes, string, BytesIO or StringIO
        sheet_names: (optional) The sheet names from the Excel file to be considered.
                                If none provided, all sheets will be considered.
        """

        def _excel_contents_cleaned(content, func, sheet_names):
            results = func(content, sheet_names=sheet_names)
            csvs_chained = results.values()
            csvs_cleaned = map(lambda x: self.get_csv_data_cleaned(x, content_type), csvs_chained)
            return chain.from_iterable(csvs_cleaned)

        xls_contents_cleaned = partial(_excel_contents_cleaned, func=_xls_contents_to_csvs,
                                       sheet_names=sheet_names)
        xls_xml_contents_to_csvs = partial(_excel_contents_cleaned, func=_xls_xml_contents_to_csvs,
                                           sheet_names=sheet_names)

        solutions = {
            'csv': {'path': [self.get_csv_data_cleaned]},
            'xls': {'path': [get_file_content_bytes, xls_contents_cleaned]},
            'xls_xml': {'path': [get_file_content_bytes, xls_xml_contents_to_csvs]},
        }

        content_type = content_type.lower()
        try:
            content_type_solution = solutions[content_type]
        except KeyError as e:
            raise KeyError(f"content_type of {e} is invalid. Options are: {', '.join(solutions.keys())}") from None

        if path:
            key = 'path'
            value = path
        elif content:
            value = content
            if isinstance(content, str):
                key = 'content_str'
            elif isinstance(content, bytes):
                key = 'content_bytes'
            elif isinstance(content, io.StringIO):
                key = 'content_stringio'
            elif isinstance(content, io.BytesIO):
                key = 'content_bytesio'
            else:
                raise ValueError('Unrecognized content. It has to be either string or BytesIO or StringIO')
        else:
            raise ValueError('Either path or content need to be passed.')

        funcs = content_type_solution[key]
        for function in funcs:
            value = function(value)
        return value
