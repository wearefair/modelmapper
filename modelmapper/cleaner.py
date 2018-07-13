"""
Functionality for cleaning the data for importing into tables that mapper has created.
"""
import io
import datetime

from itertools import chain
from functools import partial
from decimal import Decimal
from modelmapper.normalization import normalize_numberic_values
from modelmapper.mapper import Mapper, ONE_HUNDRED, SqlalchemyFieldType, INTEGER_SQLALCHEMY_TYPES
from modelmapper.excel import _xls_contents_to_csvs, _xls_xml_contents_to_csvs


def get_file_content_bytes(path):
    with open(path, 'rb') as the_file:
        return the_file.read()


def get_file_content_string(path):
    with open(path, 'r') as the_file:
        return the_file.read()


class Cleaner(Mapper):

    def get_csv_data_cleaned(self, path):
        """
        Gets csv data cleaned. Use it only if you know you have a CSV path or stringIO with CSV content.
        Otherwise use the clean method in this class.
        """
        combined_module = self._get_combined_module()
        model_info = combined_module.FIELDS

        all_items = self._get_all_values_per_clean_name(path)
        for field_name, field_values in all_items.items():
            field_info = model_info[field_name]
            self._get_field_values_cleaned_for_importing(field_name, field_info, field_values)

        # transposing
        all_lines_cleaned = zip(*all_items.values())

        for i in all_lines_cleaned:
            yield dict(zip(all_items.keys(), i))

    def _get_field_values_cleaned_for_importing(self, field_name, field_info, field_values):
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

    def _excel_contents_cleaned(self, func, content, sheet_names):
        results = func(content, sheet_names=sheet_names)
        csvs_chained = results.values()
        csvs_cleaned = map(self.get_csv_data_cleaned, csvs_chained)
        return chain.from_iterable(csvs_cleaned)

    def clean(self, content_type, path=None, content=None, sheet_names=None):
        """
        Clean the data for importing into database.
        content_type: Options: csv, xls, xls_xml
        path: (optional) The path to the file to open
        content: (optional) The content to be read. The content can be bytes, string, BytesIO or StringIO
        sheet_names: (optional) The sheet names from the Excel file to be considered.
                                If none provided, all sheets will be considered.
        """

        xls_contents_cleaned = partial(self._excel_contents_cleaned, func=_xls_contents_to_csvs,
                                       sheet_names=sheet_names)
        xls_xml_contents_to_csvs = partial(self._excel_contents_cleaned, func=_xls_xml_contents_to_csvs,
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

    #     if path:
    #         if content_type == 'csv':
    #             return self.get_csv_data_cleaned(path)
    #         else:
    #             with open(path, 'rb') as the_file:
    #                 file_contents = the_file.read()
    #                 results = excel_contents_to_csvs(file_contents, sheet_names=sheet_names)
    #                 return chain.from_iterable(results.values())
    #     elif content:
    #         if isinstance(content, io.StringIO):


    # dirpath, basename = os.path.split(path)
    # basename = basename.split('.')[0]
    #     for sheet_name, csv_file in results.items():
    #         result_content = results['Sheet1'].read()
    #         new_file_name = f'{basename}__{sheet_name}.csv'
    #         new_file_name = os.path.join(dirpath, new_file_name)
    #         with open(new_file_name, 'w') as the_file:
    #             the_file.write(result_content)
    #         print(f'exported {new_file_name}')


    #     funcs = []
    #     if isinstance(path, (bytes, str)):
    #         if not path.endswith('.csv'):
    #             funcs.append()
