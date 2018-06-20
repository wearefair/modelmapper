import csv
import os
import sys
import yaml
import decimal
from collections import defaultdict, Counter
from decimal import Decimal
from collections import namedtuple

pyversion = float(sys.version[:3])
if pyversion < 3.6:
    sys.exit('ModelMapper requires Python 3.6 or later.')


FieldStats = namedtuple('FieldStats', 'counter max_int max_decimal_precision max_decimal_scale')


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


def load_yaml(path):
    _check_file_exists(path)
    with open(path, 'r') as the_file:
        contents = the_file.read()
    return yaml.load(contents)


def _read_csv_gen(path, **kwargs):
    _check_file_exists(path)
    encoding = kwargs.pop('encoding', 'utf-8-sig')
    with open(path, 'r', encoding=encoding) as csvfile:
        for i in csv.reader(csvfile, **kwargs):
            yield i


class Mapper:

    def __init__(self, setup_path):
        clean_later = ['field_name_full_conversion']
        convert_to_set = ['null_values', 'boolean_true', 'boolean_false']
        _settings = load_yaml(setup_path)
        dirname = os.path.dirname(setup_path)
        for item, value in _settings.items():
            if isinstance(value, str) and value[-4:] in {'.yml', 'yaml'}:
                path = os.path.join(dirname, value)
                result = load_yaml(path)
                result = result if result else []
                setattr(self, item, result)
        for item in clean_later:
            result = [[self._clean_it(i), self._clean_it(j)] for i, j in getattr(self, item)]
            setattr(self, item, result)
        for item in convert_to_set:
            _settings[item] = set(_settings.get(item, []))
        _settings['booleans'] = _settings['boolean_true'] | _settings['boolean_false']
        Settings = namedtuple('Settings', ' '.join(_settings.keys()))
        self.settings = Settings(**_settings)

    def _clean_it(self, name):
        item = name.lower().strip()
        for source, to_replace in self.field_name_part_conversion:
            item = item.replace(source, to_replace)
        return item.strip('_')

    def _get_clean_field_name(self, name):
        item = self._clean_it(name)
        for source, to_replace in self.field_name_full_conversion:
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

    def _analyze_field_values(self, items):
        max_int = 0
        max_decimal_precision = 0
        max_decimal_scale = 0
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
            result.append(HasString)
        return FieldStats(counter=Counter(result), max_int=max_int,
                          max_decimal_precision=max_decimal_precision,
                          max_decimal_scale=max_decimal_scale)
