import csv
import os
import sys
import yaml
from collections import defaultdict, Counter

pyversion = float(sys.version[:3])
if pyversion < 3.6:
    sys.exit('ModelMapper requires Python 3.6 or later.')


class FileNotFound(ValueError):
    pass


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

    CONVERSION_INFO = {'field_name_part_conversion', 'field_name_full_conversion'}

    def __init__(self, setup_path):
        clean_later = ['field_name_full_conversion']
        self.settings = load_yaml(setup_path)
        dirname = os.path.dirname(setup_path)
        for item, value in self.settings.items():
            if isinstance(value, str) and value[-4:] in {'.yml', 'yaml'}:
                path = os.path.join(dirname, value)
                result = load_yaml(path)
                result = result if result else []
                setattr(self, item, result)
        for item in clean_later:
            result = [[self._clean_it(i), self._clean_it(j)] for i, j in getattr(self, item)]
            setattr(self, item, result)

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
