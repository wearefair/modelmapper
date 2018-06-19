import csv
import os
import sys
import yaml
from collections import defaultdict

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

    def __init__(self, setup_path):

        self.settings = load_yaml(setup_path)
        dirname = os.path.dirname(setup_path)
        for item in ('field_name_part_conversion', 'field_name_full_conversion'):
            path = os.path.join(dirname, self.settings[item])
            value = load_yaml(path)
            value = value if value else []
            setattr(self, item, value)

    def _get_clean_field_name(self, name):
        item = name.lower()

        for source, to_replace in self.field_name_full_conversion:
            if item == source:
                item = to_replace
                break

        for source, to_replace in self.field_name_part_conversion:
            item = item.replace(source, to_replace)
        return item.strip('_')

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
        result = defaultdict(set)
        reader = _read_csv_gen(path)
        names = next(reader)
        name_mapping = self._get_all_clean_field_names_mapping(names)
        self._verify_no_duplicate_clean_names(name_mapping)
        clean_names = list(name_mapping.values())
        for line in reader:
            for i, v in enumerate(line):
                result[clean_names[i]].add(v)
        return result
