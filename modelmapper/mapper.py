import csv
import os
import yaml


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


class Mapper:

    def __init__(self, setup_path):

        self.settings = load_yaml(setup_path)
        dirname = os.path.dirname(setup_path)
        for item in ('field_name_part_conversion', 'field_name_full_conversion'):
            path = os.path.join(dirname, self.settings[item])
            value = load_yaml(path)
            print(value)
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

    def _read_csv_gen(path, **kwargs):
        _check_file_exists(path)
        kwargs['encoding'] = kwargs.get('encoding', 'utf-8-sig')
        with open(path, 'r', **kwargs) as csvfile:
            for i in csv.DictReader(csvfile):
                yield i

    def _verify_no_duplicate_clean_names(self, names_mapping):
        clean_names_mapping = {}
        for name, clean_name in names_mapping.items():
            if clean_name in clean_names_mapping:
                raise ValueError(f"'{name}' field has a collision with '{clean_names_mapping[clean_name]}'")
            else:
                clean_names_mapping[clean_name] = name
