import os

import sys
import logging
import importlib
from ast import literal_eval
from copy import deepcopy
from collections import defaultdict

from collections import namedtuple, Counter

from modelmapper.misc import read_csv_gen, load_toml, camel_to_snake
from modelmapper.slack import slack

OVERRIDES_FILE_NAME = "{}_overrides.toml"
COMBINED_FILE_NAME = "{}_combined.py"


class Base:

    logger = logging.getLogger(__name__)
    SETUP_PATH = None

    def __init__(self, setup_path=None, debug=False):
        self.setup_path = setup_path or getattr(self, 'SETUP_PATH', None)
        if self.setup_path is None:
            raise ValueError('setup_path needs to be passed to init or SETUP_PATH needs to be a class attribute.')
        if not self.setup_path.endswith('_setup.toml'):
            raise ValueError('The path needs to end with _setup.toml')
        self.debug = debug
        self.setup_dir = os.path.dirname(self.setup_path)
        sys.path.append(self.setup_dir)
        clean_later = ['field_name_full_conversion', 'ignore_fields_in_signature_calculation',
                       'identify_header_by_column_names', 'fields_to_be_encrypted', 'fields_to_be_scrubbed']
        convert_to_set = ['null_values', 'boolean_true', 'boolean_false', 'datetime_formats',
                          'ignore_lines_that_include_only_subset_of',
                          'ignore_fields_in_signature_calculation', 'identify_header_by_column_names']
        self._original_settings = load_toml(self.setup_path)['settings']
        self.settings = deepcopy(self._original_settings)
        for item in clean_later:
            self._clean_settings_items(item)
        for item in convert_to_set:
            self.settings[item] = set(self.settings.get(item, []))
        key = 'default_value_for_field_when_casting_error'
        self.settings[key] = self.settings.get(key) or r'{}'
        self.settings[key] = {self._clean_it(i): v for i, v in literal_eval(self.settings[key]).items()}
        slack_http_endpoint = self.settings['slack_http_endpoint']
        # attempt to get passed in value from ENV VAR, defaulting to passed in value if not present
        slack_http_endpoint = os.environ.get(slack_http_endpoint, slack_http_endpoint)
        self.settings['should_reprocess'] = self.settings.get('should_reprocess', False)
        self.settings['slack_http_endpoint'] = slack_http_endpoint
        self.settings['identifier'] = identifier = os.path.basename(self.setup_path).replace('_setup.toml', '')
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
        # self.settings['field_name_part_conversion'].insert(0, ['\n', '_']).insert(0, ['\r\n', '_'])
        _max_int = ((i, int(v)) for i, v in self.settings['max_int'].items())
        self.settings['max_int'] = dict(sorted(_max_int, key=lambda x: x[1]))
        Settings = namedtuple('Settings', ' '.join(self.settings.keys()))
        self.settings = Settings(**self.settings)
        self.questionable_fields = {}
        self.solid_decisions = {}
        self.failed_to_infer_fields = set()
        self.empty_fields = set()

    def _clean_it(self, item):
        conv = (self.settings['field_name_part_conversion'] if isinstance(self.settings, dict)
                else self.settings.field_name_part_conversion)
        item = item.replace('\r\n', '_').replace('\n', '_')
        item = camel_to_snake(item)
        for source, to_replace in conv:
            item = item.replace(source, to_replace)
        return item.strip('_')

    def _clean_settings_items(self, item):
        """
        Normalizes list or nested lists
        """
        if item not in self.settings:
            self.settings[item] = []
        try:
            first_value = self.settings[item][0]
        except IndexError:
            pass
        else:
            if isinstance(first_value, list):
                self.settings[item] = [[self._clean_it(i), self._clean_it(j)] for i, j in self.settings[item]]
            else:
                self.settings[item] = list(map(self._clean_it, self.settings[item]))

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

    def _get_combined_module(self):
        combined_module_str = self.settings.combined_file_name[:-3]
        return importlib.import_module(combined_module_str)

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
        reader = read_csv_gen(path,
                              identify_header_by_column_names=self.settings.identify_header_by_column_names,
                              cleaning_func=self._clean_it)
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
                        field_name = clean_names[i]
                    except IndexError:
                        raise ValueError("Your data might have new lines in the field names. "
                                         "Please fix that and try again.")
                    else:
                        if field_name not in self.settings.fields_to_be_scrubbed:
                            result[field_name].append(v)
        return result

    def slack(self, text):
        if self.settings.slack_username and \
           self.settings.slack_channel and \
           self.settings.slack_http_endpoint:
            return slack(
                text,
                username=self.settings.slack_username,
                channel=self.settings.slack_channel,
                slack_http_endpoint=self.settings.slack_http_endpoint
            )
