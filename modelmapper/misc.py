import re
import clevercsv as csv
import io
import os
import string
import enum
import logging
import pprint
import pytoml
import cchardet
from itertools import chain
from string import ascii_lowercase, digits

logger = logging.getLogger(__name__)

_ESCAPE_ACCEPTABLED = frozenset(ascii_lowercase + digits)

current_dir = os.path.dirname(os.path.abspath(__file__))

START_LINE = "    # --------- THE FOLLOWING FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. {} --------\n"
END_LINE = "    # --------- THE ABOVE FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. {} --------\n"
CHUNK_SIZE = 2048  # The chunk needs to be big enough that covers a couple of rows of data.


valid_chars_for_string = set(string.ascii_letters.lower())
valid_chars_for_integer = set(string.digits)

MAX_DATE_INTEGER = 30001230125959  # year 3000, December 30 12:59:59
MIN_DATE_INTEGER = 10101  # year 1, month 1, day 1

_days = r"\b(?:mon(?:day)?|tue(?:sday)?|tues(?:day)?|wed(?:nesday)?|thu(?:rsday)?|thur(?:sday)?|fri(?:day)?|sat(?:urday)?|sun(?:day))"
_months = r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|(?:nov|dec)(?:ember)?)"
_either_space_or_nothing = r"(?!\S)"

MONTH_OR_DAY_REGEX = re.compile(r"(" + _months + _either_space_or_nothing + r")|(" + _days + _either_space_or_nothing + r")")

_camel_to_snake_regex1 = re.compile(r'(.)([A-Z][a-z]+)')
_camel_to_snake_regex2 = re.compile(r'([a-z0-9])([A-Z])')

def camel_to_snake(name):
    """
    Based on https://stackoverflow.com/a/1176023/1497443
    """
    name = _camel_to_snake_regex1.sub(r'\1_\2', name)
    return _camel_to_snake_regex2.sub(r'\1_\2', name).lower().strip()



def add_strings_and_integers_to_set(item):
    item = item.copy()
    item |= valid_chars_for_string
    item |= valid_chars_for_integer
    item.add(' ')
    return item


class FileNotFound(ValueError):
    pass


def _check_file_exists(path):
    if not os.path.exists(path):
        raise FileNotFound(f'{path} does not exist')


def convert_dict_keys(obj, keys, func):
    if isinstance(keys, (str, bytes)):
        convert_dict_key(obj, keys, func)
    else:
        for key in keys:
            convert_dict_key(obj, key, func)


def convert_dict_key(obj, key, func):
    if isinstance(obj, dict):
        for child_key, child in obj.items():
            if child_key == key:
                try:
                    obj[child_key] = func(child)
                except Exception as e:
                    logger.error(f'failed to convert key {key} with the value of {child} into {func.__name__}: {e}')
            elif isinstance(child, dict):
                convert_dict_key(child, key, func)


def convert_dict_item_type(obj, _type, func):
    if isinstance(obj, dict):
        for child_key, child in obj.items():
            if isinstance(child, dict):
                convert_dict_item_type(child, _type, func)
            elif isinstance(child, _type):
                try:
                    obj[child_key] = func(child)
                except Exception as e:
                    logger.error(f'failed to convert key {child_key} with the value of {child} into {func.__name__}: {e}')
            else:
                convert_dict_item_type(child, _type, func)
    elif isinstance(obj, list):
        obj[:] = list(map(lambda x: func(x) if isinstance(x, _type) else x, obj))


def load_toml(path, keys_to_convert_to_set=None):
    _check_file_exists(path)
    with open(path, 'r') as the_file:
        contents = the_file.read()
    loaded = pytoml.loads(contents)
    if keys_to_convert_to_set:
        convert_dict_keys(loaded, keys=keys_to_convert_to_set, func=set)
    return loaded


def write_toml(path, contents, auto_generated_from=None, keys_to_convert_to_list=None, types_to_str=(enum.Enum,)):
    convert_dict_item_type(contents, _type=types_to_str, func=str)
    if keys_to_convert_to_list:
        convert_dict_keys(contents, keys=keys_to_convert_to_list, func=list)
    dump = pytoml.dumps(contents)
    if auto_generated_from:
        dump = f"# NOTE: THIS FILE IS AUTO GENERATED BASED ON THE ANALYSIS OF {auto_generated_from}.\n# DO NOT MODIFY THIS FILE DIRECTLY.\n{dump}"
    with open(path, 'w') as the_file:
        the_file.write(dump)
    return dump


def write_settings(path, contents):
    contents = contents if 'settings' in contents else {'settings': contents}
    template_setup_path = os.path.join(current_dir, 'templates/setup_template.toml')
    with open(template_setup_path, 'r') as the_file:
        lines = the_file.readlines()
    comments = {}
    for line in lines:
        if '=' in line:
            parts = line.split('=')
            comments[parts[0]] = parts[-1].split('#')[-1].strip()
    dump = pytoml.dumps(contents)
    dump_lines = dump.split('\n')
    for i, line in enumerate(dump_lines):
        for key, comment in comments.items():
            if line.startswith(key):
                dump_lines[i] = f'{line}  # {comment}'
                break
    result = '\n'.join(dump_lines)
    with open(path, 'w') as the_file:
        the_file.write(result)
    return result

def write_full_python_file(path, variable_name, contents, header=''):
    """
    Rewrites a whole Python file
    """
    content_lines = [
        "# flake8: noqa",
        "# NOTE: THIS FILE IS AUTO GENERATED BY MODEL MAPPER BASED ON CSV DATA. DO NOT MODIFY THE FILE.\n",
        header,
        "",
        f"{variable_name} = {pprint.pformat(contents, indent=4)}\n",
        ]

    with open(path, 'w') as the_file:
        the_file.write("\n".join(content_lines))


def update_file_chunk_content(path, code, identifier='', start_line=None, end_line=None, check_only=False):
    """
    Rewrites a chunk of a file only between the start and end lines.
    """
    start_line = start_line or START_LINE.format(identifier)
    end_line = end_line or END_LINE.format(identifier)

    with open(path, 'r') as model_file:
        model_lines = model_file.readlines()

    new_model_lines = []
    inside = False
    is_block_added = False
    for line in model_lines:
        if line == start_line:
            inside = True
        elif line == end_line:
            inside = False

        if inside:
            if not is_block_added:
                new_model_lines.append(line)
                new_model_lines.extend(code)
                is_block_added = True
        else:
            new_model_lines.append(line)

    if not is_block_added:
        raise ValueError(f'{path} is not properly setup. We can not find the start line and end line indicators.\nPlease add the following lines at the proper places in that file\n{start_line}\n{end_line}')

    if not check_only:
        with open(path, 'w') as model_file:
            model_file.write("".join(new_model_lines))

def analyze_csv_format(iostream, **kwargs):
    """From csv fileobj detects delimiter, raw headers, and whether or not a header is contained in csv.

    Args:
        iostream (_io.TextIOWrapper): fileobj containing csv data.
        **kwargs (dict): keyword arguments for csv.reader().

    Returns:
        str: delimiter used by csv
        bool: does the csv fileobj contain headers?
        set: raw headers passed by user in setup.toml

    Raises:        csv.Error : Malformed csv.

    """
    raw_headers = kwargs.pop('identify_header_by_column_names', None)
    delimiter = kwargs.pop('delimiter', None)
    sample = iostream.read(CHUNK_SIZE)
    sniffer = csv.Sniffer()
    has_header = True

    if delimiter is None:
        try:
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
        except csv.Error as e:
            raise csv.Error('csv.Sniffer() could not detect the dialect of your file',
                            'Please specify the csv_delimiter in your setup.toml.',
                            str(e)) from None

    if not raw_headers:
        try:
            has_header = sniffer.has_header(sample)
        except csv.Error as e:
            logger.exception(f"sniffing csv header failed: {e}")
            has_header = False

    # reset the file pointer to beginning
    iostream.seek(0)

    return delimiter, has_header, raw_headers


def do_nothing(x):
    return x


def find_header(iostream, **kwargs):
    """From an open csv file descriptor, locates header and returns iterable data from there.

    Args:
        iostream (_io.TextIOWrapper): fileobj containing csv data.
        **kwargs (dict): keyword arguments for csv.reader().

    Returns:
        iterable: csv data started from the head
    """
    delimiter, has_header, raw_headers = analyze_csv_format(iostream, **kwargs)

    if not raw_headers:
        # user did not provide the headers but sniffer found some
        if has_header:
            return csv.reader(iostream, delimiter=delimiter)
        # no user provided headers and sniffer could not find any.
        # we cannot locate the headers
        else:
            raise csv.Error('csv.Sniffer() could not detect file headers and modelmapper was not provided the raw headers',
                            'Please add a subset of the raw headers to the `identify_header_by_column_names` key in your setup.toml.')

    records = csv.reader(iostream, delimiter=delimiter)
    # find headers
    cleaning_func = kwargs.pop('cleaning_func', None) or do_nothing
    for record in records:
        if record and raw_headers <= set(map(cleaning_func, record)):  # finding if the raw headers are subset of the record
            return chain([record], records)  # chaining the header line (record)
    raise ValueError('Could not find the headers line. Please double check the identify_header_by_column_names that were provided.')


def read_csv_gen(path_or_stringio, **kwargs):
    """
    Takes a path_or_stringio to a file or a StringIO object and creates a CSV generator
    """
    if isinstance(path_or_stringio, (str, bytes)):
        _check_file_exists(path_or_stringio)

        with open(path_or_stringio, 'rb') as csvfile:
            content = csvfile.read()
            content = decode_bytes(content)
            # the sniffer has problems when only \r is used for new line
            content = content.replace('\r\n', '\n').replace('\r', '\n')
            content_io = io.StringIO(content)
            for row in find_header(content_io, **kwargs):
                yield row
    elif isinstance(path_or_stringio, io.StringIO):
        for row in find_header(path_or_stringio, **kwargs):
            yield row
    else:
        raise TypeError('Either a path to the file or StringIO object needs to be passed.')


def named_tuple_to_compact_dict(named_tuple_obj, include_enums=False):
    """
    Convert new style of Named Tuple with defaults into dictionary
    """
    _dict = named_tuple_obj._asdict()
    result = {}
    for k, v in _dict.items():
        # if it is not the default value and not enum
        if v != named_tuple_obj._field_defaults[k] and not isinstance(v, enum.Enum):
            result[k] = v
        if include_enums and isinstance(v, enum.Enum):
            result[k] = v.value
    return result


def escape_word(word):
    """
    Use this to create consistent escaped words
    """
    result = []
    last_i = None
    for i in word.lower().strip():
        if i in _ESCAPE_ACCEPTABLED:
            result.append(i)
        else:
            i = '_'
            if i != last_i:
                result.append(i)
        last_i = i
    return ''.join(result).strip('_')


def get_combined_dict(comparison_func, *dicts):
    dicts = list(dicts)
    if comparison_func:
        dicts.sort(key=comparison_func, reverse=True)
    result = {}
    while dicts:
        item = dicts.pop()
        for k, v in item.items():
            if k in result and isinstance(v, set):
                result[k] |= v
            elif k in result and isinstance(v, list):
                result[k].extend(v)
            else:
                result[k] = v
    return result


def _validate_file_has_start_and_end_lines(user_input, path, identifier):
    try:
        update_file_chunk_content(path=path, code=[], identifier=identifier, check_only=True)
    except ValueError as e:
        print(e)
        return False
    else:
        return True


class cached_property:  # NOQA
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.func.__name__] = self.func(instance)
        return res


class DefaultList(list):
    """
    List with default value.
    It lets you set an index that bigger than the list's current length.
    Kind of how shitty Javascript arrays work. We need it to deal with Excel files.

    aa = DefaultList()
    >>> aa.append(1)
    >>> aa[2] = 3
    >>> print(aa)
    [1, None, 3]

    >>> aa = DefaultList([1, 2, 3])
    >>> aa[6] = 'Nice, I like it.'
    >>> print(aa)
    [1, 2, 3, None, None, None, 'Nice, I like it.']

    >>> aa = DefaultList([1, 2, 3], default='yes')
    >>> aa[5] = 'Nice, I like it.'
    >>> print(aa)
    [1, 2, 3, 'yes', 'yes', 'Nice, I like it.']

    >>> items = DefaultList([1, 2, 3], default=dict)
    >>> items[5]['key'] = 'Nice, I like it.'
    >>> print(items)
    [1, 2, 3, {}, {}, {'key': 'Nice, I like it.'}]
    """
    def __init__(self, *args, **kwargs):
        default = kwargs.pop('default', None)
        self.default = default if callable(default) else lambda: default
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        current_len = len(self)
        if key == current_len:
            self.append(value)
        elif key < current_len:
            super().__setitem__(key, value)
        elif key > current_len:
            diff = key - current_len
            for i in range(diff):
                self.append(self.default())
            self.append(value)

    def __getitem__(self, key):
        try:
            value = super().__getitem__(key)
        except IndexError:
            value = self.default()
            self.__setitem__(key, value)
        return value


def generator_chunker(gen, chunk_size):
    """
    Create generator to yield lists of items at a time of size: chunk_size 
    """
    try:
        while True:
            chunk = []
            while len(chunk) < chunk_size:
                chunk.append(next(gen))
            yield chunk
    except StopIteration:
        yield chunk



def generator_updater(data_gen, **kwargs):
    """
    Update each item in a generator with kwargs.
    Expects the generator to be yielding dictionaries.
    """
    for item in data_gen:
        item.update(**kwargs)
        yield item

BIG_ENDIAN_HEADER = b'\xfe\xff'
LITTLE_ENDIAN_HEADER = b'\xff\xfe'
UTF8_HEADER = b'\xef\xbb\xbf'

def decode_bytes(content):
    try:
        if content.startswith(UTF8_HEADER):
            return content.decode('utf-8-sig')
        if content.startswith(BIG_ENDIAN_HEADER):
            return content[len(BIG_ENDIAN_HEADER):].decode('utf-16-be')
        if content.startswith(LITTLE_ENDIAN_HEADER):
            return content[len(LITTLE_ENDIAN_HEADER):].decode('utf-16-le')
        return content.decode('utf-8')
    except Exception:
        encoding_info = cchardet.detect(content)
        logger.info(f"Encoding detected to be {encoding_info['encoding']} with confidence of {encoding_info['confidence']}.")
        return content.decode(encoding_info['encoding'])
