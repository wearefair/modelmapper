import csv
import os
import enum
import pytoml


class FileNotFound(ValueError):
    pass


def _check_file_exists(path):
    if not os.path.exists(path):
        raise FileNotFound(f'{path} does not exist')


def load_toml(path):
    _check_file_exists(path)
    with open(path, 'r') as the_file:
        contents = the_file.read()
    return pytoml.loads(contents)


def write_toml(path, contents, auto_generated_from=None):
    dump = pytoml.dumps(contents)
    if auto_generated_from:
        dump = f"# NOTE: THIS FILE IS AUTO GENERATED BASED ON THE ANALYSIS OF {auto_generated_from}.\n# DO NOT MODIFY THIS FILE DIRECTLY.\n{dump}"
    with open(path, 'w') as the_file:
        the_file.write(dump)


def read_csv_gen(path, **kwargs):
    _check_file_exists(path)
    encoding = kwargs.pop('encoding', 'utf-8-sig')
    with open(path, 'r', encoding=encoding) as csvfile:
        for i in csv.reader(csvfile, **kwargs):
            yield i


def named_tuple_to_compact_dict(named_tuple_obj, include_enums=False):
    """
    Convert new style of Named Tuple with defaults into dictionary
    """
    _dict = named_tuple_obj._asdict()
    result = {}
    for k, v in _dict.items():
        if v != named_tuple_obj._field_defaults[k] and not isinstance(v, enum.Enum):
            result[k] = v
        if include_enums and isinstance(v, enum.Enum):
            result[k] = v.value
        elif isinstance(v, set):
            result[k] = list(v)
    return result


_ESCAPE_ACCEPTABLED = set('1234567890qwertyuiopasdfghjklzxcvbnm')


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
    dicts.sort(key=comparison_func)
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
