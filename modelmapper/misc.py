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


def write_toml(path, contents):
    with open(path, 'w') as the_file:
        the_file.write(pytoml.dumps(contents))


def read_csv_gen(path, **kwargs):
    _check_file_exists(path)
    encoding = kwargs.pop('encoding', 'utf-8-sig')
    with open(path, 'r', encoding=encoding) as csvfile:
        for i in csv.reader(csvfile, **kwargs):
            yield i


def named_tuple_to_compact_dict(named_tuple_obj):
    """
    Convert new style of Named Tuple with defaults into dictionary
    """
    _dict = named_tuple_obj._asdict()
    result = {}
    for k, v in _dict.items():
        if v != named_tuple_obj._field_defaults[k]:
            result[k] = v
        if isinstance(v, enum.Enum):
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
