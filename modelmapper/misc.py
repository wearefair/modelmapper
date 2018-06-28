import csv
import os
import enum
import logging
import pprint
import pytoml

logger = logging.getLogger(__name__)


START_LINE = "    # --------- THE FOLLOWING FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. {} --------\n"
END_LINE = "    # --------- THE ABOVE FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. {} --------\n"


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


def update_file_chunk_content(path, code, identifier='', start_line=None, end_line=None):
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

    with open(path, 'w') as model_file:
        model_file.write("".join(new_model_lines))


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
        # if it is not the default value and not enum
        if v != named_tuple_obj._field_defaults[k] and not isinstance(v, enum.Enum):
            result[k] = v
        if include_enums and isinstance(v, enum.Enum):
            result[k] = v.value
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
