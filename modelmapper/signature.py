from collections import Mapping
from decimal import Decimal

import mmh3


def generate_row_signature(row, model=None, ignore_fields=None):
    """ Generates a 64 bit hash of the given row
        Arguments:
            row: A dictionary or list of tuples
            model: Sqlalchemy model that coresponses to the given row
            ignore_fields: Fields to be ignored in the calcuation of the hash
        Returns:
            Integer: the hash value of the given row
    """
    if isinstance(row, list):
        for each in row:
            if not isinstance(each, tuple) or len(each) != 2:
                raise TypeError("row must either be a dictionary or a list of tuples each with a size of 2")
        row_dict = dict(row)
    elif isinstance(row, Mapping):
        row_dict = row
    else:
        raise TypeError('Row needs to be a list of tuples or a dictionary')

    default_dropped_row = drop_model_defaults(row_dict, model)
    normalized_row = normalize_decimal_columns(default_dropped_row)
    sorted_row = sort_row_values(normalized_row)
    row_bytes = get_byte_str_of_row(sorted_row, ignore_fields)
    return get_hash_of_bytes(row_bytes)


def normalize_decimal_columns(row):
    """Remove trailing zeros from Decimal fields"""
    normalized_row = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            normalized_row[k] = v.normalize()
        else:
            normalized_row[k] = v
    return normalized_row


def drop_model_defaults(row, model):
    """Remove all columns from the row that are equal to their sqlalchemy default"""
    if model is None:
        new_row = row.copy()
        return new_row
    new_row = {}
    for column in model.__table__.columns:
        if column.description not in row.keys():
                continue
        elif column.default is None or row[column.description] != column.default.arg:
            new_row[column.description] = row[column.description]
    return new_row


def sort_row_values(row, ignore_fields=None):
    """Sort given row by key value"""
    return sorted(row.items(), key=lambda t: str(t[0]))


def get_byte_str_of_row(row, ignore_fields=[]):
    """Format byte string to be hashed"""
    if isinstance(row, list):
        items = row
    elif isinstance(row, Mapping):
        items = row.items()
    row_bytes = b','.join(
        [str(f'{k}:{v}').encode('utf-8') for k, v in items if k not in ignore_fields and v is not None])
    return row_bytes


def get_hash_of_bytes(item):
    return mmh3.hash64(item, x64arch=False)[0]
