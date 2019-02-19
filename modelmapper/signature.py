from collections import Mapping
from decimal import Decimal

import mmh3

BITS_MAP = {
    32: 'hash',
    64: 'hash64',
    128: 'hash128',
}


def generate_row_signature(row, model=None, ignore_fields=None, signature_size=64, x64arch=False):
    """ Generates a 64 bit hash of the given row
        Arguments:
            row: A dictionary or list of tuples
            model: Sqlalchemy model that coresponses to the given row
            ignore_fields: Fields to be ignored in the calcuation of the hash
            signature_size: Options between 32, 64 and 128 bytes
            x64arch: A murmur flag to optimize between x86 and x64 operating systems
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
    if signature_size >= 64:
        return get_hash_of_bytes(row_bytes, bits=signature_size, x64arch=x64arch)
    return get_hash_of_bytes(row_bytes, bits=signature_size)


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


def get_hash_of_bytes(item, bits=64, **kwargs):
    """Run selected  Murmur Hash function on given byte string"""
    hash_type = BITS_MAP.get(int(bits))
    if hash_type is None:
        raise ValueError(f'get_hash_of_bytes only accepts: 32, 64, or 128 as bits values. Given: {bits}')
    hash_value = getattr(mmh3, hash_type)(item, **kwargs)
    if isinstance(hash_value, tuple):
        return hash_value[0]
    return hash_value
