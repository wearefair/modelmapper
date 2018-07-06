NUMERIC_REMOVE = (',', '$', '%', '-')


def normalize_numberic_values(value, absolute=False):
    if value.startswith('(') and value.endswith(')'):
        value = value.strip('()')
        value = f'-{value}'
    for i in NUMERIC_REMOVE:
        if not(i == '-' and not absolute):
            value = value.replace(i, '')
    return value
