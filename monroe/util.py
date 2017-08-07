import re
from collections import Iterable


def aslist(sequence):
    if isinstance(sequence, str):
        return re.split('[\s,]+', sequence)
    elif isinstance(sequence, Iterable):
        return list(sequence)
    raise TypeError('sequence must be str or list-like')
