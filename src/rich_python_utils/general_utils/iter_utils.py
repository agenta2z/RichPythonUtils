import uuid
from itertools import chain, islice
from typing import Iterator, Iterable, Union, List


def with_uuid(it, prefix='', suffix=''):
    yield from ((prefix + str(uuid.uuid4()) + suffix, x) for x in it)


def with_names(it, name_format: str = None, name_prefix='', name_suffix=''):
    if name_format is None or name_format == 'uuid':
        return with_uuid(it=it, prefix=name_prefix, suffix=name_suffix)
    else:
        for i, x in enumerate(it):
            yield name_prefix + name_format.format(i) + name_suffix, x

