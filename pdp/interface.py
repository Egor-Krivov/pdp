from functools import partial
from collections import namedtuple

from .base import start_source, start_one2one_transformer, start_many2one_transformer, start_one2many_transformer

ComponentDescription = namedtuple('TransformerDescription', ['start', 'n_workers', 'buffer_size'])


def Source(iterable, *, buffer_size):
    return ComponentDescription(partial(start_source, iterable=iterable), 1, buffer_size)


def One2One(f, *, n_workers=1, buffer_size):
    return ComponentDescription(partial(start_one2one_transformer, f=f, n_workers=n_workers), n_workers, buffer_size)


def Many2One(chunk_size, *, buffer_size):
    return ComponentDescription(partial(start_many2one_transformer, chunk_size=chunk_size, n_workers=1),
                                1, buffer_size)


def One2Many(f, *, n_workers=1, buffer_size):
    return ComponentDescription(partial(start_one2many_transformer, f=f, n_workers=n_workers), n_workers, buffer_size)
