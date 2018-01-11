import numpy as np

from .pipeline import Pipeline
from .interface import TransformerDescription, Source, One2One


def product_generator(source: Source, *transformers: TransformerDescription):
    """Creates generator for pipeline output from provided pipeline parts.

    Examples
    --------
    >>> def make_power_batch_iter(n, power):
    >>>     source_data = list(range(n))
    >>>     return product_generator(Source(source_data, buffer_size=3),
    >>>                              One2One(lambda x: x ** power, buffer_size=3))
    >>>
    >>> for i in make_power_batch_iter(4, 2):
    >>>     print(i)
    0, 1, 4, 9
    """
    with Pipeline(source, *transformers) as p:
        yield from p


def combine_batches(inputs):
    return [np.array(o) for o in zip(*inputs)]


def pack_args(func):
    def new_func(inputs):
        return func(*inputs)

    return new_func
