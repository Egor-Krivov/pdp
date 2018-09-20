from .interface import One2One, Many2One
from .utils import combine_batches


def make_batch_blocks(batch_size, buffer_size):
    return (Many2One(chunk_size=batch_size, buffer_size=3),
            One2One(combine_batches, buffer_size=buffer_size))
