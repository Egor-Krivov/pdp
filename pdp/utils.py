import numpy as np


def combine_batches(inputs):
    return [np.array(o) for o in zip(*inputs)]


def pack_args(func):
    def new_func(inputs):
        return func(*inputs)
    return new_func
