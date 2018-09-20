def combine_batches(inputs):
    return tuple(zip(*inputs))


def unpack_args(func):
    def new_func(inputs):
        return func(*inputs)

    return new_func
