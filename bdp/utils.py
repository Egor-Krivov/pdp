def pack_args(func):
    def new_func(inputs):
        return func(*inputs)
    return new_func
