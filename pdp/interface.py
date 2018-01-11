from functools import partial

from .backend import Backend, check_backend
from .base import start_source, start_one2one_transformer, \
    start_many2one_transformer, start_one2many_transformer


class Source:
    def __init__(self, iterable, *, backend=Backend.THREAD, buffer_size):
        self.backend = check_backend(backend)
        self.buffer_size = buffer_size
        self.start = partial(start_source, iterable=iterable, backend=backend)


class TransformerDescription:
    def __init__(self, backend, n_workers, buffer_size):
        self.backend = check_backend(backend)
        self.n_workers = n_workers
        self.buffer_size = buffer_size


class One2One(TransformerDescription):
    def __init__(self, f, *, backend=Backend.THREAD, n_workers=1, buffer_size):
        super().__init__(backend, n_workers, buffer_size)
        self.start = partial(start_one2one_transformer, f=f, backend=backend, n_workers=n_workers)


class Many2One(TransformerDescription):
    def __init__(self, chunk_size, *, backend=Backend.THREAD, buffer_size):
        super().__init__(backend, 1, buffer_size)
        self.start = partial(start_many2one_transformer, chunk_size=chunk_size,
                             backend=backend, n_workers=self.n_workers)


class One2Many(TransformerDescription):
    def __init__(self, f, *, backend=Backend.THREAD, n_workers=1, buffer_size):
        super().__init__(backend, n_workers, buffer_size)
        self.start = partial(start_one2many_transformer, f=f, backend=backend, n_workers=n_workers)
