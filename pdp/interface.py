from collections import namedtuple

Transformer = namedtuple('Transformer',
                         ['f', 'backend', 'n_workers', 'buffer_size'])


def make_one2one(f, backend='thread', n_workers=1, buffer_size=1):
    return Transformer(f, backend, n_workers, buffer_size)


def make_one2many(chunk_size, backend='thread', n_workers=1, buffer_size=1):
    return Transformer(chunk_size, backend, n_workers, buffer_size)


def make_many2one(extract, backend='thread', n_workers=1, buffer_size=1):
    return Transformer(extract, backend, n_workers, buffer_size)




