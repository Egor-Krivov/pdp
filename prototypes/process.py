import unittest
from itertools import product

import numpy as np

from pdp import One2One, Source, Pipeline, PROCESS, THREAD

buffer_size = 4
data_size = 400

backend = PROCESS
n_workers = 1

def f(x):
    return x ** 2

data_in = np.random.randn(data_size)
data_out_true = data_in ** 2

pipeline = Pipeline(
    Source(data_in, backend=backend, buffer_size=buffer_size),
    One2One(f, backend=backend, n_workers=n_workers,
            buffer_size=buffer_size)
)

with pipeline:
    data_out = [*pipeline]

if n_workers > 1:
    data_out_true = sorted(data_out_true)
    data_out = sorted(data_out)

np.testing.assert_equal(data_out, data_out_true)
