import unittest
from itertools import product

import numpy as np

from pdp import One2Many, Source, Pipeline, THREAD, PROCESS


class TestPipelineOne2Many(unittest.TestCase):
    def setUp(self):
        self.buffer_size = 100
        self.data_size = 1_000

    def check_pass(self, backend, n_workers):
        def f(x):
            return [x + i for i in range(int(abs(x) * 3))]

        data_in = np.random.randn(self.data_size)
        data_out_true = sum(map(f, data_in), [])

        pipeline = Pipeline(
            Source(data_in, backend=backend, buffer_size=self.buffer_size),
            One2Many(f, backend=backend, n_workers=n_workers,
                    buffer_size=self.buffer_size)
        )

        with pipeline:
            data_out = [*pipeline]

        if n_workers > 1:
            data_out_true = sorted(data_out_true)
            data_out = sorted(data_out)

        np.testing.assert_equal(data_out, data_out_true)

    def test_pass(self):
        for n_workers, backend in product([1, 10], [THREAD]):
            with self.subTest(f'backend = {backend}; n_workers = {n_workers}'):
                self.check_pass(backend, n_workers)
