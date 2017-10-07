import unittest
from itertools import product

import numpy as np

from pdp import Many2One, Source, Pipeline, THREAD, PROCESS


class TestPipelineMany2One(unittest.TestCase):
    def setUp(self):
        self.buffer_size = 100
        self.data_size = 1_000
        self.chunk_size = 50

    def check_pass(self, backend):
        data_in = np.random.randn(self.data_size)
        data_out_true = [data_in[i:i + self.chunk_size]
                         for i in range(0, self.data_size, self.chunk_size)]

        pipeline = Pipeline(
            Source(data_in, backend=backend, buffer_size=self.buffer_size),
            Many2One(self.chunk_size, backend=backend,
                     buffer_size=self.buffer_size)
        )

        with pipeline:
            data_out = [*pipeline]

        np.testing.assert_equal(data_out, data_out_true)

    def test_pass(self):
        for backend in [THREAD]:
            with self.subTest(f'backend = {backend}'):
                self.check_pass(backend)
