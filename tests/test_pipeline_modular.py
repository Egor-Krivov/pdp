import unittest
from abc import ABC, abstractmethod

import numpy as np

from pdp import Source, Many2One, One2One, One2Many, Pipeline, StopEvent


class CheckPipeline(ABC):
    def setUp(self):
        self.buffer_size = 10
        self.data_size = 10_000

    @abstractmethod
    def check_pass(self, n_workers):
        pass

    def test_pass(self):
        for n_workers in (1, 4, 10):
            with self.subTest(f'n_workers = {n_workers}'):
                self.check_pass(n_workers)


class TestPipelineOne2One(CheckPipeline, unittest.TestCase):
    def check_pass(self, n_workers):
        def f(x):
            return x ** 2

        data_in = np.random.randn(self.data_size)
        data_out_true = data_in ** 2

        pipeline = Pipeline(
            Source(data_in, buffer_size=self.buffer_size),
            One2One(f, n_workers=n_workers, buffer_size=self.buffer_size)
        )

        with pipeline:
            data_out = [*pipeline]

        if n_workers > 1:
            data_out_true = sorted(data_out_true)
            data_out = sorted(data_out)

        np.testing.assert_equal(data_out, data_out_true)

    def check_fail(self, n_workers):
        def f(x):
            raise ValueError()

        data_in = np.random.randn(self.data_size)

        pipeline = Pipeline(
            Source(data_in, buffer_size=self.buffer_size),
            One2One(f, n_workers=n_workers, buffer_size=self.buffer_size)
        )

        with self.assertRaises(StopEvent):
            with pipeline:
                data_out = [*pipeline]

    def test_fail(self):
        for n_workers in (1, 4, 10):
            with self.subTest(f'n_workers = {n_workers}'):
                self.check_fail(n_workers)


class TestPipelineOne2Many(CheckPipeline, unittest.TestCase):
    def check_pass(self, n_workers):
        def f(x):
            return [x + i for i in range(3)]

        data_in = np.random.randn(self.data_size)
        data_out_true = sum(map(f, data_in), [])

        pipeline = Pipeline(
            Source(data_in, buffer_size=self.buffer_size),
            One2Many(f, n_workers=n_workers, buffer_size=self.buffer_size)
        )

        with pipeline:
            data_out = [*pipeline]

        if n_workers > 1:
            data_out_true = sorted(data_out_true)
            data_out = sorted(data_out)

        np.testing.assert_equal(data_out, data_out_true)


class TestPipelineMany2One(unittest.TestCase):
    def setUp(self):
        self.buffer_size = 100
        self.data_size = 1_000
        self.chunk_size = 50

    def test_pass(self):
        data_in = np.random.randn(self.data_size)
        data_out_true = [data_in[i:i + self.chunk_size] for i in range(0, self.data_size, self.chunk_size)]

        pipeline = Pipeline(
            Source(data_in, buffer_size=self.buffer_size),
            Many2One(self.chunk_size, buffer_size=self.buffer_size)
        )

        with pipeline:
            data_out = [*pipeline]

        np.testing.assert_equal(data_out, data_out_true)
