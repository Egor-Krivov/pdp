import time
import unittest
from itertools import product
from functools import lru_cache

import numpy as np

from pdp import One2One, One2Many, Many2One, Source, Pipeline, THREAD, \
    pack_args, combine_batches


class Patient:
    def __init__(self, patient_id, x, y):
        self.patient_id = patient_id
        self.x = x
        self.y = y

    def __hash__(self):
        return hash(self.patient_id)


class TestPipeline(unittest.TestCase):
    def setUp(self):
        pass

    def test_pipeline(self, backend=THREAD):
        patient_ids = [f'patient_{i}' for i in range(5)]

        @lru_cache(len(patient_ids))
        def load_data(patient_id):
            x = np.random.randn(2, 240, 240, 240)
            y = np.random.randn(240, 240, 240)
            return Patient(patient_id, x, y)

        @lru_cache(len(patient_ids))
        def find_cancer(patient: Patient):
            cancer_mask = patient.x.sum(axis=0) > 0

            return patient.x, patient.y

        @pack_args
        def work(x, y):
            x.mean()
            return x, y

        @pack_args
        def first2(x, y):
            return y[:2]

        pipeline = Pipeline(
            Source(patient_ids * 10, backend=backend, buffer_size=10),
            One2One(load_data, backend=backend, buffer_size=10),
            One2One(find_cancer, backend=backend, buffer_size=100),
            One2One(work, backend=backend, buffer_size=10, n_workers=3),
            One2Many(first2, backend=backend, buffer_size=50),
            Many2One(5, backend=backend, buffer_size=10),
            One2One(combine_batches, backend=backend, buffer_size=3)
        )

        with pipeline:
            for y in pipeline:
                time.sleep(0.01)
