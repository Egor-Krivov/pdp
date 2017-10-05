# import time
# import unittest
# from itertools import product
# from functools import lru_cache
#
# import numpy as np
#
# from pdp import One2One, One2Many, Many2One, Source, Pipeline, THREAD, pack_args
#
#
# class Patient:
#     def __init__(self, patient_id, x, y):
#         self.patient_id = patient_id
#         self.x = x
#         self.y = y
#
#     def __hash__(self):
#         return hash(self.patient_id)
#
#
# class TestPipeline(unittest.TestCase):
#     def setUp(self):
#         pass
#
#     def check_pass(self, backend, n_workers):
#         patient_ids = [f'patient_{i}' for i in range(10)]
#
#         @lru_cache(len(patient_ids))
#         def load_data(patient_id):
#             x = np.random.randn(4, 240, 240, 240)
#             y = np.random.randn(240, 240, 240)
#             return x
#
#         @lru_cache(len(patient_ids))
#         def find_cancer(patient: Patient):
#             cancer_mask = patient.x.sum(axis=0) > 0
#
#             return patient.x, np.mean(patient.y[cancer_mask]) + patient.y
#
#
#
#
#             with pipeline:
#                 for x in pipeline:
#                     time.sleep(100)
#                     print('after sleep', flush=True)
#
#         def f(x):
#             return x ** 2
#
#         data_in = np.random.randn(self.data_size)
#         data_out_true = data_in ** 2
#
#         pipeline = Pipeline(
#             Source(patient_ids, backend=backend, buffer_size=100),
#             One2One(load_data, backend=backend, n_workers=1,
#                     buffer_size=100)
#             One2One
#         )
#
#         with pipeline:
#             data_out = [*pipeline]
#
#         if n_workers > 1:
#             data_out_true = sorted(data_out_true)
#             data_out = sorted(data_out)
#
#         np.testing.assert_equal(data_out, data_out_true)
#
#     def test_pass(self):
#         for n_workers, backend in product([1, 10], [THREAD]):
#             with self.subTest(f'backend = {backend}; n_workers = {n_workers}'):
#                 self.check_pass(backend, n_workers)
