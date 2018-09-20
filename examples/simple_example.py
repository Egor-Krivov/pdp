import time

import numpy as np

from pdp import One2One, Many2One, Source, Pipeline, unpack_args, combine_batches

patient_ids = [f'patient_{i}' for i in range(5)] * 100


def load_data(patient_id):
    # Could be replaced with IO
    x = np.random.randn(2, 100, 100, 100)
    y = np.random.randn(100, 100, 100)
    return x, y


# Pack args for all functions that have multiple arguments
@unpack_args
def augment(x, y):
    x = x * (1 + 0.1 * np.random.randn(*x.shape))
    return x, y


batch_size = 128

pipeline = Pipeline(
    Source(patient_ids * 10, buffer_size=10),
    One2One(load_data, buffer_size=10),
    One2One(augment, buffer_size=10, n_workers=3),
    Many2One(batch_size, buffer_size=10),
    One2One(combine_batches, buffer_size=3)
)

with pipeline:
    for x, y in pipeline:
        time.sleep(0.1)
        del x, y
