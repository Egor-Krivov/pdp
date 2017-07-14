import time
from functools import lru_cache

import numpy as np
from numpy_work import work
from tqdm import tqdm

from pdp import Pipeline, LambdaTransformer, Source
from data_loaders import Brats2017

data_loader = Brats2017('/mount/export/brats2017/processed')


@lru_cache()
def load_data(patient):
    mscan = data_loader.load_mscan(patient)
    segm = data_loader.load_segm(patient)

    return mscan, segm


if __name__ == '__main__':
    patients = list(data_loader.patients)
    pipeline = Pipeline(Source(patients, buffer_size=5),
                        LambdaTransformer(load_data, buffer_size=10),
                        LambdaTransformer(work, buffer_size=10))

    with pipeline:
        for s in tqdm(pipeline):
            s.mean()
