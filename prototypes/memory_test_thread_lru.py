import time
from functools import lru_cache

import numpy as np
from numpy_work import work
from tqdm import tqdm

from bdp import Pipeline, LambdaTransformer, Source
from prototypes.data_loaders import Brats2017

data_loader = Brats2017('/mount/hdd/brats2017/processed')


@lru_cache()
def load_data(patient):
    mscan = data_loader.load_mscan(patient)
    segm = data_loader.load_segm(patient)

    return mscan, segm

if __name__ == '__main__':
    patients = list(data_loader.patients[:4]) * 1000
    pipeline = Pipeline(Source(patients, buffer_size=1, backend='thread'),
                        LambdaTransformer(load_data, n_workers=1,
                                          buffer_size=1000,
                                          backend='thread'))

    with pipeline:
        for msegm, mscan in pipeline:
            time.sleep(100)
            print('after sleep', flush=True)