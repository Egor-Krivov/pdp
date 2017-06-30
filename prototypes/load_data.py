from tqdm import tqdm

from data_loaders import Brats2017

data_loader = Brats2017('/mount/hdd/brats2017/processed')


def load_data(patient):
    mscan = data_loader.load_mscan(patient)
    segm = data_loader.load_segm(patient)

    return mscan, segm


if __name__ == '__main__':
    patients = data_loader.patients

    for p in tqdm(patients):
        mscan, segm = load_data(p)
