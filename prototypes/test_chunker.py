from tqdm import tqdm

from pdp import Pipeline, Source, Many2One


if __name__ == '__main__':
    patients = [f'patient_{i}' for i in range(1000)]

    pipeline = Pipeline(
        Source(patients, buffer_size=40),
        Many2One(20, buffer_size=4)
    )

    with pipeline:
        for p in tqdm(pipeline):
            print(p)

