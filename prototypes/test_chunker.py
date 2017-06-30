from tqdm import tqdm

from bdp import Pipeline, Source, Chunker


if __name__ == '__main__':
    patients = [f'patient_{i}' for i in range(1000)]

    pipeline = Pipeline(
        Source(patients, buffer_size=40),
        Chunker(10, buffer_size=4)
    )

    with pipeline:
        for p in tqdm(pipeline):
            print(p)

