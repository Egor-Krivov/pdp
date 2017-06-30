# Prototype, showing how to share dataset between processes

from multiprocessing import Process
import numpy as np
import time

import dataset

size = dataset.size


def m():
    while True:
        begin = np.random.randint(size // 2)
        end = np.random.randint(size // 2, size)
        yield dataset.global_array[begin:end].mean()


def child(f):
    for v in f():
        print(v)


def main():
    n = 5
    processes = [Process(target=child, args=(m,))
                 for i in range(n)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
