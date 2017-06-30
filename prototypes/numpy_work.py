import time
import numpy as np

n = 1500


def work(z=None):
    x = np.random.randn(n, n)
    y = np.linalg.inv(x)
    return y

if __name__ == '__main__':
    np.random.seed(12)

    start = time.time()
    work()
    end = time.time()

    print(end - start)
