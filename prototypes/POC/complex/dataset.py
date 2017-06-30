# Prototype, showing how to use tensorflow in multiple processes
import numpy as np
from tensorflow.examples.tutorials.mnist import input_data

mnist = input_data.read_data_sets("MNIST_data/", reshape=False,
                                  validation_size=0)

img_size = (28, 28)
n_classes = 10

imgs = np.array(mnist.train.images.reshape((-1, 1, *img_size)) / 255,
                dtype=np.float32)
y = np.array(mnist.train.labels, dtype=np.int32)

print(imgs.shape, flush=True)

expand_coeff = 100
imgs = np.repeat(imgs, expand_coeff, 0)
y = np.repeat(y, expand_coeff, 0)


def make_batch_iter(x, y, batch_size, shuffle=False):
    n = len(x)
    idx = np.arange(n)
    if shuffle:
        np.random.shuffle(idx)

    for i in range(0, n, batch_size):
        x_batch = x[i:i + batch_size]
        y_batch = y[i:i + batch_size]
        yield np.array(x_batch, np.float32), np.array(y_batch)

while True:
    pass
