# Shows that we can send array to the thread, also shows that threads can
# perform numpy instructions in parallel
from threading import Thread
from queue import Queue
import numpy as np
import time

MBs = 4000
size = MBs // 4


def child(array):
    array = array[0]
    while True:
        begin = np.random.randint(size//2)
        end = np.random.randint(size//2, size)
        print(array[begin:end].mean())


def main():
    n = 5

    array = np.ones((size, 1024, 1024), dtype=np.float32)


    processes = [Thread(target=child, args=([array],))
                 for i in range(n)]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
