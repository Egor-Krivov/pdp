# Prototype, showing that threads effectively share memory with main thread
from threading import Thread
from queue import Queue
import numpy as np
import time

MBs = 4000
size = MBs // 4


def child(q: Queue):
    while True:
        array = q.get()
        time.sleep(0.2)
        array.mean()


def main():
    n = 5

    array = np.ones((size, 1024, 1024), dtype=np.float32)

    q = Queue(maxsize=10)

    processes = [Thread(target=child, args=(q,))
                 for i in range(n)]
    for p in processes:
        p.start()

    while True:
        begin = np.random.randint(size // 2)
        end = np.random.randint(size // 2, size)
        q.put(array[begin:end])
        #q.qsize()


if __name__ == "__main__":
    main()
