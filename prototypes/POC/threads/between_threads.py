# Prototype, showing that threads share memory effectively between each other
from threading import Thread
from queue import Queue
import numpy as np
import time

MBs = 20000
size = MBs // 4


def produce(array, q: Queue):
    while True:
        #array = q.get()
        time.sleep(0.2)
        q.put(array[size//4:(size*3)//4])

def process(q: Queue):
    while True:
        array = q.get()
        print(array.mean())


def main():
    n = 4

    array = np.ones((size, 1024, 1024), dtype=np.float32)

    q = Queue(maxsize=10)

    producers = [Thread(target=produce, args=(array, q,))
                 for i in range(n)]

    processors = [Thread(target=process, args=(q,))
                 for i in range(n)]

    for p in producers + processors:
        p.start()

    time.sleep(10)

if __name__ == "__main__":
    main()
