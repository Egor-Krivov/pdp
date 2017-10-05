import unittest
import time
from queue import Queue as ThreadQueue
from threading import Thread
from threading import Event as ThreadEvent

import numpy as np

from .base import InterruptableQueue, StopEvent, start_one2one_transformer
from .backend import THREAD

DEFAULT_LOOP_TIMEOUT = 0.02


def set_event_after_timeout(event, timeout):
    def target():
        time.sleep(timeout)
        event.set()

    Thread(target=target).start()


class TestInterruptableQueue(unittest.TestCase):
    def setUp(self):
        self.maxsize = 10
        self.loop_timeout = DEFAULT_LOOP_TIMEOUT
        self.wait_timeout = 7.5 * self.loop_timeout
        self.receive_timeout = 0.5 * self.loop_timeout

        self.stop_event = ThreadEvent()
        self.q = InterruptableQueue(ThreadQueue(self.maxsize),
                                    self.loop_timeout, self.stop_event)

    def test_get(self):
        thread = Thread(target=self.q.get)
        thread.start()
        self.assertTrue(thread.is_alive())
        set_event_after_timeout(
            event=self.stop_event,
            timeout=self.wait_timeout + self.receive_timeout
        )
        self.assertTrue(thread.is_alive())
        time.sleep(self.wait_timeout)
        self.assertTrue(thread.is_alive())
        time.sleep(self.receive_timeout * 2)
        self.assertFalse(thread.is_alive())

    def test_put(self):
        for i in range(self.maxsize):
            self.q.put(i)

        thread = Thread(target=self.q.put, args=(-1,))
        thread.start()

        self.assertTrue(thread.is_alive())
        set_event_after_timeout(
            event=self.stop_event,
            timeout=self.wait_timeout + self.receive_timeout
        )
        self.assertTrue(thread.is_alive())
        time.sleep(self.wait_timeout)
        self.assertTrue(thread.is_alive())
        time.sleep(self.receive_timeout * 2)
        self.assertFalse(thread.is_alive())


class testOne2One(unittest.TestCase):
    def setUp(self):
        self.buffer_size = 20
        self.loop_timeout = DEFAULT_LOOP_TIMEOUT
        self.stop_event = ThreadEvent()
        self.q_in = InterruptableQueue(ThreadQueue(self.buffer_size),
                                       self.loop_timeout, self.stop_event)
        self.q_out = InterruptableQueue(ThreadQueue(self.buffer_size),
                                        self.loop_timeout, self.stop_event)

    def tearDown(self):
        self.q_in.join()
        self.q_out.join()

    def data_pass(self, n_workers):
        data_in = np.random.randn(self.buffer_size * 10)
        data_out_true = data_in ** 2

        def f(x):
            return x ** 2

        start_one2one_transformer(f, q_in=self.q_in, q_out=self.q_out,
                                  stop_event=self.stop_event, backend=THREAD,
                                  n_workers=n_workers)

        i = 0
        data_out = []
        for d in data_in:
            self.q_in.put(d)
            i += 1

            if i == self.buffer_size:
                for j in range(self.buffer_size):
                    data_out.append(self.q_out.get())
                    self.q_out.task_done()
                i = 0

        if n_workers > 1:
            data_out_true = sorted(data_out_true)
            data_out = sorted(data_out)

        np.testing.assert_equal(data_out, data_out_true)

    def test_data_pass_1_worker(self):
        self.data_pass(1)

    def test_data_pass_n_workers(self):
        self.data_pass(4)
