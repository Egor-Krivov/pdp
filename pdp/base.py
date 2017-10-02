import time
from contextlib import suppress

from queue import Empty, Full
from queue import Queue as ThreadQueue
from threading import Event as ThreadEvent
from threading import Thread
from multiprocessing import Process
from multiprocessing.pool import Pool as ProcessPool
from multiprocessing.pool import ThreadPool

from .backend import Backend
from .log import logging


DEFAULT_MONITOR_TIMEOUT = 1


class StopEvent(Exception):
    """Exception, need to stop worker. Should be
    handled internally by worker's target during execution"""
    pass


class LoopedQueue:
    def __init__(self, queue: ThreadQueue, timeout, stop_event: ThreadEvent):
        self.queue = queue
        self.timeout = timeout
        self.stop_event = stop_event

    def put(self, value):
        while not self.stop_event.is_set():
            with suppress(Full):
                self.queue.put(value, timeout=self.timeout)
                break
        else:
            raise StopEvent

    def get(self):
        while not self.stop_event.is_set():
            with suppress(Empty):
                return self.queue.get(timeout=self.timeout)
        else:
            raise StopEvent

    def __getattr__(self, name):
        return getattr(self.queue, name)


def start_transformer(process_data, q_in, q_out, backend, stop_event,
                      n_workers):
    def process_source_exhausted():
        logging.info('message about exhaustion was received '
                     'waiting for in queue to be processed')
        q_in.join()
        logging.info('in queue was processed, sending message')
        q_out.put(StopEvent)
        logging.info('message about exhaustion was send')

    def target():
        try:
            for value in iter(q_in.get, StopEvent):
                try:
                    process_data(value)
                finally:
                    q_in.task_done()
            else:
                process_source_exhausted()
        except StopEvent:
            pass
        except Exception:
            stop_event.set()
            raise

    if backend is Backend.THREAD:
        pool = ThreadPool(n_workers, target)
    else:
        pool = ProcessPool(n_workers, target)
    # For pool it means that current work if final and pool should be closed,
    # once it's finished.
    pool.close()


def start_one2one_transformer(f, *, q_in, q_out, stop_event, backend,
                              n_workers):
    def process_data(value):
        processed_value = f(value)
        q_out.put(processed_value)

    start_transformer(process_data, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_many2one_transformer(chunk_size, *, q_in, q_out,
                               stop_event, backend, n_workers):
    chunk = []

    def process_data(value):
        nonlocal chunk
        chunk.append(value)
        if len(chunk) == chunk_size:
            q_out.put(chunk)
            chunk = []

    start_transformer(process_data, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_one2many_transformer(f, *, q_in, q_out,
                               stop_event, backend, n_workers):
    def transform(value):
        for o in f(value):
            q_out.put(o)

    start_transformer(transform, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_source(iterable, q_out, stop_event, backend):
    def target():
        try:
            for value in iterable:
                q_out.put(value)
                logging.debug('New object sent further')
            else:
                q_out.put(StopEvent)
        except StopEvent:
            pass
        except Exception:
            stop_event.set()
            raise

    if backend is Backend.THREAD:
        Thread(target=target).start()
    else:
        Process(target=target).start()


def start_monitor(queues, stop_event, timeout=DEFAULT_MONITOR_TIMEOUT):
    def target():
        while not stop_event.is_set():
            logging.info('queues: {}'.format(map(lambda q: q.qsize(), queues)))
            time.sleep(timeout)

    Thread(target=target).start()
