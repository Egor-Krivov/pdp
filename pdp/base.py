import time
import logging
from functools import partial
from typing import Sequence, Iterable
from contextlib import suppress

from queue import Queue as ThreadQueue
from threading import Event as ThreadEvent
from multiprocessing import Queue as ProcessQueue
from multiprocessing import Event as ProcessEvent
from threading import Thread
from multiprocessing import Process
from queue import Empty, Full
from multiprocessing.pool import Pool as ProcessPool
from multiprocessing.pool import ThreadPool

logging.basicConfig(
    filename='pipeline.log', level=logging.INFO, filemode='w',
    format='%(levelno)d [%(asctime)s.%(msecs)03d] %(message)s',
    datefmt='%H:%M:%S')

DEFAULT_TIMEOUT = 0.1
DEFAULT_MONITOR_TIMEOUT = 1


class StopEvent(Exception):
    """Exception, need to stop worker. Should be
    handled internally by worker's target during execution"""
    pass


def choose_by_backend(backend, thread_option, process_option):
    if backend == 'thread':
        return thread_option
    elif backend == 'process':
        return process_option
    else:
        raise ValueError('Wrong backend')


class LoopedQueue:
    def __init__(self, queue: ThreadQueue, timeout, stop_event: ThreadEvent):
        self.queue = queue
        self.timeout = timeout
        self.stop_event = stop_event

    def put(self, value):
        while True:
            if self.stop_event.is_set():
                raise StopEvent
            with suppress(Full):
                self.queue.put(value, timeout=self.timeout)
                break

    def get(self):
        while True:
            if self.stop_event.is_set():
                raise StopEvent
            with suppress(Empty):
                return self.queue.get(timeout=self.timeout)

    def __getattr__(self, name):
        return getattr(self.queue, name)


def start_transformer(transform, q_in, q_out, backend, stop_event, n_workers):
    def process_source_exhausted():
        logging.info('message about exhaustion was received '  
                     'waiting in queue to be processed')
        q_in.join()
        logging.info('in queue was processed, sending message')
        q_out.put(StopEvent)
        logging.info('message about exhaustion was send')

    def outer_target():
        try:
            for value in iter(q_in.get, StopEvent):
                try:
                    transform(value, q_out)
                finally:
                    q_in.task_done()
            else:
                process_source_exhausted()
        except StopEvent:
            pass
        except Exception:
            stop_event.set()
            raise

    choose_by_backend(backend, ThreadPool, ProcessPool)(n_workers, outer_target)


def start_one2one_transformer(f, *, q_in, q_out,
                              stop_event, backend, n_workers):
    def transform(value, q):
        processed_value = f(value)
        q.put(processed_value)

    start_transformer(transform, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_many2one_transformer(chunk_size, *, q_in, q_out,
                               stop_event, backend, n_workers):
    chunk = []

    def transform(value, q):
        nonlocal chunk
        chunk.append(value)
        if len(chunk) == chunk_size:
            q.put(chunk)
            chunk = []

    start_transformer(transform, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_one2many_transformer(extract, *, q_in, q_out,
                               stop_event, backend, n_workers):
    def transform(value, q):
        list(map(q.put, extract(value)))

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

    choose_by_backend(backend, Thread, Process)(target=target).start()


def start_monitor(queues, stop_event, timeout=DEFAULT_MONITOR_TIMEOUT):
    def target():
        while not stop_event.is_set():
            logging.info('queues: {}'.format(map(lambda q: q.qsize(), queues)))
            time.sleep(timeout)

    Thread(target=target).start()


class Pipeline:
    def __init__(self, source, *transformers, timeout=DEFAULT_TIMEOUT):
        self.components = [source, *transformers]
        self.timeout = timeout

        # Connect transformers with queues
        self._connect_components(self.components)
        self.queue = self.components[-1].queue_out

        self.pipeline_active = False

        self.monitor = _PipelineMonitor(self.components)

    @staticmethod
    def _connect_components(components: Sequence[_BasicWorkersPool]):
        for c_in, c_out in zip(components[:-1], components[1:]):
            buffer_size = c_in.buffer_size
            if c_in.backend == 'thread' and c_out.backend == 'thread':
                q = ThreadQueue(buffer_size)
            else:
                q = multiprocessing.JoinableQueue(buffer_size)

            c_in.queue_out = c_out.queue_in = q

        c = components[-1]
        if c.backend == 'thread':
            c.queue_out = ThreadQueue(c.buffer_size)
        elif c.backend == 'process':
            c.queue_out = multiprocessing.JoinableQueue(c.buffer_size)
        else:
            raise ValueError('Wrong backend')

    def _stop(self):
        if self.pipeline_active:
            logging.info('Stopping pipeline...')
            self.monitor.stop()
            for c in self.components:
                c.stop()

            logging.info('Pipeline stopped')
            self.pipeline_active = False

    def _start(self):
        assert not self.pipeline_active
        for c in self.components:
            c.start()
        self.monitor.start()

        self.pipeline_active = True

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug('Exit called')
        self._stop()

    def __iter__(self):
        assert self.pipeline_active
        while True:
            try:
                data = self.queue.get(timeout=self.timeout)
            except queue.Empty:
                continue
            # This is slightly shady. In this case pipeline monitor might
            # start thinking that pipeline was exhausted. It won't
            # affect __iter__ until the next request though, but might speed
            # up stopping of iteration.
            self.queue.task_done()

            if data is SourceExhausted:
                logging.debug('Pipeline was exhausted, checking that all'
                              'workers were stopped')
                self._stop()
                logging.debug('Pipeline was exhausted, all workers'
                              'were stopped')

                raise StopIteration
            logging.info('Pipeline: next returned')
            yield data
            logging.info('Pipeline: next called')
