import logging
from typing import Sequence
from abc import ABC, abstractmethod, ABCMeta

import queue
import threading
import multiprocessing


format = "[%(filename)s:%(lineno)4s - %(funcName)20s] %(message)s"
logging.basicConfig(filename='pipeline.log', level=logging.DEBUG,
                    format=format, filemode='w')


DEFAULT_WAIT_TIMEOUT = 0.2


class ExternalStopEvent(Exception):
    """Exception, signalling about external need to stop worker. Should be
     handled internally by worker's target during execution"""
    pass


def _find_inits(backend):
    if backend == 'thread':
        get_worker = threading.Thread
        get_event = threading.Event
    elif backend == 'process':
        get_worker = multiprocessing.Process
        get_event = multiprocessing.Event
    else:
        raise ValueError('Wrong backend')
    return get_worker, get_event


class _BasicWorkersPool(ABC):
    """Base class for distributed computing of some sort. All implementations
    should support contract:
        use only put"""
    def __init__(self, n_workers, backend, buffer_size,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        self.n_workers = n_workers
        self.buffer_size = buffer_size
        self.backend = backend
        self.wait_timeout = wait_timeout

        self._init_worker, self._init_event = _find_inits(backend)

        self.workers_active = False
        self.external_stop_event = self._init_event()
        self.workers = None
        self.queue_in = None
        self.queue_out = None

    def start(self):
        assert self._check_consistency
        assert not self.workers_active

        logging.debug(f'Starting workers for {self.__class__}')
        self.workers = [self._make_worker() for _ in range(self.n_workers)]

        for w in self.workers:
            w.start()

        self.workers_active = True
        logging.debug(f'Workers for {self.__class__} started')

    def stop(self):
        assert self.workers_active
        self.external_stop_event.set()

        logging.debug(f'Stopping workers for {self.__class__}')
        logging.debug('Starting waiting for worker to finish')
        for w in self.workers:
            w.join()
        logging.debug('Worker stopped')

        self.workers_active = False

    def _put_loop(self, value):
        while not self.external_stop_event.is_set():
            try:
                self.queue_out.put(value, timeout=self.wait_timeout)
                break
            except queue.Full:
                continue
        else:
            raise ExternalStopEvent

    def _get_loop(self):
        while not self.external_stop_event.is_set():
            try:
                inputs = self.queue_in.get(timeout=self.wait_timeout)
                return inputs
            except queue.Empty:
                continue
        else:
            raise ExternalStopEvent

    def _make_worker(self):
        worker = self._init_worker(target=self._worker_target)
        return worker

    def __del__(self):
        if self.workers_active:
            self.stop()

    @abstractmethod
    def _check_consistency(self):
        """Method is called before starting workers, to ensure objects
         consistency"""

    @abstractmethod
    def _worker_target(self):
        """Method, describing worker process"""
        pass


class Transformer(_BasicWorkersPool, metaclass=ABCMeta):
    def _check_consistency(self):
        return self.queue_in is not None and self.queue_out is not None


class LambdaTransformer(Transformer):
    def __init__(self, f, n_workers, backend='thread', buffer_size=0,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        super().__init__(n_workers, backend, buffer_size, wait_timeout)
        self.f = f

    def _worker_target(self):
        try:
            while True:
                inputs = self._get_loop()
                logging.debug('Object received')
                processed_data = self.f(inputs)
                logging.debug('Object processed')
                self._put_loop(processed_data)
                logging.debug('Object sent further')
                self.queue_in.task_done()
        except ExternalStopEvent:
            pass


class Chunker(Transformer):
    def __init__(self, chunk_size, backend='thread', buffer_size=0,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        super().__init__(1, backend, buffer_size, wait_timeout)
        self.chunk_size = chunk_size

    def _worker_target(self):
        chunk = []
        try:
            while True:
                inputs = self._get_loop()
                chunk.append(inputs)

                if len(chunk) == self.chunk_size:
                    self._put_loop(chunk)
                    chunk = []

                self.queue_in.task_done()
        except ExternalStopEvent:
            pass


class Source(_BasicWorkersPool):
    def __init__(self, iterable, backend='thread', buffer_size=0):
        super().__init__(1, backend, buffer_size)
        self.iterable = iterable

        self.source_exhausted_event = self._init_event()

    def _check_consistency(self):
        return self.queue_out is not None

    def _worker_target(self):
        iterable = iter(self.iterable)
        try:
            while True:
                inputs = next(iterable)
                logging.debug('New object got from generator')
                self._put_loop(inputs)
                logging.debug('New object sent further')

        except StopIteration:
            logging.debug('Stop iteration happened on iterable')
            self.source_exhausted_event.set()
        except ExternalStopEvent:
            pass


class _PipelineMonitor:
    def __init__(self, components: Sequence[_BasicWorkersPool],
                 source_exhausted_event, pipeline_exhausted_event,
                 wait_timeout):
        self.wait_timeout = wait_timeout

        self.components = components
        self.source_exhausted_event = source_exhausted_event
        self.stop_monitor_event = threading.Event()
        self.pipeline_exhausted_event = pipeline_exhausted_event

        self.monitor = None

    def _monitor_target(self):
        while not self.stop_monitor_event.is_set():
            if not self.source_exhausted_event.wait(timeout=self.wait_timeout):
                continue
            else:
                # Wait for all components to finish and send signal
                logging.debug('Monitor got exhausted signal')
                for c in self.components:
                    c.queue_out.join()

                logging.debug('Queue exhausted')
                self.pipeline_exhausted_event.set()
                break

    def start(self):
        self.monitor = threading.Thread(target=self._monitor_target)
        self.monitor.start()

    def stop(self):
        self.stop_monitor_event.set()
        self.monitor.join()


class Pipeline:
    def __init__(self, source: Source, *transformers: Transformer,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        self.components = [source, *transformers]
        self.wait_timeout = wait_timeout

        # Connect transformers with queues
        self._connect_components(self.components)
        self.queue = self.components[-1].queue_out

        self.pipeline_active = False

        # Inner connections to stop pipeline, if source is exhausted
        self.pipeline_exhausted_event = threading.Event()
        self.monitor = _PipelineMonitor(
            self.components, source.source_exhausted_event,
            self.pipeline_exhausted_event, wait_timeout=wait_timeout)

    @staticmethod
    def _connect_components(components: Sequence[_BasicWorkersPool]):
        for c_in, c_out in zip(components[:-1], components[1:]):
            buffer_size = c_in.buffer_size
            if c_in.backend == 'thread' and c_out.backend == 'thread':
                q = queue.Queue(buffer_size)
            else:
                q = multiprocessing.JoinableQueue(buffer_size)

            c_in.queue_out = c_out.queue_in = q

        c = components[-1]
        if c.backend == 'thread':
            c.queue_out = queue.Queue(c.buffer_size)
        elif c.backend == 'process':
            c.queue_out = multiprocessing.JoinableQueue(c.buffer_size)
        else:
            raise ValueError('Wrong backend')

    def _stop_pipeline(self):
        if self.pipeline_active:
            logging.debug('Stopping pipeline...')
            for c in self.components:
                c.stop()
            self.monitor.stop()

            logging.debug('Pipeline stopped')
            self.pipeline_active = False

    def __enter__(self):
        self.monitor.start()
        for c in self.components:
            c.start()

        self.pipeline_active = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug('Exit called')
        self._stop_pipeline()
    #
    # def __del__(self):
    #     self._stop_pipeline()

    def __iter__(self):
        while not self.pipeline_exhausted_event.is_set():
            try:
                data = self.queue.get(timeout=self.wait_timeout)
                self.queue.task_done()

                yield data
            except queue.Empty:
                logging.debug('Empty pipeline, waiting for next object')
            except Exception:
                self._stop_pipeline()
                raise
        else:
            self._stop_pipeline()
