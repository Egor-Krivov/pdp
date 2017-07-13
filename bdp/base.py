import time
import queue
import logging
import threading
import multiprocessing
from typing import Sequence, Iterable
from collections import deque
from abc import ABC, abstractmethod, ABCMeta

logging.basicConfig(
    filename='pipeline.log', level=logging.INFO, filemode='w',
    format='%(levelno)d [%(asctime)s.%(msecs)03d] %(message)s',
    datefmt='%H:%M:%S')

DEFAULT_WAIT_TIMEOUT = 0.1
DEFAULT_MONITOR_TIMEOUT = 1


class ExternalStop(Exception):
    """Exception, signalling about external need to stop worker. Should be
    handled internally by worker's target during execution"""
    pass


class SourceExhausted(Exception):
    """Exception, signalling about exhaustion of the source. Should be
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
        self.source_exhausted_event = self._init_event()
        self.workers = None
        self.queue_in = None
        self.queue_out = None

    def _check_external_stop(self):
        if self.external_stop_event.is_set():
            logging.info('Discovered external stop, raising error')
            raise ExternalStop

    def start(self):
        assert self._check_consistency
        assert not self.workers_active

        logging.info(f'Starting workers for {self.__class__}')
        self.workers = [self._init_worker(target=self._worker_target)
                        for _ in range(self.n_workers)]
        for w in self.workers:
            w.start()

        self.workers_active = True
        logging.info(f'Workers for {self.__class__} started')

    def stop(self):
        assert self.workers_active
        self.external_stop_event.set()

        logging.info(f'Stopping workers for {self.__class__}')
        logging.info('Starting waiting for worker to finish')
        for w in self.workers:
            w.join()
        logging.info('Worker stopped')

        self.workers_active = False

    def _put_loop(self, value):
        while True:
            self._check_external_stop()

            try:
                self.queue_out.put(value, timeout=self.wait_timeout)
                break
            except queue.Full:
                continue

    def _process_source_exhausted_loop(self):
        logging.info('processing source exhausted')
        self.source_exhausted_event.set()

        while True:
            self._check_external_stop()
            try:
                self.queue_out.put(SourceExhausted, timeout=self.wait_timeout)
                break
            except queue.Full:
                continue

    def _get_loop(self):
        while True:
            if self.source_exhausted_event.is_set():
                raise SourceExhausted
            self._check_external_stop()

            try:
                inputs = self.queue_in.get(timeout=self.wait_timeout)
            except queue.Empty:
                continue

            if inputs is not SourceExhausted:
                return inputs
            else:
                logging.info('source was exhausted')
                self.queue_in.task_done()
                logging.info('{}'.format(self.queue_in.qsize()))
                self.queue_in.join()
                logging.info('in queue was exhausted, sending message')
                self._process_source_exhausted_loop()
                logging.info('message about exhaustion was send')
                raise SourceExhausted

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
        except (ExternalStop, SourceExhausted):
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
        except (ExternalStop, SourceExhausted):
            pass


class Source(_BasicWorkersPool):
    def __init__(self, iterable: Iterable, backend='thread', buffer_size=0):
        super().__init__(1, backend, buffer_size)
        self.iterable = iterable

    def _check_consistency(self):
        return self.queue_out is not None

    def _worker_target(self):
        iterator = iter(self.iterable)
        try:
            while True:
                try:
                    inputs = next(iterator)
                    logging.debug('New object got from iterator')
                    self._put_loop(inputs)
                    logging.debug('New object sent further')
                except StopIteration:
                    logging.debug('Stop iteration happened on iterator')
                    self._process_source_exhausted_loop()
                    raise SourceExhausted
        except (ExternalStop, SourceExhausted):
            pass


class _PipelineMonitor:
    def __init__(self, components: Sequence[_BasicWorkersPool],
                 wait_timeout=DEFAULT_MONITOR_TIMEOUT):
        self.wait_timeout = wait_timeout

        self.components = components
        self.stats = deque(maxlen=1000)

        self.external_stop_event = threading.Event()
        self.worker = None

    def _target(self):
        while not self.external_stop_event.is_set():
            s = [c.queue_out.qsize() for c in self.components]
            logging.info(f'queues: {s}')
            time.sleep(self.wait_timeout)

    def start(self):
        self.worker = threading.Thread(target=self._target)
        self.worker.start()

    def stop(self):
        self.external_stop_event.set()
        self.worker.join()


class Pipeline:
    def __init__(self, source: Source, *transformers: Transformer,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        self.components = [source, *transformers]
        self.wait_timeout = wait_timeout

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

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug('Exit called')
        self._stop()

    def __iter__(self):
        assert self.pipeline_active
        while True:
            try:
                data = self.queue.get(timeout=self.wait_timeout)
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
