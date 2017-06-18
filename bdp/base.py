from abc import ABC, abstractmethod, ABCMeta
from typing import Sequence

import queue
import threading
import multiprocessing


DEFAULT_WAIT_TIMEOUT = 1


class ExternalStopEvent(Exception):
    """Exception, signalling about external need to stop worker. Should be
     handled internally by worker's target during execution"""
    pass


def _find_get_worker_and_event(backend):
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
        self.wait_timeout = wait_timeout

        self._get_worker, self._get_event = _find_get_worker_and_event(backend)

        self.workers_active = False
        self.external_stop_event = self._get_event()
        self.workers = None
        self.queue_in = None
        self.queue_out = queue.Queue(maxsize=self.buffer_size)

    def start_workers(self):
        assert self._check_consistency
        assert not self.workers_active

        self.workers = [self._start_worker() for _ in range(self.n_workers)]

        for w in self.workers:
            w.start()

        self.workers_active = True

    def stop_workers(self):
        assert self.workers_active
        self.external_stop_event.set()

        print('Starting waining for worker')
        for w in self.workers:
            w.join()
        print('Worker stopped')

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

    def _start_worker(self):
        worker = self._get_worker(target=self._worker_target)
        return worker

    def __del__(self):
        print('Del called on {}'.format(self.__class__))
        if self.workers_active:
            print('Stopping workers...')
            self.stop_workers()
            print('Workers stopped')
        else:
            print('All workers were already stopped')

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
        return self.queue_in is not None


class LambdaTransformer(Transformer):
    def __init__(self, f, n_workers, backend='thread', buffer_size=0,
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        super().__init__(n_workers, backend, buffer_size, wait_timeout)
        self.f = f

    def _worker_target(self):
        try:
            while True:
                inputs = self._get_loop()
                processed_data = self.f(inputs)
                self._put_loop(processed_data)
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
        self.source_exhausted_event = self._get_event()

    def _check_consistency(self):
        return True

    def _worker_target(self):
        iterable = iter(self.iterable)
        try:
            while True:
                inputs = next(iterable)
                self._put_loop(inputs)

        except StopIteration:
            print('Stop it iteration')
            self.source_exhausted_event.set()
        except ExternalStopEvent:
            pass


class Pipeline:
    def __init__(self, source: Source, *transformers: Sequence[Transformer],
                 wait_timeout=DEFAULT_WAIT_TIMEOUT):
        self.wait_timeout = wait_timeout
        self.components = [source, *transformers]
        # Connect queues
        for c_p, c_n in zip(self.components, transformers):
            c_n.queue_in = c_p.queue_out

        self.queue = self.components[-1].queue_out

        self.pipeline_active = False

        # Inner connections to stop pipeline, if source is exhausted
        self.source_exhausted_event = source.source_exhausted_event
        self.pipeline_exhausted = threading.Event()
        self.stop_monitor_event = threading.Event()
        self.monitor = None

    def _stop_pipeline(self):
        if self.pipeline_active:
            print('Stopping pipeline...', flush=True)
            for c in self.components:
                c.stop_workers()

            self.stop_monitor_event.set()

            self.pipeline_active = False

    def __enter__(self):
        self._start_monitor()
        for c in self.components:
            c.start_workers()

        self.pipeline_active = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('Exit called')
        self._stop_pipeline()

    def __del__(self):
        self._stop_pipeline()

    def _monitor_target(self):
        while not self.stop_monitor_event.is_set():
            if not self.source_exhausted_event.wait(timeout=self.wait_timeout):
                continue
            else:
                # Wait for all components to finish and send signal
                print('Monitor got exhausted signal')
                for c in self.components:
                    c.queue_out.join()

                print('Queue exhausted')
                self.pipeline_exhausted.set()
                break

    def _start_monitor(self):
        monitor = threading.Thread(target=self._monitor_target)
        monitor.start()

    def __iter__(self):
        while not self.pipeline_exhausted.is_set():
            try:
                data = self.queue.get(timeout=self.wait_timeout)

                self.queue.task_done()
                yield data
            except queue.Empty:
                print('Empty pipeline, waiting for next object', flush=True)
            except Exception:
                self._stop_pipeline()
                raise
        else:
            self._stop_pipeline()
