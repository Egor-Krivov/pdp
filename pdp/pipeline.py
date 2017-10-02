from queue import Queue as ThreadQueue
from multiprocessing import JoinableQueue as ProcessQueue
from threading import Event as ThreadEvent
from multiprocessing import Event as ProcessEvent

from .base import LoopedQueue, start_monitor, StopEvent
from .backend import Backend
from .log import logging

DEFAULT_TIMEOUT = 0.1


class Pipeline:
    def __init__(self, source, *transformers, timeout=DEFAULT_TIMEOUT):
        self.components = [source, *transformers]
        self.timeout = timeout

        self.stop_event = {True: ProcessEvent, False: ThreadEvent}[
            max(map(lambda x: x.backend is Backend.PROCESS, self.components))
        ]()
        self.queues = self._connect_components(self.components)

        self.pipeline_active = False

    def _connect_components(self, components):
        queues = []
        pick = {True: ThreadQueue, False: ProcessQueue}

        for c_in, c_out in zip(components[:-1], components[1:]):
            Queue = pick[c_in.backend is Backend.THREAD and
                         c_out.backend is Backend.THREAD]

            queues.append(LoopedQueue(Queue(c_in.buffer_size), self.timeout,
                                      stop_event=self.stop_event))

        Queue = pick[components[-1].backend is Backend.THREAD]
        queues.append(LoopedQueue(Queue(components[-1].buffer_size),
                                  self.timeout, stop_event=self.stop_event))
        return queues

    def _start(self):
        assert not self.pipeline_active
        for c in self.components:
            c.start()
        start_monitor(self.queues, self.stop_event)

        self.pipeline_active = True

    def _stop(self):
        if self.pipeline_active:
            logging.info('Stopping pipeline...')
            self.stop_event.set()
            logging.info('Pipeline stopped')
            self.pipeline_active = False

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug('Exit called')
        self._stop()

    def __iter__(self):
        assert self.pipeline_active
        queue = self.queues[-1]
        while True:
            data = queue.get()
            # This is slightly shady. In this case pipeline monitor might
            # start thinking that pipeline was exhausted. It won't
            # affect __iter__ until the next request though, but might speed
            # up stopping of iteration.
            queue.task_done()

            if data is StopEvent:
                logging.debug('Pipeline was exhausted, checking that all'
                              'workers were stopped')
                self._stop()
                logging.debug('Pipeline was exhausted, all workers'
                              'were stopped')

                raise StopIteration
            logging.info('Pipeline: next returned')
            yield data
            logging.info('Pipeline: next called')
