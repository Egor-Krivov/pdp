from queue import Queue as ThreadQueue
from multiprocessing import JoinableQueue as ProcessQueue
from threading import Event as ThreadEvent
from multiprocessing import Event as ProcessEvent

from .base import InterruptableQueue, SourceExhausted, StopEvent
from .monitor import start_monitor
from .backend import Backend
from .log import logging

DEFAULT_TIMEOUT = 0.1


class Pipeline:
    def __init__(self, source, *transformers, timeout=DEFAULT_TIMEOUT):
        self.components = [source, *transformers]
        self.timeout = timeout

        if Backend.PROCESS in map(lambda c: c.backend, self.components):
            self.stop_event = ProcessEvent()
        else:
            self.stop_event = ThreadEvent()
        self.queues = self._connect_components(self.components, self.stop_event, self.timeout)

        self.pipeline_active = False

    @staticmethod
    def _connect_components(components, stop_event, timeout):
        queues = []
        pick = {True: ThreadQueue, False: ProcessQueue}

        for c_in, c_out in zip(components[:-1], components[1:]):
            Queue = pick[c_in.backend is Backend.THREAD and c_out.backend is Backend.THREAD]

            logging.debug(Queue)

            queues.append(InterruptableQueue(Queue(c_in.buffer_size), timeout, stop_event=stop_event))

        Queue = pick[components[-1].backend is Backend.THREAD]
        logging.debug(Queue)
        queues.append(InterruptableQueue(Queue(components[-1].buffer_size), timeout, stop_event=stop_event))
        return queues

    def _start(self):
        assert not self.pipeline_active
        self.components[0].start(q_out=self.queues[0], stop_event=self.stop_event)
        for c, q_in, q_out in zip(self.components[1:], self.queues, self.queues[1:]):
            c.start(q_in=q_in, q_out=q_out, stop_event=self.stop_event)

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

        try:
            for data in iter(queue.get, SourceExhausted()):
                queue.task_done()

                logging.info('Pipeline: next returned')
                yield data
                logging.info('Pipeline: next called')
            else:
                logging.debug('Pipeline was exhausted, checking that all workers were stopped')
                self._stop()
                logging.debug('Pipeline was exhausted, all workers were stopped')
        except StopEvent:
            raise StopEvent('pdp: Error in the tread or process was detected, check output for details.') from None
