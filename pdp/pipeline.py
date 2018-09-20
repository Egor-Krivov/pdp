import time
from queue import Queue
from threading import Event, Thread

from .base import InterruptableQueue, SourceExhausted, StopEvent
from .log import logging

DEFAULT_TIMEOUT = 0.1
DEFAULT_MONITOR_TIMEOUT = 1


class Pipeline:
    def __init__(self, source, *transformers, timeout=DEFAULT_TIMEOUT):
        self.components = [source, *transformers]
        self.stop_event = Event()

        self.queues = [InterruptableQueue(Queue(component.buffer_size), timeout, stop_event=self.stop_event)
                       for component in self.components]

        self.pipeline_active = False

    def _maybe_start(self):
        if not self.pipeline_active:
            self.components[0].start(q_out=self.queues[0], stop_event=self.stop_event)
            for c, q_in, q_out in zip(self.components[1:], self.queues, self.queues[1:]):
                c.start(q_in=q_in, q_out=q_out, stop_event=self.stop_event)

            start_monitor(self.queues, self.stop_event)

            self.pipeline_active = True

    def __iter__(self):
        if not self.pipeline_active:
            self._maybe_start()

        queue = self.queues[-1]

        try:
            for data in iter(queue.get, SourceExhausted()):
                queue.task_done()
                logging.info('Pipeline: next returned')
                yield data
                logging.info(f'Pipeline: next called {queue.qsize()}')
            else:
                logging.debug('Pipeline was exhausted, checking that all workers were stopped')
                self.close()
                logging.debug('Pipeline was exhausted, all workers were stopped')
        except StopEvent:
            raise StopEvent('pdp: Error in the tread or process was detected, check output for details.') from None

    def close(self):
        if self.pipeline_active:
            logging.info('Stopping pipeline...')
            self.stop_event.set()
            logging.info('Pipeline stopped')
            self.pipeline_active = False

    def __enter__(self):
        self._maybe_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug('Exit called')
        self.close()


def start_monitor(queues, stop_event, timeout=DEFAULT_MONITOR_TIMEOUT):
    def target():
        while not stop_event.is_set():
            logging.info('queues: {}'.format([queue.qsize() for queue in queues]))
            time.sleep(timeout)

    Thread(target=target).start()
