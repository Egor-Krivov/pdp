from contextlib import suppress

from queue import Empty, Full
from queue import Queue
from threading import Event
from threading import Thread
from multiprocessing.pool import ThreadPool

from .log import logging


class StopEvent(Exception):
    """Exception, used to stop worker. Should be handled internally by worker's target during execution."""


class SourceExhausted:
    """Message, that is send through pipe if source was exhausted. When received, each worker waits for it's colleagues
    at the same stage to finish processing and transmits it further."""

    def __eq__(self, other):
        return type(other) is SourceExhausted


class InterruptableQueue:
    """Queue with get and put that can be interrupted by setting ``stop_event`` flag."""
    def __init__(self, queue: Queue, timeout, stop_event: Event):
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


def process_source_exhausted(q_in, q_out):
    """Only one worker from the pool receives SourceExhausted message, the rest finishes processing already received
    objects, get stuck waiting for the next input and get closed with the whole pipeline once ``stop_event`` is set."""
    logging.info('message about exhaustion was received')
    q_in.join()
    logging.info('transformer: in queue was joined, sending message')
    q_out.put(SourceExhausted())
    logging.info('transformer: message about exhaustion was sent further')


def start_transformer(process_data, q_in, q_out, stop_event, n_workers):
    def target():
        try:
            for value in iter(q_in.get, SourceExhausted()):
                try:
                    logging.debug('transformer: data received, processing...')
                    process_data(value)
                    logging.debug('transformer: data was processed')
                finally:
                    q_in.task_done()
            else:
                logging.info('transformer: "source exhausted" received')
                q_in.task_done()
                process_source_exhausted(q_in, q_out)
                logging.info('transformer: "source exhausted" transmitted')
        except StopEvent:
            logging.info('transformer: stop event raised')
        except Exception:
            logging.error('transformer: error occurred setting event...')
            stop_event.set()
            logging.error('transformer: event was set')
            raise

    pool = ThreadPool(n_workers, target)
    # For pool it means that current task is the last one and pool should be closed, once it's finished.
    pool.close()


def start_one2one_transformer(f, *, q_in, q_out, stop_event, n_workers):
    def process_data(value):
        q_out.put(f(value))

    start_transformer(process_data, q_in, q_out, stop_event=stop_event, n_workers=n_workers)


def start_many2one_transformer(chunk_size, *, q_in, q_out, stop_event, n_workers):
    chunk = []

    def process_data(value):
        nonlocal chunk
        chunk.append(value)
        if len(chunk) == chunk_size:
            q_out.put(chunk)
            chunk = []

    start_transformer(process_data, q_in, q_out, stop_event=stop_event, n_workers=n_workers)


def start_one2many_transformer(f, *, q_in, q_out, stop_event, n_workers):
    def transform(value):
        values = f(value)
        for o in values:
            q_out.put(o)

    start_transformer(transform, q_in, q_out, stop_event=stop_event, n_workers=n_workers)


def start_source(iterable, q_out, stop_event):
    def target():
        try:
            for value in iterable:
                logging.debug('Source: sending new object...')
                q_out.put(value)
                logging.debug('Source: new object was sent further')
            else:
                logging.info('Source: iterable was exhausted, transmitting message...')
                q_out.put(SourceExhausted())
                logging.info('Source: message was transmitted')
        except StopEvent:
            pass
        except Exception:
            logging.error('Source: error occured, setting event...')
            stop_event.set()
            logging.error('Source: event was set')
            raise

    Thread(target=target).start()
