from contextlib import suppress

from queue import Empty, Full
from queue import Queue as ThreadQueue
from threading import Event as ThreadEvent
from threading import Thread
from multiprocessing import Process
from multiprocessing.pool import Pool as ProcessPool
from multiprocessing.pool import ThreadPool

from .backend import choose_backend
from .log import logging

# TODO chunker eats remaining data

DEFAULT_MONITOR_TIMEOUT = 1


class StopEvent(Exception):
    """Exception, need to stop worker. Should be
    handled internally by worker's target during execution."""
    pass


class SourceExhausted:
    """Message, that is send through pipe if source was exhausted. When
    received, each worker waits for it's colleagues at the same stage and
    transmits it further."""
    def __eq__(self, other):
        return type(other) is SourceExhausted


class InterruptableQueue:
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
        logging.info('Transformer: message about exhaustion was received '
                     'waiting for in queue to be processed')
        # Wait for other threads from current pool to process all data
        q_in.join()
        logging.info('Transformer: in queue was processed, sending message')
        q_out.put(SourceExhausted())
        logging.info('Transformer: message about exhaustion was sent')

    def target():
        try:
            for value in iter(q_in.get, SourceExhausted()):
                try:
                    logging.debug('Transformer: data received, processing...')
                    process_data(value)
                    logging.debug('Transformer: data was processed')
                finally:
                    q_in.task_done()
            else:
                logging.info('Transformer: "source exhausted" received')
                q_in.task_done()
                process_source_exhausted()
                logging.info('Transformer: "source exhausted" transmitted')
        except StopEvent:
            pass
        except Exception:
            logging.error('Transformer: error occured setting event...')
            stop_event.set()
            logging.error('Transformer: event was set')
            raise

    pool = choose_backend(backend, ThreadPool, ProcessPool)(n_workers, target)
    # For pool it means that current work is final and pool should be closed,
    # once it's finished.
    pool.close()


def start_one2one_transformer(f, *, q_in, q_out, stop_event, backend,
                              n_workers):
    def process_data(value):
        q_out.put(f(value))

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


def start_one2many_transformer(f, *, q_in, q_out, stop_event, backend,
                               n_workers):
    def transform(value):
        for o in f(value):
            q_out.put(o)

    start_transformer(transform, q_in, q_out, stop_event=stop_event,
                      backend=backend, n_workers=n_workers)


def start_source(iterable, q_out, stop_event, backend):
    def target():
        try:
            for value in iterable:
                logging.debug('Source: sending new object...')
                q_out.put(value)
                logging.debug('Source: new object was sent further')
            else:
                logging.info('Source: iterable was exhausted, transmitting '
                             'message...')
                q_out.put(SourceExhausted())
                logging.info('Source: message was transmitted')
        except StopEvent:
            pass
        except Exception:
            logging.error('Source: error occured, setting event...')
            stop_event.set()
            logging.error('Source: event was set')
            raise

    choose_backend(backend, Thread, Process)(target=target).start()
