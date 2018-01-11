import time
from threading import Thread
from .log import logging

DEFAULT_MONITOR_TIMEOUT = 1


def start_monitor(queues, stop_event, timeout=DEFAULT_MONITOR_TIMEOUT):
    def target():
        while not stop_event.is_set():
            logging.info('queues: {}'.format([*map(lambda q: q.qsize(), queues)]))
            time.sleep(timeout)

    Thread(target=target).start()
