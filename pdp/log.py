import logging


def start_logging():
    logging.basicConfig(
        filename='pdp.log', level=logging.DEBUG, filemode='w',
        format='%(levelno)d [%(asctime)s.%(msecs)03d] %(message)s',
        datefmt='%H:%M:%S'
    )
