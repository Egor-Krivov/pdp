import logging

logging.basicConfig(
    filename='pdp.log', level=logging.ERROR, filemode='w',
    format='%(levelno)d [%(asctime)s.%(msecs)03d] %(message)s',
    datefmt='%H:%M:%S')
