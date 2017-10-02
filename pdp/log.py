import logging

logging.basicConfig(
    filename='pipeline.log', level=logging.INFO, filemode='w',
    format='%(levelno)d [%(asctime)s.%(msecs)03d] %(message)s',
    datefmt='%H:%M:%S')
