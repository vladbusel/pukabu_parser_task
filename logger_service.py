import logging
from logging import StreamHandler, Formatter
import sys

logging.basicConfig(filename='parser.log', 
                    format='[%(asctime)s: %(levelname)s] %(message)s', 
                    level=logging.DEBUG)
logger = logging.getLogger('parser_logger')
logger.setLevel(logging.DEBUG)
stream_handler = StreamHandler(stream=sys.stdout)
stream_handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(stream_handler)
