import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file = os.path.join(os.getcwd(), 'archival.log')
ch = logging.FileHandler(file)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - '
                              'p%(process)s {%(pathname)s:%(lineno)d} - %(message)s',
                              datefmt='%Y-%b-%d %H:%M:%S')
ch.setFormatter(formatter)
logger.addHandler(ch)
