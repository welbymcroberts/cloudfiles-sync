from log import Logging
from cloud_providers.swift import *

__author__ = 'Welby.McRoberts'

def setup_logging():
    _logger = Logging()
    _logger.setup(console_level="DEBUG")

def setup_config():
    pass

setup_config()
setup_logging()

swift = Swift(username='bob',api_key='jeff')
swift.connect()