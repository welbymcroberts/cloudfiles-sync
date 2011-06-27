from log import Logging
import threading
from Queue import Queue
from cloud_providers.swift import *

__author__ = 'Welby.McRoberts'

def setup_logging():
    _logger = Logging()
    _logger.setup(console_level="DEBUG")

def setup_config():
    pass

setup_config()
setup_logging()

swift = Swift(username='TESTING',api_key='TESTING')
swift.connect(pool_count=2)
container='bob'
local='/tmp/test'
remote='test'

q = Queue()
q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})
q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})

class CSThread(threading.Thread):

    def __init__ (self):
        from log import Logging
        _logger = Logging()
        self._log = _logger.log
        self.runno = 0
        threading.Thread.__init__(self)
    def run(self):
        while True:
            task = q.get()
            if task['direction'] == 'get':
                self._log.debug('Run %d' % self.runno)
                swift.get(task['container'],task['remote'],task['local'])
                self.runno += 1
            elif task['direction'] == 'put':
                self._log.debug('Run %d' % self.runno)
                swift.put(task['container'],task['local'],task['remote'])
                self.runno += 1
threads = []
for i in range(5):
    threads.append(CSThread())
for i in range(5):
    threads[i].start()
