import ConfigParser
import optparse
import sys
from log import Logging
import threading
from Queue import Queue
from cloud_providers.swift import *
from file_lists.local import *
from file_lists.swift import *

def setup_logging(console_level="WARNING",file_level="WARNING",file_name="cloud-sync.log"):
    _logger = Logging()
    _logger.setup(console_level=console_level,file_level=file_level,file_name=file_name)

def setup_config():
    op = optparse.OptionParser()
    cp = ConfigParser.ConfigParser()
    cp.read(['/etc/cloud-sync/cloudsync.ini', os.path.expanduser('~/.cloudsync.ini') ])
    general = op.add_option_group('General')
    general.add_option('-d','--console-level', dest='console_level', help='Log Level for Console (DEBUG,INFO,WARNING,ERROR,CRITICAL)', default=cp.get('general','console_level'))
    general.add_option('-l','--file-level', dest='file_level', help='Log Level for File (DEBUG,INFO,WARNING,ERROR,CRITICAL)',default=cp.get('general','file_level'))
    general.add_option('-f','--logfile', dest='log_file', help='Log File Name',default=cp.get('general','log_file'))
    general.add_option('-m','--md5', dest="md5", help="Enable MD5 Comparison",default=cp.get('general','md5'))
    api = op.add_option_group('API')
    api.add_option('-u','--username', dest="username", help="API Username (ex: 'welby.mcroberts')",default=cp.get('api','username'))
    api.add_option('-k','--key', dest="key", help="API Key (ex 'abcdefghijklmnopqrstuvwxyz12345",default=cp.get('api','key'))
    api.add_option('-a','--authurl', dest="authurl", help='Auth URL (ex: https://lon.auth.api.rackspacecloud.com/v1.0 )',default=cp.get('api','uri'))
    api.add_option('-s','--connections',dest="connections",help='Number of Connections to API',default=cp.get('api','connections'))
    api.add_option('-t','--timeout',dest="timeout",help='API Timeout',default=cp.get('api','timeout'))
    api.add_option('-n','--servicenet',dest="servicenet",help='Use Servicenet',default=cp.get('api','servicenet'))
    api.add_option('-z','--useragent',dest="useragent",help='Override Useragent',default=cp.get('api','useragent'))

    cloud = op.add_option_group('Cloud')


    return op.parse_args()


(op_results,op_args) = setup_config()


setup_logging(op_results.console_level, op_results.file_level, op_results.log_file)


swift = Swift(username=op_results.username,api_key=op_results.key,
              timeout=int(op_results.timeout),
              servicenet=op_results.servicenet,
              useragent=op_results.useragent,
              auth_url=op_results.authurl)
swift.connect(pool_count=int(op_results.connections))
container='bobette'



fl = DirectoryList('/home/welby/Pictures/')
cp = SwiftList(swift,container)
fl.compare(cp.file_list)

for file in fl.sync_list:
     swift.put(container,'/home/welby/Pictures/'+file,file)


#q = Queue()
#q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
#q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})
#q.put({'direction': 'kill'})
#q.put({'direction': 'kill'})
#class CSThread(threading.Thread):
#
#    def __init__ (self):
#        from log import Logging
#        self._log = Logging().log
#        self.runno = 0
#        threading.Thread.__init__(self)
#    def run(self):
#        while True:
#            task = q.get()
#            if task['direction'] == 'get':
#                self._log.debug('Run %d' % self.runno)
#                swift.get(task['container'],task['remote'],task['local'])
#                self.runno += 1
#            elif task['direction'] == 'put':
#                self._log.debug('Run %d' % self.runno)
#                swift.put(task['container'],task['local'],task['remote'])
#                self.runno += 1
#            elif task['direction'] == 'kill':
#                sys.exit()
#threads = []
#for i in range(5):
#    threads.append(CSThread())
#for i in range(5):
#    threads[i].start()

