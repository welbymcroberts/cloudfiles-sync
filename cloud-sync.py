#!/usr/bin/env python
import ConfigParser
import optparse
from cloud_providers.swift import *
from file_lists.local import *
from file_lists.swift import *
from urllib import quote
import os
import sys
import threading
from Queue import Queue

class Worker(threading.Thread):
    def __init__(self):
        self._log = Logging().log
        threading.Thread.__init__(self)
        self.kill_received = False
        self.runno = 0

    def run(self):
        while not self.kill_received:
            self.work()

    def work(self):
        task = q.get()
        if task['direction'] == 'get':
            self._log.debug('Run %d' % self.runno)
            task['clouds']['swift'].get(task['source']['container'],quote(task['file'],'/'),
                                task['dest']['container']+task['file'])
            self.runno += 1
        elif task['direction'] == 'put':
            self._log.debug('Run %d' % self.runno)
            task['clouds']['swift'].put(task['dest']['container'],task['source']['container']+task['file'],
                                quote(task['file'],'/'))
            self.runno += 1
        elif task['direction'] == 'kill':
                self.kill_received = True




def setup_logging(console_level="WARNING",file_level="WARNING",file_name="cloud-sync.log"):
    _logger = Logging()
    _logger.setup(console_level=console_level,file_level=file_level,file_name=file_name)
    return Logging().log

def config_get(cp,section,name,default):
    try:
        c = cp.get(section,name)
        return c
    except ConfigParser.NoSectionError:
        return default
    except ConfigParser.NoOptionError:
        return default

def setup_config():
    usage = "usage: %prog [options] source destination"
    op = optparse.OptionParser(usage=usage)
    cp = ConfigParser.ConfigParser()
    cp.read(['/etc/cloud-sync/cloudsync.ini', os.path.expanduser('~/.cloudsync.ini') ])
    general = op.add_option_group('General')
    general.add_option('-d','--console-level', dest='console_level',
                       help='Log Level for Console (DEBUG,INFO,WARNING,ERROR,CRITICAL)',
                       default=config_get(cp,'general','console_level','CRITICAL'))
    general.add_option('-l','--file-level', dest='file_level',
                       help='Log Level for File (DEBUG,INFO,WARNING,ERROR,CRITICAL)',
                       default=config_get(cp,'general','file_level','WARNING'))
    general.add_option('-f','--logfile', dest='log_file', help='Log File Name',
                       default=config_get(cp,'general','log_file','/var/log/cloud-sync.log'))
    general.add_option('-m','--md5', dest="md5", help="Enable MD5 Comparison",
                       default=config_get(cp,'general','md5',True))
    general.add_option('-T','--threads', dest="numthreads", help="Number of Threads",
                       default=config_get(cp,'general','numthreads',1))
    api = op.add_option_group('API')
    api.add_option('-u','--username', dest="username", help="API Username (ex: 'welby.mcroberts')",
                   default=config_get(cp,'api','username','I_HAVE_NOT_SET_MY_USER_NAME'))
    api.add_option('-k','--key', dest="key", help="API Key (ex 'abcdefghijklmnopqrstuvwxyz12345",
                   default=config_get(cp,'api','key','I_HAVE_NOT_SET_MY_KEY'))
    api.add_option('-a','--authurl', dest="authurl",
                   help='Auth URL (ex: https://lon.auth.api.rackspacecloud.com/v1.0 )',
                   default=config_get(cp,'api','authurl','https://lon.auth.api.rackspacecloud.com/v1.0'))
    api.add_option('-s','--connections',dest="connections",
                   help='Number of Connections to API',
                   default=config_get(cp,'api','connections',1))
    api.add_option('-t','--timeout',dest="timeout",help='API Timeout',
                   default=config_get(cp,'api','timeout',10))
    api.add_option('-n','--servicenet',dest="servicenet",help='Use Servicenet',
                   default=config_get(cp,'api','servicenet',False))
    api.add_option('-z','--useragent',dest="useragent",help='Override Useragent',
                   default=config_get(cp,'api','useragent','com.whmcr.cloudsync'))
    (op_results,op_args) = op.parse_args()
    if len(op_args) != 2:
       op.error("Incorrect number of arguments")
    return (op_results,op_args)

def setup_clouds(op_results,op_args):
    clouds = {}
    for arg in op_args:
        if 'cf://' in arg or 'swift://' in arg:
            clouds['swift'] = Swift(username=op_results.username,api_key=op_results.key,
              timeout=int(op_results.timeout),
              servicenet=op_results.servicenet,
              useragent=op_results.useragent,
              auth_url=op_results.authurl)
    for cloud in clouds:
        clouds[cloud].connect(pool_count=int(op_results.connections))
    return clouds
def setup_source(clouds,op_results,op_args):
    if 'swift://' in op_args[0]:
        list = SwiftList(clouds['swift'],op_args[0][8:])
        container = op_args[0][8:]
        return {'list': list, 'container': container, 'type': 'swift'}
    else:
        list = DirectoryList(op_args[0])
        return {'list': list, 'container': op_args[0], 'type': 'local'}
def setup_dest(clouds,op_results,op_args):
    if 'swift://' in op_args[1]:
        list = SwiftList(clouds['swift'],op_args[1][8:])
        container = op_args[1][8:]
        return {'list': list, 'container': container, 'type': 'swift'}
    else:
        list = DirectoryList(op_args[1])
        return {'list': list, 'container': op_args[1], 'type': 'local'}
def main():
    (op_results,op_args) = setup_config()
    _log = setup_logging(op_results.console_level, op_results.file_level, op_results.log_file)
    for opt, value in op_results.__dict__.items():
         _log.debug('Setting %s is %s' % (opt,str(value)))
    clouds = setup_clouds(op_results,op_args)

    source = setup_source(clouds,op_results,op_args)
    dest = setup_dest(clouds,op_results,op_args)
    source['list'].compare(dest['list'].file_list)

    threads =[]
    for i in op_results.numthreads:
        t = Worker()
        threads.append(t)
        t.start()
    threads = [t.join(1) for t in threads if t is not None and t.isAlive()]
    if dest['type'] == 'swift':
        for file in source['list'].sync_list:
            q.put({'direction': 'put', 'source': source, 'dest': dest, 'file': file, 'clouds': clouds })
    else:
        for file in source['list'].sync_list:
            q.put({'direction': 'get', 'source': source, 'dest': dest, 'file': file, 'clouds': clouds })
    q.put({'direction': 'kill'})
    while len(threads) > 0:
        try:
            threads = [t.join(1) for t in threads if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            for t in threads:
                t.kill_received = True


if __name__ == '__main__':
    q = Queue()
    main()

#q = Queue()
#q.put({'container': container,'direction': 'put', 'remote': remote, 'local': local})
#q.put({'container': container,'direction': 'get', 'remote': remote, 'local': local})
#q.put({'direction': 'kill'})
#q.put({'direction': 'kill'})
#threads = []
#for i in range(5):
#    threads.append(CSThread())
#for i in range(5):
#    threads[i].start()

