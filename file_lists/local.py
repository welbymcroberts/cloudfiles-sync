from file_list import FileList
import os
from log import Logging
from datetime import datetime
_log = Logging().log

class DirectoryList(FileList):
    def __init__(self,directory,follow=True,md5=True):
        self.directory = directory
        self.follow = follow
        self.file_list = {}
        self.md5 = md5
        self.updateList()
    def updateList(self):
        for root,dirs,files in os.walk(self.directory,followlinks=self.follow):
            for local_file in files:
                local_file = os.path.normpath(local_file)
                clean_root = os.path.normpath(root)
                clean_root = os.path.relpath(clean_root,self.directory)
                if clean_root == '.':
                    clean_root = ''
                else:
                    clean_root += '/'
                if self.md5:
                    try:
                        import hashlib
                        self.hash = hashlib.md5()
                    except ImportError:
                        import md5
                        self.hash = md5.new()
                    try:
                        self.hash.update(open(str(root)+'/'+str(local_file),'rb').read())
                    except:
                        self.hash.update('')
                    try:
                        self.file_list[clean_root+local_file] = {'name': clean_root+local_file,
                                                             'hash': self.hash.hexdigest(),
                                                             'size': os.path.getsize(str(root)+'/'+str(local_file)),
                                                             'last_modified': datetime.fromtimestamp(os.path.getmtime(str(root)+'/'+str(local_file))),
                                                             'local_path': self.directory+'/'+clean_root+local_file,
                                                             'test': os.stat(str(root)+'/'+str(local_file)).st_mtime}
                    except OSError:
                        _log.warn('Could not access %s/%s skiping' % (str(root),str(local_file)))

                else:
                    try:
                        self.file_list[clean_root+local_file] = {'name': clean_root+local_file,
                                                             'size': os.path.getsize(str(root)+str(local_file)),
                                                             'last_modified': datetime.fromtimestamp(os.path.getmtime(str(root)+str(local_file))),
                                                             'local_path': self.directory+'/'+clean_root+local_file}
                    except OSError:
                        _log.warn('Could not access %s/%s skiping' % (str(root),str(local_file)))
