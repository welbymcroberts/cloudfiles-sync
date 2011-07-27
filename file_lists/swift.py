from file_list import FileList
from log import Logging
import time
from urllib import unquote
from datetime import datetime
_log = Logging().log

class SwiftList(FileList):
    def __init__(self,swift,container):
        self.swift = swift
        self.container = container
        self.file_list = {}
        self.updateList()
        self.md5 = True
    def updateList(self):
        fl = self.swift.getFullFileList(self.container)
        for file in fl:
            self.file_list[unquote(file)] = { 'name': fl[file]['name'],
                                     'hash': fl[file]['hash'],
                                     'size': fl[file]['bytes'],
                                     'last_modified': datetime(*time.strptime(fl[file]['last_modified'][:19],"%Y-%m-%dT%H:%M:%S")[:6]) }