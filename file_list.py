import sys
from log import Logging
_log = Logging().log
class FileList():
    """
    Base class for FileLists
    """
    def add(self,file):
        """
        A small function to add the file to the sync list

        @type file: str
        @param file: File Name
        """
        self.sync_list.append(file)
    def compare(self,other_list):
        """
        Compares two file lists

        @type other_list: dict
        @param other_list: A Dictionary of files from a FileList (specifcally FileList.file_list)
        """

        self.sync_list = []
        for file in self.file_list:
            try:
                if self.md5 == True:
                    _log.debug('File: %s' % file)
                    _log.debug('HASH Local: %s Remote %s' % (self.file_list[file]['hash'],other_list[file]['hash']))
                    if other_list[file]['hash'] == self.file_list[file]['hash']:
                        _log.debug('MD5 Match')
                    else:
                        _log.debug('No MD5 Match')
                        self.add(file)
                if other_list[file]['size'] == self.file_list[file]['size']:
                    _log.debug('Size Match')
                else:
                    _log.debug('No Size Match')
                    self.add(file)
                if other_list[file]['last_modified'] >= self.file_list[file]['last_modified']:
                    _log.debug('last_modified is newer - Ignoring')
                else:
                    _log.debug('last_modified is Older - adding to sync list')
                    self.add(file)
            except KeyError:
                _log.info('File %s not found in other file list - Adding to sync list' % file)
                self.add(file)