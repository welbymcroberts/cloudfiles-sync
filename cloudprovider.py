import sys
from log import Logging
__author__ = 'Welby.McRoberts'
_logger = Logging()
_log = _logger.log
class CloudProvider():
    """
    Base class for Cloudproviders
    """
    def AuthenticationFailed(self):
        """
        Raised when Authentication Fails - Usually wrong login details
        """
        _log.critical('Authentication Failure - Exiting')
        sys.exit(1)
    def AuthenticationError(self):
        """
        Raised when Authentication Has an error, for an unknown reason
        """
        _log.critical('Authentication Failure Check your Username/Password/API Details- Exiting')
        sys.exit(1)

    def NoSuchContainer(self,container_name):
        """
        Raised when A request for an unknown container occurs
        """
        if container_name == False:
            _log.warn("Container/Bucket doesn't exist")
        else:
            _log.warn("Container/Bucket doesn't exist - creating")
            self.createContainer(container_name)
    def InvalidContainerName(self):
        """
        Raised when a request for a container that has a bad name is made
        """
        _log.critical("Container/Bucket name is invalid - Exiting")
        sys.exit(1)
    def InvalidObjectName(self):
        """
        Raised when a request for an invalid object occurs
        """
        _log.warn("File does not exist")
    def callback(self,done,total):
        """
        This function does nothing more than print out a % completed to INFO
        """
        try:
            _log.debug("%d completed of %d - %d%%" %(done,total, int((float(done)/float(total))*100)))
        except ZeroDivisionError:
            _log.debug("%d completed of %d - %d%%" %(done,total, int((float(done)/1)*100)))
        if done == total:
            _log.debug("%d completed of %d - %d%%" %(done,total, 100))
    def callback100(self,remote):
        """
        This function does nothing more than print out a 100% completed to INFO
        """
        _log.info("%s completed" %(remote))


