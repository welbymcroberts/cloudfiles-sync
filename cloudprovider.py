import sys
from log import Logging
__author__ = 'Welby.McRoberts'
_logger = Logging()
_log = _logger.log
class CloudProvider():
    """
    Base class for Cloudproviders
    """
    class errors:
        def AuthenticationFailed(self,Error):
            """
            Raised when Authentication Fails - Usually wrong login details
            """
            _log.critical('Authentication Failure - Exiting')
            sys.exit(1)
        def AuthenticationError(self,Error):
            """
            Raised when Authentication Has an error, for an unknown reason
            """
            _log.critical('Authentication Failure Check your Username/Password/API Details- Exiting')
            sys.exit(1)

        def NoSuchContainer(self,Error):
            """
            Raised when A request for an unknown container occurs
            """
            _log.warn("Container/Bucket doesn't exist")
        
    def callback(self,done,total):
        """
        This function does nothing more than print out a % completed to STDOUT
        """
        try:
            sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/float(total))*100), self.file_number, self.total_files))
        except ZeroDivisionError:
            sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/1)*100), self.file_number, self.total_files))
        sys.stdout.flush()
        if done == total:
            sys.stdout.write("\n")
            sys.stdout.flush()

