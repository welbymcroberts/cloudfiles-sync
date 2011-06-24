from cloudprovider import CloudProvider
import cloudfiles
import cloudfiles.errors
from log import Logging
_logger = Logging()
_log = _logger.log
__author__ = 'Welby.McRoberts'

class Swift(CloudProvider):
    def __init__(self, username=None, api_key=None, timeout=5, servicenet=False,
                 useragent='com.github.welbymcroberts.cloudfiles-sync'):
        """
        Accepts keyword args for Swift Username and API Key

        @type username: str
        @param username: Swift Username
        @type api_key: str
        @param api_key: Swift API Key
        @type servicenet: bool
        @param servicenet: Use Service net - Rackspace Cloud files specific
        @type user_agent: str
        @param user_agent: A user agent string used for all requests
        @type timeout: int
        @param timeout: Timeout value in seconds for a request
        """
        self.connection = None
        self.username = username
        self.api_key = api_key
        self.servicenet = servicenet
        self.user_agent = useragent
        self.timeout = timeout


    def connect(self, pool=False, pool_count=5):
        """
        Spins up the connection(s) to Swift

        @type pool: bool
        @param pool: Create a connection pool
        @type pool_count: int
        @param pool_count: number of connections to use in the pool
        """

        self.pool = pool
        if self.pool:
            self.pool_count = pool_count
        else:
            self.pool_count = 1
        _log.debug('Connecting to SWIFT')
        try:
            self.connection_pool = cloudfiles.ConnectionPool(username=self.username,api_key=self.api_key,
                                                         timeout=self.timeout,poolsize=self.pool_count)
            i=1
            connections = []
            while i <= pool_count:
                self.connection_pool.get()
                i += 1
        except cloudfiles.errors.AuthenticationError:
            self.errors.AuthenticationError('Eep')
        except cloudfiles.errors.AuthenticationFailed:
            self.errors.AuthenticationFailed('Moo')

    def get(self,container,remote,local):
        """
        Preforms the Get/Download of file

        @type container: str
        @param container: Container Name
        @type remote: str
        @param remote: Remote file name
        @type local: str
        @param local: Local file name
        """

        connection = self.pool.get()
        connection.get_container(container).get_object(remote).save_to_filename(local,callback=self.callback)
        self.pool.put(connection)
        
    def put(self,container,local,remote):
        """
        Preforms the Put/Upload of file

        @type container: str
        @param container: Container Name
        @type remote: str
        @param remote: Remote file name
        @type local: str
        @param local: Local file name
        """
        connection = self.pool.get()
        connection.get_container(container).create_object(remote).load_from_filename(local,callback=self.callback)
        self.pool.put(connection)
