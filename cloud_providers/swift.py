from cloudprovider import CloudProvider
import cloudfiles
import cloudfiles.errors
from log import Logging
_log = Logging().log

class Swift(CloudProvider):
    def __init__(self, username=None, api_key=None, timeout=5, servicenet=False,
                 useragent='com.whmcr.cloudfiles-sync', auth_url=cloudfiles.uk_authurl):
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
        self.connection_pool = None
        self.username = username
        self.api_key = api_key
        self.servicenet = servicenet
        self.user_agent = useragent
        self.timeout = timeout
        self.auth_url = auth_url


    def connect(self, pool=True, pool_count=5):
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
        _log.info('Connecting to SWIFT')
        try:
            _log.debug('User: %s, api_key: %s, timeout: %d, poolsize: %d, authurl: %s' %(self.username, self.api_key, self.timeout, self.pool_count, self.auth_url))
            self.connection_pool = cloudfiles.ConnectionPool(username=self.username,api_key=self.api_key,
                                                         timeout=self.timeout,poolsize=self.pool_count)
            self.connection_pool.connargs['authurl'] = self.auth_url
            self.connection_pool.connargs['servicenet'] = self.servicenet
            i=1
            connections = []
            while i <= pool_count:
                _log.debug('Connecting to SWIFT - connection %d' % (i) )
                self.connection_pool.get()
                i += 1
        except cloudfiles.errors.AuthenticationError as e:
            self.AuthenticationError()
        except cloudfiles.errors.AuthenticationFailed as e:
            self.AuthenticationFailed()

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

        try:
            _log.debug('Getting Connection')
            connection = self.connection_pool.get()
            _log.info('Saving cf://%s:%s to %s' %(container,remote,local))
            connection.get_container(container).get_object(remote).save_to_filename(local,callback=self.callback)
            self.callback100(remote)
        except cloudfiles.errors.InvalidContainerName as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidContainerName()
        except cloudfiles.errors.NoSuchContainer as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.NoSuchContainer(False)
            self.get(container,remote,local)
        except cloudfiles.errors.InvalidObjectName as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidObjectName()
        finally:
            _log.debug('Returning Connection to the pool')
            self.connection_pool.put(connection)

    def createContainer(self,name):
        """
        Create a container
        
        @type name: str
        @param name: Container Name
        """
        try:
            _log.debug('Getting Connection')
            connection = self.connection_pool.get()
            _log.debug('Creating %s' % (name))
            connection.create_container(name)
        except cloudfiles.errors.InvalidContainerName:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidContainerName()
        finally:
            _log.debug('Returning Connection to the pool')
            self.connection_pool.put(connection)
    def getFullFileList(self,container):
        """
        This returns a File List from SWIFT
        """
        file_list = {}
        try:
            _log.debug('Getting Connection')
            connection = self.connection_pool.get()
            _log.info('Getting size of container %s' % container)
            cont = connection.get_container(container)
            _log.debug('Total number of files in container is %d' %  cont.object_count)
            file_list = {}
            i = 0
            runs = (cont.object_count/10000)+1
            while i < runs:
                if i == 0:
                    _log.debug('Getting file list 0-9999')
                    files = cont.list_objects_info()
                else:
                    _log.debug('Getting file list %d-%d' % ((i*10000),((i+1)*10000)-1))
                    files = cont.list_objects_info(marker=marker)
                for file in files:
                    file_list[file['name']] = file
                    marker = file
                i += 1
        except cloudfiles.errors.InvalidContainerName as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidContainerName()
        except cloudfiles.errors.NoSuchContainer as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.NoSuchContainer(False)
            self.get(container,remote,local)
        finally:
            _log.debug('Returning Connection to the pool')
            self.connection_pool.put(connection)
            return file_list
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
        try:
            _log.debug('Getting Connection')
            connection = self.connection_pool.get()
            _log.info('Saving cf://%s:%s to %s' %(container,remote,local))
            connection.get_container(container).create_object(remote).load_from_filename(local,callback=self.callback)
            self.callback100(remote)
        except cloudfiles.errors.InvalidContainerName as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidContainerName()
        except cloudfiles.errors.NoSuchContainer as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.NoSuchContainer(container)
            self.put(container,local,remote)
        except cloudfiles.errors.InvalidObjectName as e:
            """
            Raised if a invalid contianer name has been used
            """
            self.InvalidObjectName()
        finally:
            _log.debug('Returning Connection to the pool')
            self.connection_pool.put(connection)
