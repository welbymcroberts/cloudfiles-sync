import cloudfiles
import sys,os
import ConfigParser 
import math
import optparse


class Config:
    """Configuration class to hold all config vars and tests"""
    def __init__(self):
        """Initalisor for config, will read in config from file, and check if config is valid"""
        self.cp = ConfigParser.ConfigParser()
        self.op = optparse.OptionParser()
        self.optionParserSetup()
        (self.op_results,self.op_args) = self.op.parse_args()
        self.config = {}
        self.cp.read(['/etc/cfsync/cfsync.ini', os.path.expanduser('~/.cfsync.ini') ])
        self.get('api','username','api_username',True)
        self.get('api','key','api_key',True)
        self.get('api','url','api_url',True)
        self.checkApi()
        self.get('destination','container','dest_container',True)
        #TODO return contianer object ?
        self.get('general','md5','gen_md5',False)
        self.get('general','verbose','gen_verbose',False)
        self.get('general','filelist','gen_filelist',False)
        self.get('destination','remove','dest_remove',False)
        self.get('general','progress','gen_progress',False)
        self.getContainer()
    def get(self,section,option,destination,required=False):
        """Wrapper arround ConfigParser.get that will asign a defaul if declaration is not mandatory"""
        try:
            #Try getting the vars from config ini file
            try:
                self.config[destination] = self.cp.get(section,option)
            except:
                #We're not going to complain if the values arn't in the file
                pass
            
            #Try overriding the values with optparse
            try:
                if (eval('self.op_results.%s' % destination) != None):
                    self.config[destination] = eval('self.op_results.%s' % destination)
            except:
                #No Key, no point in doing anything!
                pass
            self.config[destination]
            
        except:
            try:
                self.config[destination] = eval('self.op_results.%s' % destination)
            except Exception, e:
                print e
                if required == True:
                    print "[%s]%s Not found, please edit your config file, or supply this as a command line option" % (section,option)
                    sys.exit(1)

    def checkApi(self):
        """Checks to ensure that the API details are within limits"""
        if len(self.config['api_key']) < 8:
            print "Your API Key does not look right. Please re-check!"
            sys.exit(1)
        if len(self.config['api_username']) < 2:
            print "Your API Username does not look right. Please re-check!"
            sys.exit(1)



    def optionParserSetup(self):
        """Setup the options for optparser"""
        self.op.add_option('-u','--username', dest="api_username", help="API Username (ex: 'welby.mcroberts')")
        self.op.add_option('-k','--key', dest="api_key", help="API Key (ex: 'abcdefghijklmnopqrstuvwxyz123456')")
        self.op.add_option('-a','--authurl', dest="api_url", help="Auth URL (ex: https://lon.auth.api.rackspacecloud.com/v1.0 )")
        self.op.add_option('-c','--container', dest="dest_contianer", help="Container Name")
        self.op.add_option('-m','--md5', dest="gen_md5", action="store_true", help="Enable MD5 comparision")
        self.op.add_option('-v','--verbose', dest="gen_verbose", action="store_true", help="Enable Verbose mode (prints out a lot more info!)")
        self.op.add_option('-s','--stdin', dest="gen_filelist", action="store_true",help="Take file list from STDIN (legacy mode)", default=True )
        self.op.add_option('-r','--remove', dest="dest_remove", action="store_true", help="Remove the files on the remote side if they don't exist locally", default=False)
        self.op.add_option('-p','--progress', dest="gen_progress", action="store_true", help="Show progress", default=False)

    def getContainer(self):
        self.cf = cloudfiles.get_connection(self.config['api_username'],self.config['api_key'],authurl=self.config['api_url'])
        self.cfContainers = self.cf.get_all_containers()
        for container in self.cfContainers:
            if container.name == self.config['dest_container']:
                self.container = container
        try:
            self.container
        except:
            self.container = self.cf.create_container(self.config['dest_container'])
class FileList:
    """FileList class for file lists"""
    def __init__(self,config,listtype,container=None):
        self.file_list = {}
        if config['gen_filelist'] == True:
            self.file_list_stdin = sys.stdin.readlines()
        else:
            #TODO need to have the rsync style side of things here
            pass
        if listtype == 'remote':
            self.container = container
            self.buildRemote()
        elif listtype == 'local':
            self.buildLocal()
    def buildLocal(self):
        """Builds the list of files for Local files"""
        for local_file in self.file_list_stdin:
            self.file_list[local_file.rstrip()] = { 'name': local_file.rstrip() }
    def getRemoteFiles(self,marker=None):
        return self.container.list_objects_info(marker=marker)
    def buildRemote(self):
        """Builds file list for remote files"""
        # Set the run number as 0
        self.runNumber = 0

        
        if (self.container.object_count > 10000):
            self.numberTimes = math.ceil((self.container.object_count+0.00/10000))
        else:
            self.numberTimes = 1
        while self.runNumber < self.numberTimes:
            if self.runNumber == 0:
                remote_file_list = self.getRemoteFiles()
            else:
                remote_file_list = self.getRemoteFiles(marker=self.lastFile)
            for remote_file in remote_file_list:
                self.file_list[remote_file['name']] = remote_file
            self.lastFile = remote_file
            self.runNumber = self.runNumber + 1
            
#### Main program loop

# Read config
c = Config()
# Read file list
fl = FileList(c.config,'local')
local_file_list = fl.file_list
if c.config['gen_verbose'] == True:
    print "Setting up CF now"
#Setup the connection
backup_container = c.container



def printdebug(m,mv=()):
    if c.config['gen_verbose'] == True:
        print m % mv
# We've now got our container, lets get a file list

if c.config['gen_verbose'] == True:
    print "Building Remote file list"
remote_file_list = FileList(c.config,'remote',container=backup_container).file_list
if c.config['gen_verbose'] == True:
    print "%d Remote files" % len(remote_file_list)

def callback(done,total):
    """This function does nothing more than print out a % completed to STDOUT"""
    if (c.config['gen_verbose'] == True) or (c.config['gen_progress'] == True):
        sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/float(total))*100), file_number, len(local_file_list)))
        sys.stdout.flush()
        if ( done == total ):
            sys.stdout.write("\n")
            sys.stdout.flush

def upload_cf(local_file):
    u = backup_container.create_object(local_file)
    u.load_from_filename(local_file,callback=callback)
    callback(u.size,u.size)
def remove_cf(remote_file):
    backup_container.delete_object(remote_file)

file_number = 0
for local_file in local_file_list:
        local_file = local_file.rstrip()
        if c.config['gen_md5'] == True:
            try:
                import hashlib
                local_file_hash = hashlib.md5()
            except ImportError:
                import md5
                local_file_hash = md5.new()
            local_file_hash.update(open(local_file,'rb').read())
        local_file_size = os.stat(local_file).st_size/1024
        #check to see if we're in remote_file_list
        try:
            if len(remote_file_list[local_file]['name']) > 0:
                #has it been modified
                if remote_file_list[local_file]['last_modified'] < os.stat(local_file).st_mtime :
                    printdebug("Remote file is older, uploading %s (%dK) ",(local_file, local_file_size))
                    upload_cf(local_file)
                #is the md5 different locally to remotly
                elif (c.config['gen_md5'] == True and remote_file_list[local_file]['hash'] != local_file_hash.hexdigest()):
                    printdebug("Remote file hash %s does not match local %s, uploading %s (%dK)",(remote_file_list[local_file]['hash'], local_file_hash.hexdigest(), local_file, local_file_size))
                    upload_cf(local_file)
                else:
                    printdebug("Remote file hash and date match, skipping %s",(local_file))
            else:
                # You shouldn't get here! but lets upload, just incase
                printdebug("this shouldn't have happened!")
                upload_cf(local_file)
        except KeyError:
                printdebug("Remote file does not exist, uploading %s (%dK)",(local_file, local_file_size))
                upload_cf(local_file)
        file_number = file_number + 1

for remote_file in remote_file_list:
    try:
        local_file_list[str(remote_file)]
    except:
        if c.config['dest_remove'] == True:
            printdebug("Removing %s",remote_file)
            remove_cf(remote_file)

