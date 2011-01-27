import cloudfiles
import sys,os
import ConfigParser 
import math
import optparse

from pprint import pprint as pp
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
        self.get('general','remove','dest_remove',False)


    def get(self,section,option,destination,required=False):
        """Wrapper arround ConfigParser.get that will asign a defaul if declaration is not mandatory"""
        try:
            self.config[destination] = self.cp.get(section,option)
	except:
            try:
                self.config[destination] = eval('self.op_results.%s' % destination)
            except:
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
        self.op.add_option('-u','--username', dest="api_username", help="API Username (ex: 'welby.mcroberts')")
        self.op.add_option('-k','--key', dest="api_key", help="API Key (ex: 'abcdefghijklmnopqrstuvwxyz123456')")
        self.op.add_option('-a','--authurl', dest="api_url", help="Auth URL (ex: https://lon.auth.api.rackspacecloud.com/v1.0 )")
        self.op.add_option('-c','--container', dest="dest_contianer", help="Container Name")
        self.op.add_option('-m','--md5', dest="gen_md5", action="store_true", help="Enable MD5 comparision")
        self.op.add_option('-v','--verbose', dest="gen_verbose", action="store_true", help="Enable Verbose mode (prints out a lot more info!)")
        self.op.add_option('-s','--stdin', dest="gen_filelist", action="store_true",help="Take file list from STDIN (legacy mode)", default=True )
        self.op.add_option('-r','--remove', dest="dest_remove", action="store_true", help="Remove the files on the remote side if they don't exist locally", default=False)

class FileList:
    def __init__(self,config,listtype):
        if config['gen_filelist'] == True:
            self.file_list_stdin = sys.stdin.readlines()
            self.file_list = {}
        else:
            #TODO need to have the rsync style side of things here
            pass
        if listtype == 'remote':
            pass
        elif listtype == 'local':
            for local_file in self.file_list_stdin:
                self.file_list[local_file.rstrip()] = { 'name': local_file.rstrip() }
            
#### Main program loop

# Read config
c = Config()
# Read file list
fl = FileList(c.config,'local')
local_file_list = fl.file_list

#Setup the connection
cf = cloudfiles.get_connection(c.config['api_username'],c.config['api_key'],authurl=c.config['api_url'])

#Get a list of containers
containers = cf.get_all_containers()

# Lets setup the container
for container in containers:
    if container.name == c.config['dest_container']:
            backup_container = container

#Create the container if it does not exsit
try:
    backup_container
except NameError:
    backup_container = cf.create_container(c.config['dest_container'])


def printdebug(m,mv=()):
    if c.config['gen_verbose'] == True:
        print m % mv
# We've now got our container, lets get a file list
def build_remote_file_list(container):
    runs = 0
    last = ''
    remotefiles = {}
    if ( container.object_count > 10000 ):
        times = math.ceil((container.object_count+0.00)/10000)
    else:
        times = 1
    while runs < times:
        if len(last) > 0:
	    remote_file_list = container.list_objects_info(marker=last['name'])
	else:
	    remote_file_list = container.list_objects_info()
        for remote_file in remote_file_list:
            remotefiles[remote_file['name']] = remote_file
	    last = remote_file
	runs = runs + 1
    return remotefiles

remote_file_list = build_remote_file_list(backup_container)

def callback(done,total):
    """This function does nothing more than print out a % completed to STDOUT"""
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
    u = backup_container.delete_object(remote_file)

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

