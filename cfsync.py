import cloudfiles
import sys,os
import ConfigParser 
import math



class Config:
    """Configuration class to hold all config vars and tests"""
    def __init__(self):
        """Initalisor for config, will read in config from file, and check if config is valid"""
        self.cp = ConfigParser.ConfigParser()
	self.cp.read(['/etc/cfsync/cfsync.ini', os.path.expanduser('~/.cfsync.ini') ])
	self.api_username = self.get('api','username',True)
	self.api_key = self.get('api','key',True)
	self.api_url = self.get('api','url',True)
        self.checkApi()
	self.dest_container = self.get('destination','container',True)
	#TODO return contianer object ?
	self.gen_md5 = self.get('general','md5',False,False)
	self.gen_verbose = self.get('general','verbose',False,False)
        self.gen_filelist = self.get('general','filelist',False,'STDIN')
    def get(self,section,option,required=False,default=None):
        """Wrapper arround ConfigParser.get that will asign a defaul if declaration is not mandatory"""
        try:
	    return self.cp.get(section,option)
	except:
	    if required == True:
	        print "[%s]%s Not found, please edit your config file, or supply this as --%s-%s=value" % (section,option,section,option)
		sys.exit(1)
            else:
	        return default
    def checkApi(self):
        """Checks to ensure that the API details are within limits"""
        if len(self.api_key) < 8:
	    print "Your API Key does not look right. Please re-check!"
	    sys.exit(1)
	if len(self.api_username) < 2:
	    print "Your API Username does not look right. Please re-check!"
	    sys.exit(1)

class FileList:
    def __init__(self,config):
        if config.gen_filelist == "STDIN":
            self.list = sys.stdin.readlines()
        else:
            #TODO need to have the rsync style side of things here
            pass

#### Main program loop

# Read config
config = Config()
# Read file list
fl = FileList(config)
local_file_list = fl.list

#Setup the connection
cf = cloudfiles.get_connection(config.api_username,config.api_key,authurl=config.api_url)

#Get a list of containers
containers = cf.get_all_containers()

# Lets setup the container
for container in containers:
    if container.name == config.dest_container:
            backup_container = container

#Create the container if it does not exsit
try:
    backup_container
except NameError:
    backup_container = cf.create_container(config.dest_container)


def printdebug(m,mv=()):
    if config.gen_verbose == True:
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

file_number = 0
for local_file in local_file_list:
        local_file = local_file.rstrip()
        if config.gen_md5 == True:
	    import hashlib
	    local_file_hash = hashlib.md5()
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
                elif (config.gen_md5 == True and remote_file_list[local_file]['hash'] != local_file_hash.hexdigest()):
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
