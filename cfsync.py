import cloudfiles
import sys,os
import ConfigParser 
import math

# Read in config
config = ConfigParser.ConfigParser()

# Read the system wide config
config.read(['/etc/cfsync/cfsync.ini', os.path.expanduser('~/.cfsync.ini') ])

#Ensure that the config exsits
if len(config.get('api','key')) <8 or len(config.get('api','username')) <2:
    print "Check you config in either ~/.cfsync.ini or /etc/cfsync/cfsync.ini"
    sys.exit(2)

#Check we've got an api_username
api_username = config.get('api','username')
api_key = config.get('api','key')
auth_url = config.get('api','url')
dest_container= config.get('destination','container')
md5 = config.get('destination','md5')

local_file_list = sys.stdin.readlines()

#Setup the connection
cf = cloudfiles.get_connection(api_username, api_key, authurl=auth_url)

#Get a list of containers
containers = cf.get_all_containers()

# Lets setup the container
for container in containers:
    if container.name == dest_container:
            backup_container = container

#Create the container if it does not exsit
try:
    backup_container
except NameError:
    backup_container = cf.create_container(dest_container)

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
        if len(last['name']) > 0:
	    remote_file_list = container.list_objects_info(marker=last['name'])
	else:
	    remote_file_list = container.list_objects_info()
        for remote_file in remote_file_list:
            remotefiles[remote_file['name']] = remote_file
	    last = remote_file
	runs = runs + 1
    return remotefiles

remote_file_list = build_remote_file_list(backup_container)
file_number = 0

def callback(done,total):
    """This function does nothing more than print out a % completed to STDOUT"""
    sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/float(total))*100), file_number, len(local_file_list)))
    sys.stdout.flush()
    if ( done == total ):
        sys.stdout.write("\n")
        sys.stdout.flush

def upload_cf(local_file):
    file_number = file_number + 1
    u = backup_container.create_object(local_file)
    u.load_from_filename(local_file,callback=callback)
    callback(u.size,u.size)

for local_file in local_file_list:
        if md5:
	    local_file_hash = hashlib.md5()
            local_file_hash.update(open(local_file,'rb').read())
        local_file = local_file.rstrip()
        local_file_size = os.stat(local_file).st_size/1024
        #check to see if we're in remote_file_list
        try:
	    if len(remote_file_list[local_file]['name']) > 0:
                #has it been modified
                if remote_file_list[local_file]['last_modified'] < os.stat(local_file).st_mtime :
                    print "Remote file is older, uploading %s (%dK) " % (local_file, local_file_size)
                    upload_cf(local_file)
                #is the md5 different locally to remotly
                elif md5 && remote_file_list[local_file]['hash'] != local_file_hash.hexdigest():
                    print "Remote file hash %s does not match local %s, uploading %s (%dK)" % (remote_file_list[local_file]['hash'], local_file_hash.hexdigest(), local_file, local_file_size)
                    upload_cf(local_file)
                else:
                    print "Remote file hash and date match, skipping %s" % (local_file)
            else:
	        # You shouldn't get here! but lets upload, just incase
		print "this shouldn't have happened!"
	        upload_cf(local_file)
        except KeyError:
                print "Remote file does not exist, uploading %s (%dK)" % (local_file, local_file_size)
                upload_cf(local_file)
