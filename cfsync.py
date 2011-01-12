api_username="YOUR_USERNAME_HERE"
api_key="YOUR_KEY_HERE"
# https://auth.api.rackspacecloud.com/v1.0 for the US 
# https://lon.auth.api.rackspacecloud.com/v1.0 for the UK
auth_url="https://lon.auth.api.rackspacecloud.com/v1.0"
dest_container="backups"

api_username="welbymcrob"
api_key="dfcda55244575e2f6e59f035a481a94a"
auth_url="https://lon.auth.api.rackspacecloud.com/v1.0"
dest_container="backups"

#############
## DO NOT EDIT AFTER THIS LINE
##############
import cloudfiles
import sys,os
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
remote_file_list = backup_container.list_objects_info()


def upload_cf(local_file):
    u = backup_container.create_object(local_file)
    u.load_from_filename(local_file)
	#TODO compare hashes

for local_file in local_file_list:
        local_file = local_file.rstrip()
	local_file_size = os.stat(local_file).st_size/1024
        #check to see if we're in remote_file_list
        #TODO make this a bit nicer, continuall itrations = bad
        rf=0
        for remote_file in remote_file_list:
            if remote_file['name'] == local_file:
                    rf = rf + 1
                    if remote_file['last_modified'] < os.stat(local_file).st_mtime:
                        print "Remote file is older, uploading %s (%dK)" % (local_file,local_file_size)
                        upload_cf(local_file)
                    else:
                        print "Remote file is same age, skiping %s (%dK)" % (local_file,local_file_size)
                    upload_cf(local_file)
        if rf < 1:
            print "Uploading to CF - %s (%dK)" % (local_file,local_file_size)
            upload_cf(local_file)

