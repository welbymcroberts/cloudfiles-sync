import cloudfiles
import sys,os
import ConfigParser 
import math
import optparse
import re
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
        self.get('destination','container','container_name',True)
        #TODO return contianer object ?
        self.get('general','md5','gen_md5',False)
        self.get('general','verbose','gen_verbose',False)
        self.get('general','stdin','gen_stdin',False)
        self.get('destination','remove','dest_remove',False)
        self.get('general','progress','gen_progress',False)
        self.get('general','follow_links','gen_follow',False)
        rex = re.compile("cf://(.*)$")
        m = rex.findall(self.op_args[0])
        m2 = rex.findall(self.op_args[1])
        if(m):
            #we've got a CLI override of cloudfiles contianer
            self.config['container_name'] = m[0]
            self.config['local'] = self.op_args[1]
            self.config['dir'] = 'from'
        elif(m2):
            self.config['local'] = self.op_args[0]
            self.config['container_name'] = m2[0]
            self.config['dir'] = 'to'
        if(self.config['gen_verbose'] == True):
            print 'Container = ' + self.config['container_name']
            print 'local = ' + self.config['local']
            print 'Direction = ' + self.config['dir']
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
        self.op.add_option('-c','--container', dest="container_name", help="Container Name")
        self.op.add_option('-m','--md5', dest="gen_md5", action="store_true", help="Enable MD5 comparision")
        self.op.add_option('-v','--verbose', dest="gen_verbose", action="store_true", help="Enable Verbose mode (prints out a lot more info!)")
        self.op.add_option('-s','--stdin', dest="gen_stdin", action="store_true",help="Take file list from STDIN (legacy mode)", default=False )
        self.op.add_option('-r','--remove', dest="dest_remove", action="store_true", help="Remove the files on the remote side if they don't exist locally", default=False)
        self.op.add_option('-p','--progress', dest="gen_progress", action="store_true", help="Show progress", default=False)
        self.op.add_option('-l','--follow', dest="gen_follow", action="store_true", help="Follow links?", default=False)

    def getContainer(self):
        self.cf = cloudfiles.get_connection(self.config['api_username'],self.config['api_key'],authurl=self.config['api_url'])
        self.cfContainers = self.cf.get_all_containers()
        for container in self.cfContainers:
            if container.name == self.config['container_name']:
                self.container = container
        try:
            self.container
        except:
            self.container = self.cf.create_container(self.config['container_name'])
class FileList:
    """FileList class for file lists"""
    def __init__(self,config,listtype,container=None,path=None):
        self.file_list = {}
        if listtype == 'remote':
            self.container = container
            self.buildRemote()
        elif listtype == 'local':
            self.buildLocal()
    def buildLocal(self):
        """Builds the list of files for Local files"""
        if c.config['gen_stdin'] == True:
                self.file_list_stdin = sys.stdin.readlines()
                for local_file in self.file_list_stdin:
                    self.file_list[local_file.rstrip()] = { 'name': local_file.rstrip() }
        else:
            try:
                path = c.config['local']
            except:
                print "We didn't get a local path, please see --help"
                sys.exit(1)
            for root,dirs,files in os.walk(path,followlinks=c.config['gen_follow']):
                for local_file in files:
                    self.file_list[root+'/'+local_file.rstrip()] = { 'name': root+'/'+local_file}
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
            try:
                self.lastFile = remote_file
            except UnboundLocalError:
                #Empty contianer?
                pass
            self.runNumber = self.runNumber + 1
class Sync:
    def __init__(self):
        self.file_number = 0
        self.direction = c.config['dir']
        if self.direction == 'to':
            self.source = c.config['local']
            self.destination = 'cf://'+c.config['container_name']
        elif self.direction == 'from':
            self.source = 'cf://'+c.config['container_name']
            self.destination = c.config['local']
        self.local_file_list  = self.localFileList()
        self.remote_file_list = self.remoteFileList()
    def localFileList(self):
        return FileList(c.config,'local').file_list
    def remoteFileList(self):
        return FileList(c.config,'remote',container=c.container).file_list
    def md5(self,lf):
        if c.config['gen_md5'] == True:
            try:
                import hashlib
                local_file_hash = hashlib.md5()
            except ImportError:
                import md5
                local_file_hash = md5.new()
            local_file_hash.update(open(lf,'rb').read())
            return local_file_hash
    def checkFile(self):
        try:
            if len(self.remote_file_list[self.lf]['name']) >0:
                if self.remote_file_list[self.lf]['last_modified'] < os.stat(self.lf).st_mtime:
                    printdebug("Remote file is older, uploading %s (%dK) ",(self.lf, self.lf_size))
                    return True
                #is the md5 different locally to remotly
                elif (c.config['gen_md5'] == True and self.remote_file_list[self.lf]['hash'] != self.lf_hash.hexdigest()):
                    printdebug("Remote file hash %s does not match local %s, uploading %s (%dK)",(self.remote_file_list[self.lf]['hash'], self.lf_hash.hexdigest(), self.lf, self.lf_size))
                    return True
                else:
                    printdebug("Remote file hash and date match, skipping %s",(self.lf))
                    return False
        except KeyError:
            printdebug("Remote file does not exist, uploading %s (%dK)",(self.lf, self.lf_size))
            return True
    def doSync(self):
        printdebug('Syncing from %s => %s', (self.source,self.destination))
        if self.direction == 'to':
            for lf in self.local_file_list:
                self.lf = lf.rstrip()
                self.lf_hash = self.md5(self.lf)    
                self.lf_size=os.stat(self.lf).st_size/1024
                if(self.checkFile() == True):
                    self.upload()
                else:
                    #We do nothing
                    pass
                pass
            for rf in self.remote_file_list:
                try:
                    self.local_file_list[str(rf)]
                except:
                    if c.config['dest_remove'] == True:
                        printdebug("Removing %s",rf)
                        self.removeCF(rf)
        elif self.direction == 'from':
            # Download from CF
            pass
        self.file_number = self.file_number + 1
    def upload(self):
        u = c.container.create_object(self.lf)
        u.load_from_filename(self.lf,callback=callback)
        callback(u.size,u.size)
    def removeCF(self,file):
        c.container.delete_object(file)
        
def callback(done,total):
    """This function does nothing more than print out a % completed to STDOUT"""
    if (c.config['gen_verbose'] == True) or (c.config['gen_progress'] == True):
        try:
            sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/float(total))*100), file_number, len(local_file_list)))
        except ZeroDivisionError:
            sys.stdout.write("\r %d completed of %d - %d%% (%d of %d)" %(done,total, int((float(done)/1)*100), file_number, len(local_file_list)))
        sys.stdout.flush()
        if ( done == total ):
            sys.stdout.write("\n")
            sys.stdout.flush

def remove_cf(remote_file):
    backup_container.delete_object(remote_file)
def printdebug(m,mv=()):
    if c.config['gen_verbose'] == True:
        print m % mv
def mainLoop():
    global c
    global local_file_list
    global remote_file_list
    global file_number
    c = Config()
    s = Sync()
    # Putting this here until code is cleaned up
    local_file_list = s.local_file_list
    remote_file_list = s.remote_file_list
    file_number = s.file_number
    s.doSync()
    #for remote_file in remote_file_list:
    #    try:
    #        local_file_list[str(remote_file)]
    #    except:
    #        if c.config['dest_remove'] == True:
    #            printdebug("Removing %s",remote_file)
    #            remove_cf(remote_file)
if __name__ == "__main__":
    mainLoop()