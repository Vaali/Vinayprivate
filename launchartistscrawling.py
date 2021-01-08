import sys
import os
import codecs
import re
from config import DiscogsDataDirectory, NumberOfProcesses, NumberofFolders, IsIncremental
from config import NumberofThreads
from datetime import datetime, date
import loggingmodule
import itertools
import glob
from songsutils import GetSize
from subprocess import Popen, PIPE
from multiprocessing.pool import ThreadPool


logger_decisions = loggingmodule.initialize_logger1('completed','completedartists.log')
logger_error = loggingmodule.initialize_logger('errors_launchartists','errors_launchartists.log')
logger_std_out = loggingmodule.initialize_logger_stdout('stdoutmodule1')

def LaunchProcessFromThreads( foldername ):
    try:
        cmd = ['python' , 'getvideosfinal.py', str(foldername) ]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        #proc.wait()
        print stderr
    except Exception as e:
        logger_error.exception(e)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    filenameList = []
    t1 = datetime.now()
    try:
        lastdirectory = 0
        logger_error.error("Discogs Main Program Starting")
        directory = DiscogsDataDirectory
        folders = NumberofFolders
        folders = int(folders)
        prev_time = 0
        timeFile = directory + "/timelog.txt"
        if(IsIncremental == 1 or IsIncremental == 3):
            try:
                with open(timeFile,"r") as f:
                    prev_time = int(f.read())
            except IOError as e:
                print e
        directorylist = list()
        if(os.path.exists(directory+'/lastdirectory.txt')):
            fread = codecs.open(directory+'/lastdirectory.txt','r','utf-8')
            lines = fread.readlines()
            if(len(lines) > 0):
                if(lines[-1].strip() != ""):
                    lastdirectory = int(lines[-1])
            fread.close()
            fwrite = codecs.open(directory+'/lastdirectory.txt','a','utf-8')
        else:
            fwrite = codecs.open(directory+'/lastdirectory.txt','w','utf-8')

        #Folders list count to control the numebr of folders done
        for dirs in os.listdir(directory):
            found = re.search(r'[0-9]+',str(dirs),0)
            if (found and (lastdirectory <= int(dirs))):
                directorylist.append(int(dirs))
        directorylist = sorted(directorylist)
        splitlist = list(itertools.izip_longest(*(iter(directorylist),) * folders))
        logger_error.error(splitlist)
        for split in splitlist:
            #foldlist = list()
            foldlist = {}
            #t1=time.time()
            foldercompletelist = {}
            folderstartedlist = {}
            logger_error.error("Getting the Folders List")
            for dirs in split:
                if(dirs == None):
                    continue
                for curr_dir, sub_dir, filenames in os.walk(directory+'/'+str(dirs)):
                            strg = curr_dir
                            foldlist[strg] = GetSize(strg)
            sortedfolders = sorted(foldlist.iteritems(), key=lambda (k,v): (v,k),reverse = True)
            logger_error.error("Folders List:")
            n = len(sortedfolders)
            print sortedfolders
            logger_error.error("Starting Processes:")
            #sortedfolders = [('ptest/3/4927925', 1184002)]
            tp = ThreadPool(NumberofThreads)
            for folder in sortedfolders:
                print folder
                tp.apply_async(LaunchProcessFromThreads, (folder,))
            tp.close()
            tp.join()
        fwrite.close()
    except Exception as e:
        logger_error.exception(e)
    t2=datetime.now()
    print "time=" +str(t2-t1)
