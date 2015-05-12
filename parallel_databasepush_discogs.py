from multiprocessing import Process
import os
import re
import time
import logging
import codecs
from datetime import datetime, date, timedelta
import itertools
def func(strg):
	os.system(strg)
logging.basicConfig(filename='parallel_discogs_part1.log',level=logging.DEBUG,format='%(asctime)s %(process)s %(thread)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

try:
    lastdirectory = 0
    logging.debug("Discogs Main Program Starting")
    directory = raw_input("Enter directory: ")
    m1 = raw_input("Enter m: ")
    folders = raw_input("Enter number of folders: ")
    folders = int(folders)
    m1=int(m1)
    directorylist = list()
    if(os.path.exists(directory+'/lastdirecotry.txt')):
        fread = codecs.open(directory+'/lastdirecotry.txt','r','utf-8')
        lines = fread.readlines()
        if(len(lines) > 0):
            lastdirectory = int(lines[-1])
        fread.close()
    fwrite = codecs.open(directory+'/lastdirecotry.txt','a','utf-8')
    
    ''' Folders list count to control the numebr of folders done'''
    for dirs in os.listdir(directory):
        found = re.search(r'[0-9]+',str(dirs),0)
        if (found and (lastdirectory < int(dirs))):
            directorylist.append(int(dirs))
    directorylist = sorted(directorylist)
    splitlist = list(itertools.izip_longest(*(iter(directorylist),) * folders))
    logging.debug(splitlist)
    for split in splitlist:
        jobs=[]
        foldlist = list()
        t1=time.time()
        foldercompletelist = {}
        folderstartedlist = {}
        logging.debug("Getting the Folders List")
        for dirs in split:
            if(dirs == None):
                continue
            for curr_dir, sub_dir, filenames in os.walk(directory+'/'+str(dirs)):
                            strg = curr_dir
                            foldlist.append(strg)
        logging.debug("Folders List:")
        print foldlist
        foldercompletenewlist = {}
        for s in foldercompletelist:
            foldercompletenewlist[foldercompletelist[s]] = s

        folderstartednewlist = {}
        for s in folderstartedlist:
            folderstartednewlist[folderstartedlist[s]] = s
        n = len(foldlist)
        print n
        m = m1
        argcount=0
        blockcount = 0
        logging.debug("Creating blocks")
        #m = 1
        if(n<m):
            temp = n
            n = m
            m = temp
        k = n/m

        if(k > 2500):
            k = n/2500
            argcount = 2500
            blockcount = k+1
        else:
            argcount = n/m
            blockcount = m+1

        filepartition = []
        for i in range(0,blockcount):
            filepartition.append("python discogs_dump.py ")
        index = 0
        count = 0
        for j in range(0,blockcount):
            last = index+argcount
            if(last > n):
                last = n
            for i in foldlist[index:last]:
                filepartition[j] = filepartition[j] + " " +i
            index = last
        print filepartition
        logging.debug("Starting Processes:")
        while (count < len(filepartition)):
            if(count + m > len(filepartition)):
                m = len(filepartition) - count
            for i in range(0,m):
                proc = Process(target=func,args=(filepartition[count],))
                jobs.append(proc)
                proc.start()
                count = count + 1
                #print filepartition[count]
            for proc in jobs:
                proc.join()
            jobs = []
        print time.time()-t1
        logging.debug("completed for split : "+','.join(map(str,split)))
        fwrite.write(str(split[0]))
        fwrite.write("\n")
        if(split[-1]!= None):
            fwrite.write(str(split[-1]))
            fwrite.write("\n")
    fwrite.close()
except Exception as e:
		logging.exception(e)

		
