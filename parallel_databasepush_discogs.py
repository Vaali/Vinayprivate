from multiprocessing import Process
import os
import re
import time
import logging
import codecs
from datetime import datetime, date, timedelta
def func(strg):
	os.system(strg)
logging.basicConfig(filename='songsparserpart1.log',level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

directory = raw_input("Enter directory: ")
m = raw_input("Enter m: ")
m=int(m)
foldlist = list()
jobs=[]
t1=time.time()
foldercompletelist = {}
folderstartedlist = {}
for dirs in os.listdir(directory):
    found = re.search(r'[0-9]+',str(dirs),0)
    if found:
		for curr_dir, sub_dir, filenames in os.walk(directory+'/'+dirs):
				strg = curr_dir
				foldlist.append(strg)

print foldlist
foldercompletenewlist = {}
for s in foldercompletelist:
	foldercompletenewlist[foldercompletelist[s]] = s

folderstartednewlist = {}
for s in folderstartedlist:
	folderstartednewlist[folderstartedlist[s]] = s
n = len(foldlist)
print n
argcount=0
blockcount = 0
#m = 1
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
	print index
	if(last > n):
		last = n
	for i in foldlist[index:last]:
		filepartition[j] = filepartition[j] + " " +i
	index = last
print filepartition
while (count < len(filepartition)):
	if(count + m > len(filepartition)):
		m = len(filepartition) - count
	for i in range(0,m):
		proc = Process(target=func,args=(filepartition[count],))
		jobs.append(proc)
		proc.start()
		#jobs.append(proc)
        count = count + 1
        proc.join()
print time.time()-t1

		
