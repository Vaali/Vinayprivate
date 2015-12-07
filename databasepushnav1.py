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
  	print dirs
  	#print f
#ipdir = str(sys.argv[1])
  	if found:
		for curr_dir, sub_dir, filenames in os.walk(directory+'/'+dirs):
			for sd in sub_dir:
				#print os.path.join(curr_dir,sd)
				f = re.search(r'[0-9]+',str(sd),0)
				if not f:
					continue
				strg = "python mysong_p1.py " + os.path.join(curr_dir,sd)
				#print strg
				if(dirs not in foldercompletelist):
					foldercompletelist[dirs] = int(sd)
				else:
					if(foldercompletelist[dirs] < int(sd)):
						foldercompletelist[dirs] = int(sd)
				if(sd not in folderstartedlist):
					folderstartedlist[dirs] = int(sd)
				else:
					if(folderstartedlist[dirs] > int(sd)):
						folderstartedlist[dirs] = int(sd)
				foldlist.append(strg)

#for d in os.listdir(dirt):
#for d in os.listdir(str(os.getcwd())+"/"+dirt):
#	f = re.search(r'[0-9]+',str(d),0)
#	if f:
#		print d
		#	strg = "python new_mysong.py "+str(os.getcwd())+"/"+dirt+"/"+str(d)+' '+str(d)
		#strg="python mysong_p2.py " +dirt+"/"+str(d)+' '+str(d)
		#foldlist.append(strg)
foldercompletenewlist = {}
for s in foldercompletelist:
	foldercompletenewlist[foldercompletelist[s]] = s

folderstartednewlist = {}
for s in folderstartedlist:
	folderstartednewlist[folderstartedlist[s]] = s
n = len(foldlist)
#print foldercompletenewlist
print n
count=0
#m = 1
k = n/m
t = n%m
if t > 0:
	blocknumber = k+1
else:
	blocknumber = k
for j in range(1,blocknumber+1):
	p=j-1
	if p == blocknumber:
		q = t
	else:
		q = m
	for i in foldlist[p*m:p*m+q]:
		#print i
		proc = Process(target=func,args=(i,))
		findex = i.rfind(' ')
		foldname = i[findex+1:len(i)]
		#print "folder name =========================="
		#print foldname
		count = count + 1
		#print "count="+str(count)
		#jobs.append(p)
		proc.start()
		#dirname = i[i.find('mysong_p1.py')+13:]
		dirname = i[i.rfind("/")+1:]
		#print dirname
		if(int(dirname) in folderstartednewlist):
			print dirname
			fwritetext = codecs.open((directory+"/completed.txt"),'a','utf8')
			t=datetime.fromtimestamp(time.time())
			fwritetext.write(t.strftime("%D %H:%M"))
			fwritetext.write("\t "+folderstartednewlist[int(dirname)]+"\t started \n")
			fwritetext.close()


		if(int(dirname) in foldercompletenewlist):
			print dirname
			fwritetext = codecs.open((directory+"/completed.txt"),'a','utf8')
			t=datetime.fromtimestamp(time.time())
			fwritetext.write(t.strftime("%D %H:%M"))
			fwritetext.write("\t "+foldercompletenewlist[int(dirname)]+"\t completed \n")
			fwritetext.close()
		

	proc.join()
print time.time()-t1
