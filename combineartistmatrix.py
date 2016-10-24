import sys
import re
import os
import codecs
import urllib
from lxml import etree
from datetime import datetime, date, timedelta
import songs_api as api
from multiprocessing import Pool
import logging
import ConfigParser
import time
import loggingmodule
reload(sys)
if(not os.path.exists('logs')):
	os.mkdir('logs')
else:
    try:
        os.remove('logs/combined.txt')
    except OSError:
        pass
sys.setdefaultencoding('utf8')
'''formatter = logging.Formatter('%(message)s')
logger_genre = logging.getLogger('simple_logger')
hdlr_1 = logging.handlers.RotatingFileHandler(
              'logs/combined.txt', maxBytes=100*1024*1024*1024, backupCount=500)
hdlr_1.setFormatter(formatter)
logger_genre.addHandler(hdlr_1)
logger_genre = logging.getLogger('simple_logger')

formatter1 = logging.Formatter('%(message)s')
logger_errors = logging.getLogger('simple_logger1')
hdlr_2 = logging.handlers.RotatingFileHandler(
              'combined_artists.log', maxBytes=1024*1024*1024, backupCount=10)
hdlr_2.setFormatter(formatter1)
logger_errors.addHandler(hdlr_2)
logger_errors = logging.getLogger('simple_logger1')'''

def combinefiles(directory):
    try:
        d = directory
        if(len(d.strip()) == 0):
            return
        directory = d.strip()
        path = directory +'/matrix.txt'
        print path
        if os.path.exists(path):
            fopen = codecs.open(path,'r','utf-8')
            for line in fopen: 
                logger_genre.error(line)
                #print line
        else:
            print 'nofile'
    except Exception as e:
        logger_errors.exception(e)


directory = raw_input("Enter directory: ")
m = raw_input("Enter m: ")
m=int(m)
foldlist = list()
jobs=[]
t1=datetime.now()
logger_genre = loggingmodule.initialize_logger('combined.txt',False)
logger_errors = loggingmodule.initialize_logger1('combined_artisterrors.log')

for dirs in os.listdir(directory):
  	found = re.search(r'[0-9]+',str(dirs),0)
  	if found:
		for curr_dir, sub_dir, filenames in os.walk(directory+'/'+dirs):
			for sd in sub_dir:
				f = re.search(r'[0-9]+',str(sd),0)
				if not f:
					continue
				strg = os.path.join(curr_dir,sd)
				foldlist.append(strg)
print len(foldlist)
try:
	p =Pool(processes=int(m))
	p.map(combinefiles,foldlist)
	p.close()
	p.join()
except Exception as e:
	logger_errors.exception(e)
t2=datetime.now()

print "time=" +str(t2-t1)
