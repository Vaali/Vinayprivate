import sys
import re
import os
import codecs
import urllib
import urlparse
import urllib2
import difflib
#import libxml2
from lxml import etree
from datetime import datetime, date, timedelta
import songs_api as api
from multiprocessing import Pool
import logging
import ConfigParser
import random
import time
import operator
config = ConfigParser.ConfigParser()
reload(sys)
sys.setdefaultencoding('utf8')
formatter = logging.Formatter('%(message)s')
logger_genre = logging.getLogger('simple_logger')
hdlr_1 = logging.FileHandler('combined.txt')
hdlr_1.setFormatter(formatter)
logger_genre.addHandler(hdlr_1)
logger_genre = logging.getLogger('simple_logger')
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG, filename='errors_combined.log')

def combinefiles(directory):
    try:
        d = directory
        if(len(d.strip()) == 0):
			return
        directory = d.strip()
        path = directory +'/matrix.txt'
        if os.path.exists(path):
            fopen = codecs.open(path,'r','utf-8')
            for line in fopen: 
                    logger_genre.debug(line)
    except Exception as e:
        logging.exception(e)


directory = raw_input("Enter directory: ")
m = raw_input("Enter m: ")
m=int(m)
foldlist = list()
jobs=[]
t1=datetime.now()
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
try:
	p =Pool(processes=int(m))
	p.map(combinefiles,foldlist)
	p.close()
	p.join()
except Exception as e:
	logging.exception(e)
t2=datetime.now()

print "time=" +str(t2-t1)
