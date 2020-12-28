import sys
import os
import glob
import loggingmodule
import soundcloud_api as api
from multiprocessing import Pool
from generatexmls_soundcloud import CalculateScale
import time
import csv
import re
from datetime import datetime, date, timedelta
import codecs


reload(sys)
sys.setdefaultencoding('utf8')

def getMonths(publisheddate):
    publisheddate = str(publisheddate)
    now = datetime.now()
    m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),publisheddate)
    #n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),publisheddate)
    ydate = m.group()
    dd = ydate
    yy = int(str(dd)[0:4])
    mm = int(str(dd)[5:7])
    total = (now.year-yy)*12+(now.month-mm)
    if total < 1:
        total = 1
    return total

def updateViewCounts(filename):
    try:
        try:
            oldsong = api.parse(filename)
        except Exception as e:
			logger_matrix.exception("Error")
			return

        with open('results.csv', mode='r') as infile:
            reader = csv.reader(infile)
            mydict = dict((rows[0],rows[1]) for rows in reader)
        if(oldsong.Id not in mydict):
            return
        oldsong.set_viewcount(int(mydict[oldsong.Id]))
        currAudioList = oldsong.soundcloudList.soundcloudAudio
        for scaudio in currAudioList:
            if(scaudio.Id not in mydict):
                continue
            scaudio.set_playback_count(int(mydict[scaudio.Id]))

        #oldsong.set_soundcloudList(currAudioList)    
        oldsong.set_viewCountGroup(CalculateScale(int(mydict[oldsong.Id])))
        print filename
        total = getMonths(oldsong.videoDate)
        if(total != 0):
            oldsong.viewcountRate = float(oldsong.viewcount)/total
        fname = os.path.basename(filename)
        newfilename = os.path.join('solr_newData12',fname)
        fx = codecs.open(newfilename,"w","utf-8")
        fx.write('<?xml version="1.0" ?>\n')
        oldsong.export(fx,0)
        fx.close()
    except Exception as e:
		logger_matrix.exception("Error")
		return




if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('updatexml','updatesoundcloudxmls.log')
    directory = raw_input("Enter directory: ")
    if not os.path.exists(directory):
        print 'directory doesnt exists'
        exit()
    m = raw_input("Enter m: ")
    m=int(m)
    filelist = list()
    t1=time.time()
    try:
        filelist = glob.glob(directory+"/*.xml")
        p =Pool(processes=int(m))
        p.map(updateViewCounts,filelist)
        p.close()
        p.join()
    except Exception as e:
        logger_matrix.exception("Error")

    print time.time()-t1
