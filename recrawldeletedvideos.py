'''
Vinay Kumar Pamarthi
2/21/2015

Traverse through the folder of deleted videos and recrawl them to get the correct videos
the second time.

required programs : songs_api.py 


'''
import sys
import songs_api as api
import codecs
import urllib
import urllib2
from urllib2 import Request,urlopen, URLError, HTTPError
import simplejson
import re
from datetime import datetime, date, timedelta
import time
import os
import logging
from multiprocessing import Pool
import glob
import loggingmodule
import random
import managekeys
from songsutils import moveFiles,movefilestodeleted,resetZeroTagsFix,movefilestocompleted
from youtubeapis import youtubecalls,youtubedlcalls
from config import IsYoutudeApi,RecrawlDirectory, NumberOfProcesses, RecrawlOutputDirectory

reload(sys)
sys.setdefaultencoding('utf8')
logger_crawl = loggingmodule.initialize_logger('crawlerrors','errors_recrawldeletedvideos.log')



'''
Utility functions
'''
	
def getVideo(oldsong):
	flist = ""
	ftArtistlist = oldsong.ftArtistList
	songName = oldsong.songName
	for f in ftArtistlist.ftArtistName:
		ttt=f.strip("-")
		flist = flist+" "+ttt
	ftartists = flist[1:]
	allArtists = oldsong.artist.artistName[0].strip("-")+" "+ftartists
	oldsongdetails = {}
	oldsongdetails['artist'] = oldsong.artist
	oldsongdetails['ftArtistList'] = oldsong.ftArtistList
	oldsongdetails['connPhraseList'] = oldsong.connPhraseList
	oldsongdetails['songName'] = oldsong.songName
	#oldsongdetails['album'] = oldsong.album
	if(IsYoutudeApi == 1):
		ytapi = youtubecalls(manager)
		Video = ytapi.searchYoutube(allArtists, songName, oldsongdetails )
	else: 
		ytubecalls = youtubedlcalls()
		Video = ytubecalls.searchYoutube(allArtists, songName, oldsongdetails)
		if( Video == None):
			Video = ytubecalls.searchYoutube(allArtists, songName, oldsongdetails, False)


	now = datetime.now()
	#if searchResult.has_key('items') and len(searchResult['items'])!= 0:
	if( Video != None ):
		oldsong.rating = Video['rating']
		if( oldsong.rating == None or oldsong.rating == 0):
			oldsong.rating = 0.0
		oldsong.url = Video['Url']
		oldsong.match = Video['Match']
		oldsong.tm = Video['TotalMatch']
		oldsong.sm = Video['SongMatch']
		oldsong.am = Video['ArtistMatch']
		oldsong.title = Video['Title']
		oldsong.published = Video['PublishedDate']
		oldsong.videoId = Video['VideoId']
		oldsong.length = Video['Duration']
		oldsong.youtubeId = Video['VideoId']
		if( IsYoutudeApi == 1):
			m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),oldsong.published)
			n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),oldsong.published)
			ydate = m.group()+n.group()
			dd = ydate
		else:
			dd = Video['PublishedDate']
		yy = int(str(dd)[0:4])
		mm = int(str(dd)[4:6])
		total = (now.year-yy)*12+(now.month-mm)
		if total < 1:
			total = 1
		if(total != 0):
			oldsong.viewcountRate = float(oldsong.viewcount)/total	
		if('youtubedldata' in Video):
			oldsong.set_youtubedldata(Video['youtubedldata'])
		if(now.month<10):
			mm = '0'+str(now.month)
		else:
			mm = str(now.month)
		if(now.day<10):
			dd = '0'+str(now.day)
		else:
			dd = str(now.day)
		oldsong.crawldate = str(now.year)+"-"+mm+"-"+dd
		oldsong.viewcount = int(Video['ViewCount'])
		return oldsong
	else:
		return None


def getNewVideo(filename):
	try:
		print filename
		oldsong = api.parse(filename)
		oldsong = resetZeroTagsFix(oldsong)
		newsong = getVideo(oldsong)
		if(newsong == None):
			logging.error(filename)
			movefilestodeleted(filename)
			return
		else:
			movefilestocompleted(filename)
		url = newsong.url
		filename1 = RecrawlOutputDirectory + "/0000" +url[-11:] + ".xml"
		fx = codecs.open(filename1,"w","utf-8")
		fx.write('<?xml version="1.0" ?>\n')
		newsong.export(fx,0)
		fx.close()
	except Exception as e:
		logger_crawl.exception(e)	

#output_directory = foldername+'/deletedvideos'
#getNewVideo(str(sys.argv[1]))
if __name__ == '__main__':
	manager = managekeys.ManageKeys(0)
	t1=datetime.now()
	manager.reset_projkeys()
	directory = RecrawlDirectory
	outputdirectory = RecrawlOutputDirectory
	if(not os.path.exists(outputdirectory)):
		os.mkdir(outputdirectory)
	m = NumberOfProcesses
	m=int(m)
	try:
		filelist = glob.glob(directory+"/*.xml")
		#print filelist
		p =Pool(processes=int(m))
		p.map(getNewVideo,filelist)
		p.close()
		p.join()
	except Exception as e:
		logger_crawl.exception("Error")
	t2=datetime.now()
	print "time=" +str(t2-t1)