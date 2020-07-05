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
from getvideosfinal import CalculateMatch
import loggingmodule
import random
import managekeys
from songsutils import moveFiles,movefilestodeleted
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
	key = "AIzaSyBTTnptHRDud60a-h3buMcPwzKhdrGZ1S0"
	'''key = manager.getkey()
	#print key
	if(key == ""):
			logger_crawl.error(manager.get_blocked_keys())
			manager.keys_exhausted()
			key = manager.getkey()'''
	'''if('cover' not in songName.lower()):
		search_url = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover&alt=json&type=video&maxResults=5&key="+key+"&videoCategoryId=10"
		#"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover"+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&maxResults=5&key="+
	else:
		search_url = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&maxResults=5&key="+key+"&videoCategoryId=10"
		#"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&maxResults=5&key="+
	try:
		searchResult = simplejson.load(urllib2.urlopen(search_url),"utf-8")
	except HTTPError as e:
		if(e.code == 403 and "Forbidden" in e.reason):
			logger_crawl.error("Daily Limit Exceeded")
			logger_crawl.error(manager.get_blocked_keys())
			manager.removekey(key)
			manager.add_blockedkey(key)
			manager.keys_exhausted()
		else:
			logger_crawl.error(e.message)
		return
	except Exception as e:
		logger_crawl.error(e)
		return'''
	now = datetime.now()
	#if searchResult.has_key('items') and len(searchResult['items'])!= 0:
	if( Video != None ):
		'''i = 0
		selectedVideoViewCount=0
		currentVideoViewCount=0
		iindex=-1	
		selectedVideoMatch = ""
		selectedVideoTotalMatch = 0
		selectedVideoSongMatch = 0
		selectedVideoArtistMatch = 0
		selectedVideoTitle = ""
		selectedVideoUrl = ""
		selectedVideoDuration = 0
		selectedVideolikes = 0
		selectedVideodislikes = 0
		selectedVideoPublishedDate = ""
		for videoresult in searchResult['items']:
			searchEntry = searchResult['items'][i]
			[currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch,error_str] = CalculateMatch(oldsong,searchEntry['snippet']['title'],searchEntry['snippet']['description'],True)
			if(currentVideoDecision == "correct"):
				youtubeVideoId = searchEntry['id']['videoId']
				videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key="+key+"&part=statistics,contentDetails,status"
				try:
					videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
				except Exception as e:
					logger_crawl.error("Error")
					continue
				if videoResult.has_key('items'):
					videoEntry = videoResult['items'][0]
					currentVideoViewCount = videoEntry['statistics']['viewCount']
					if('likeCount' in videoEntry['statistics']):
						currentVideolikes = videoEntry['statistics']['likeCount']
						currentVideodislikes = videoEntry['statistics']['dislikeCount']
					else:
						currentVideolikes = 0
						currentVideodislikes = 0
					currentVideoEmbedded = videoEntry['status']['embeddable']
					currentVideoStatus = videoEntry['status']['privacyStatus']
					if(currentVideoEmbedded == False or currentVideoStatus != 'public'):
						continue
					if (int(selectedVideoViewCount) < int(currentVideoViewCount)):
						selectedVideoViewCount = currentVideoViewCount
						selectedVideoMatch = currentVideoMatch
						selectedVideoTotalMatch = currentVideoTotalMatch
						selectedVideoSongMatch = currentVideoSongMatch
						selectedVideoArtistMatch = currentVideoArtistMatch
						selectedVideoTitle = searchEntry['snippet']['title']
						selectedVideoUrl = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
						selectedVideoId = youtubeVideoId
						selectedVideoPublishedDate = searchEntry['snippet']['publishedAt']
						selectedVideoDuration = ParseTime(videoEntry['contentDetails']['duration'])
						selectedVideolikes = currentVideolikes
						selectedVideodislikes = currentVideodislikes
						iindex=i
			i = i + 1'''
		'''if(iindex == -1):
			return	'''					
		'''if(int(selectedVideolikes) !=0 and int(selectedVideodislikes)!=0):
			oldsong.rating = (float(selectedVideolikes)*5)/(float(selectedVideolikes)+float(selectedVideodislikes))
			print oldsong.rating'''
		oldsong.rating = Video['rating']
		oldsong.url = Video['Url']
		oldsong.match = Video['Match']
		oldsong.tm = Video['TotalMatch']
		oldsong.sm = Video['SongMatch']
		oldsong.am = Video['ArtistMatch']
		oldsong.title = Video['Title']
		oldsong.published = Video['PublishedDate']
		oldsong.videoId = Video['VideoId']
		oldsong.length = Video['Duration']
		if( IsYoutudeApi == 1):
			m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),oldsong.published)
			n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),oldsong.published)
			ydate = m.group()+" "+n.group()
			dd = ydate
			yy = int(str(dd)[0:4])
			mm = int(str(dd)[5:7])
			total = (now.year-yy)*12+(now.month-mm)
			if total < 1:
				total = 1	
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
		global outputdirectory
		oldsong = api.parse(filename)
		newsong = getVideo(oldsong)
		if(newsong == None):
			logging.error(filename)
			movefilestodeleted(filename)
			return
		url = newsong.url
		filename1 = outputdirectory + "/0000" +url[-11:] + ".xml"
		fx = codecs.open(filename1,"w","utf-8")
		fx.write('<?xml version="1.0" ?>\n')
		newsong.export(fx,0)
		fx.close()
	except Exception ,e:
		logger_crawl.exception(e)	

#output_directory = foldername+'/deletedvideos'
#getNewVideo(str(sys.argv[1]))
if __name__ == '__main__':
	manager = managekeys.ManageKeys(0)
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

