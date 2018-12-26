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

reload(sys)
sys.setdefaultencoding('utf8')
logger_crawl = loggingmodule.initialize_logger('crawlerrors','errors_recrawldeletedvideos.log')



'''
Utility functions
'''

def ParseTime(time):
	time = time.replace('PT','')
	hours = 0
	minutes = 0
	seconds = 0
	if(time.find('H') != -1):
		hours = time[:time.find('H')]
		time = time[time.find('H')+1:]
	if(time.find('M')!= -1): 
		minutes = time[:time.find('M')]
		time = time[time.find('M')+1:]
	if(time.find('S')!= -1):
		seconds = time[:time.find('S')]
		time = time[time.find('S')+1:]
	return (((int(hours)*60)+int(minutes))*60 + int(seconds))

def CalculateMatch12(oldsong,vid_title):
	list = ""
	conlist = ""
	artistName = oldsong.artist.artistName[0]
	ftArtistlist = oldsong.ftArtistList
	connectorList = oldsong.connPhraseList.connPhrase
	songName = oldsong.songName
	tm = 0
	sm = 0
	am = 0
	fList = ""
	for f in ftArtistlist.ftArtistName:
		fList = fList+" "+f
	ftartists = ""
	if(len(fList)!=0):
		ftartists = fList[0:]
	allArtists = artistName+" "+ftartists
	for c in connectorList:
		if(c != None):
			conlist = conlist+" "+c	
	vid_title = vid_title.replace(',','')
	songName = songName.replace(',','')
	tempName = songName.replace('-','')
	temp_title = vid_title.replace('-','')
	tempName = tempName.replace('\'','')
	temp_title = temp_title.replace('\'','')
	if(temp_title.lower().find(tempName.lower())!= -1):
		substring_song = "true"
	else:
		#mysong.set_substring_song("false")
		substring_song = "false"
	if(vid_title.lower().find(artistName.lower())!= -1):
		#mysong.set_substring_artist("true")
		substring_artist = "true"
	else:
		#mysong.set_substring_artist("false")
		substring_artist = "false"
	
	yname = vid_title
	bhiphen = False
	#Remove the unwanted words
	yname = yname.lower().replace("full version","")
	yname = yname.lower().replace("lyrics on screen","")
	yname = yname.lower().replace("official music video","")
	yname = yname.lower().replace("with lyrics","")
	yname = yname.lower().replace("full album","")
	yname = yname.lower().replace("official song","")
	yname = yname.lower().replace("radio edit","")
	yname = yname.lower().replace("m/v","")


	ftArtistSet = re.findall("\w+",ftartists.lower(),re.U)
	ftAMatch = 0
	ftMatch = 0
	songMatch = 0
	for artist in ftArtistSet:
		if(yname.find(artist)!= -1):
			ftAMatch = ftAMatch + 1
	if(len(ftArtistSet)!=0):
		ftMatch = ftAMatch*100/len(ftArtistSet)
	remove = "lyrics official video hd hq edit music lyric audio acoustic videoclip featuring ft feat radio remix and"
	diffset = re.findall("\w+",remove.lower(),re.U)
	yfullset = re.findall("\w+",yname.lower(),re.U)
	ydiffset = set(yfullset) - set(diffset) 
	yresultset = [o for o in yfullset if o in ydiffset]
	if "feat" in yresultset:
		totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower(),re.U)
	else:
		totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
	common =[]
	common = (set(yresultset).intersection(set(totalset)))
	if float(len(yresultset)) !=0:
		percentMatch = len(common)*100/float(len(yresultset))
	for f in ftArtistSet:
		yname.replace(f,"")
	yname = yname.lower().replace("feat.","")
	yname = yname.lower().replace("ft.","")
	yname = yname.lower().replace("featuring","")
	y1 = yname.find("-")
	y2 = yname.find(":")
	#check if - is present
	if((y1 != -1) or (y2 != -1)):
		bhiphen = True
		if(y1 != -1):
			aname = yname[0:y1]
			sname = yname[y1+1:]
		else:
			aname = yname[0:y2]
			sname = yname[y2+1:]
		aname.strip()
		sname.strip()
		snameset1 = set(re.findall("\w+",sname.lower(),re.U))-set(diffset) - set(ftArtistSet)
		snameset = set(snameset1)
		sreadset = re.findall("\w+",songName.lower(),re.U)
		common1 = (set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		anameset = set(anameset) - set(diffset) - set(ftArtistSet)
		#if "feat" in anameset:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower(),re.U)
		#else:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = (set(anameset).intersection(set(areadset)))
		if float(len(anameset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
		else:
			artistMatch = 0
		tempArMatch = artistMatch
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = (set(leftset).intersection(set(arnameset)))
		rightIntersection = (set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset))
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
		tm = percentMatch
		sm = songMatch
		am = artistMatch
	if(((y1 != -1) or (y2 != -1)) and (leftMatch != 100.0 and am != 100.0 and sm!= 100.0)): # right match if left match is zero.
		bhiphen = True
		if(y1 != -1):
			sname = yname[0:y1]
			aname = yname[y1+1:]
		else:
			sname = yname[0:y2]
			aname = yname[y2+1:]
		aname.strip()
		sname.strip()
		snameset1 = set(re.findall("\w+",sname.lower(),re.U))-set(diffset) - set(ftArtistSet)
		snameset = set(snameset1)-set(ftArtistSet)
		sreadset = re.findall("\w+",songName.lower(),re.U)	
		common1 = (set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = (set(anameset).intersection(set(areadset)))
		if float(len(anameset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
		else:
			artistMatch = 0
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = (set(leftset).intersection(set(arnameset)))
		rightIntersection = (set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset))
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
		tm = percentMatch
		sm = songMatch
		am = artistMatch
	if((y1 == -1) and (y2 == -1)):	
		arnameset = set(re.findall("\w+",artistName.lower(),re.U)) - set(diffset) - set(ftArtistSet)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = (set(leftset).intersection(set(arnameset)))
		rightIntersection = (set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset)) 
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		if(leftMatch > rightMatch):
			songreadset = set(re.findall("\w+",songName.lower(),re.U)) - set(diffset) - set(ftArtistSet)	
			common_set = (set(yresultset[-len(songreadset):]).intersection(set(songreadset)))
			yresultset = (set(yresultset) - set(arnameset))
			if float(len(songreadset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
			tm = percentMatch
			sm = songMatch
			am = leftMatch
		else:
			songreadset = set(re.findall("\w+",songName.lower(),re.U)) - set(diffset) - set(ftArtistSet)	
			common_set = (set(yresultset[:len(songreadset)]).intersection(set(songreadset)))
			yresultset = (set(yresultset) - set(arnameset))
			if float(len(songreadset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
			tm = percentMatch
			sm = songMatch
			am = leftMatch	

	decision = "Incorrect"

	# if all substraing match is true for all and the number of words is greater than 1 for atleast one.
	if(substring_artist == "true" and substring_song == "true" and (len(ftartists) == 0 or (len(ftartists)!=0 and ftMatch == 100.0)) and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1) and percentMatch > 60.0):
		decision = "correct"
	#if song is false then look for song match and length must be greater than 1
	elif(substring_song == "false" and songMatch  >= 80.0 and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1)):
		decision = "correct"
	#if artist  is false look for artistmatch left or [right and total match]
	elif(substring_artist == "false" and (leftMatch == 100.0  or  (rightMatch == 100.0 and percentMatch  > 60.0)) and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1)):
		decision = "correct"
	#if only one words for both song and artist ,check total match and leftmatch for - case.
	elif(substring_artist == "true" and substring_song == "true"  and (percentMatch > 80.0 or (leftMatch == 100.0 and bhiphen))):
		decision = "correct"
	#no hiphen , song match shd be 100 and left or right should be 100 
	elif(substring_artist == "true" and substring_song == "true" and not bhiphen and songMatch == 100.0 and (leftMatch == 100.0 or rightMatch == 100.0) and percentMatch > 60.0):
		decision = "correct"

	if(bhiphen == "true" and (songMatch == 0  or (leftMatch == 0.0 and rightMatch == 0.0))):
		decision = "Incorrect"
	print decision
	return decision,match,tm,sm,am
	
def getVideo(oldsong):
	flist = ""
	ftArtistlist = oldsong.ftArtistList
	songName = oldsong.songName
	for f in ftArtistlist.ftArtistName:
		ttt=f.strip("-")
		flist = flist+" "+ttt
	ftartists = flist[1:]
	allArtists = oldsong.artist.artistName[0].strip("-")+" "+ftartists
	#key = "AIzaSyB34POCUa53BcFsdPURNsvm0i6AX4kqjWo"
	key = manager.getkey()
	print key
	if(key == ""):
			logger_crawl.error(manager.get_blocked_keys())
			manager.keys_exhausted()
			key = manager.getkey()
	if('cover' not in songName.lower()):
		searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover&alt=json&type=video&max-results=5&key="+key+"&videoCategoryId=10"
		#"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover"+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&max-results=5&key="+
	else:
		searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&max-results=5&key="+key+"&videoCategoryId=10"
		#"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&max-results=5&key="+
	try:
		searchResult = simplejson.load(urllib2.urlopen(searchUrl),"utf-8")
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
		return
	now = datetime.now()
	if searchResult.has_key('items') and len(searchResult['items'])!= 0:
		i = 0
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
						selectedVideoPublishedDate = searchEntry['snippet']['publishedAt']
						selectedVideoDuration = ParseTime(videoEntry['contentDetails']['duration'])
						selectedVideolikes = currentVideolikes
						selectedVideodislikes = currentVideodislikes
						iindex=i
			i = i + 1
		if(iindex == -1):
			return						
		if(int(selectedVideolikes) !=0 and int(selectedVideodislikes)!=0):
			oldsong.rating = (float(selectedVideolikes)*5)/(float(selectedVideolikes)+float(selectedVideodislikes))
			print oldsong.rating
		oldsong.url = selectedVideoUrl
		oldsong.match = selectedVideoMatch
		oldsong.tm = selectedVideoTotalMatch
		oldsong.sm = selectedVideoSongMatch
		oldsong.am = selectedVideoArtistMatch
		oldsong.title = selectedVideoTitle
		oldsong.published = selectedVideoPublishedDate
		m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),oldsong.published)
		n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),oldsong.published)
		ydate = m.group()+" "+n.group()
		dd = ydate
		yy = int(str(dd)[0:4])
		mm = int(str(dd)[5:7])
		total = (now.year-yy)*12+(now.month-mm)
		if total < 1:
			total = 1	
		oldsong.length = selectedVideoDuration
		if(now.month<10):
			mm = '0'+str(now.month)
		else:
			mm = str(now.month)
		if(now.day<10):
			dd = '0'+str(now.day)
		else:
			dd = str(now.day)
		oldsong.crawldate = str(now.year)+"-"+mm+"-"+dd
		oldsong.viewcount = int(selectedVideoViewCount)
	return oldsong
def getNewVideo(filename):
	try:
		print filename
		global outputdirectory
		oldsong = api.parse(filename)
		newsong = getVideo(oldsong)
		if(newsong == None):
			logging.error(filename)
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
	manager = managekeys.ManageKeys()
	manager.reset_projkeys()
	directory = raw_input("Enter directory: ")
	outputdirectory = raw_input("Enter output directory: ")
	if(not os.path.exists(outputdirectory)):
		os.mkdir(outputdirectory)
	m = raw_input("Enter m: ")
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

