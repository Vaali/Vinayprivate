import sys
import songs_api as api
import codecs
import re
from datetime import datetime, date, timedelta
import os
import time
import glob
from multiprocessing import Pool
import loggingmodule
from itertools import repeat
import random
from songsutils import is_songname_same_artistname,moveFiles
import youtube_dl

def getMonths(currentPublishedDate):
	now = datetime.now()	
	pbdate = datetime.strptime(currentPublishedDate, '%Y%m%d')
	dd = pbdate.day
	yy = pbdate.year
	mm = pbdate.month
	total = (now.year-yy)*12+(now.month-mm)
	if total < 1:
		total = 1
	return total

def getCurrentTime():
	now = datetime.now()
	if(now.month<10):
		mm = '0'+str(now.month)
	else:
		mm = str(now.month)
	if(now.day<10):
		dd = '0'+str(now.day)
	else:
		dd = str(now.day)
	hh = now.hours
	mins = now.mins


def movefilestodeleted(filename):
    moveFiles(filename,'deletedvideos')

def movefilestofailed(filename):
    moveFiles(filename,'failedvideos')

def movefilestowrong(filename):
    moveFiles(filename,'wrongvideos')

def getDelta(oldDate,oldViewcount,newViewcount):
	now = datetime.now()
	days = (now - oldDate).days
	if(days == 0):
		return -1
	delta = (newViewcount - oldViewcount)/days
	return delta
def GetyoutubeVideoDetailsFromYDL(videoid):
    url = 'https://www.youtube.com/watch?v='+str(videoid)
    ydl_opts = {}
    videoResult = {}
    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            meta = ydl.extract_info(url, download=False)
        videoResult['items'] = []
        videoEntry = {}
        videoEntry['statistics'] = {}
        videoEntry['snippet'] = {}
        videoEntry['status'] = {}
        if('view_count' in meta):
            videoEntry['statistics']['viewCount'] = meta['view_count']
        if('like_count' in meta):
            videoEntry['statistics']['likeCount'] = meta['like_count']
        if('dislike_count' in meta):
            videoEntry['statistics']['dislikeCount'] = meta['dislike_count']
        if('upload_date' in meta):
            videoEntry['snippet']['publishedAt'] = meta['upload_date']
        videoEntry['status']['privacyStatus'] = 'public'
        videoEntry['status']['embeddable'] = True
        videoResult['items'].append(videoEntry)
        return videoResult
    except Exception as e:
        logger_matrix.exception(e)
        return None


def updateXml(filename):
    try:
        print(filename)
        try:
            oldsong = api.parse(filename)
            if(is_songname_same_artistname(oldsong.songName,oldsong.artist.artistName[0]) == True):
                movefilestowrong(filename)
                return
        except Exception as e:
            logger_matrix.exception("Error")
            return
        videoResult = GetyoutubeVideoDetailsFromYDL(str(oldsong.youtubeId))
        print(videoResult)
        if(videoResult == None):
            return
        print("new values")
        if videoResult.has_key('items'):
            if(len(videoResult['items']) == 0):
                logger_matrix.exception("Error :No items returned "+ filename + "\n")
                movefilestodeleted(filename)
                return
            videoEntry = videoResult['items'][0]
            currentVideoViewCount = 0
            currentVideolikes = 0
            currentVideodislikes = 0
            currentVideoStatus = 'public'
            currentVideoEmbedded = 'true'
            if('viewCount' in videoEntry['statistics']):
                currentVideoViewCount = videoEntry['statistics']['viewCount']
            if('likeCount' in videoEntry['statistics']):
                currentVideolikes = videoEntry['statistics']['likeCount']
            if('dislikeCount' in videoEntry['statistics']):
                currentVideodislikes = videoEntry['statistics']['dislikeCount']
            if('embeddable' in videoEntry['statistics']):
                currentVideoEmbedded = videoEntry['status']['embeddable']
            if('privacyStatus' in videoEntry['statistics']):
                currentVideoStatus = videoEntry['status']['privacyStatus']
            currentPublishedDate = videoEntry['snippet']['publishedAt']
            if(currentVideoEmbedded == False or currentVideoStatus != 'public'):
                logger_matrix.exception("Error :No items returned "+ filename + "\n")
                movefilestodeleted(filename)
                return
            if(int(currentVideolikes) !=0 and int(currentVideodislikes)!=0):
                currentVideorating = (float(currentVideolikes)*5)/(float(currentVideolikes)+float(currentVideodislikes))
            else:
                currentVideorating =0
        crawlHistoryList = oldsong.crawlHistoryList
        if(crawlHistoryList == None):
            crawlHistoryList = api.crawlHistoryList()
        crawlHistory = api.crawlHistory()

	    #print oldsong.crawlDate.strftime("%Y-%m-%d")
        oldVideoRating = oldsong.rating
        if(oldVideoRating == None):
            oldVideoRating = 0
        crawlHistory.set_Views(oldsong.viewcount)
        crawlHistory.set_Date(oldsong.crawlDate.strftime("%Y-%m-%d"))
        currDelta = getDelta(oldsong.crawlDate,oldsong.viewcount,int(currentVideoViewCount))
        if(currDelta == -1):
            return
        crawlHistory.set_Delta(int(currDelta))
        crawlHistoryList.add_crawlHistory(crawlHistory)
        oldsong.set_rating(currentVideorating)
        oldsong.set_crawlDelta(currDelta)
        oldsong.set_crawlHistoryList(crawlHistoryList)
        oldsong.crawlDate =  datetime.now()
        oldsong.viewcountRate = float(currentVideoViewCount)/getMonths(currentPublishedDate)
        oldsong.viewcount = int(currentVideoViewCount)
        genreTag = oldsong.genreTag
        if(genreTag == None or genreTag == ''):
            genreTag = GetgenreTag(oldsong)
            oldsong.set_genreTag(genreTag)
            
        fx = codecs.open(filename,"w","utf-8")
        fx.write('<?xml version="1.0" ?>\n')
        oldsong.export(fx,0)
        fx.close()
    except Exception as e:
        logger_matrix.exception("Error")
        return

def GetgenreTag(oldsong):
    print('getting genres tags')
    level1  = oldsong.level1Genres.genreName
    level2 = oldsong.level2Genres.genreName
    current_genres = []
    for g in level1:
        if(g.lower() not in current_genres):
            current_genres.append(g.lower())
    for g in level2:
        if(g.lower() not in current_genres):
            current_genres.append(g.lower())
    genre_tags = sorted(current_genres)
    combinedgenrestring = '@'.join(genre_tags)
    return combinedgenrestring

def updatexmls_youtubedl():
    try:
        print(filename)
        try:
            oldsong = api.parse(filename)
            if(is_songname_same_artistname(oldsong.songName,oldsong.artist.artistName[0]) == True):
                movefilestowrong(filename)
                return
        except Exception as e:
            logger_matrix.exception("Error")
            return
        
    except Exception as e:
        logger_matrix.exception(e)
	


if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('updatexml','updatexmls.log')
    directory = raw_input("Enter directory: ")
    if not os.path.exists(directory):
        print('directory doesnt exists')
        exit()
    m = raw_input("Enter m: ")
    m=int(m)
    filelist = list()
    t1=time.time()
    try:
        updateXml('solr_newData11_old/0000aiYfOWu5ZhY.xml')
        '''filelist = glob.glob(directory+"/*.xml")
        p =Pool(processes=int(m))
        p.map(updateXml,filelist)
        p.close()
        p.join()'''
    except Exception as e:
        logger_matrix.exception("Error")

    print(time.time()-t1)
