import sys
import songs_api as api
import codecs
import urllib2
import simplejson
import re
from datetime import datetime, date, timedelta
import os
import shutil
import time
import glob
from multiprocessing import Pool,Manager
import traceback
import loggingmodule
from solr import SolrConnection
from solr.core import SolrException
from itertools import repeat
import random

manager = Manager()
blocked_keys = manager.list()
curr_keys = manager.list()

proj_keys = ['AIzaSyBX-WCpgMHu_9OGpfkdQJD3SMsJTcDCscE','AIzaSyAMGC-oG66RxddL1BrYupDPKGlUV16Fy0I','AIzaSyAUeAM6MhxAO-QoByqZTmkYkTbvcheyxAU']
proj_keys +=['AIzaSyAUeAM6MhxAO-QoByqZTmkYkTbvcheyxAU','AIzaSyBplSw0EhZiACVBkMdqvrLkenQ_PTau9v0','AIzaSyBDVAu4lVhNGfH06875DatHXcz3u-1gKCI']
proj_keys +=['AIzaSyBt8RP9znGKvWVlHvn8GWdNcLvVRduB4Ak','AIzaSyCl3UV_9k0aLBPI1viEkFoX1wqBPaV26NQ','AIzaSyALgk-GhC4LvlV9WvB8528vyX8ZK29grVc']
proj_keys +=['AIzaSyCFKavfiehRz1aD7cgugi3wWy-4_e6unWw','AIzaSyC3WtfwN8Jzff32AZln7rDmWI9vsJvj01s']

#logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG, filename='errors.log')
def getKey():
    return curr_keys[random.randint(0,len(curr_keys)-1)]

def getMonths(currentPublishedDate):
	now = datetime.now()	
	m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),currentPublishedDate)
	n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),currentPublishedDate)
	ydate = m.group()+" "+n.group()
	dd = ydate
	yy = int(str(dd)[0:4])
	mm = int(str(dd)[5:7])
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
def moveFiles(filename,movetype):
    fname = filename[filename.rfind('/')+1:]
    foldername = filename[:filename.rfind('/')]
    output_directory = foldername+'/'+movetype
    if(not os.path.exists(output_directory)):
		os.makedirs(output_directory)
    if(os.path.exists(os.path.join(output_directory, fname))):
		os.remove(os.path.join(output_directory, fname))
    dest = output_directory+'/'+fname
    shutil.move(filename,dest)

def movefilestodeleted(filename):
    moveFiles(filename,'deletedvideos')

def movefilestofailed(filename):
    moveFiles(filename,'failedvideos')

def getDelta(oldDate,oldViewcount,newViewcount):
	now = datetime.now()
	days = (now - oldDate).days
	if(days == 0):
		return -1
	delta = (newViewcount - oldViewcount)/days
	return delta

def waitforkeys():
    tomorrow = datetime.today() + timedelta(1)
    midnight = datetime(year=tomorrow.year, month=tomorrow.month,day=tomorrow.day, hour=0, minute=0, second=0)
    timetomidnight = (midnight - datetime.now()).seconds
    time.sleep(3600*4+timetomidnight)

def resetProjKeys():
    global curr_keys
    curr_keys = manager.list()
    for key in proj_keys:
        curr_keys.append(key)
        print 'adding key'


def updateXml(filename):
    try:
        print filename
        keys = getKey()
        try:
			oldsong = api.parse(filename)
        except Exception as e:
			logger_matrix.exception("Error")
			return
        
        videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(oldsong.youtubeId)+"&key="+keys+"&part=statistics,snippet,status"
        
        try:
			videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
        except Exception as e:
            if(e.code == 403 and "Forbidden" in e.reason):
                logger_matrix.error("Daily Limit Exceeded")
                logger_matrix.error(blocked_keys)
                if(keys in proj_keys):
                    proj_keys.remove(keys)
                if(keys not in blocked_keys):
                    blocked_keys.append(keys)
                    
                if(len(blocked_keys) == len(proj_keys)):
                    logger_matrix.error("sleeping")
                    print 'sleeping'
                    waitforkeys()
                    resetProjKeys()
                    
                print "error"
            else:
                print e
                logger_matrix.exception("Error loading json"+ videoUrl + "\n")
            movefilestofailed(filename)
            
            return
        print "new values"
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
    print 'getting genres tags'
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

def updateGenreTags((filename,cutoff)):
    #global connection_genre
    #global connection_artist
    #response = connection.query(q="*:*",fq=[artistName],version=2.2,wt = 'json')
    #intersect = int(response.results.numFound)
    try:
        oldsong = api.parse(filename)

        print 'getting genres'
        '''try:
            genreTag = oldsong.genreTag
            #print genreTag 
            if(genreTag == None or genreTag == ''):
                genreTag = GetgenreTag(oldsong)
                oldsong.set_genreTag(genreTag)
            artistId = 'artistName:"'+str(genreTag)+ '"'
            response_genre = connection_genre.query(q="*:*",fq=[artistId],version=2.2,wt = 'json')
            intersect = int(response_genre.results.numFound)
            if(intersect > 0):
                print 'genres tags found'
                simGenreTagsList = api.similarGenresTagList()
                for result in response_genre.results:
                    count = len(result['similarartistName'])
                    #print count
                    currList = result['similarartistName']
                    currScores = result['similarCosineDistance']
                    currListId = result['similarartistId']
                    print cutoff
                    currScores = currScores[:cutoff]
                    currList = currList[:cutoff]
                    currListId = currListId[:cutoff]
                    count = len(currList)
                    
                    for i in range(0,count):
                        simGenreTag = api.similarGenreTag()
                        simGenreTag.set_genreTagName(currList[i])
                        simGenreTag.set_genreTagScore(currScores[i])
                        simGenreTag.set_genreTagId(int(currListId[i]))
                        simGenreTagsList.add_similarGenreTag(simGenreTag)
                    oldsong.set_similarGenresTagList(simGenreTagsList)
                    oldsong.set_genreTagId(int(result['artistId']))
        except Exception as e:
            logger_matrix.exception('genres writing error')
            logger_matrix.exception(e) '''

        print 'getting artists'
        try:
            artistId = oldsong.artistId
            artistId = 'artistId:"'+str(artistId)+ '"'
            print artistId
            response_artist = connection_artist.query(q="*:*",fq=[artistId],version=2.2,wt = 'json')
            intersect = int(response_artist.results.numFound)
            if(intersect > 0):
                print 'found it'
                similarArtistList = api.similarArtistList()
                for result in response_artist.results:
                    currList = result['similarartistName']
                    currScores = result['similarartistPopularityAll']
                    currListId = result['similarartistId']
                    count = len(currList)
                    for i in range(0,count):
                        similarArtist = api.similarArtist()
                        similarArtist.set_similarArtistName(currList[i])
                        similarArtist.set_similarArtistScore(currScores[i])
                        similarArtist.set_similarArtistId(int(currListId[i]))
                        similarArtistList.add_similarArtist(similarArtist)
                    oldsong.set_similarArtistList(similarArtistList)
                    #oldsong.set_genreTagId(int(result['artistId']))

        except Exception as e:
            logger_matrix.exception('artist writing error')
            logger_matrix.exception(e)
        fx = codecs.open(filename,"w","utf-8")
	fx.write('<?xml version="1.0" ?>\n')
	oldsong.export(fx,0)
	fx.close()
    
    except Exception as e:
        logger_matrix.exception(e)
	return
	
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('updatexml','updatexmls.log')
    directory = raw_input("Enter directory: ")
    if not os.path.exists(directory):
        print 'directory doesnt exists'
        exit()
    m = raw_input("Enter m: ")
    m=int(m)
    choiceUpdate = int(raw_input("Enter 0 to update views \n 1 to update genretags and simartits\n"))
    filelist = list()
    
    t1=time.time()
    connection_genre = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/genretags')
    connection_artist = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/similar_artists1')
    
    resetProjKeys()
    print len(curr_keys)
    '''kk = getKey()
    if(kk in curr_keys):
        curr_keys.remove(kk)
    print len(curr_keys)'''
    try:
        filelist = glob.glob(directory+"/*.xml")
        p =Pool(processes=int(m))
        if(choiceUpdate == 0):
            curr_keys = proj_keys
            blocked_keys = set([])
            p.map(updateXml,filelist)
        else:
            cutoff = int(raw_input("Enter the cutoff point\n"))
            print cutoff
            #print filelist[8]
            p.map(updateGenreTags,zip(filelist,repeat(cutoff)))
            #updateGenreTags(directory+"/0000u4UNot_W9_Q.xml")
        p.close()
        p.join()
    except Exception as e:
        logger_matrix.exception("Error")

    print time.time()-t1
