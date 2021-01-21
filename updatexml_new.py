import sys
import songs_api as api
import codecs
import re
from datetime import datetime, date, timedelta
import os
import time
import glob
from multiprocessing import Pool
import traceback
import loggingmodule
from solr import SolrConnection
from solr.core import SolrException
from itertools import repeat
import random
from songsutils import is_songname_same_artistname,movefilestodeleted,movefilestofailed,movefilestowrong
from songsutils import resetZeroTagsFix
from youtubeapis import youtubecalls,youtubedlcalls
import managekeys
from config import IsYoutudeApi,DataDirectory, NumberOfProcesses, IsUpdateViewCounts, CrawlDaysWindow
from config import SolrGenreTagsUrl,SolrSimilarArtistsUrl

def getMonths(currentPublishedDate):
    now = datetime.now()
    if( IsYoutudeApi == 1):
        m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),currentPublishedDate)
        n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),currentPublishedDate)
        ydate = m.group()+" "+n.group()
        dd = ydate
        yy = int(str(dd)[0:4])
        mm = int(str(dd)[5:7])
    else:
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

def getDelta(oldDate,oldViewcount,newViewcount):
    now = datetime.now()
    oldDate = datetime.combine(oldDate, datetime.min.time())
    days = (now - oldDate).days
    if(days == 0):
		return -1
    delta = (newViewcount - oldViewcount)/days
    return delta


def updateXml(filename):
    try:
        print(filename)
        if( IsYoutudeApi == 1):
            ytubecalls = youtubecalls(manager)
        else:
            ytubecalls = youtubedlcalls()
        try:
            oldsong = api.parse(filename,silence=True)
            oldsong = resetZeroTagsFix(oldsong)
            if(is_songname_same_artistname(oldsong.songName,oldsong.artist.artistName[0]) == True):
                movefilestowrong(filename)
                return
        except Exception as e:
            logger_matrix.exception("Error")
            return
        #videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(oldsong.youtubeId)+"&key="+key+"&part=statistics,snippet,status"
        videoResult = ytubecalls.getyoutubevideodetails(oldsong.youtubeId)
        if(videoResult == None):
            movefilestodeleted(filename)
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
            currentYoutubedldata = ''
            if( 'youtubedldata' in videoEntry ):
                currentYoutubedldata = videoEntry['youtubedldata']
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
            if(int(currentVideolikes) !=0 or int(currentVideodislikes)!=0):
                currentVideorating = (float(currentVideolikes)*5)/(float(currentVideolikes)+float(currentVideodislikes))
            else:
                currentVideorating =0.0
        crawlHistoryList = oldsong.crawlHistoryList
        if(crawlHistoryList == None):
            crawlHistoryList = api.crawlHistoryList()
        crawlHistory = api.crawlHistory()

	    #print oldsong.crawlDate.strftime("%Y-%m-%d")
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
        ydate = datetime.strptime(currentPublishedDate,'%Y%m%d')
        oldsong.set_youtubeDate(ydate)
        oldsong.set_youtubedldata(currentYoutubedldata)
        print(currentYoutubedldata)
        genreTag = oldsong.genreTag
        if(genreTag == None or genreTag == ''):
            genreTag = GetgenreTag(oldsong)
            oldsong.set_genreTag(genreTag)
            
        fx = codecs.open(filename,"w","utf-8")
        fx.write('<?xml version="1.0" ?>\n')
        oldsong.export(fx,0)
        fx.close()
    except Exception as e:
        logger_matrix.exception(e)
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

def updateGenreTags( args ):
    #global connection_genre
    #global connection_artist
    #response = connection.query(q="*:*",fq=[artistName],version=2.2,wt = 'json')
    #intersect = int(response.results.numFound)
    (filename,cutoff) = args
    try:
        oldsong = api.parse(filename)
        oldsong = resetZeroTagsFix(oldsong)
        print('getting genres')
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

        print('getting artists')
        try:
            artistId = oldsong.artistId
            artistId = 'artistId:"'+str(artistId)+ '"'
            print(artistId)
            response_artist = connection_artist.query(q="*:*",fq=[artistId],version=2.2,wt = 'json')
            intersect = int(response_artist.results.numFound)
            if(intersect > 0):
                print('found it')
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


def updatexmls_youtubedl():
    try:
        print(filename)
        try:
            oldsong = api.parse(filename)
            oldsong = resetZeroTagsFix(oldsong)
            if(is_songname_same_artistname(oldsong.songName,oldsong.artist.artistName[0]) == True):
                movefilestowrong(filename)
                return
        except Exception as e:
            logger_matrix.exception(e)
            return
        
    except Exception as e:
        logger_matrix.exception(e)
	
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':

    logger_matrix = loggingmodule.initialize_logger('updatexml','updatexmls.log')
    manager = managekeys.ManageKeys(0)
    manager.reset_projkeys()
    directory = DataDirectory
    if not os.path.exists(directory):
        print('directory doesnt exists')
        exit()
    choiceUpdate = IsUpdateViewCounts
    #int(raw_input("Enter 0 to update views \n 1 to update genretags and simartists\n"))
    filelist = list()
    
    t1=time.time()
    connection_genre = SolrConnection(SolrGenreTagsUrl)
    connection_artist = SolrConnection(SolrSimilarArtistsUrl)
    #updateXml('solr_newData11_old/0000aiYfOWu5ZhY.xml')
    try:
        filelist = glob.glob(directory+"/*.xml")
        result = filter(lambda x:datetime.now()-timedelta(CrawlDaysWindow) > datetime.fromtimestamp(os.path.getmtime(x)), filelist )
        p =Pool(processes=int(NumberOfProcesses))
        if(choiceUpdate == 1):
            p.map(updateXml, result)
        else:
            cutoff = int(raw_input("Enter the cutoff point\n"))
            print(cutoff)
            p.map(updateGenreTags,zip(filelist,repeat(cutoff)))
        p.close()
        p.join()
    except Exception as e:
        logger_matrix.exception(e)
    
    print(time.time()-t1)
