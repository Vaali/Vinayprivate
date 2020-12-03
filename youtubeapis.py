import managekeys
import loggingmodule
from songsutils import is_songname_same_artistname, CalculateMatch, GetYearFromTitle
from songsutils import ParseTime,movefilestofailed, moveFiles
import urllib
from urllib2 import urlopen, URLError, HTTPError
import simplejson
import youtube_dl
import json
from config import CacheDir
logger_youtube = loggingmodule.initialize_logger('youtubeapis','youtubeapis.log')
import warnings
warnings.simplefilter("error")
import random
import os




#Base = declarative_base()
class youtubecalls():
    youtubebaseurl = "https://www.googleapis.com/youtube/v3/"
    key = ""
    def __init__(self,manager):
        self.manager = manager
    
    def getKey(self):
        youtubecalls.key = self.manager.getkey()
        if(youtubecalls.key == ""):
            self.manager.keys_exhausted()
            youtubecalls.key = self.manager.getkey()
            if(youtubecalls.key == ""):
                logger_youtube.error(self.manager.get_blocked_keys())
                self.manager.keys_exhausted()
                logger_youtube.error('Waking up')
                youtubecalls.key = self.manager.getkey()

    def getyoutuberesults( self, query, search = 0 ):
        #getkeys
        self.getKey()
        if(search == 0):
            currUrl = youtubecalls.youtubebaseurl+'videos?'+"&key="+youtubecalls.key
        else:
            currUrl = youtubecalls.youtubebaseurl+'search?'+"&key="+youtubecalls.key
        currUrl += query
        print currUrl
        try:
            videoResult = simplejson.load(urlopen(currUrl),"utf-8")
            return videoResult
        except HTTPError as e:
            if(e.code == 403 and "Forbidden" in e.reason):
                logger_youtube.error("Daily Limit Exceeded")
                logger_youtube.error(self.manager.get_blocked_keys())
                self.manager.removekey(youtubecalls.key)
                self.manager.add_blockedkey(youtubecalls.key)
                self.manager.keys_exhausted()    
            else:
                print(e)
                logger_youtube.exception("Error loading json"+ currUrl + "\n")
            #movefilestofailed(filename)
            return None
        except Exception as e:
            print(e)
            return None

    def getyoutubevideodetails( self, youtubeId ):
        query = "&id="+str(youtubeId)+"&part=statistics,status,contentDetails,snippet"
        return self.getyoutuberesults( query )

    def searchYoutube( self, allArtists, songName, oldvideodetails ):
        if('cover' not in songName.lower()):
            search_url = "&part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover&alt=json&type=video&maxResults=5&videoCategoryId=10"
            #"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"+-cover"+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&maxResults=5&key="+
        else:
            search_url = "&part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&maxResults=5&videoCategoryId=10"
            #"https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&maxResults=5&key="+
        searchResult = self.getyoutuberesults( search_url , 1 )
        if( searchResult == None):
            return None
        if searchResult.has_key('items') and len(searchResult['items'])!= 0:
            i = 0
            Video = {}
            Video['ViewCount']=0
            currentVideoViewCount=0
            iindex=-1
            Video['Match'] = ''
            Video['TotalMatch'] = 0
            Video['SongMatch'] = 0
            Video['ArtistMatch'] = 0
            Video['Title'] = ''
            Video['Url'] = ''
            Video['Duration'] = 0
            Video['likes'] = 0
            Video['dislikes'] = 0
            Video['PublishedDate'] = ''
            Video['rating'] = 0
            print(len(searchResult['items']))
            for videoresult in searchResult['items']:
                searchEntry = searchResult['items'][i]
                currentVideo = {}
                [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails, searchEntry['snippet']['title'],searchEntry['snippet']['description'],logger_youtube, True)
                if(currentVideo['Decision'] == "correct"):
                    youtubeVideoId = searchEntry['id']['videoId']
                    #videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key="+youtubecalls.key+"&part=statistics,contentDetails,status"
                    videoResult = self.getyoutubevideodetails(youtubeVideoId)
                    '''try:
                        videoResult = simplejson.load(urlopen(videoUrl),"utf-8")
                    except Exception as e:
                        logger_youtube.error(e)
                        continue'''
                    if(videoResult == None):
                        print "error"
                        continue
                    if videoResult.has_key('items'):
                        videoEntry = videoResult['items'][0]
                        currentVideo['ViewCount'] = videoEntry['statistics']['viewCount']
                        if('likeCount' in videoEntry['statistics']):
                            currentVideo['likes'] = videoEntry['statistics']['likeCount']
                            currentVideo['dislikes'] = videoEntry['statistics']['dislikeCount']
                        else:
                            currentVideo['likes'] = 0
                            currentVideo['dislikes'] = 0
                        currentVideo['Embedded'] = videoEntry['status']['embeddable']
                        currentVideo['Status'] = videoEntry['status']['privacyStatus']
                        if(currentVideo['Embedded'] == False or currentVideo['Status'] != 'public'):
                            continue
                        if (int(Video['ViewCount']) < int(currentVideo['ViewCount'])):
                            Video['ViewCount'] = currentVideo['ViewCount']
                            Video['Match'] = currentVideo['Match']
                            Video['TotalMatch'] = currentVideo['TotalMatch']
                            Video['SongMatch'] = currentVideo['SongMatch']
                            Video['ArtistMatch'] = currentVideo['ArtistMatch']
                            Video['Title'] = searchEntry['snippet']['title']
                            Video['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                            Video['VideoId'] = youtubeVideoId
                            Video['PublishedDate'] = searchEntry['snippet']['publishedAt']
                            Video['Duration'] = ParseTime(videoEntry['contentDetails']['duration'])
                            Video['likes'] = currentVideo['likes']
                            Video['dislikes'] = currentVideo['dislikes']
                            iindex=i
                i = i + 1
            if(iindex == -1):
                print "no match"
                return None
            else:
                if(int(Video['likes']) !=0 and int(Video['dislikes'])!=0):
			        Video['rating'] = (float(Video['likes'])*5)/(float(Video['likes'])+float(Video['dislikes']))
                return Video
        return None
    
    def crawlyoutube(self, allArtists, songName, flag,mostpopular, oldvideodetails, IsAllintitle = True ):
        if(flag == 0):
            searchUrl = "&part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&max-results=5&videoCategoryId=10"
        else:
            searchUrl = "&part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(songName))+"&alt=json&type=video&max-results=5&videoCategoryId=10"
            mostpopular = 1
        print searchUrl
        selectedVideo =None
        searchResult = self.getyoutuberesults( searchUrl , 1 )  
        if ( searchResult != None and searchResult.has_key('items') and len(searchResult['items'])!= 0 ):
            i = 0
            selectedVideo = {}
            selectedVideo['ViewCount']=0
            iindex=-1
            selectedVideo['Match'] = ""
            selectedVideo['TotalMatch'] = 0
            selectedVideo['SongMatch'] = 0
            selectedVideo['ArtistMatch'] = 0
            selectedVideo['Title'] = ""
            selectedVideo['Url'] = ""
            selectedVideo['Duration'] = 0
            selectedVideo['likes'] = 0
            selectedVideo['dislikes'] = 0
            selectedVideo['PublishedDate'] = ""
            selectedVideo['Year'] = 0
            selectedVideo['errorstr'] = ""
            selectedVideo['VideoId'] = ""
            for videoresult in searchResult['items']:
                currentVideo = {}
                currentVideo['ViewCount']=0
                searchEntry = searchResult['items'][i]
                [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails,searchEntry['snippet']['title'],searchEntry['snippet']['description'],logger_youtube)
                currentVideo['errorstr'] = error_str
                if(currentVideo['Decision'] == "correct"):# || currentVideoDecision == "Incorrect"):
                    currentVideo['Year'] = GetYearFromTitle(searchEntry['snippet']['title'],songName)
                    youtubeVideoId = searchEntry['id']['videoId']
                    #videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key="+key+"&part=statistics,contentDetails,status"
                    videoResult = self.getyoutubevideodetails(youtubeVideoId)
                    if(videoResult.has_key('items') and  (len(videoResult['items'])>0)):
                        videoEntry = videoResult['items'][0]
                        currentVideo['ViewCount'] = videoEntry['statistics']['viewCount']
                        if('likeCount' in videoEntry['statistics']):
                            currentVideo['likes'] = videoEntry['statistics']['likeCount']
                            currentVideo['dislikes'] = videoEntry['statistics']['dislikeCount']
                        else:
                            currentVideo['likes'] = 0
                            currentVideo['dislikes'] = 0
                        currentVideo['Embedded'] = videoEntry['status']['embeddable']
                        currentVideo['Status'] = videoEntry['status']['privacyStatus']
                        if(currentVideo['Embedded'] == False or currentVideo['Status'] != 'public'):
                            continue
                        if (int(selectedVideo['ViewCount']) < int(currentVideo['ViewCount']) and (mostpopular == 0)):
                            selectedVideo['ViewCount'] = currentVideo['ViewCount']
                            selectedVideo['Match'] = currentVideo['Match']
                            selectedVideo['TotalMatch'] = currentVideo['TotalMatch']
                            selectedVideo['SongMatch'] = currentVideo['SongMatch']
                            selectedVideo['ArtistMatch'] = currentVideo['ArtistMatch']
                            selectedVideo['Title'] = videoEntry['snippet']['title']
                            selectedVideo['Year'] = currentVideo['Year']
                            selectedVideo['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                            selectedVideo['PublishedDate'] = videoEntry['snippet']['publishedAt']
                            selectedVideo['Duration'] = ParseTime(videoEntry['contentDetails']['duration'])
                            selectedVideo['likes'] = currentVideo['likes']
                            selectedVideo['dislikes'] = currentVideo['dislikes']
                            selectedVideo['errorstr'] = currentVideo['errorstr']
                            selectedVideo['VideoId'] = youtubeVideoId
                            iindex=i
                        if (mostpopular == 1):
                            selectedVideo['ViewCount'] = currentVideo['ViewCount']
                            selectedVideo['Match'] = currentVideo['Match']
                            selectedVideo['Year'] = currentVideo['Year']
                            selectedVideo['TotalMatch'] = currentVideo['TotalMatch']
                            selectedVideo['SongMatch'] = currentVideo['SongMatch']
                            selectedVideo['ArtistMatch'] = currentVideo['ArtistMatch']
                            selectedVideo['Title'] = videoEntry['snippet']['title']
                            selectedVideo['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                            selectedVideo['PublishedDate'] = videoEntry['snippet']['publishedAt']
                            selectedVideo['Duration'] = ParseTime(videoEntry['contentDetails']['duration'])
                            selectedVideo['likes'] = currentVideo['likes']
                            selectedVideo['dislikes'] = currentVideo['dislikes']
                            selectedVideo['errorstr'] = currentVideo['errorstr']
                            selectedVideo['VideoId'] = youtubeVideoId
                            iindex=i
                            break
                        if (selectedVideo['TotalMatch'] == currentVideo['TotalMatch'] and (mostpopular == 1) and int(selectedVideo['ViewCount']) < int(currentVideo['ViewCount'])):
                            selectedVideo['ViewCount'] = currentVideo['ViewCount']
                            selectedVideo['Match'] = currentVideo['Match']
                            selectedVideo['TotalMatch'] = currentVideo['TotalMatch']
                            selectedVideo['SongMatch'] = currentVideo['SongMatch']
                            selectedVideo['ArtistMatch'] = currentVideo['ArtistMatch']
                            selectedVideo['Year'] = currentVideo['Year']
                            selectedVideo['Title'] = videoEntry['snippet']['title']
                            selectedVideo['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                            selectedVideo['PublishedDate'] = videoEntry['snippet']['publishedAt']
                            selectedVideo['Duration'] = ParseTime(videoEntry['contentDetails']['duration'])
                            selectedVideo['likes'] = currentVideo['likes']
                            selectedVideo['dislikes'] = currentVideo['dislikes']
                            selectedVideo['errorstr'] = currentVideo['errorstr']
                            selectedVideo['VideoId'] = youtubeVideoId
                            iindex=i
                i = i + 1   
        return selectedVideo


class youtubedlcalls():
    youtubebaseurl = 'https://www.youtube.com/watch?v='

    def searchYoutube( self, allArtists, songName, oldvideodetails, IsAllintitle = True ):
        videoResult = []
        try:
            if( IsAllintitle ):
                url = "allintitle:"+(str(allArtists))+" "+(str(songName))
            else:
                url = (str(allArtists))+" "+(str(songName))
            
            if('cover' not in songName.lower()):
                url += " -cover"

            ydl_opts = {
                'noplaylist': True,
                'cachedir': os.path.join(CacheDir,str(random.randint(0,1000000))),
                'ignoreerrors': False,
                'skipdownload': True
                }
            url = 'ytsearch5:{}'.format(url)
            print url
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                meta = ydl.extract_info(url, download=False)
                ydl.cache.remove()
                if( meta!= None and 'entries' in meta and len(meta['entries']) > 0 ):
                    currentVideo = {}
                    iindex = -1
                    i = 0
                    Video = {}
                    matchedVideoList = {}
                    Video['ViewCount'] = 0
                    Video['rating'] = 0
                    Video['likes'] = 0
                    Video['dislikes'] = 0
                    for entry in meta['entries']:
                        searchEntry = entry
                        searchEntry.pop('formats', None)
                        searchEntry.pop('requested_formats', None)
                        [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails, searchEntry['title'],searchEntry['description'],logger_youtube,True)
                        if( currentVideo['Decision'] == "Incorrect" and '- Topic' in searchEntry['uploader'] ):
                            title = searchEntry['title'] + ' - ' +  str(searchEntry['uploader'].replace('- Topic',''))
                            print title
                            [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails, title,searchEntry['description'],logger_youtube,True)
                        
                        if(currentVideo['Decision'] == "correct"):
                            matchedVideoList[i] = searchEntry
                            youtubeVideoId = searchEntry['id']
                            currentVideo['ViewCount'] = searchEntry['view_count']
                            if('like_count' in searchEntry and searchEntry['like_count'] != None):
                                currentVideo['likes'] = searchEntry['like_count']
                            else:
                                currentVideo['likes'] = 0
                            if('dislike_count' in searchEntry and searchEntry['dislike_count'] != None):
                                currentVideo['dislikes'] = searchEntry['dislike_count']
                            else:
                                currentVideo['dislikes'] = 0
                            currentVideoEmbedded = True
                            currentVideoStatus = 'public'
                            if (int(Video['ViewCount']) < int(currentVideo['ViewCount'])):
                                    Video['ViewCount'] = currentVideo['ViewCount']
                                    Video['Match'] = currentVideo['Match']
                                    Video['TotalMatch'] = currentVideo['TotalMatch']
                                    Video['SongMatch'] = currentVideo['SongMatch']
                                    Video['ArtistMatch'] = currentVideo['ArtistMatch']
                                    Video['Title'] = searchEntry['title']
                                    Video['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                                    Video['VideoId'] = youtubeVideoId
                                    Video['PublishedDate'] = searchEntry['upload_date']
                                    Video['Duration'] = searchEntry['duration']
                                    Video['likes'] = currentVideo['likes']
                                    Video['dislikes'] = currentVideo['dislikes']
                                    iindex=i
                        i = i + 1
                    if(iindex == -1):
                        return None
                    else:
                        matchedVideoList.pop(iindex)
                        Video['youtubedldata'] = list(matchedVideoList.values())
                        if(int(Video['likes']) !=0 and int(Video['dislikes'])!=0):
                            Video['rating'] = (float(Video['likes'])*5)/(float(Video['likes'])+float(Video['dislikes']))
                        return Video
        except Exception as e:
            logger_youtube.exception(e)
            return None


    def getyoutubevideodetails(self,videoid):
        url = self.youtubebaseurl+str(videoid)
        ydl_opts = {
            'noplaylist': True,
            'cachedir': os.path.join(CacheDir,str(random.randint(0,1000000))),
            'ignoreerrors': False,
            'skipdownload': True
        }
        videoResult = {}
        try:
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                meta = ydl.extract_info(url, download=False)
                ydl.cache.remove()
                if( meta == None ):
                    return None
                meta.pop('formats', None)
                meta.pop('requested_formats', None)
                videoResult['items'] = []
                videoEntry = {}
                videoEntry['statistics'] = {}
                videoEntry['snippet'] = {}
                videoEntry['status'] = {}
                if('view_count' in meta ):
                    videoEntry['statistics']['viewCount'] = meta['view_count']
                if('like_count' in meta and meta['like_count'] != None):
                    videoEntry['statistics']['likeCount'] = meta['like_count']
                else:
                    videoEntry['statistics']['likeCount'] = 0
                if('dislike_count' in meta and meta['dislike_count'] != None):
                    videoEntry['statistics']['dislikeCount'] = meta['dislike_count']
                else:
                    videoEntry['statistics']['dislikeCount'] = 0
                if('upload_date' in meta):
                    videoEntry['snippet']['publishedAt'] = meta['upload_date']
                videoEntry['status']['privacyStatus'] = 'public'
                videoEntry['status']['embeddable'] = True
                videoEntry['youtubedldata'] = json.dumps(meta)
                videoResult['items'].append(videoEntry)
                
                return videoResult
        except Exception as e:
            logger_youtube.exception(e)
            return None
        return None
    
    def crawlyoutube(self, allArtists, songName, flag,mostpopular, oldvideodetails ):
        try:
            if( flag == 0 ):
                searchUrl = "allintitle:"+(str(allArtists))+" "+(str(songName))
            else:
                searchUrl = (str(allArtists))+" "+(str(songName))
            
            print searchUrl
            ydl_opts = {
                    'noplaylist': True,
                    'cachedir': os.path.join(CacheDir,str(random.randint(0,1000000))),
                    'ignoreerrors': False,
                    'skipdownload': True
                    }
            url = 'ytsearch5:{}'.format(searchUrl)
            Video =None
            iindex = -1
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                meta = ydl.extract_info(url, download=False)
                if( meta != None and 'entries' in meta and len(meta['entries']) > 0 ):
                    currentVideo = {}
                    iindex = -1
                    i = 0
                    Video = {}
                    matchedVideoList = {}
                    Video['ViewCount'] = 0
                    for entry in meta['entries']:
                        searchEntry = entry
                        searchEntry.pop('formats', None)
                        searchEntry.pop('requested_formats', None)
                        [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails, searchEntry['title'],searchEntry['description'],logger_youtube)
                        if( currentVideo['Decision'] == "Incorrect" and '- Topic' in searchEntry['uploader'] ):
                            title = searchEntry['title'] + ' - ' +  str(searchEntry['uploader'].replace('- Topic',''))
                            print title
                            [currentVideo['Decision'],currentVideo['Match'],currentVideo['TotalMatch'],currentVideo['SongMatch'],currentVideo['ArtistMatch'],error_str] = CalculateMatch(oldvideodetails, title,searchEntry['description'],logger_youtube)
                        

                        if(currentVideo['Decision'] == "correct"):
                            currentVideo['Year'] = GetYearFromTitle(searchEntry['title'],songName)
                            matchedVideoList[i] = searchEntry
                            youtubeVideoId = searchEntry['id']
                            currentVideo['ViewCount'] = searchEntry['view_count']
                            if('like_count' in searchEntry and searchEntry['like_count'] != None):
                                currentVideo['likes'] = searchEntry['like_count']
                            else:
                                currentVideo['likes'] = 0
                            if('dislike_count' in searchEntry and searchEntry['dislike_count'] != None):
                                currentVideo['dislikes'] = searchEntry['dislike_count']
                            else:
                                currentVideo['dislikes'] = 0
                            
                            currentVideoEmbedded = True
                            currentVideoStatus = 'public'
                            if (int(Video['ViewCount']) < int(currentVideo['ViewCount'])):
                                    Video['ViewCount'] = currentVideo['ViewCount']
                                    Video['Match'] = currentVideo['Match']
                                    Video['TotalMatch'] = currentVideo['TotalMatch']
                                    Video['SongMatch'] = currentVideo['SongMatch']
                                    Video['ArtistMatch'] = currentVideo['ArtistMatch']
                                    Video['Title'] = searchEntry['title']
                                    Video['Url'] = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                                    Video['VideoId'] = youtubeVideoId
                                    Video['PublishedDate'] = searchEntry['upload_date']
                                    Video['Duration'] = searchEntry['duration']
                                    Video['likes'] = currentVideo['likes']
                                    Video['dislikes'] = currentVideo['dislikes']
                                    Video['errorstr'] = error_str
                                    Video['Year'] = currentVideo['Year']
                                    if(int( Video['likes'] ) !=0 and int( Video['dislikes'] )!=0):
                                        Video['rating'] = (float(Video['likes'])*5)/(float(Video['likes'])+float(Video['dislikes']))
                                    iindex=i
                        i = i + 1
            ydl.cache.remove()
            if(iindex == -1):
                return None
            else:
                matchedVideoList.pop(iindex)
                Video['youtubedldata'] = list(matchedVideoList.values())
                return Video
        except Exception as e:
            logger_youtube.exception(e)
            return None

        
