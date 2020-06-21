import managekeys
import loggingmodule
from songsutils import is_songname_same_artistname,moveFiles
from urllib2 import urlopen, URLError, HTTPError
import simplejson
import youtube_dl
import json
logger_matrix = loggingmodule.initialize_logger('youtubeapis','youtubeapis.log')

#Base = declarative_base()
class youtubecalls():
    __youtubebaseurl__ = "https://www.googleapis.com/youtube/v3/videos?"

    def __init__(self,manager):
        self.manager = manager

    def getyoutuberesults(self,youtubeId):
        #getkeys

        key = self.manager.getkey()
        if(key == ""):
            self.manager.keys_exhausted()
            key = self.manager.getkey()
            if(key == ""):
                logger_matrix.error(self.manager.get_blocked_keys())
                self.manager.keys_exhausted()
                logger_matrix.error('Waking up')
                key = self.manager.getkey()
        query = "&id="+str(youtubeId)+"&part=statistics,snippet,status"
        currUrl = youtubecalls.__youtubebaseurl__+"&key="+key
        currUrl += query
        try:
            videoResult = simplejson.load(urlopen(currUrl),"utf-8")
            return videoResult
        except HTTPError as e:
            if(e.code == 403 and "Forbidden" in e.reason):
                logger_matrix.error("Daily Limit Exceeded")
                logger_matrix.error(self.manager.get_blocked_keys())
                self.manager.removekey(key)
                self.manager.add_blockedkey(key)
                self.manager.keys_exhausted()    
            else:
                print(e)
                logger_matrix.exception("Error loading json"+ currUrl + "\n")
            movefilestofailed(filename)
            return None


class youtubedlcalls():
    __youtubebaseurl__ = 'https://www.youtube.com/watch?v='

    def getyoutuberesults(self,videoid):
        url = self.__youtubebaseurl__+str(videoid)
        ydl_opts = {
            'noplaylist': True
        }
        videoResult = {}
        try:
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                ydl.cache.remove()
                meta = ydl.extract_info(url, download=False)
            meta.pop('formats', None)
            meta.pop('requested_formats', None)
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
            videoEntry['youtubedldata'] = json.dumps(meta)
            videoResult['items'].append(videoEntry)
            
            return videoResult
        except Exception as e:
            logger_matrix.exception(e)
            return None
        
