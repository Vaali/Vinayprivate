import sys
import json
import glob
import logging
import codecs
import urllib
import urllib2
import simplejson
import pickle
from datetime import datetime, date, timedelta
import re
from solr import SolrConnection
from solr.core import SolrException
import time


reload(sys)
sys.setdefaultencoding('utf8')
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('simple_logger')
logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG, filename='errors_discogs_dump11.log')
solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_artists')

class Video(object):
	pass

class Album_Data():
	pass

class Audio(object):
	pass

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

def getArtistAliasList(sorted_list):
    artist_alias_list = {}
    final_artist_alias_list = []
    for song in sorted_list:
		if(song['artist_id'] in artist_alias_list):
			artist_alias_list[song['artist_id']] = artist_alias_list[song['artist_id']] + 1
		else:
			artist_alias_list[song['artist_id']] = 1
    for i in artist_alias_list:
		artist_alias_list[i] = (artist_alias_list[i]*100)/len(sorted_list)
    artist_alias_list = sorted(artist_alias_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
    for artist in artist_alias_list:
		if(artist[1] > 4.0):
			final_artist_alias_list = getAliasFromArtistsSolr(final_artist_alias_list,artist[0])
    return final_artist_alias_list

def getAliasFromArtistsSolr(final_artist_alias_list,artist_id):
    global solrConnection
    artistId = 'artistId:"'+str(artist_id)+ '"'
    intersect = 0
    try:
        try:
            response = solrConnection.query(q="*:*",fq=[artistId],version=2.2,wt = 'json')
            intersect = int(response.results.numFound)
        except SolrException as e:
            logging.exception(e)
            return final_artist_alias_list
        if(intersect <= 0):
            return final_artist_alias_list
        for result in response.results:
            if('artistName' in result):
                if(result['artistName'] not in final_artist_alias_list):
                        final_artist_alias_list.append(result['artistName']) 
            #if('artistAliasesName' in result):
            #    for artalias in result['artistAliasesName']:
            #        if(artalias.lower() not in final_artist_alias_list):
            #            final_artist_alias_list.append(artalias.lower())
            #if('artistNameVariations' in result):
            #    for artnamevar in result['artistNameVariations']:
            #        if(artnamevar.lower() not in final_artist_alias_list):
            #            final_artist_alias_list.append(artnamevar.lower())
    except Exception, e:
            logging.exception(e)
    return final_artist_alias_list

def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)


def check(date1,date2):
    list1 = date1.split('-')
    list2 = date2.split('-')
    if(len(list1) != 3):
        list1.append(13)
        list1.append(32)
    if(len(list2) != 3):
        list2.append(13)
        list2.append(32)
	if(list1[0] == 1001):
		return 2
	if(list2[0] == 1001):
		return 1
	if(list1[0] > list2[0]):
		return 2
	if(list1[0] < list2[0]):
		return 1
	if(len(list1) == 1 and len(list2) == 1):
		return 1
	if(len(list1) > len(list2)):
		return 1
	if(len(list2) > len(list2)):
		return 2
	if(list1[1] > list2[1]):
		return 2
	if(list1[1]< list2[1]):
		return 1
	if(list1[2] > list2[2]):
		return 2
	if(list1[2]< list2[2]):
		return 1

def get_song_list(directory,songs_list,full_country_list,aliases):
    releases_list = glob.glob(directory+"/*.json")
    for release in releases_list:
        try:
            filename = release
            with codecs.open(filename,"r","utf-8") as input:
                curr_album = json.load(input)
            #if('anv' in curr_album and curr_album['anv'] != None and curr_album['anv'].lower() not in aliases):
            #    aliases.append(curr_album['anv'].lower())
            #if(curr_album['artist_name'].lower() not in aliases):
            #    aliases.append(curr_album['artist_name'].lower())
            for track in curr_album['tracks']:
                if(track == None):
                    continue
                song = {}
                song['styles'] = curr_album['styles']
                song['genres'] = curr_album['genres']
                song['artistName'] = re.sub(r'\(.*?\)', '', curr_album['artist_name']).strip()
                song['artist_id'] = curr_album['artist_id']
                song['featArtists'] = []
                song['connectors'] = []
                if('artists' in track):
                    for artist in track['artists']:
                        if(artist == None):
                            continue
                        artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip()
                        if('artist_name' in artist and artist['artist_name'].lower() != song['artistName'].lower()):
                            song['featArtists'].append(artist['artist_name'])
                            song['connectors'].append(artist['join_relation'])
                song['name'] = track['title']
                if('duration' in curr_album):
                    song['duration'] = track['duration']
                albumInfo = {}
                albumInfo['albumName'] = curr_album['title']
                albumInfo['year'] = curr_album['released_date']
                albumInfo['country'] = curr_album['country']
                if(curr_album['country'] not in full_country_list):
                    full_country_list[curr_album['country']] = 1
                else:
                    full_country_list[curr_album['country']] = full_country_list[curr_album['country']] + 1
                albumInfo['language'] = "English"
                song['albumInfo'] = [albumInfo]
                songs_list.append(song)        
        except Exception, e:
            logging.exception(e)
    return songs_list,full_country_list,aliases

def CalculateMatch(curr_elem,vid_title):
    try:
        list = ""
        conlist = ""
        artistName = curr_elem['artistName']
        ftArtistName = curr_elem['featArtists']
        connectorList = curr_elem['connectors']
        songName = curr_elem['name']
        tm = 0
        sm = 0
        am = 0
        fList = ""
        for f in ftArtistName:
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
            substring_song = "false"
        if(vid_title.lower().find(artistName.lower())!= -1):
            substring_artist = "true"
        else:
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
        #print percentMatch
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
            #print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
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
            #print			
            #if artistMatch > tempArMatch:
            match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
            tm = percentMatch
            sm = songMatch
            am = artistMatch
        if((y1 == -1) and (y2 == -1)):	
            #print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
            arnameset = set(re.findall("\w+",artistName.lower(),re.U)) - set(diffset) - set(ftArtistSet)
            leftset = yresultset[:len(arnameset)]
            rightset = yresultset[-len(arnameset):]
            leftIntersection = (set(leftset).intersection(set(arnameset)))
            rightIntersection = (set(rightset).intersection(set(arnameset)))
            leftMatch = 0
            rightMatch = 0
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
    except Exception, e:
            logging.exception(e)
    return decision,match,tm,sm,am

def getVideo(curr_elem):
    global hits
    global request_count
    global misses
    alist = list()
    ylist = list()
    video = Video()
	#album_details = Album_Data()
    video.artist = curr_elem['artistName']
    video.ftArtist = curr_elem['featArtists']
    video.name = curr_elem['name']
    video.connectors = curr_elem['connectors']
    songs_list = curr_elem['albumInfo']
    #combine all the album information for the songs into a list.
    for l in songs_list:
		#alist.append(getAlbumInfo(l))
		#ylist.append(getYear(l))
		album_details = Album_Data()
		album_details.albumname = l['albumName']
		album_details.year = l['year']
		if('country' in l and l['country'] != None and len(l['country'])> 0):
			album_details.country = l['country']
		else:
			album_details.country = 'No Country'			
		if(len(l['language'])> 0):
			album_details.language = l['language']
		else:
			album_details.country = 'No Language'

		'''if(len(l['barCode'])> 0):
			album_details.barcode = l['barCode']'''
		alist.append(album_details.__dict__)
    if(len(alist)==0):
        print curr_elem
    video.album = alist
    video.year = curr_elem['year']
    video.language = curr_elem['language']
    video.songcountry = curr_elem['songcountry']
    flist = ""
	#Apostolos
    for f in video.ftArtist:
		ttt=f.strip("-")
		flist = flist+" "+ttt
		
    ftartists = flist[1:]
    allArtists = video.artist.strip("-")+" "+ftartists
    if('cover' not in video.name.lower()):
		searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"+-cover"+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU"
    else:
		searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&channelID=UC-9-kyTW8ZkZNDHQJ6FgpwQ&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU"
    try:
		searchResult = simplejson.load(urllib2.urlopen(searchUrl),"utf-8")
		request_count = request_count + 2
    except Exception as e:
		request_count = request_count + 2
		logging.exception("Error")
		misses = misses + 1
		return None
		#logging.warning('No results from google',e)
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
		#selectedVideoImgUrl = ""
        selectedVideoPublishedDate = ""
        for videoresult in searchResult['items']:
            searchEntry = searchResult['items'][i]
            [currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch] = CalculateMatch(curr_elem,searchEntry['snippet']['title'])
            if(currentVideoDecision == "correct"):
                youtubeVideoId = searchEntry['id']['videoId']
                videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&part=statistics,contentDetails,status"
                try:
                    videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
                    request_count = request_count + 7
                except Exception as e:
                    request_count = request_count + 7
                    logging.exception("Error")
                    continue
                if videoResult.has_key('items'):
                    videoEntry = videoResult['items'][0]
                    currentVideoViewCount = videoEntry['statistics']['viewCount']
                    currentVideolikes = videoEntry['statistics']['likeCount']
                    currentVideodislikes = videoEntry['statistics']['dislikeCount']
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
            else:
                j=0
                #print currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch,curr_elem['name'],searchEntry['snippet']['title']
            i = i + 1
        if(iindex == -1):
			misses = misses + 1
			return None
        video1 = Video()
        video1.artist = curr_elem['artistName']
        video1.ftArtist = curr_elem['featArtists']
        video1.name = curr_elem['name']
        video1.connectors = curr_elem['connectors']
        video1.album = alist
        video1.year = curr_elem['year']
        video1.language = curr_elem['language']
        video1.songcountry = curr_elem['songcountry']
        if('artistalias' in curr_elem):
            video1.artistalias = curr_elem['artistalias']
        video1.genres = curr_elem['genres']
        video1.styles = curr_elem['styles']
        if(int(selectedVideolikes) !=0 and int(selectedVideodislikes)!=0):
			video1.rating = (float(selectedVideolikes)*5)/(float(selectedVideolikes)+float(selectedVideodislikes))
			#print video1.rating
        video1.lang_count = curr_elem['lang_count']
        video1.url = selectedVideoUrl
        video1.match = selectedVideoMatch
        video1.tm = selectedVideoTotalMatch
        video1.sm = selectedVideoSongMatch
        video1.am = selectedVideoArtistMatch
        video1.title = selectedVideoTitle
        video1.published = selectedVideoPublishedDate
        m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),video1.published)
        n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),video1.published)
        ydate = m.group()+" "+n.group()
        dd = ydate
        yy = int(str(dd)[0:4])
        mm = int(str(dd)[5:7])
        total = (now.year-yy)*12+(now.month-mm)
        if total < 1:
            total = 1
        video1.length = selectedVideoDuration
        if(now.month<10):
			mm = '0'+str(now.month)
        else:
			mm = str(now.month)
        if(now.day<10):
			dd = '0'+str(now.day)
        else:
			dd = str(now.day)
        video1.crawldate = str(now.year)+"-"+mm+"-"+dd
        video1.viewcount = selectedVideoViewCount
        if(total != 0):
			video1.viewcountRate = float(video1.viewcount)/total
        #v.append(video1.__dict__)
        #video1 = None
        hits = hits + 1
        return video1
    else:
		misses = misses + 1
        return None

def crawlArtist(directory):
    songs_list = list()
    global misses
    global hits
    full_lang_list = {}
    full_country_list ={}
    aliases = []
    songs_list,full_country_list,aliases = get_song_list(directory,songs_list,full_country_list,aliases)
    sorted_list = sorted(songs_list,key = lambda x:x['name'].lower()) 
    final_song_list = {}
    hits = 0
    misses = 0
    global request_count
    request_count = 0
    try:
        curr_time = "2020-14-33"
        curr_language = ""
        curr_song = {}
        artist_alias_list = []
        artist_alias_list = getArtistAliasList(sorted_list)
        for song in sorted_list:
            Item_id = song['name'].lower()
            Item_id = Item_id + " "
            if(len(song['featArtists'])!= 0):
                temp_str = ','.join(song['featArtists'])
                Item_id = Item_id + temp_str.lower()
            Item_id.strip()
            k = 0
            if(Item_id not in final_song_list):
                final_song_list[Item_id] = song
                final_song_list[Item_id]['year'] = song['albumInfo'][0]['year']
                if(final_song_list[Item_id]['year'] == None):
                    final_song_list[Item_id]['year'] = '1001'
                final_song_list[Item_id]['language'] = song['albumInfo'][0]['language']
                final_song_list[Item_id]['songcountry'] = song['albumInfo'][0]['country']
                lang_count = {}
                lang_count[final_song_list[Item_id]['language']] = 1
                final_song_list[Item_id]['lang_count'] = lang_count
            else:
                stemp = final_song_list[Item_id]
                for s in song['albumInfo']:
                    lang_count = {}
                    if(stemp.has_key('lang_count')):
                        lang_count = stemp['lang_count']
                    if(s['language'].strip() != ''):
                        if(s['language'] in lang_count):
                            lang_count[s['language']] = lang_count[s['language']] + 1
                        else:
                            lang_count[s['language']] = 1
                        #for full count of all the songs language
                    stemp['lang_count'] = lang_count
                    #print stemp
                    if(stemp['year'] == '1001' and s['year'] != None):
                        stemp['year'] = (s['year'])
                    elif (s['year'] != '1001' and s['year'] != None):
                        k = check(s['year'],stemp['year'])
                        if(k==1):
                            stemp['year'] = (s['year'])
                            stemp['language'] = s['language']
                        stemp['songcountry']  = s['country']
                    if(k==3 and stemp['language'].find(s['language']) == -1):
                        stemp['year'] = (s['year'])
                        stemp['language'] = s['language']+'&'+stemp['language']
                        stemp['songcountry']  = s['country']
                    stemp['albumInfo'].append(s)
        total_count = 0
        for i in full_lang_list:
            total_count = total_count + full_lang_list[i]
        percent_lang = {}
        for i in full_lang_list:
            percent_lang[i] = (full_lang_list[i]*100.0)/total_count
        percent_lang = sorted(percent_lang.iteritems(), key=lambda (k,v): (v,k),reverse = True)
        change_language = ''
        if(len(percent_lang) != 0 and percent_lang[0][1] > 97.0):
            change_language = percent_lang[0][0]
        else:
            change_language = ''
        full_lang_list = sorted(full_lang_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)

        full_country_list_sort = sorted(full_country_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
        '''if(len(full_country_list) != 0):
            if(artist_country not in full_country_list):
                artist_country = full_country_list_sort[0][0]'''
        vid = list()
        with open(directory + '/uniquelist.txt', 'wb') as f:
			pickle.dump(final_song_list.keys(), f)
        parallel_songs_list = []
        for s in final_song_list.values():
            lang_dict = s['lang_count']
            s['songcountry'] = ''
            temp_lang_list = sorted(lang_dict.iteritems(), key=lambda (k,v): (v,k),reverse = True)
            if( len(temp_lang_list) >1 and temp_lang_list[0][1] == temp_lang_list[1][1]):
                s['language'] = full_lang_list[0][0]
            elif(len(temp_lang_list)!=0):
                s['language'] = temp_lang_list[0][0]
            if(change_language != '' and s['language']!= change_language):
                fchange_language.write(s['name']+ '\t'+s['language'] + '\t' + change_language+'\n')
                s['language'] = change_language
            if(not s.has_key('artistName')):# or s['artistName'] not in aliases):
                continue
            
            t1=time.time()
            if(s['artistName'] in artist_alias_list):
                for art_alias in  artist_alias_list:
                    s['artistName'] = art_alias
                    s['artistalias'] = artist_alias_list
                    #curr_vid = getVideo(s,vid)
                    parallel_songs_list.append(s)
            else:
                #curr_vid = getVideo(s,vid)
                parallel_songs_list.append(s)
            
            #print time.time() - t1
        
        print "Hits:"+str(hits)+" Misses:"+str(misses)
        write(vid,directory+"/dump")

    except Exception, e:
        logging.exception(e)

        
reload(sys)
sys.setdefaultencoding('utf8')
filenameList = []
if(len(sys.argv) > 0):
    filenameList = sys.argv[1:]
for filename in filenameList:
	try:
		crawlArtist(str(filename))
		logger.exception("completed")
	except Exception as e:
		print e,filename