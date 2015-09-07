# -*- coding: utf-8 -*-
import sys
import json
import simplejson
import re
import os
import codecs
import urllib
import urlparse
import urllib2
import difflib
#import libxml2
from lxml import etree
from datetime import datetime, date, timedelta
import songs_api as api
from multiprocessing import Pool
import logging
import ConfigParser
import random
import time
import pickle
import collections


reload(sys)
sys.setdefaultencoding('utf8')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)s - %(thread)s:%(message)s')
logger_error = logging.getLogger('simple_logger')
hdlr = logging.FileHandler('errors_getVideos.log')
hdlr.setFormatter(formatter)
logger_error.addHandler(hdlr)
logger_error = logging.getLogger('simple_logger')


formatter1 = logging.Formatter('%(message)s')
logger_decisions = logging.getLogger('simple_logger1')
hdlr_1 = logging.FileHandler('decisions_new.log')
hdlr_1.setFormatter(formatter1)
logger_decisions.addHandler(hdlr_1)
logger_decisions = logging.getLogger('simple_logger1')

def generatexmls(dirlist):
    try:
        d = dirlist
        #for d in dirlist:
        if(len(d.strip()) == 0):
			#continue
			return
        directory = d.strip()
        dindex = directory.rfind("/")
        vids = []
        try:
			vids = load(directory+'/dump')
        except Exception as e:
			#continue
			return
        artistId = directory[dindex+1:]
        for v in vids:
            t3=datetime.now()
            if(int(artistId) == 194):
                artistId = v['artist_id']
            genXML(v,avgcnt,avgcntrece,artistId)
    except Exception as e:
        logger_error.exception(e)

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


def getVideoFromYoutube(curr_elem):
    retvid = None
    bret = False
    artname = curr_elem['artistName']
    try:
        retvid,bret = getVideo(curr_elem,0)
        if('anv' in curr_elem):
            curr_elem['artistName'] = curr_elem['anv']
            retvid,bret = getVideo(curr_elem,0)
            if(retvid != None):
                for rv in retvid:
                    rv.artist = artname
        if(retvid == None):
            curr_elem['artistName'] = artname
            retvid,bret = getVideo(curr_elem,1)
        else:
            emptyvid = 0
            for rv in retvid:
                if('url' in rv.__dict__):
                    emptyvid = 1
            if(emptyvid == 0):
                curr_elem['artistName'] = artname
                retvid,bret = getVideo(curr_elem,1)
    except Exception as e:
        logger_error.exception('getVideoFromYoutube')
    return retvid,bret


def getVideo(curr_elem,flag):
    try:
        global request_count
        mostpopular = 0
        videolist = []
        #print curr_elem
        #print curr_elem['artistName']
        alist = list()
        ylist = list()
        video12 = Video()
        bret = False
        #album_details = Album_Data()
        video12.artist = curr_elem['artistName']
        video12.ftArtist = curr_elem['featArtists']
        video12.name = curr_elem['name']
        video12.connectors = curr_elem['connectors']
        songs_list = curr_elem['albumInfo']
        unique_albums = []
        #combine all the album information for the songs into a list.
        for l in songs_list:
            album_details = Album_Data()
            #album_details.albumname = removeStemCharacters(l['albumName'])
            album_details.albumname = l['albumName']
            album_details.year = l['year']
            if('country' in l and l['country'] != None and len(l['country'])> 0):
                album_details.country = l['country']
            else:
                album_details.country = 'No Country'			
            '''if(len(l['language'])> 0):
                album_details.language = l['language']
            else:
                album_details.country = 'No Language' 
            '''
            if(l['albumName'].lower().strip() not in unique_albums):
                unique_albums.append(l['albumName'].lower().strip())
                #album_details.albumname = removeStemCharacters(l['albumName'])
                album_details.albumname = l['albumName']
                if(l['year'] != None):
                    album_details.albumname = album_details.albumname + " " + l['year']
                alist.append(album_details.__dict__)
        if(len(alist)==0):
            print curr_elem
        video12.album = alist
        video12.year = curr_elem['year']
        video12.language = 'English'
        video12.songcountry = curr_elem['songcountry']
        flist = ""
        #Apostolos
        try:

            video1 = Video()
            video2 = Video()
            video1.artist = curr_elem['artistName']
            video1.ftArtist = curr_elem['featArtists']
            video1.name = curr_elem['name']
            video1.connectors = curr_elem['connectors']
            video1.album = alist
            video1.year = curr_elem['year']
            video1.language = 'English'
            video1.songcountry = curr_elem['songcountry']
            if('anv' in curr_elem):
                video1.anv = curr_elem['anv']
            if('artistalias' in curr_elem):
                video1.artistalias = curr_elem['artistalias']
            video1.genres = curr_elem['genres']
            video1.styles = curr_elem['styles']
            video1,bret = getYoutubeUrl(video1,flag,0)
            #video2,bret = getYoutubeUrl(video1,flag,1)#comment it to get more videos Apostolos
            #else:
            #    return None
        except Exception as e:
            logger_error.exception(e)
            return None,bret
    except Exception as e:
            logger_error.exception(e)
            return None,bret
    return [video1,video2],bret

def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)

class Video():
	pass


class Album_Data():
	pass

class Audio(object):
	pass

def RemoveStemCharacters(currString):
    currString = currString.replace('"','').strip()
    currString = currString.replace('’','').strip()
    currString = currString.replace("'",'').strip()
    currString = currString.replace("‘",'').strip()
    currString = currString.replace("?",'').strip()
    currString = currString.replace(',','').strip()
    return currString

def GetYearFromTitle(vid_title):
    returnYear = 0
    yearList = re.findall(r'\d\d\d\d+',vid_title)
    print yearList
    if(len(yearList) != 0):
        returnYear = int(yearList[0])
    return returnYear

def CalculateMatch(video,vid_title):
    try:
        #soundex = fuzzy.Soundex(len(vid_title))
        list = ""
        conlist = ""
        artistName = video.artist
        ftArtistName = video.ftArtist
        connectorList = video.connectors
        songName = video.name
        fList = ""
        albumname = ""
        decision = "Incorrect"
        stemwords = [ 'screen','m/v','artist','ft','featuring','live','hd','1080P','video','mix','feat','official','lyrics','music','cover','version','original','\
hq','band','audio','album','world','instrumental','intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod','piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single','official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','song','digital','hour','nature','motion','sounds','sound','ballad','unplugged','singers','singer','legend','legends', 'french','strings','string','classic','cast','act','full','screen','radio','remix','song','edit','tracks','remaster','reissue','review','re-issue','trailer','studio','improvization','solo','download','tour','dvd','festival','remastered']
        '''stemwords = ['video','mix','feat','official','lyrics','music','cover','version','original','hq','band','audio','album','world','instrumental', 'intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod', 'piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single', 'official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','digital','hour','nature','motion','sounds','ballad','unplugged','singers','legend', 'french','strings','classic','cast','act','full','screen','radio','remix','song','edit','tracks']'''
        stemcharacters = ['[',']','(',')','\'','"','.','’']
        youtubematch = vid_title.lower()
        diffset = []
        substring_album = "false"
        #remove the characters form songname and youtubename
        for c in stemcharacters:
            youtubematch = youtubematch.replace(c,'')
            songName = songName.replace(c,'')
        artist_order = {}
        #remvoe the stemwords form youtube name
        songnameset = re.findall("\w+",songName.lower(),re.U)
        for word in stemwords:
            if(word not in songnameset):
                diffset.append(word)
                pattern = '\\b'+word+'\\b'
                youtubematch = re.sub(pattern,'', youtubematch)
        
        for c in connectorList:
            if(c != None):
                conlist = conlist+" "+c	
        #Find positions of artist ,song and feat artists
        songpos = youtubematch.lower().find(songName.lower())
        artpos = youtubematch.lower().find(artistName.lower())
        for l in video.album:
            albumpos = youtubematch.lower().find(l['albumname'].lower())
            if(albumpos != -1):
                substring_album = "true"
                artist_order[albumpos] = l['albumname']
                albumname = l['albumname']
                break
        ftart_substring = []
        ftartpos = []
        ftartistmatch = []
        if(songpos != -1):
            substring_song = "true"
            artist_order[songpos] = songName
        else:
            substring_song = "false"
        if(artpos != -1):
            substring_artist = "true"
            artist_order[artpos] = artistName
        else:
            substring_artist = "false"
        for f in ftArtistName:
            fList = fList+" "+f
            currpos = youtubematch.lower().find(f.lower())
            ftartpos.append(currpos)
            if(currpos != -1):
                ftartistmatch.append(True)
                artist_order[artpos] = f
            else:
                ftartistmatch.append(False)
             
        ftartists = ""
        if(len(fList)!=0):
            ftartists = fList[0:]
        allArtists = artistName+" "+ftartists
        ftArtistSet = re.findall("\w+",ftartists.lower(),re.U)
        ftAMatch = 0
        ftMatch = 0
        songMatch = 0
        leftMatch = 0
        rightMatch = 0
        
        yfullset = re.findall("\w+",youtubematch.lower(),re.U)
        ydiffset = set(yfullset) - set(diffset)
        yresultset = [o for o in yfullset if o in ydiffset]
        if "feat" in yresultset:
            totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower(),re.U)
        else:
            totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
        if(substring_album == "true"):
            totalset = totalset + re.findall("\w+",albumname.lower(),re.U)
        common =[]
        common = (set(yresultset).intersection(set(totalset)))
        percentMatch = 0.0
        if float(len(yresultset)) !=0:
            percentMatch = len(common)*100/float(len(yresultset))
        
        youtubesongset = set(re.findall("\w+",youtubematch.lower(),re.U))- set(diffset) - set(allArtists) - set(re.findall("\w+",albumname.lower(),re.U))
        songset = set(re.findall("\w+",songName.lower(),re.U))
        
        common1 = (youtubesongset).intersection(songset)
        if(len(songset) != 0):
            songMatch = len(common1)*100/float(len(songset))
        #check if - is present
        comparestring = "";
        for key in sorted(artist_order):
            comparestring = comparestring + " " + artist_order[key]
        yname = youtubematch
        yname = yname.lower().replace("feat.","")
        yname = yname.lower().replace("ft.","")
        yname = yname.lower().replace("featuring","")
        y1 = yname.find("-")
        y2 = yname.find(":")
        bhiphen = False
        
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
            if(len(snameset) > len(sreadset)):
                if float(len(snameset)) !=0:
                    songMatch = len(common1)*100/float(len(snameset))
            else:
                if float(len(sreadset)) !=0:
                    songMatch = len(common1)*100/float(len(sreadset))
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
            lam = leftMatch
            ram = rightMatch
            """if('light my fire' in vid_title.lower()):
                print youtubematch
                print vid_title.lower()
                print songset
                print common1
                print snameset
                print songMatch
                print 'xxx--xxx-xxxxx-xxx--xxx' """
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
            if(len(snameset) > len(sreadset)):
                if float(len(snameset)) !=0:
                    songMatch = len(common1)*100/float(len(snameset))
            else:
                if float(len(sreadset)) !=0:
                    songMatch = len(common1)*100/float(len(sreadset))
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
            lam = leftMatch
            ram = rightMatch
            """if('light my fire' in vid_title.lower()):
                print youtubematch
                print vid_title.lower()
                print songset
                print common1
                print snameset
                print songMatch
                print songName
                print 'xxx--xxx-xxxxx-xxx--xxx' """
        if((y1 == -1) and (y2 == -1)):	
            #print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
            arnameset = set(re.findall("\w+",artistName.lower(),re.U)) - set(diffset) - set(ftArtistSet)
            leftset = yresultset[:len(arnameset)]
            rightset = yresultset[-len(arnameset):]
            leftIntersection = (set(leftset).intersection(set(arnameset)))
            rightIntersection = (set(rightset).intersection(set(arnameset)))
            if float(len(arnameset))  !=0:
                leftMatch = len(leftIntersection)*100/float(len(arnameset)) 
                rightMatch = len(rightIntersection)*100/float(len(arnameset))
            if(leftMatch > rightMatch):
                songreadset = set(re.findall("\w+",songName.lower(),re.U)) - set(diffset) - set(ftArtistSet) - set(arnameset)	
                common_set = (set(yresultset).intersection(set(songreadset)))
                tempset = set(yresultset[-len(songreadset):])
                yresultset = (set(yresultset) - set(arnameset))
                if float(len(songreadset)) !=0:
                    songMatch = len(common_set)*100/float(len(songreadset))
                match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
                tm = percentMatch
                sm = songMatch
                am = leftMatch
                lam = leftMatch
                ram = rightMatch 
            else:
                songreadset = set(re.findall("\w+",songName.lower(),re.U)) - set(diffset) - set(ftArtistSet) - set(arnameset)	
                common_set = (set(yresultset).intersection(set(songreadset)))
                yresultset = (set(yresultset) - set(arnameset))
                
                if float(len(songreadset)) !=0:
                    songMatch = len(common_set)*100/float(len(songreadset))
                match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
                tm = percentMatch
                sm = songMatch
                am = leftMatch
                lam = leftMatch
                ram = rightMatch 
        decision = "Incorrect"
        condition = 0
        # if all substraing match is true for all and the number of words is greater than 1 for atleast one.
        if((am+sm+lam+ram>180) and bhiphen and tm > 30 and sm>30):
            decision = "correct"
            condition = 6    
        elif(sm+am+ram>140  and not bhiphen and (len(artistName.strip().split()) > 1) and tm>30 and sm>30):
            decision = "correct"
            condition = 7 
        elif(tm>70 and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1) and sm>30 and  (am>0 or lam>0 or ram>0)):
            decision = "correct"
            condition = 8 
        elif(substring_artist == "true" and substring_song == "true" and (len(ftartists) == 0 or (len(ftartists)!=0 and ftMatch == 100.0)) and ( len(artistName.strip().split()) > 1) and percentMatch > 60.0):
            decision = "correct"
            condition = 1
        #if song is false then look for song match and length must be greater than 1
        elif(substring_song == "false" and songMatch  >= 80.0 and ( len(artistName.strip().split()) > 1) and  (am>0 or lam>0 or ram>0)):
            decision = "correct"
            condition = 2
        #if artist  is false look for artistmatch left or [right and total match]
        elif(substring_artist == "false" and (leftMatch == 100.0  or  (rightMatch == 100.0 and percentMatch  > 60.0)) and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1) and sm>30):
            decision = "correct"
            condition = 3
        #if only one words for both song and artist ,check total match and leftmatch for - case.
        elif(substring_artist == "true" and substring_song == "true"  and (percentMatch > 80.0 or (leftMatch == 100.0 and bhiphen and len(artistName.strip().split()) > 1))):
            decision = "correct"
            condition = 4
        #no hiphen , song match shd be 100 and left or right should be 100 
        elif(substring_artist == "true" and substring_song == "true" and not bhiphen and songMatch == 100.0 and (leftMatch == 100.0 or rightMatch == 100.0) and percentMatch > 60.0 and ( len(artistName.strip().split()) > 1)):
            decision = "correct"
            condition = 5
        if(bhiphen == "true" and (songMatch == 0  or (leftMatch == 0.0 and rightMatch == 0.0))):
            decision = "Incorrect"
        logger_decisions.error(decision)
        logger_decisions.error(condition)
        logger_decisions.error(match)
        logger_decisions.error(artistName)
        logger_decisions.error(songName)
        logger_decisions.error(ftArtistName)
        logger_decisions.error("substring_album:")
        logger_decisions.error(substring_album)
        logger_decisions.error(vid_title)
        logger_decisions.error(comparestring)
        logger_decisions.error("substring_song : ")
        logger_decisions.error(substring_song)
        logger_decisions.error("substring_artist : ")
        logger_decisions.error(substring_artist)
        logger_decisions.error("ftartistmatch : ")
        logger_decisions.error(ftartistmatch)
        logger_decisions.error("Total match : ")
        logger_decisions.error(percentMatch)
        logger_decisions.error(youtubematch)
        #logger_decisions.error(fuzz.ratio(youtubematch.lower(),comparestring.lower()))
        logger_decisions.error("phonetic distance : ")
        #logger_decisions.error(fuzz.ratio(soundex(youtubematch.lower()),soundex(comparestring.lower())))
        logger_decisions.error('-----------------')
    except Exception, e:
            logger_error.exception(e)
    return decision,match,tm,sm,am


def getYoutubeUrl(video,flag,mostpopular):
    global request_count
    bret = False
    try:
        flist = ""
        for f in video.ftArtist:
            ttt=f.strip("-")
            flist = flist+" "+ttt
        ftartists = flist
        allArtists = video.artist.strip("-")+" "+ftartists
        if(flag == 0):
            '''if('cover' not in video.name.lower()):
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"+-cover"+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
            else:'''
            searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
        else:
            '''if('cover' not in video.name.lower()):
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"+-cover"+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
            else:'''
            searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
            mostpopular = 1
        print searchUrl
        try:
            searchResult = simplejson.load(urllib2.urlopen(searchUrl),"utf-8")
            request_count = request_count + 2
            #print searchResult
        except Exception as e:
            request_count = request_count + 2
            logger_error.exception("Error")
            return video,bret
        now = datetime.now()
        try:
            if searchResult.has_key('items') and len(searchResult['items'])!= 0:
                i = 0
                selectedVideoViewCount=0
                currentVideoViewCount=0
                iindex=-1
                earliestindex = 9
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
                selectedVideoYear = 0
                for videoresult in searchResult['items']:
                    searchEntry = searchResult['items'][i]
                    [currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch] = CalculateMatch(video,searchEntry['snippet']['title'])
                    if(currentVideoDecision == "correct"):# || currentVideoDecision == "Incorrect"):
                        currentVideoYear = GetYearFromTitle(searchEntry['snippet']['title'])
                        youtubeVideoId = searchEntry['id']['videoId']
                        videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&part=statistics,contentDetails,status"
                        try:
                            videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
                            request_count = request_count + 7
                        except Exception as e:
                            request_count = request_count + 7
                            logger_error.exception(e)
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
                            if (int(selectedVideoViewCount) < int(currentVideoViewCount) and (mostpopular == 0)):
                                selectedVideoViewCount = currentVideoViewCount
                                selectedVideoMatch = currentVideoMatch
                                selectedVideoTotalMatch = currentVideoTotalMatch
                                selectedVideoSongMatch = currentVideoSongMatch
                                selectedVideoArtistMatch = currentVideoArtistMatch
                                selectedVideoTitle = searchEntry['snippet']['title']
                                selectedVideoYear = currentVideoYear
                                selectedVideoUrl = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                                selectedVideoPublishedDate = searchEntry['snippet']['publishedAt']
                                selectedVideoDuration = ParseTime(videoEntry['contentDetails']['duration'])
                                selectedVideolikes = currentVideolikes
                                selectedVideodislikes = currentVideodislikes
                                iindex=i
                            if ((selectedVideoTotalMatch) < int(currentVideoTotalMatch) and (mostpopular == 1)):
                                selectedVideoViewCount = currentVideoViewCount
                                selectedVideoMatch = currentVideoMatch
                                selectedVideoYear = currentVideoYear
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
                            if (selectedVideoTotalMatch == currentVideoTotalMatch and (mostpopular == 1) and int(selectedVideoViewCount) < int(currentVideoViewCount)):
                                selectedVideoViewCount = currentVideoViewCount
                                selectedVideoMatch = currentVideoMatch
                                selectedVideoTotalMatch = currentVideoTotalMatch
                                selectedVideoSongMatch = currentVideoSongMatch
                                selectedVideoArtistMatch = currentVideoArtistMatch
                                selectedVideoYear = currentVideoYear
                                selectedVideoTitle = searchEntry['snippet']['title']
                                selectedVideoUrl = "https://www.youtube.com/watch?v="+str(youtubeVideoId)
                                selectedVideoPublishedDate = searchEntry['snippet']['publishedAt']
                                selectedVideoDuration = ParseTime(videoEntry['contentDetails']['duration'])
                                selectedVideolikes = currentVideolikes
                                selectedVideodislikes = currentVideodislikes
                                iindex=i
                    i = i + 1
                #get the videos
                if(iindex != -1):
                        bret = True
                        if(int(selectedVideolikes) !=0 and int(selectedVideodislikes)!=0):
                            video.rating = (float(selectedVideolikes)*5)/(float(selectedVideolikes)+float(selectedVideodislikes))
                        
                        video.url = selectedVideoUrl
                        video.match = selectedVideoMatch
                        video.tm = selectedVideoTotalMatch
                        video.sm = selectedVideoSongMatch
                        video.am = selectedVideoArtistMatch
                        video.title = selectedVideoTitle
                        if(selectedVideoYear != 0):
                            video.videoYear = selectedVideoYear
                            curr_year = int(str(video.year).split('-')[0])
                            if(curr_year == 1001 or (curr_year > int(video.videoYear))):
                                video.year = video.videoYear
                        video.published = selectedVideoPublishedDate
                        m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),video.published)
                        n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),video.published)
                        ydate = m.group()+" "+n.group()
                        dd = ydate
                        yy = int(str(dd)[0:4])
                        mm = int(str(dd)[5:7])
                        total = (now.year-yy)*12+(now.month-mm)
                        if total < 1:
                            total = 1
                        video.length = selectedVideoDuration
                        if(now.month<10):
                            mm = '0'+str(now.month)
                        else:
                            mm = str(now.month)
                        if(now.day<10):
                            dd = '0'+str(now.day)
                        else:
                            dd = str(now.day)
                        video.crawldate = str(now.year)+"-"+mm+"-"+dd
                        video.viewcount = selectedVideoViewCount
                        if(total != 0):
                            video.viewcountRate = float(video.viewcount)/total
                else:
                        misses = 1
        except Exception as e:
            logger_error.exception('getYoutubeUrl')
            logger_error.exception(e)
    except Exception as e:
        logger_error.exception('getYoutubeUrl')
        logger_error.exception(e)
    return video,bret

request_count = 0
t2=datetime.now()
directory = raw_input("Enter directory: ")
m = raw_input("Enter m: ")
m=int(m)
foldlist = list()
jobs=[]
t1=datetime.now()
foldercompletelist = {}
folderstartedlist = {}
for dirs in os.listdir(directory):
  	found = re.search(r'[0-9]+',str(dirs),0)
  	print dirs
  	if found:
		for curr_dir, sub_dir, filenames in os.walk(directory+'/'+dirs):
			for sd in sub_dir:
				f = re.search(r'[0-9]+',str(sd),0)
				if not f:
					continue
				strg = os.path.join(curr_dir,sd)
				foldlist.append(strg)
for fl in foldlist:
    try:
        vid = list()
        misses = 0
        hits = 0
        infile = fl + '/songslist.txt'
        fread = open(infile,'r')
        parallel_songs_list = pickle.load(fread)
        songs_pool = Pool()
        songs_pool =Pool(processes=20)
        return_pool = songs_pool.map(getVideoFromYoutube,parallel_songs_list)
        print len(return_pool)
        for ret_val in return_pool:
            if(ret_val[0] == None ):
                misses = misses+1
            else:
                for rv in ret_val[0]:
                    if('url' not in rv.__dict__):
                        misses = misses + 1
                    else:
                        hits = hits + 1
                        #print rv.__dict__
                        #tv = collections.OrderedDict(rv.__dict__)
                        vid.append(rv.__dict__)
        print "Hits:"+str(hits)+" Misses:"+str(misses)
        write(vid,fl+"/dump")
    except Exception as e:
            logger_error.exception(e)
t2=datetime.now()

print "time=" +str(t2-t1)