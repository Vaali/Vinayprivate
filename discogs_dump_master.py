# encoding=utf8
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
from multiprocessing import Pool
import time
import copy
import operator
#from fuzzywuzzy import fuzz
#import fuzzy
#from apiclient.errors import HttpError

reload(sys)
sys.setdefaultencoding('utf8')

formatter1 = logging.Formatter('%(message)s')
logger_decisions = logging.getLogger('simple_logger1')
hdlr_1 = logging.FileHandler('decisions.log')
hdlr_1.setFormatter(formatter1)
logger_decisions.addHandler(hdlr_1)
logger_decisions = logging.getLogger('simple_logger1')

formatter2 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)s - %(thread)s:%(message)s')
logger_error = logging.getLogger('simple_logger2')
hdlr_2 = logging.FileHandler('errors_discogs_dump11.log')
hdlr_2.setFormatter(formatter2)
logger_error.addHandler(hdlr_2)
logger_error = logging.getLogger('simple_logger2')

solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_artists')
#change here to add stem words
stemwords = ["(Edited Short Version)","(Alternate Early Version)","(Alternate Version)","Mono","(Radio Edit)","(Original Album Version)","(Different Mix)","(Music Film)","Stereo","(Single Version)"]
class Video():
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
		if(artist[1] > 100.0):
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

def remove_stemwords(songName):
    global stemwords
    for stem in stemwords:
        if(stem in songName):
            songName = songName.replace(stem,"")
        if(stem.lower() in songName):
            songName = songName.replace(stem.lower(),"")
    return songName
    

def check(date1,date2):
    date1 = str(date1)
    date2 = str(date2)
    if(date1 == date2):
        return 3
    list1 = date1.split('-')
    list2 = date2.split('-')
    if(len(list1) != 3):
        list1.append('13')
        list1.append('32')
    if(len(list2) != 3):
        list2.append('13')
        list2.append('32')
    if(list1[0] == '1001'):
        return 2
    if(list2[0] == '1001'):
        return 1
    if(list1[0] > list2[0]):
        return 2
    if(list1[0] < list2[0]):
        return 1
    if(list1[1] > list2[1]):
        return 2
    if(list1[1]< list2[1]):
        return 1
    if(list1[2] > list2[2]):
        return 2
    if(list1[2]< list2[2]):
        return 1

def get_song_list(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel):
    releases_list = glob.glob(directory+"/release*.json")
    for release in releases_list:
        try:
            filename = release
            with codecs.open(filename,"r","utf-8") as input:
                curr_album = json.load(input)
            earlier_year_skip = False #skip for the albums which have no artist attched to them
            
            for track in curr_album['tracks']:
                if(track == None):
                    continue
                if(track['title'] == ""):
                    continue
                bskip = False
                song = {}
                song['styles'] = curr_album['styles']
                song['genres'] = curr_album['genres']
                song['year'] = curr_album['released_date']
                song['country'] = curr_album['country']
                song['featArtists'] = []
                song['connectors'] = []
                song['extraArtists'] = []
                song['extraArtistsconnectors'] = []
                for artist in curr_album['releaseartists']:
                    if(artist == None):
                        continue
                    artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip()
                    if(', the' in artist['artist_name'].lower()):
                                artist['artist_name'] = artist['artist_name'].lower().replace(', the','')
                                artist['artist_name'] = 'the '+ artist['artist_name']
                    if(artist['position'] == 1):
                        song['artistName'] = re.sub(r'\(.*?\)', '', artist['artist_name'].lower()).strip()
                        song['artist_id'] = artist['artist_id']
                        #add anvs for the main artist alone
                        if('anv' in artist and artist['anv'] != None):
                            song['anv'] = artist['anv']
                        if(artist['join_relation'] != None):
                            song['connectors'].append(artist['join_relation'])
                    elif(artist['artist_name'].lower() not in song['featArtists'] and ('artistName' not in song or (artist['artist_name'].lower() != song['artistName'].lower()))):
                        song['featArtists'].append(artist['artist_name'].lower())
                        if(artist['join_relation'] != None):
                            song['connectors'].append(artist['join_relation'])
                if('artists' in track):
                    for artist in track['artists']:
                        if(artist == None):
                            continue
                        if(artist['artist_id'] == 355):
                                earlier_year_skip = True
                                bskip = True
                        artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip()
                        if(', the' in artist['artist_name'].lower()):
                                artist['artist_name'] = artist['artist_name'].lower().replace(', the','')
                                artist['artist_name'] = 'the '+ artist['artist_name']
                        if('artist_name' in artist and artist['artist_name'].lower() != song['artistName'].lower()):
                            if(artist['artist_name'].lower() not in song['featArtists']):
                                    song['featArtists'].append(artist['artist_name'].lower())
                            if(artist['join_relation'] not in song['connectors']):
                                    song['connectors'].append(artist['join_relation'])
                        if('artist_name' in artist and artist['artist_name'].lower() == song['artistName'].lower()):
                                if(artist['join_relation'] not in song['featArtists']):
                                    song['connectors'].append(artist['join_relation'])
                if(bskip == True):
                    continue
                if('extraartists' in track and track['extraartists'] != None):
                        extartists = track['extraartists']
                        for extart in extartists:
                            if(extart == None):
                                continue
                            extart['artist_name'] = re.sub(r'\(.*?\)', '', extart['artist_name']).strip()
                            if(', the' in extart['artist_name'].lower()):
                                extart['artist_name'] = extart['artist_name'].lower().replace(', the','')
                                extart['artist_name'] = 'the '+ extart['artist_name']
                            if('role' in extart and extart['role'] != None):
                                if(extart['role'].lower() == 'featuring' and ('artistName' not in song or (extart['artist_name'].lower() != song['artistName'].lower()))):
                                    if(extart['artist_name'] not in song['featArtists']):
                                        song['featArtists'].append(extart['artist_name'].lower())
                                    if(extart['role'] not in song['connectors']):
                                        song['connectors'].append(extart['role'])
                                else:
                                    song['extraArtists'].append(extart['artist_name'].lower())
                                    song['extraArtistsconnectors'].append(extart['role'])
                
                song['name'] = track['title'].replace('"','').strip()
                song['name'] = song['name'].replace('’','').strip()
                song['name'] = song['name'].replace("'",'').strip()
                song['name'] = song['name'].replace("‘",'').strip()
                #song['name'] = song['name'].replace("?",'').strip
                song['name'] = remove_stemwords(song['name'])
                if('duration' in curr_album):
                    song['duration'] = track['duration']
                albumInfo = {}
                albumInfo['albumName'] = curr_album['title']
                albumInfo['year'] = curr_album['released_date']
                albumInfo['country'] = curr_album['country']
                '''if(curr_album['country'] not in full_country_list):
                    full_country_list[curr_album['country']] = 1
                else:
                    full_country_list[curr_album['country']] = full_country_list[curr_album['country']] + 1'''
                albumInfo['language'] = "English"
                song['albumInfo'] = [albumInfo]
                songs_list.append(song)
            if(earlier_year_skip == False):
                option = check(curr_album['released_date'],ear_year)
                if(option == 3 and ear_rel== False):
                    ear_count = curr_album['country']
                    ear_year = curr_album['released_date']
                    ear_rel = False
                if(option == 1):
                        ear_count = curr_album['country']
                        ear_year = curr_album['released_date']
        except Exception, e:
            logging.exception(e)
    return songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel


'''  
Get the songslist from the master files.
'''
def get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel):
    releases_list = glob.glob(directory+"/master*.json")
    ear_conflict = False
    for release in releases_list:
        try:
            filename = release
            with codecs.open(filename,"r","utf-8") as input:
                curr_master = json.load(input)
            release_album = str(curr_master['main_release'])
            earlier_year_skip = False #skip for the albums which have no artist attched to them
            for curr_album in curr_master['releaselist']:
                curr_rel = False
                if(curr_album == None):
                    continue    
                if(curr_album['release_id'] == release_album):
                    curr_rel = True
                    if(curr_album['country'] not in full_country_list):
                            full_country_list[curr_album['country']] = 1
                    else:
                            full_country_list[curr_album['country']] = full_country_list[curr_album['country']] + 1
                    #song['release_album'] = True
                
                for track in curr_album['tracks']:
                    if(track == None):
                        continue
                    if(track['title'] == ""):
                        continue
                    
                    song = {}
                    song['styles'] = curr_album['styles']
                    song['genres'] = curr_album['genres']
                    song['year'] = curr_album['released_date']
                    song['country'] = curr_album['country']
                    song['featArtists'] = []
                    song['connectors'] = []
                    song['extraArtists'] = []
                    song['extraArtistsconnectors'] = []
                    
                    
                    #if unknown artist present in the list of artist,skip the album
                    bskip = False
                    
                    if(curr_album['release_id'] == release_album):
                        song['release_album'] = True
                    for artist in curr_album['releaseartists']:
                        if(artist == None):
                            continue
                        artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip()
                        if(', the' in artist['artist_name'].lower()):
                                artist['artist_name'] = artist['artist_name'].lower().replace(', the','')
                                artist['artist_name'] = 'the '+ artist['artist_name']
                        if(artist['position'] == 1):
                            song['artistName'] = re.sub(r'\(.*?\)', '', artist['artist_name'].lower()).strip()
                            song['artist_id'] = artist['artist_id']
                            #add anvs for the main artist alone
                            if('anv' in artist and artist['anv'] != None):
                                song['anv'] = artist['anv']
                            if(artist['join_relation'] != None):
                                song['connectors'].append(artist['join_relation'])
                        elif(artist['artist_name'].lower() not in song['featArtists'] and ('artistName' not in song or (artist['artist_name'].lower() != song['artistName'].lower()))):
                            song['featArtists'].append(artist['artist_name'].lower())
                            if(artist['join_relation'] != None):
                                song['connectors'].append(artist['join_relation'])
                    if('artists' in track):
                        for artist in track['artists']:
                            if(artist == None):
                                continue
                            if(artist['artist_id'] == 355):
                                bskip = True
                            artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip()
                            if(', the' in artist['artist_name'].lower()):
                                        artist['artist_name'] = artist['artist_name'].lower().replace(', the','')
                                        artist['artist_name'] = 'the '+ artist['artist_name']
                            if('artist_name' in artist and artist['artist_name'].lower() != song['artistName'].lower()):
                                if(artist['artist_name'].lower() not in song['featArtists']):
                                    song['featArtists'].append(artist['artist_name'].lower())
                                if(artist['join_relation'] not in song['connectors']):
                                    song['connectors'].append(artist['join_relation'])
                            if('artist_name' in artist and artist['artist_name'].lower() == song['artistName'].lower()):
                                if(artist['join_relation'] not in song['featArtists']):
                                    song['connectors'].append(artist['join_relation'])
                    
                    if(bskip == True):
                        earlier_year_skip = True
                        continue
                    if('extraartists' in track and track['extraartists'] != None):
                        extartists = track['extraartists']
                        for extart in extartists:
                            if(extart == None):
                                continue
                            extart['artist_name'] = re.sub(r'\(.*?\)', '', extart['artist_name']).strip()
                            if(', the' in extart['artist_name'].lower()):
                                        extart['artist_name'] = extart['artist_name'].lower().replace(', the','')
                                        extart['artist_name'] = 'the '+ extart['artist_name']
                            if('role' in extart and extart['role'] != None):
                                if(extart['role'].lower() == 'featuring'):
                                    if(extart['artist_name'].lower() not in song['featArtists'] and ('artistName' not in song or (extart['artist_name'].lower() != song['artistName'].lower()))):
                                        song['featArtists'].append(extart['artist_name'].lower())
                                    if(extart['role'] not in song['connectors']):
                                        song['connectors'].append(extart['role'])
                                else:
                                    song['extraArtists'].append(extart['artist_name'].lower())
                                    song['extraArtistsconnectors'].append(extart['role'])
                                    
                        
                    song['name'] = track['title'].replace('"',"").strip()
                    song['name'] = song['name'].replace('’','').strip()
                    song['name'] = song['name'].replace("'",'').strip()
                    song['name'] = song['name'].replace("‘",'').strip()
                    song['name'] = remove_stemwords(song['name'])
                    #song['name'] = song['name'].replace("?",'').strip()
                    '''if('lazy' in song['name'].lower()):
                        print '------------------------------------'
                        print song['genres']
                        print song['year']
                        print song['styles']
                        print curr_album['release_id']
                        print curr_master['id']'''
                    if('duration' in curr_album):
                        song['duration'] = track['duration']
                    albumInfo = {}
                    albumInfo['albumName'] = curr_album['title']
                    albumInfo['year'] = curr_album['released_date']
                    albumInfo['country'] = curr_album['country']
                    '''if(curr_rel == True):
                        if(curr_album['country'] not in full_country_list):
                            full_country_list[curr_album['country']] = 1
                        else:
                            full_country_list[curr_album['country']] = full_country_list[curr_album['country']] + 1'''
                    albumInfo['language'] = "English"
                    song['albumInfo'] = [albumInfo]
                    songs_list.append(song)
                if(earlier_year_skip == False):
                    option = check(curr_album['released_date'],ear_year)
                    if(option == 3 and ear_rel== False and curr_rel == True):
                        ear_count = curr_album['country']
                        ear_year = curr_album['released_date']
                        ear_rel = curr_rel
                    elif(option ==3 and curr_rel == True):
                        ear_conflict = True

                    if(option == 1):
                        ear_count = curr_album['country']
                        ear_year = curr_album['released_date']
                        ear_rel = curr_rel
        except Exception, e:
            logging.exception(e)
    return songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict

def CalculateMatch_work(curr_elem,vid_title):
    try:
        #soundex = fuzzy.Soundex(len(vid_title))
        list = ""
        conlist = ""
        artistName = curr_elem['artistName']
        ftArtistName = curr_elem['featArtists']
        connectorList = curr_elem['connectors']
        songName = curr_elem['name']
        fList = ""
        albumname = ""
        stemwords = [ 'screen','m/v','artist','ft','featuring','live','hd','1080P','video','mix','feat','official','lyrics','music','cover','version','original','\
hq','band','audio','album','world','instrumental','intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod','piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single','official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','song','digital','hour','nature','motion','sounds','sound','ballad','unplugged','singers','singer','legend','legends', 'french','strings','string','classic','cast','act','full','screen','radio','remix','song','edit','tracks','remaster','reissue','review','re-issue','trailer','studio','improvization','solo','download','tour','dvd','festival']
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
                youtubematch = re.sub(r'\b'+word+'\b', '', youtubematch)
        for c in connectorList:
            if(c != None):
                conlist = conlist+" "+c	
        #Find positions of artist ,song and feat artists
        songpos = youtubematch.lower().find(songName.lower())
        artpos = youtubematch.lower().find(artistName.lower())
        for l in curr_elem['albumInfo']:
            albumpos = youtubematch.lower().find(l['albumName'].lower())
            if(albumpos != -1):
                substring_album = "true"
                artist_order[albumpos] = l['albumName']
                albumname = l['albumName']
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
            lam = leftMatch
            ram = rightMatch
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
            lam = leftMatch
            ram = rightMatch 
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
                common_set = (set(yresultset[-len(songreadset):]).intersection(set(songreadset)))
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
                common_set = (set(yresultset[:len(songreadset)]).intersection(set(songreadset)))
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
        elif(tm>70 and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1) and sm>30 and (am>0 or lam>0 or ram>0)):
            decision = "correct"
            condition = 8 
        elif(substring_artist == "true" and substring_song == "true" and (len(ftartists) == 0 or (len(ftartists)!=0 and ftMatch == 100.0)) and ( len(artistName.strip().split()) > 1) and percentMatch > 60.0):
            decision = "correct"
            condition = 1
        #if song is false then look for song match and length must be greater than 1
        elif(substring_song == "false" and songMatch  >= 80.0 and ( len(artistName.strip().split()) > 1) and (am>0 or lam>0 or ram>0)):
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
            logging.exception(e)
    return decision,match,tm,sm,am

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
hq','band','audio','album','world','instrumental','intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod','piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single','official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','song','digital','hour','nature','motion','sounds','sound','ballad','unplugged','singers','singer','legend','legends', 'french','strings','string','classic','cast','act','full','screen','radio','remix','song','edit','tracks','remaster','reissue','review','re-issue','trailer','studio','improvization','solo','download','tour','dvd','festival']
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
                youtubematch = re.sub(r'\b'+word+'\b', '', youtubematch)
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
            lam = leftMatch
            ram = rightMatch
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
            lam = leftMatch
            ram = rightMatch 
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
            logging.exception(e)
    return decision,match,tm,sm,am


def CalculateMatch_old(curr_elem,vid_title):
    try:
        list = ""
        conlist = ""
        artistName = curr_elem['artistName']
        ftArtistName = curr_elem['featArtists']
        connectorList = curr_elem['connectors']
        songName = curr_elem['name']
        lam = 0
        ram = 0
        tm = 0
        sm = 0
        am = 0
        leftMatch = 0
        rightMatch = 0
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
        songName = songName.replace('(','')
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
        '''yname = yname.lower().replace("full version","")
        yname = yname.lower().replace("lyrics on screen","")
        yname = yname.lower().replace("official music video","")
        yname = yname.lower().replace("with lyrics","")
        yname = yname.lower().replace("full album","")
        yname = yname.lower().replace("official song","")
        yname = yname.lower().replace("radio edit","")
        yname = yname.lower().replace("m/v","")
        yname = yname.lower().replace("track","")
        yname = yname.lower().replace("album","")
        yname = yname.lower().replace("artist","")'''
        stemwords = ['video','mix','feat','official','lyrics','music','cover','version','original','hq','band','audio','album','world','instrumental', 'intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod', 'piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single', 'official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','digital','hour','nature','motion','sounds','ballad','unplugged','singers','legend', 'french','strings','classic','cast','act','full','screen','radio','remix','song','edit','tracks']
        stemcharacters = ['[',']','(',')','\'','"','.','-','’']
        youtubematch = yname.lower()
        diffset = []
        songnameset = re.findall("\w+",songName.lower(),re.U)
        for c in stemcharacters:
            youtubematch = youtubematch.replace(c,'')
        for word in stemwords:
            if(word not in songnameset):
                diffset.append(word)
                youtubematch = re.sub(r'\b'+word+'\b', '', youtubematch)
                
        ftArtistSet = re.findall("\w+",ftartists.lower(),re.U)
        ftAMatch = 0
        ftMatch = 0
        songMatch = 0
        for artist in ftArtistSet:
            if(yname.find(artist)!= -1):
                ftAMatch = ftAMatch + 1
        if(len(ftArtistSet)!=0):
            ftMatch = ftAMatch*100/len(ftArtistSet)
        #remove = "lyrics official video hd hq edit music lyric audio acoustic videoclip featuring ft feat radio remix and"
        #diffset = re.findall("\w+",remove.lower(),re.U)
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
            lam = leftMatch
            ram = rightMatch
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
            lam = leftMatch
            ram = rightMatch 
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
                common_set = (set(yresultset[-len(songreadset):]).intersection(set(songreadset)))
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
                common_set = (set(yresultset[:len(songreadset)]).intersection(set(songreadset)))
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
        if((sm+lam+ram>120) and bhiphen):
            decision = "correct"
            condition = 6    
        elif(sm+am+ram>140  and not bhiphen and (len(artistName.strip().split()) > 1)):
            decision = "correct"
            condition = 7 
        elif(tm>80 and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1)):
            decision = "correct"
            condition = 8 
        elif(substring_artist == "true" and substring_song == "true" and (len(ftartists) == 0 or (len(ftartists)!=0 and ftMatch == 100.0)) and ( len(artistName.strip().split()) > 1) and percentMatch > 60.0):
            decision = "correct"
            condition = 1
        #if song is false then look for song match and length must be greater than 1
        elif(substring_song == "false" and songMatch  >= 80.0 and ( len(artistName.strip().split()) > 1)):
            decision = "correct"
            condition = 2
        #if artist  is false look for artistmatch left or [right and total match]
        elif(substring_artist == "false" and (leftMatch == 100.0  or  (rightMatch == 100.0 and percentMatch  > 60.0)) and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1)):
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
    except Exception, e:
            print 'getVideo'
            logging.exception(e)
    logger_decisions.error(decision)
    logger_decisions.error(condition)
    logger_decisions.error(match)
    logger_decisions.error(artistName)
    logger_decisions.error(songName)
    logger_decisions.error(ftArtistName)
    logger_decisions.error(vid_title)
    logger_decisions.error(am+sm)
    logger_decisions.error(fuzz.ratio(youtubematch.lower(),artistName.lower()+" "+songName.lower()))
    logger_decisions.error(fuzz.ratio(artistName.lower()+" "+songName.lower(),youtubematch.lower()))
    logger_decisions.error('-----------------')
    
    return decision,match,tm,sm,am


def getVideo(curr_elem,flag):
    try:
        global request_count
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
            if(l['albumName'].lower() not in unique_albums):
                unique_albums.append(l['albumName'].lower())
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
            #video1.lang_count = curr_elem['lang_count']
            video1.artist = curr_elem['artistName']
            video1.ftArtist = curr_elem['featArtists']
            video1.name = curr_elem['name']
            video1.connectors = curr_elem['connectors']
            video1.album = alist
            video1.year = curr_elem['year']
            video1.language = 'English'
            video1.songcountry = curr_elem['songcountry']
            if('artistalias' in curr_elem):
                video1.artistalias = curr_elem['artistalias']
            video1.genres = curr_elem['genres']
            video1.styles = curr_elem['styles']
            video1,bret = getYoutubeUrl(video1,flag)
            return video1,bret
            #else:
            #    return None
        except Exception as e:
            logging.exception(e)
            return None,bret
    except Exception as e:
            logging.exception(e)
            return None,bret

    
def getYoutubeUrl(video,flag):
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
            if('cover' not in video.name.lower()):
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"+-cover"+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
            else:
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
        else:
            if('cover' not in video.name.lower()):
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"+-cover"+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
            else:
                searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&videoCategoryId=10"
        print searchUrl
        try:
            searchResult = simplejson.load(urllib2.urlopen(searchUrl),"utf-8")
            request_count = request_count + 2
            #print searchResult
        except Exception as e:
            request_count = request_count + 2
            logging.exception("Error")
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
                for videoresult in searchResult['items']:
                    searchEntry = searchResult['items'][i]
                    [currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch] = CalculateMatch(video,searchEntry['snippet']['title'])
                    if(currentVideoDecision == "correct"):# || currentVideoDecision == "Incorrect"):
                        youtubeVideoId = searchEntry['id']['videoId']
                        videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&part=statistics,contentDetails,status"
                        try:
                            videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
                            request_count = request_count + 7
                        except Exception as e:
                            request_count = request_count + 7
                            logging.exception(e)
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
                                if(i< earliestindex):
                                    earliestindex = i
                    i = i + 1
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
            logging.exception('getYoutubeUrl')
            logging.exception(e)
    except Exception as e:
        logging.exception('getYoutubeUrl')
        logging.exception(e)
    return video,bret

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
                retvid.artist = artname
        if((retvid == None) or ('url' not in retvid.__dict__)):
            curr_elem['artistName'] = artname
            retvid,bret = getVideo(curr_elem,1)
        
    except Exception as e:
        logging.exception('getVideoFromYoutube')
    return retvid,bret

def getVideo_1(curr_elem,flag):
    try:
        global request_count
        alist = list()
        ylist = list()
        video2 = Video()
        #album_details = Album_Data()
        video2.artist = curr_elem['artistName']
        video2.ftArtist = curr_elem['featArtists']
        video2.name = curr_elem['name']
        video2.connectors = curr_elem['connectors']
        songs_list = curr_elem['albumInfo']
        unique_albums = []
        #combine all the album information for the songs into a list.
        for l in songs_list:
            album_details = Album_Data()
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
            if(l['albumName'].lower() not in unique_albums):
                unique_albums.append(l['albumName'].lower())
                alist.append(album_details.__dict__)
        if(len(alist)==0):
            print curr_elem
        video2.album = alist
        video2.year = curr_elem['year']
        video2.language = 'English'
        video2.songcountry = curr_elem['songcountry']
        flist = ""
        #Apostolos
        for f in video2.ftArtist:
            ttt=f.strip("-")
            flist = flist+" "+ttt
        ftartists = flist
        allArtists = video2.artist.strip("-")+" "+ftartists
        if('cover' not in video2.name.lower()):
            searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video2.name))+"+-cover"+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU"
            print searchUrl
        else:
            searchUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video2.name))+"&alt=json&type=video&max-results=5&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU"
        #if('Uptown Funk' in curr_elem['name']):
            print searchUrl
        try:
            searchResult = simplejson.load(urllib2.urlopen(searchUrl),"utf-8")
            request_count = request_count + 2
            #print searchResult
        except Exception as e:
            request_count = request_count + 2
            logging.exception("Error")
            return None,True
            #logging.warning('No results from google',e)
        now = datetime.now()
        try:
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
                    '''if(curr_elem['name'] == "Uptown Funk"):
                        print searchUrl
                        print curr_elem
                        print searchEntry['snippet']['title']
                        print searchEntry['id']['videoId']
                        print currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch'''
                    if(currentVideoDecision == "correct"):# || currentVideoDecision == "Incorrect"):
                        youtubeVideoId = searchEntry['id']['videoId']
                        videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&part=statistics,contentDetails,status"
                        try:
                            videoResult = simplejson.load(urllib2.urlopen(videoUrl),"utf-8")
                            request_count = request_count + 7
                        except Exception as e:
                            request_count = request_count + 7
                            logging.exception(e)
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
                    return None,True
                video1 = Video()
                video1.artist = curr_elem['artistName']
                video1.ftArtist = curr_elem['featArtists']
                video1.name = curr_elem['name']
                video1.connectors = curr_elem['connectors']
                video1.album = alist
                video1.year = curr_elem['year']
                video1.language = 'English'
                video1.songcountry = curr_elem['songcountry']
                if('artistalias' in curr_elem):
                    video1.artistalias = curr_elem['artistalias']
                video1.genres = curr_elem['genres']
                video1.styles = curr_elem['styles']
                if(int(selectedVideolikes) !=0 and int(selectedVideodislikes)!=0):
                    video1.rating = (float(selectedVideolikes)*5)/(float(selectedVideolikes)+float(selectedVideodislikes))
                    #print video1.rating
                #video1.lang_count = curr_elem['lang_count']
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
                return video1,True
            else:
                return None,True
        except Exception as e:
            logging.exception(e)
    except Exception as e:
            logging.exception(e)

def crawlArtist(directory):
    songs_list = list()
    global misses
    global hits
    full_lang_list = {}
    full_country_list ={}
    aliases = []
    ear_count = ""
    ear_year = 1001
    ear_rel = False
    songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict = get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel)
    print ear_count
    print ear_year
    songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel = get_song_list(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel)
    print ear_count
    print ear_year
    sorted_list_country = sorted(full_country_list.items(), key=operator.itemgetter(1),reverse = True)
    print sorted_list_country
    #sorted(full_country_list,key = lambda x:x['name'].lower())
    artist_country = ear_count
    if(len(sorted_list_country) > 0):
        artist_country = sorted_list_country[0][0]
    print '----------------------'
    print artist_country
    print ear_count
    print 'xxxxxxxxxxxxxxxxxxxxxx'
    if(ear_conflict == True):
        print 'countries mismatch'
        ear_count = artist_country
    sorted_list = sorted(songs_list,key = lambda x:x['name'].lower()) 
    final_song_list = {}
    hits = 0
    misses = 0
    global request_count
    request_count = 0
    if(len(sorted_list) == 0):
        return
    try:
        curr_time = "2020-14-33"
        curr_language = ""
        curr_song = {}
        artist_alias_list = []
        artist_alias_list = getArtistAliasList(sorted_list)
        for song in sorted_list:
            Item_id = song['name'].lower()
            #Item_id = Item_id + ","
            Item_id = Item_id + "," + song['artistName']
            if(len(song['featArtists'])!= 0):
                temp_str = ','.join(song['featArtists'])
                Item_id = Item_id + temp_str.lower()
            Item_id.strip()
            k = 0
            genre = song['genres']
            if(genre != None):
                genre = genre.replace("{","")
                genre = genre.replace("}","")
                genre = genre.replace("\"Folk, World, & Country\"","fwc")
                genre = genre.split(',')
                if("fwc" in genre):
                    genre.remove("fwc")
                    genre.append("Folk, World, & Country")
                        
            else:
                genre = []
            style = song['styles']
            if(style != None):    
                style = style.replace("{","")
                style = style.replace("}","")
                style = style.split(',')
            else:
                style = []
            if(Item_id not in final_song_list):
                song['genres_count'] = {}
                song['styles_count'] = {}
                song['gcount'] = 0
                song['scount'] = 0
                song['genres'] = genre
                song['styles'] = style
                '''for g in genre:
                    if(g not in song['genres_count']):
                        song['genres_count'][g] = 1
                        song['genres'].append(g)
                    else:
                        song['genres_count'][g] = song['genres_count'][g] + 1
                    song['gcount'] = song['gcount'] + 1
                    
                
                for s in style:
                    if(s not in song['styles_count']):
                        song['styles_count'][s] = 1
                        song['styles'].append(s)
                    else:
                        song['styles_count'][s] = song['styles_count'][s] + 1
                    song['scount'] = song['scount'] + 1'''
                
                final_song_list[Item_id] = song
                final_song_list[Item_id]['year'] = song['year']
                if(final_song_list[Item_id]['year'] == None):
                    final_song_list[Item_id]['year'] = 1001
                #final_song_list[Item_id]['language'] = song['language']
                final_song_list[Item_id]['songcountry'] = ear_count
                if(ear_count == None):
                    final_song_list[Item_id]['songcountry'] = artist_country
                elif(artist_country == None):
                    final_song_list[Item_id]['songcountry'] = ear_count
                elif(ear_count.lower() != artist_country.lower()):
                    final_song_list[Item_id]['songcountry'] = final_song_list[Item_id]['songcountry'] + ',' + artist_country 
                
                #lang_count = {}
                #lang_count[final_song_list[Item_id]['language']] = 1
                #final_song_list[Item_id]['lang_count'] = lang_count
            else:
                stemp = final_song_list[Item_id]
                '''for g in genre:
                    if(g not in stemp['genres_count']):
                        stemp['genres_count'][g] = 1
                        stemp['genres'].append(g)
                    else:
                        stemp['genres_count'][g] = stemp['genres_count'][g] + 1
                    stemp['gcount'] = stemp['gcount'] + 1
                for s in style:
                    if(s not in stemp['styles_count']):
                        stemp['styles_count'][s] = 1
                        stemp['styles'].append(s)
                    else:
                        stemp['styles_count'][s] = stemp['styles_count'][s] + 1
                    stemp['scount'] = stemp['scount'] + 1'''
                for cl in song['connectors']:
                    if(cl not in stemp['connectors'] and cl != None):
                        stemp['connectors'].append(cl)
                '''for s in song['albumInfo']:
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
                    stemp['albumInfo'].append(s)'''
                '''if('lazy' in song['name'].lower()):
                    print '-----------------------------'
                    print song['year']
                    print song['genres']
                    print song['styles']
                    print '##########################' '''
                if(song['year'] != None):
                    if(stemp['year'] == None or stemp['year'] == 1001):
                        #if('release_album' not in stemp): #If the previous songis from a release album , dont replace the year and genre
                            stemp['year'] = song['year']
                            stemp['genres']= genre
                            stemp['styles']= style
                            stemp['country'] = song['country']
                            if('release_album' in song):
                                    stemp['release_album'] = song['release_album']
                            if('anv' in song):
                                stemp['anv'] = song['anv']
                    else:
                        k = check(song['year'],stemp['year'])
                        if(k == 1):                                
                                stemp['year'] = song['year']
                                stemp['genres']= genre
                                stemp['styles']= style
                                stemp['country'] = song['country']
                                if('release_album' in song):
                                    stemp['release_album'] = song['release_album']
                                if('anv' in song):
                                    stemp['anv'] = song['anv']
                        if(k == 3):
                            if('release_album' not in stemp):
                                stemp['year'] = song['year']
                                stemp['genres']= genre
                                stemp['styles']= style
                                stemp['country'] = song['country']
                                if('release_album' in song):
                                    stemp['release_album'] = song['release_album']
                                if('anv' in song):
                                    stemp['anv'] = song['anv']
                                
        total_count = 0
        for i in full_lang_list:
            total_count = total_count + full_lang_list[i]
        percent_lang = {}
        '''for i in full_lang_list:
            percent_lang[i] = (full_lang_list[i]*100.0)/total_count
        percent_lang = sorted(percent_lang.iteritems(), key=lambda (k,v): (v,k),reverse = True)'''
        change_language = ''
        for s in final_song_list:
            logger_decisions.error(s)
            logger_decisions.error(final_song_list[s]['genres'])
            logger_decisions.error(final_song_list[s]['styles'])
            '''for g in final_song_list[s]['genres']:
                logger_decisions.error(g)
                #percent = final_song_list[s]['genres_count'][g]*100/final_song_list[s]['gcount']
                #logger_decisions.error(percent)
            for sty in final_song_list[s]['styles']:
                logger_decisions.error(sty)
                #percent = final_song_list[s]['styles_count'][sty]*100/final_song_list[s]['scount']
                #logger_decisions.error(percent)
            '''
            logger_decisions.error('----------------------------------------')     
            
        '''if(len(percent_lang) != 0 and percent_lang[0][1] > 97.0):
            change_language = percent_lang[0][0]
        else:
            change_language = ''
        full_lang_list = sorted(full_lang_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)

        full_country_list_sort = sorted(full_country_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)'''
        '''if(len(full_country_list) != 0):
            if(artist_country not in full_country_list):
                artist_country = full_country_list_sort[0][0]'''
        vid = list()
        with open(directory + '/uniquelist.txt', 'wb') as f:
			pickle.dump(final_song_list.keys(), f)
        parallel_songs_list = []
        finalsongs = final_song_list.values()
        '''for s in final_song_list:
            if('uptown funk' in s.lower()):
                print final_song_list[s]'''
        for s in finalsongs:
            #lang_dict = s['lang_count']
            curr_elem = dict(s)
            #s['songcountry'] = ''
            '''temp_lang_list = sorted(lang_dict.iteritems(), key=lambda (k,v): (v,k),reverse = True)
            if( len(temp_lang_list) >1 and temp_lang_list[0][1] == temp_lang_list[1][1]):
                s['language'] = full_lang_list[0][0]
            elif(len(temp_lang_list)!=0):
                s['language'] = temp_lang_list[0][0]
            if(change_language != '' and s['language']!= change_language):
                fchange_language.write(s['name']+ '\t'+s['language'] + '\t' + change_language+'\n')
                s['language'] = change_language'''
            if(not s.has_key('artistName')):# or s['artistName'] not in aliases):
                continue
            if(s['artistName'] in artist_alias_list):
                for art_alias in  artist_alias_list:
                    curr_elem = dict(s)
                    curr_elem['artistName'] = art_alias
                    #curr_elem['songcountry'] = artist_country
                    parallel_songs_list.append(curr_elem)
            else:
                #curr_elem['songcountry'] = artist_country
                parallel_songs_list.append(curr_elem)
            
        t1=time.time()
        print len(parallel_songs_list)
        songs_pool = Pool()
        songs_pool =Pool(processes=10)
        return_pool = songs_pool.map(getVideoFromYoutube,parallel_songs_list)
        print len(return_pool)
        for ret_val in return_pool:
            if(ret_val[0] == None ):
                misses = misses+1
            else:
                if('url' not in ret_val[0].__dict__):
                    misses = misses + 1
                else:
                    vid.append(ret_val[0].__dict__)
                    hits = hits + 1
        print "Hits:"+str(hits)+" Misses:"+str(misses)
        print time.time() - t1
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
		#logger.info("completed for artist :"+filename)
	except Exception as e:
		logging.exception(e)
