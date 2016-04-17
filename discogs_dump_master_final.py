# encoding=utf8
import sys
import json
import glob
import logging
import logging.handlers
import codecs
import pickle
import re
from solr import SolrConnection
from solr.core import SolrException
import time
import operator
from fuzzywuzzy import fuzz
import fuzzy
import os
import concurrent.futures
import itertools
from multiprocessing import Pool


reload(sys)
sys.setdefaultencoding('utf8')

formatter1 = logging.Formatter('%(message)s')
logger_decisions = logging.getLogger('simple_logger1')
hdlr_1 = logging.handlers.RotatingFileHandler(
              'decisions.log', maxBytes=1024*1024*1024, backupCount=10)
#hdlr_1 = logging.FileHandler('decisions.log')
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
stemwords_uniquelist = ["(Edited Short Version)","(Alternate Early Version)","(Alternate Version)","(Mono)","(Radio Edit)","(Original Album Version)","(Different Mix)","(Music Film)","(Stereo)","(Single Version)","Stereo","Mono","(Album Version)","Demo","(Demo Version)"]
class Video():
	pass

class Album_Data():
	pass

class Audio(object):
	pass
def removeStemCharacters(currString):
    currString = currString.replace('"','').strip()
    currString = currString.replace('’','').strip()
    currString = currString.replace("'",'').strip()
    currString = currString.replace("‘",'').strip()
    currString = currString.replace("?",'').strip()
    currString = currString.replace(',','').strip()
    currString = currString.replace('#','').strip()
    return currString
    
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

def GetYearFromTitle(vid_title):
    returnYear = 0
    yearList = re.findall(r'\d\d\d\d+',vid_title)
    if(len(yearList) != 0):
        returnYear = int(yearList[0])
        if(returnYear < 1940):
            returnYear = 0
    return returnYear

def getAliasFromArtistsSolr(final_artist_alias_list,artist_id):
    global solrConnection
    artistId = 'artistId:"'+str(artist_id)+ '"'
    intersect = 0
    try:
        try:
            response = solrConnection.query(q="*:*",fq=[artistId],version=2.2,wt = 'json')
            intersect = int(response.results.numFound)
        except SolrException as e:
            logger_error.exception(e)
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
            logger_error.exception(e)
    return final_artist_alias_list

def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)

def remove_stemwords(songName):
    global stemwords_uniquelist
    for stem in stemwords_uniquelist:
        if(stem in songName):
            songName = songName.replace(stem,"")
        if(stem.lower() in songName):
            songName = songName.replace(stem.lower(),"")
    return songName.strip()

def get_released_date(releaseDate):
    ''' 20041109 '''
    if('-' in releaseDate):
        return releaseDate
    if(len(releaseDate) == 4):
        return releaseDate
    retstring = ""
    retstring = releaseDate[0:4]
    retstring = retstring + '-'
    retstring = retstring + releaseDate[4:6]
    retstring = retstring + '-'
    retstring = retstring + releaseDate[6:8]
    return retstring

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


def GetArtist(artistObj):
    if(artistObj == None):
        return None,None,None
    artistName = ""
    ftArtistList = []
    connList = []
    for artist in artistObj:
        if(artist == None or artist['artist_name'] == None):
            continue
        artName = re.sub(r'\(.*?\)', '', artist['artist_name']).strip().lower()
        if(', the' in artName):
            artName = artName.replace(', the','')
            artName = 'the '+ artName
        if(artist['position'] == 1):
            artistName = artName
        else:
            ftArtistList.append(artName)
        if(artist['join_relation'] != None):
            connList.append(artist['join_relation'])
    return artistName,ftArtistList,connList


def GetUniquesongs(songs_list,final_song_list,isMaster,same_album,ear_count,full_songs_list = []):
    #artist_country = None
    for song in songs_list:
        keySong = song['name'].lower()
        #keySong = removeStemCharacters(keySong)
        keySong = keySong + "," + song['artistName']
        if(len(song['featArtists'])!= 0):
            temp_str = ','.join(song['featArtists'])
            keySong = keySong + "," +temp_str.lower()
        keySong.strip()
        AddedSong = False
        isPresentSong = False
        song['genres'],song['styles'] = getGenresAndStyles(song['genres'],song['styles'])
        if(keySong not in final_song_list):
            ''' First check in the full songlist. '''
            isPresentSong,matchedsong = checkIfSongExists(song,full_songs_list)
            if(isPresentSong == True):
                continue
            isPresentSong,matchedsong = checkIfSongExists(song,final_song_list)
            if(isPresentSong == False):
                song['genres_count'] = {}
                song['styles_count'] = {}
                song['gcount'] = 0
                song['scount'] = 0
                song['yearList'] = []
                if(isMaster == True):
                    song['release_album'] = True
                final_song_list[keySong] = song
                song['yearList'].append(song['year'])
                final_song_list[keySong]['year'] = song['year']
                if(final_song_list[keySong]['year'] == None):
                    final_song_list[keySong]['year'] = 1001
                final_song_list[keySong]['songcountry'] = ear_count
                """if(ear_count == None):
                    final_song_list[keySong]['songcountry'] = artist_country
                elif(artist_country == None):
                    final_song_list[keySong]['songcountry'] = ear_count
                elif(ear_count.lower() != artist_country.lower()):
                    final_song_list[keySong]['songcountry'] = ear_count.lower()"""
                AddedSong = True
        if(isPresentSong == True):
                keySong = matchedsong
        if(AddedSong == False):
                stemp = final_song_list[keySong]
                for cl in song['connectors']:
                    if(cl not in stemp['connectors'] and cl != None):
                        stemp['connectors'].append(cl)
                stemp['yearList'].append(song['year'])
                albums =  song['albumInfo']
                #print albums
                for album in albums:
                    stemp['albumInfo'].append(album)
                if(same_album == False):
                    if(song['year'] != None and song['year'] != 1001):
                        if(stemp['year'] == None or stemp['year'] == 1001):
                                if(song['isCompilation'] == False):
                                    stemp['year'] = song['year']
                                    stemp['genres'],stemp['styles']= song['genres'],song['styles']
                                    stemp['country'] = song['country']
                                if(isPresentSong == True):
                                    stemp['name'] = song['name']
                                if('release_album' in song and song['release_album'] == True):
                                        stemp['release_album'] = song['release_album']
                                if('anv' in song):
                                    stemp['anv'] = song['anv']
                        else:
                            k = 0
                            if(song['isCompilation'] == False):
                                k = check(song['year'],stemp['year'])
                            if(k == 1):
                                if('release_album' in song):
                                        if(isPresentSong == True):
                                            stemp['name'] = song['name']
                                        stemp['year'] = song['year']
                                        stemp['release_album'] = song['release_album']
                                        stemp['genres'],stemp['styles']= song['genres'],song['styles']
                                        stemp['country'] = song['country']
                                        if('anv' in song):
                                            stemp['anv'] = song['anv']
                            if(k == 3):
                                if('release_album' not in stemp):
                                    stemp['year'] = song['year']
                                    stemp['genres'],stemp['styles']= song['genres'],song['styles']
                                    stemp['country'] = song['country']
                                    if('release_album' in song):
                                        stemp['release_album'] = song['release_album']
                                    if('anv' in song):
                                        stemp['anv'] = song['anv']
                    final_song_list[keySong] = stemp
    return final_song_list

def IsReleaseCollection(formats):
    bRet = False
    for format in formats:
        if(format == None):
            continue
        descriptions = format['descriptions']
        #descriptions = descriptions.split(",")
        for desc in descriptions:
            desc = desc.lower()
            if(desc == "collections" or desc == "mixed" or desc == "compilation"):
                bRet = True
    return bRet




def get_song_list(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,final_song_list,full_songs_list):
    releases_list = []
    global prev_time
    global IsIncremental
    retVal = checkpreviousfull(directory)
    if(IsIncremental == 1 and retVal == 1):
        master_list = glob.glob(directory+"/release*.json")
        for fileName in master_list:
            if( os.path.getmtime(fileName) > prev_time):
                '''with codecs.open(fileName,"r","utf-8") as input1:
                    curr_album = json.load(input1)
                    releases_list.append(curr_album)'''
                releases_list.append(fileName)
        #print prev_time
        #print releases_list
    else:
        releases_list = []
        for filename in glob.glob(directory+"/release*.json"):
            '''with codecs.open(filename,"r","utf-8") as input1:
                    curr_album = json.load(input1)
                    releases_list.append(curr_album)'''
            releases_list.append(filename)
    for release in releases_list:
        try:
            Iscompilation = False
            songs_list = []
            #curr_album = release
            filename = release
            with codecs.open(filename,"r","utf-8") as input1:
                curr_album = json.load(input1)
            earlier_year_skip = False #skip for the albums which have no artist attched to them
            if('formats' in curr_album and IsReleaseCollection(curr_album['formats']) == True):
                #print curr_album['formats']
                Iscompilation  = True
            #print curr_album['formats']
            for track in curr_album['tracks']:
                if(track == None):
                    continue
                if(track['position'] == "" and track['duration'] == ""):
                    #print track['title']
                    continue
                if(track['title'] == ""):
                    continue
                bskip = False
                song = {}
                if(curr_album['released_date'] != None):
                        curr_album['released_date'] = get_released_date(curr_album['released_date'])
                temp_year_album = 0
                temp_year_album = GetYearFromTitle(curr_album['title'])
                #print temp_year_album
                if(curr_album['released_date'] != None and temp_year_album != 0):
                    curr_year = int(str(curr_album['released_date']).split('-')[0])
                    if(curr_year == 1001 or (curr_year > int(temp_year_album))):
                        curr_album['released_date'] = str(temp_year_album)
                elif(temp_year_album != 0):
                    curr_album['released_date'] = str(temp_year_album)
                #print curr_album['released_date']
                song['styles'] = curr_album['styles']
                song['genres'] = curr_album['genres']
                song['year'] = curr_album['released_date']
                song['country'] = curr_album['country']
                song['featArtists'] = []
                song['connectors'] = []
                song['extraArtists'] = []
                song['extraArtistsconnectors'] = []
                song['isCompilation'] = Iscompilation
                for artist in curr_album['releaseartists']:
                    if(artist == None):
                        continue
                    artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip().lower()
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
                if('artists' in track and len(track['artists']) > 1):
                    for artist in track['artists']:
                        if(artist == None):
                            continue
                        if(artist['artist_id'] == 355):
                                earlier_year_skip = True
                                bskip = True
                    retlist = GetArtist(track['artists'])
                    if(retlist[0] != None):
                        song['featArtists'] = retlist[1]
                        song['connectors'] = retlist[2]
                        song['artistName'] = retlist[0]
                if(bskip == True):
                    continue
                if('extraartists' in track and track['extraartists'] != None):
                        extartists = track['extraartists']
                        for extart in extartists:
                            if(extart == None):
                                continue
                            extart['artist_name'] = re.sub(r'\(.*?\)', '', extart['artist_name']).strip().lower()
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
                
                song['name'] = track['title']
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
                if(song['isCompilation'] == True):
                    earlier_year_skip = True
            if(earlier_year_skip == False):
                option = check(curr_album['released_date'],ear_year)
                if(option == 3 and ear_rel== False):
                    ear_count = curr_album['country']
                    ear_year = curr_album['released_date']
                    ear_rel = False
                if(option == 1):
                        ear_count = curr_album['country']
                        ear_year = curr_album['released_date']
            #final_song_list = GetUniquesongs(release_song_list,final_song_list,True,False,ear_count)
        except Exception, e:
            logger_error.exception(e)
        final_song_list = GetUniquesongs(songs_list,final_song_list,False,False,ear_count,full_songs_list)
        print len(songs_list)
        print len(final_song_list)
        print "------------"
    return songs_list,final_song_list,full_country_list,aliases,ear_count,ear_year,ear_rel


'''  
Get the songslist from the master files.
'''

def get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list):
    releases_list = []
    global prev_time
    global IsIncremental
    retVal = checkpreviousfull(directory)
    print 'here'
    print retVal
    if(IsIncremental == 1 and retVal == 1):
        master_list = glob.glob(directory+"/master*.json")
        for fileName in master_list:
            if( os.path.getmtime(fileName) > prev_time):
                '''with codecs.open(fileName,"r","utf-8") as input1:
                    curr_master = json.load(input1)
                    releases_list.append(curr_master)'''
                releases_list.append(fileName)
        #print prev_time
        print releases_list
    else:
        releases_list = []
        for filename in glob.glob(directory+"/master*.json"):
            releases_list.append(filename)
            '''with codecs.open(filename,"r","utf-8") as input1:
                    curr_master = json.load(input1)
                    releases_list.append(curr_master)'''
    ear_conflict = False
    release_song_list = []
    combined_songs_list = []
    final_song_list = {}
    releases_list = sorted(releases_list)
    for release in releases_list:
        try:
            Iscompilation  = False
            #curr_master = release
            filename = release
            with codecs.open(filename,"r","utf-8") as input1:
                curr_master = json.load(input1)
            release_song_list = []
            curr_song_list =[]
            release_album = str(curr_master['main_release'])
            earlier_year_skip = False #skip for the albums which have no artist attched to them
            release_album = str(curr_master['main_release'])
            remove_nulls = [k for k in  curr_master['releaselist'] if k!= None]
            sorted_releases_list = sorted(remove_nulls, key=lambda k: int(k['release_id']))
            for curr_album in sorted_releases_list:
                earlier_year_skip = False
                curr_rel = False
                #print curr_album['formats']
                if('formats' in curr_album and IsReleaseCollection(curr_album['formats']) == True):
                    #print curr_album['formats']
                    Iscompilation  = True
                    #continue
                #print curr_album['formats']
                if(curr_album == None):
                    continue    
                if(curr_album['release_id'] == release_album):
                    curr_rel = True
                    if(curr_album['country'] not in full_country_list):
                            full_country_list[curr_album['country']] = 1
                    else:
                            full_country_list[curr_album['country']] = full_country_list[curr_album['country']] + 1
                else:
                    earlier_year_skip = True
                temp_year_album = 0
                temp_year_album = GetYearFromTitle(curr_album['title'])
                if(curr_album['released_date'] != None and temp_year_album != 0):
                    curr_year = int(str(curr_album['released_date']).split('-')[0])
                    if(curr_year == 1001 or (curr_year > int(temp_year_album))):
                        curr_album['released_date'] = str(temp_year_album)
                elif(temp_year_album != 0):
                    curr_album['released_date'] = str(temp_year_album)
                for track in curr_album['tracks']:
                    if(track == None):
                        continue
                    if(track['position'] == "" and track['duration'] == ""):
                        #print track['title']
                        continue
                    if(track['title'] == ""):
                        continue
                    
                    song = {}
                    if(curr_album['released_date'] != None):
                        curr_album['released_date'] = get_released_date(curr_album['released_date'])
                    song['styles'] = curr_album['styles']
                    song['genres'] = curr_album['genres']
                    song['year'] = curr_album['released_date']
                    song['country'] = curr_album['country']
                    song['featArtists'] = []
                    song['connectors'] = []
                    song['extraArtists'] = []
                    song['extraArtistsconnectors'] = []
                    song['isCompilation'] = Iscompilation
                    #if unknown artist present in the list of artist,skip the album
                    bskip = False
                    
                    if(curr_album['release_id'] == release_album):
                        song['release_album'] = True
                    #Get the release artists
                    for artist in curr_album['releaseartists']:
                        if(artist == None):
                            continue
                        artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip().lower()
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
                    if('artists' in track and len(track['artists']) > 1):
                        for artist in track['artists']:
                            if(artist == None):
                                continue
                            if(artist['artist_id'] == 355):
                                bskip = True
                        retlist = GetArtist(track['artists'])
                        if(retlist[0] != None):
                            song['featArtists'] = retlist[1]
                            song['connectors'] = retlist[2]
                            song['artistName'] = retlist[0]
                    if(bskip == True):
                        earlier_year_skip = True
                        continue
                    if('extraartists' in track and track['extraartists'] != None):
                        extartists = track['extraartists']
                        for extart in extartists:
                            if(extart == None):
                                continue
                            extart['artist_name'] = re.sub(r'\(.*?\)', '', extart['artist_name']).strip().lower()
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
                                    
                        
                    song['name'] = track['title']
                    song['name'] = remove_stemwords(song['name'])
                    #song['name'] = song['name'].replace("?",'').strip()
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
                    if(curr_rel == True):
                        release_song_list.append(song)
                    else:
                        curr_song_list.append(song)
                    combined_songs_list.append(song)
                    if(song['isCompilation'] == True):
                        earlier_year_skip = True
                    if(earlier_year_skip == False):
                        option = check(curr_album['released_date'],ear_year)
                        if(option == 3 and ear_rel== False and curr_rel == True):
                            ear_count = curr_album['country']
                            ear_year = curr_album['released_date']
                            ear_rel = curr_rel
                        elif(option ==3 and curr_rel == True):
                            if(ear_count != curr_album['country']):
                                ear_conflict = True
                        if(option == 1):
                            ear_count = curr_album['country']
                            ear_year = curr_album['released_date']
                            ear_rel = curr_rel
                            ear_conflict = False
        except Exception as e:
            logger_error.exception(e)
        final_song_list = GetUniquesongs(release_song_list,final_song_list,True,False,ear_count,full_song_list)
        final_song_list = GetUniquesongs(curr_song_list,final_song_list,False,True,ear_count,full_song_list)
        
    return combined_songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict,final_song_list

def checkFtArtist(ftartist1,ftartist2):
    if(len(ftartist1) != len(ftartist2)):
        return False
    ft1 = set(ftartist1)
    ft2 = set(ftartist2)
    intersect = ft1.union(ft2) - ft1.intersection(ft2)
    if(len(intersect) == 0):
        return True
    return False
            
def checkIfSongExists(curr_song,songs_list):
    retVal = False
    matched_song = ""
    song_name = curr_song['name']
    for s in songs_list:
        #print song
        song = songs_list[s]['name']
        if(len(song) > len(song_name)):
            soundex = fuzzy.Soundex(len(song))
        else:
            soundex = fuzzy.Soundex(len(song_name))
        phonectic_distance = fuzz.ratio(soundex(song),soundex(song_name))
        if('(' in song.lower() and '(' in song_name.lower()):
            parmatch,tryagain = getparanthesismatch(song.lower(),song_name.lower())
            if(parmatch == True):
                if(curr_song['artistName'].lower() == songs_list[s]['artistName'].lower() and checkFtArtist(curr_song['featArtists'],songs_list[s]['featArtists']) == True):
                    retVal = True
                    #print song_name + ' -------------- ' + song
                    #print "paranthesis match"
                    matched_song = s
                    break
        normal_distance = fuzz.ratio(song.lower(),song_name.lower())
        if(phonectic_distance >= 90 and normal_distance >= 85):
            if(curr_song['artistName'].lower() != songs_list[s]['artistName'].lower()):
                continue
            if(checkFtArtist(curr_song['featArtists'],songs_list[s]['featArtists']) == False):
                continue
            retVal = True
            #print song_name + ' -------------- ' + song
            #print songs_list[s]['year'] 
            #print curr_song['year']
            #print str(phonectic_distance) + " ######### " + str(normal_distance)
            matched_song = s
            break
    return retVal,matched_song


def getparanthesismatch(source,destination):
    try:
        sourcelist = re.findall('(.*?)\(([^(].*?)\)(.*)',source)
        destinationlist = re.findall('(.*?)\(([^(].*?)\)(.*)',destination)
        if(len(sourcelist) ==0 or len(destinationlist) == 0):
            return False,1
        slist = [s for s in sourcelist[0] if s != "" ]
        dlist = [s for s in destinationlist[0] if s != "" ]
        
        part1_dist = fuzz.ratio(slist[0].lower(),dlist[0].lower())
        part2_dist = 0
        if(len(slist) >1 and len(dlist) > 1):
            fuzz.ratio(slist[1].lower(),dlist[1].lower())
        
        if(part1_dist >= 85 and part2_dist >=85):
            return True,0
        else:
            return False,0
    except Exception, e:
        logger_error.exception(source)
        logger_error.exception(destination)
        logger_error.exception(e)
        return False,0
        
def getGenresAndStyles(genres,styles):
    genre = []
    style = []
    if(genres != None):
        genre = genres.replace("{","")
        genre = genre.replace("}","")
        genre = genre.replace("\"Folk, World, & Country\"","fwc")
        genre = genre.split(',')
        if("fwc" in genre):
            genre.remove("fwc")
            genre.append("Folk, World, & Country")
    else:
        genre = []
    if(styles != None):    
        style = styles.replace("{","")
        style = style.replace("}","")
        style = style.split(',')
    else:
        style = []
    return genre,style


def crawlArtist(directory):
    logger_decisions.error(directory)
    if(os.path.basename(directory) == '194' or os.path.basename(directory) == '355'):
        logger_decisions.error('skipping ' +directory)
        return
    try:
        songs_list = list()
        global misses
        global hits
        global IsIncremental
        full_lang_list = {}
        full_country_list ={}
        aliases = []
        ear_count = ""
        ear_year = 1001
        ear_rel = False
        master_ear_count = ""
        master_ear_rel = ""
        bskipflag = 0
        final_song_list = {}
        ear_conflict = False
        full_song_list = []
        ##Get the songs from the full trial
        retVal = checkpreviousfull(directory)
        if(IsIncremental == 1 and retVal == 1):
            infile = directory + '/songslist.txt'
            try:
                fread = open(infile,'r')
            except IOError as e:
                logger_error.exception(e)
            parallel_songs_list = pickle.load(fread)
            full_song_list = GetSongsFromFullList(parallel_songs_list)
        songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict,final_song_list = get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list)
        print ear_count
        print ear_year
        if(len(songs_list) != 0):
            master_ear_count = ear_count
            master_ear_year = ear_year
            bskipflag = 1
        songs_list,final_song_list,full_country_list,aliases,ear_count,ear_year,ear_rel = get_song_list(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,final_song_list,full_song_list)
        print ear_count
        print ear_year
        if(bskipflag == 1):
            ear_count = master_ear_count
            ear_year = master_ear_year
        sorted_list_country = sorted(full_country_list.items(), key=operator.itemgetter(1),reverse = True)
        #sorted(full_country_list,key = lambda x:x['name'].lower())
        artist_country = ear_count
        if(len(sorted_list_country) > 0):
            artist_country = sorted_list_country[0][0]
        if(ear_conflict == True):
            ear_count = artist_country
        sorted_list = sorted(songs_list,key = lambda x:x['name'].lower()) 
        hits = 0
        misses = 0
        global request_count
        request_count = 0
        if(len(sorted_list) == 0):
            return
    except Exception, e:
        logger_error.exception(e)
        return
    try:
        curr_time = "2020-14-33"
        curr_language = ""
        curr_song = {}
        artist_alias_list = []
        artist_alias_list = getArtistAliasList(sorted_list)
        
        total_count = 0
        for i in full_lang_list:
            total_count = total_count + full_lang_list[i]
        percent_lang = {}
        change_language = ''
        '''for s in final_song_list:
            logger_decisions.error(s)
            logger_decisions.error(final_song_list[s]['year'])
            logger_decisions.error(final_song_list[s]['genres'])
            logger_decisions.error(final_song_list[s]['styles'])
            logger_decisions.error(final_song_list[s]['country'])
            logger_decisions.error(final_song_list[s]['albumInfo'])
            logger_decisions.error('----------------------------------------')'''
        vid = list()
        if(IsIncremental == 0):
            with open(directory + '/uniquelist.txt', 'wb') as f:
			    pickle.dump(final_song_list, f)
        else:
            with open(directory + '/uniquelist_incr.txt', 'wb') as f:
			    pickle.dump(final_song_list, f)
        parallel_songs_list = []
        finalsongs = final_song_list.values()
        for s in finalsongs:
            """if('people are ' in s['name'].lower()):
                        print ' xxxxxxxxxxxxxxxxxx '
                        print s['year']
                        print s['name']
                        print 'xxxxxxxxxxxxxxxxxx' """
            curr_elem = dict(s)
            #print curr_elem['songcountry'] 
            if(not s.has_key('artistName')):# or s['artistName'] not in aliases):
                continue
            if(s['artistName'] in artist_alias_list):
                for art_alias in  artist_alias_list:
                    curr_elem = dict(s)
                    curr_elem['artistName'] = art_alias
                    curr_elem['songcountry'] = ear_count
                    parallel_songs_list.append(curr_elem)
            else:
                curr_elem['songcountry'] = ear_count
                parallel_songs_list.append(curr_elem)
            
        t1=time.time()
        print len(parallel_songs_list)
        if(IsIncremental == 0):
            with open(directory + '/last_full_part1.txt', 'wb') as f1:
                f1.write(str(int(t1)))
                f1.close()
            with open(directory + '/songslist.txt', 'wb') as f:
                pickle.dump(parallel_songs_list, f)
                f.close()
        else:
            print "incremental"
            with open(directory + '/songslist_incr.txt', 'wb') as f:
                pickle.dump(parallel_songs_list, f)
                f.close()
            with open(directory + '/last_incr_part1.txt', 'wb') as f1:
                f1.write(str(int(t1)))
                f1.close()
    except Exception, e:
        logger_error.exception(e)
    logger_decisions.error(directory + "Completed ")
    logger_decisions.error('-----------------------')

def checkpreviousfull(directory):
    retVal = 0
    #if(os.path.exists(directory+'/last_full_part1.txt')):
    #    retVal = 1
    if(os.path.exists(directory+'/dump')):
        retVal = 1
    return retVal

def GetKeyFromSong(song):
    keySong = song['name'].lower()
    keySong = keySong + "," + song['artistName']
    if(len(song['featArtists'])!= 0):
        temp_str = ','.join(song['featArtists'])
        keySong = keySong + "," +temp_str.lower()
    KeySong = keySong.strip()
    return KeySong

''' Incremental Functions '''
def GetSongsFromFullList(songs_details):
    full_songs_list = {}
    for song in songs_details:
        keySong = GetKeyFromSong(song)
        full_songs_list[keySong] = song
    return full_songs_list

def GetChangedFiles(directory,times):
    changed_files = []
    master_list = glob.glob(directory+"/master*.json")
    for fileName in master_list:
        if( os.path.getmtime(fileName) > times):
            changed_files.append(fileName)
    releases_list = glob.glob(directory+"/master*.json")
    for fileName in releases_list:
        if( os.path.getmtime(fileName) > times):
            changed_files.append(fileName)
    #print changed_files

reload(sys)
sys.setdefaultencoding('utf8')
filenameList = []
t1 = time.time()
try:
    lastdirectory = 0
    logger_error.debug("Discogs Main Program Starting")
    directory = raw_input("Enter directory: ")
    m1 = raw_input("Enter m: ")
    folders = raw_input("Enter number of folders: ")
    folders = int(folders)
    m1=int(m1)
    incr = raw_input("Isincremental : ")
    incr = int(incr)
    IsIncremental = incr
    prev_time = 0
    timeFile = directory + "/timelog.txt"
    if(incr == 1):
        try:
            with open(timeFile,"r") as f:
                prev_time = int(f.read())
        except IOError as e:
            print e
    directorylist = list()
    if(os.path.exists(directory+'/lastdirectory.txt')):
        fread = codecs.open(directory+'/lastdirectory.txt','r','utf-8')
        lines = fread.readlines()
        if(len(lines) > 0):
            if(lines[-1].strip() != ""):
                lastdirectory = int(lines[-1])
        fread.close()
        fwrite = codecs.open(directory+'/lastdirectory.txt','a','utf-8')
    else:
        fwrite = codecs.open(directory+'/lastdirectory.txt','w','utf-8')

    ''' Folders list count to control the numebr of folders done'''
    for dirs in os.listdir(directory):
        found = re.search(r'[0-9]+',str(dirs),0)
        if (found and (lastdirectory <= int(dirs))):
            directorylist.append(int(dirs))
    directorylist = sorted(directorylist)
    splitlist = list(itertools.izip_longest(*(iter(directorylist),) * folders))
    logger_error.debug(splitlist)
    for split in splitlist:
        foldlist = list()
        t1=time.time()
        foldercompletelist = {}
        folderstartedlist = {}
        logger_error.debug("Getting the Folders List")
        for dirs in split:
            if(dirs == None):
                continue
            for curr_dir, sub_dir, filenames in os.walk(directory+'/'+str(dirs)):
                            strg = curr_dir
                            foldlist.append(strg)
        logger_error.debug("Folders List:")
        print foldlist

        n = len(foldlist)

        logger_error.debug("Starting Processes:")
        songs_pool = Pool()
        songs_pool =Pool(processes=m1)
        songs_pool.map_async(crawlArtist,foldlist)
        songs_pool.close()
        songs_pool.join()
        print time.time()-t1
        logger_error.debug("completed for split : "+','.join(map(str,split)))
        fwrite.write(str(split[0]))
        fwrite.write("\n")
        if(split[-1]!= None):
            fwrite.write(str(split[-1]))
            fwrite.write("\n")
    fwrite.close()
except Exception as e:
		logger_error.exception(e)



def callmefromanotherfile(filepartition):
    filepartition = filepartition.split()
    filenameList = []
    t1 = time.time()
    global IsIncremental,prev_time
    IsIncremental = int(filepartition[0])
    prev_time = int(filepartition[1])
    print IsIncremental
    print prev_time
    if(len(sys.argv) > 0):
        # second and third arguments are for the incremental option and the previous time.
        arglist = filepartition[2:]
        for arg in arglist:
            if(arg != None):
                filenameList.append(arg)
        #logger_error.exception(filenameList)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                    return_pool = executor.map(crawlArtist,filenameList)
        '''songs_pool = Pool()
        songs_pool =Pool(processes=50)
        songs_pool.map(crawlArtist,filenameList)'''
        logger_error.exception("Completed for the Files")
        #logger_error.exception(filenameList)
    except Exception, e:
        logger_error.exception(e)
print time.time()-t1