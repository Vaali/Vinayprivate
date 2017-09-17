# -*- coding: utf-8 -*-
import sys
import os
import json
import simplejson
import re
import codecs
import urllib
from urllib2 import Request,urlopen, URLError, HTTPError
from datetime import datetime, date, timedelta
import time
from multiprocessing import Pool
import logging
import logging.handlers
import pickle
from fuzzywuzzy import fuzz
from multiprocessing.pool import ThreadPool
import concurrent.futures
import itertools
import glob
import fuzzy
from solr import SolrConnection
from solr.core import SolrException
import operator



reload(sys)
sys.setdefaultencoding('utf8')
'''
Initialising the loggers

'''

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)s - %(thread)s:%(message)s')
logger_error = logging.getLogger('simple_logger')
hdlr = logging.handlers.RotatingFileHandler(
              'errors_getVideos.log', maxBytes=1024*1024*1024, backupCount=10)
#hdlr = logging.FileHandler('errors_getVideos.log')
hdlr.setFormatter(formatter)
logger_error.addHandler(hdlr)
logger_error = logging.getLogger('simple_logger')


formatter1 = logging.Formatter('%(message)s')
logger_decisions = logging.getLogger('simple_logger1')
hdlr_1 = logging.handlers.RotatingFileHandler(
              'decisions_new.log', maxBytes=1024*1024*1024, backupCount=10)
#hdlr_1 = logging.FileHandler('decisions_new.log')
hdlr_1.setFormatter(formatter1)
logger_decisions.addHandler(hdlr_1)
logger_decisions = logging.getLogger('simple_logger1')

stemwords_uniquelist = ["(Edited Short Version)","(Alternate Early Version)","(Alternate Version)","(Mono)","(Radio Edit)","(Original Album Version)","(Different Mix)","(Music Film)","(Stereo)","(Single Version)","Stereo","Mono","(Album Version)","Demo","(Demo Version)"]
solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_artists')

#class 


'''
Utility functions
'''
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

def changeName(artName):
    artNamewords = artName.split()
    retNamewords = []
    for artword in artNamewords:
         retNamewords.append(artword[0].upper()+ artword[1:])
    return " ".join(retNamewords)

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

def checkFtArtist(ftartist1,ftartist2):
    if(len(ftartist1) != len(ftartist2)):
        return False
    ft1 = set(ftartist1)
    ft2 = set(ftartist2)
    intersect = ft1.union(ft2) - ft1.intersection(ft2)
    if(len(intersect) == 0):
        return True
    return False

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

def RemoveStemCharacters(currString):
    currString = currString.replace('"','').strip()
    currString = currString.replace('’','').strip()
    currString = currString.replace("'",'').strip()
    currString = currString.replace("‘",'').strip()
    currString = currString.replace("?",'').strip()
    currString = currString.replace(',','').strip()
    return currString

def RemoveStemCharactersforComparison(currString):
    currString = currString.replace('"','').strip()
    currString = currString.replace('’','').strip()
    currString = currString.replace("'",'').strip()
    currString = currString.replace("‘",'').strip()
    currString = currString.replace("?",'').strip()
    currString = currString.replace(',','').strip()
    currString = currString.replace('(','').strip()
    currString = currString.replace(')','').strip()
    currString = currString.replace('[','').strip()
    currString = currString.replace(']','').strip()
    currString = currString.replace('&','').strip()
    currString = currString.replace('-',' ').strip()
    currString = currString.replace('´','').strip()
    return currString

def remove_stemwords(songName):
    global stemwords_uniquelist
    for stem in stemwords_uniquelist:
        if(stem in songName):
            songName = songName.replace(stem,"")
        if(stem.lower() in songName):
            songName = songName.replace(stem.lower(),"")
    return songName.strip()


'''
Data Cleaning Functions
'''
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


def GetSongsFromFullList(songs_details):
    full_songs_list = {}
    for song in songs_details:
        keySong = GetKeyFromSong(song)
        full_songs_list[keySong] = song
    return full_songs_list

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
        song['masterGenres'],song['masterStyles'] = getGenresAndStyles(song['masterGenres'],song['masterStyles'])
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
                                    stemp['release_Id'] = song['release_Id']
                                    stemp['masterRelease'] = song['masterRelease']
                                    stemp['masterGenres'] = song['masterGenres']
                                    stemp['masterStyles'] = song['masterStyles']
                                    stemp['isCompilation'] = song['isCompilation']
                                else:
                                    continue
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
                            else:
                                continue
                            if(k == 1):
                                if('release_album' in song):
                                        if(isPresentSong == True):
                                            stemp['name'] = song['name']
                                        stemp['year'] = song['year']
                                        stemp['release_album'] = song['release_album']
                                        stemp['genres'],stemp['styles']= song['genres'],song['styles']
                                        stemp['country'] = song['country']
                                        stemp['release_Id'] = song['release_Id']
                                        stemp['masterRelease'] = song['masterRelease']
                                        stemp['masterGenres'] = song['masterGenres']
                                        stemp['masterStyles'] = song['masterStyles']
                                        stemp['isCompilation'] = song['isCompilation']
                                        if('anv' in song):
                                            stemp['anv'] = song['anv']
                            if(k == 3):
                                if('release_album' not in stemp):
                                    stemp['year'] = song['year']
                                    stemp['genres'],stemp['styles']= song['genres'],song['styles']
                                    stemp['country'] = song['country']
                                    stemp['release_Id'] = song['release_Id']
                                    stemp['masterRelease'] = song['masterRelease']
                                    stemp['masterGenres'] = song['masterGenres']
                                    stemp['masterStyles'] = song['masterStyles']
                                    stemp['isCompilation'] = song['isCompilation']
                                    if('release_album' in song):
                                        stemp['release_album'] = song['release_album']
                                    if('anv' in song):
                                        stemp['anv'] = song['anv']
                    final_song_list[keySong] = stemp
    return final_song_list


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
        songs_list = []
        Iscompilation = False
        try:
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
                #temp_year_album = GetYearFromTitle(curr_album['title'])
                #print temp_year_album
                if(curr_album['released_date'] != None and temp_year_album != 0):
                    if(str(curr_album['released_date']).split('-')[0] != ''):
                        curr_year = int(str(curr_album['released_date']).split('-')[0])
                    else:
                        curr_year = 1001
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
                song['featArtistsIds'] = []
                song['connectors'] = []
                song['extraArtists'] = []
                song['extraArtistsconnectors'] = []
                song['release_Id'] = curr_album['release_id']
                song['masterRelease'] = curr_album['release_id']
                song['masterGenres'] = curr_album['genres']
                song['masterStyles'] = curr_album['styles']
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
                        #print song['artistName']
                        #print artist['artist_id']
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
                        song['featArtists'] = retlist[2]
                        song['connectors'] = retlist[3]
                        song['artistName'] = retlist[0]
                        song['artist_id'] = retlist[1]
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
                if(song['name'] == ''):
                        song['name'] = track['title']

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

def get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list):
    releases_list = []
    global prev_time
    global IsIncremental
    retVal = checkpreviousfull(directory)
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
        release_song_list = []
        curr_song_list =[]
        Iscompilation  = False
        try:
            #curr_master = release
            filename = release
            with codecs.open(filename,"r","utf-8") as input1:
                curr_master = json.load(input1)
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
                #temp_year_album = GetYearFromTitle(curr_album['title'])
                if(curr_album['released_date'] != None and temp_year_album != 0):
                    if(str(curr_album['released_date']).split('-')[0] != ''):
                        curr_year = int(str(curr_album['released_date']).split('-')[0])
                    else:
                        curr_year = 1001
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
                    song['release_Id'] = curr_album['release_id']
                    song['masterRelease'] = curr_master['id']
                    song['masterGenres'] = curr_master['genres']
                    song['masterStyles'] = curr_master['styles']
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
                            song['featArtists'] = retlist[2]
                            song['connectors'] = retlist[3]
                            song['artistName'] = retlist[0]
                            song['artist_id'] = retlist[1]
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
                    if(song['name'] == ''):
                        song['name'] = track['title']
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
        final_song_list = GetUniquesongs(release_song_list,final_song_list,False,False,ear_count,full_song_list)
        final_song_list = GetUniquesongs(curr_song_list,final_song_list,False,True,ear_count,full_song_list)

    return combined_songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict,final_song_list

def CheckifSongsExistsinSolr(sname,aname,fname):
    try:
        solrConnection1 = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_data_test')
        songName = 'stringSongName:"'+sname+'"'
        artistName = 'artistName:"'+changeName(aname)+'"'
        facet_query = [songName,artistName]
        if(len(fname) != 0):
            for f in fname:
                ftartistName = 'ftArtistName:"'+changeName(f)+'"'
                facet_query.append(ftartistName)
            #print facet_query
        response = solrConnection1.query(q="*:*",fq= facet_query,version=2.2,wt = 'json')
        intersect = int(response.results.numFound)
        #print songName
        #print artistName
        
        if(intersect > 0):
            '''for result in response.results:
                #print result['youtubeName']
                #print result['artistName']
                #print result['songName']
                print fname
                if('ftArtistName' in result):
                    result['ftArtistName']
                    
                    #print len(response.results)'''
            return True
    except Exception as e:
        logger_error.exception(e)
    return False


def crawlArtist(directory):
    logger_decisions.error(directory)
    #print os.path.basename(directory)
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
        if(IsIncremental == 0 or IsIncremental == 2):
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

        t3= time.time()
        print len(parallel_songs_list)
        if(IsIncremental == 0 or IsIncremental ==2):
            with open(directory + '/last_full_part1.txt', 'wb') as f1:
                f1.write(str(int(t3)))
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
                f1.write(str(int(t3)))
                f1.close()
    except Exception, e:
        logger_error.exception(e)
    logger_decisions.error(directory + "Completed ")
    logger_decisions.error('-----------------------')
    #print parallel_songs_list
    try:
        #print fl
        vid = list()
        misses = 0
        hits = 0
        found = 0
        fullComplete = checkpreviousfull(directory)
        print 'fullcomplete'
        print fullComplete
        '''if(IsIncremental == 0 or fullComplete == 0):
            infile = directory + '/songslist.txt'
        else:
            infile = directory + '/songslist_incr.txt'
        try:
            fread = open(infile,'r')
        except IOError as e:
            return
        parallel_songs_list = pickle.load(fread)'''
        #songs_pool = Pool()
        #songs_pool =ThreadPool(processes=5)
        print len(parallel_songs_list)
        #print parallel_songs_list
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                return_pool = executor.map(getVideoFromYoutube,parallel_songs_list)
        #print len(return_pool)
        for ret_val in return_pool:
            
            if(ret_val[2] == True):
                    found = found + 1
                    print ret_val[3]
                    continue
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
        print "Hits:"+str(hits)+" Misses:"+str(misses) + " Found : "+ str(found)
        if(IsIncremental == 0 or IsIncremental ==2):
            write(vid,directory+"/dump")
            with open(directory + '/last_full_part2.txt', 'wb') as f1:
                f1.write(str(int(time.time())))
                f1.close()
        else:
            write(vid,directory+"/dump_incr")
            with open(directory + '/last_incr_part2.txt', 'wb') as f1:
                f1.write(str(int(time.time())))
                f1.close()
    except Exception as e:
        print e
        logger_error.exception(e)

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


'''
youtube functions
'''

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




def checkFtArtist(ftartist1,ftartist2):
    if(len(ftartist1) != len(ftartist2)):
        return False
    ft1 = set(ftartist1)
    ft2 = set(ftartist2)
    intersect = ft1.union(ft2) - ft1.intersection(ft2)
    if(len(intersect) == 0):
        return True
    return False





def getVideoFromYoutube(curr_elem):
    global IsIncremental
    retvid = None
    bret = False
    artname = curr_elem['artistName']
    sname = curr_elem['name']
    ftartists = curr_elem['featArtists']
    #print '---------------------'
    #print artname
    #print '-----------------'
    if(IsIncremental ==2):
        if(CheckifSongsExistsinSolr(sname,artname,ftartists) == True):
            print 'found the song'
            retstring = sname + '------' + artname + '-----' + ','.join(ftartists)
            return retvid,True,True,retstring
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
    if(retvid != None):
        tempDictionary = retvid[0].__dict__
        '''if('errorstr' in tempDictionary):
            logger_decisions.error(tempDictionary['errorstr'])
            logger_decisions.error('-----------------')'''
    return retvid,bret,False


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
            video1.release_Id = curr_elem['release_Id']
            video1.masterRelease = curr_elem['masterRelease']
            video1.masterGenres = curr_elem['masterGenres']
            video1.masterStyles = curr_elem['masterStyles']
            video1.isCompilation = curr_elem['isCompilation']
            print curr_elem['release_Id']
            if('anv' in curr_elem):
                video1.anv = curr_elem['anv']
            if('artistalias' in curr_elem):
                video1.artistalias = curr_elem['artistalias']
            video1.genres = curr_elem['genres']
            video1.styles = curr_elem['styles']
            video1,bret = getYoutubeUrl(video1,flag,0)
            video1.artist_id = curr_elem['artist_id']
            #print curr_elem['artist_id']
            #print curr_elem['artistName']
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



def GetYearFromTitle(vid_title):
    returnYear = 0
    yearList = re.findall(r'\d\d\d\d+',vid_title)
    #print yearList
    if(len(yearList) != 0):
        returnYear = int(yearList[0])
        if(vid_title == yearList[0]):
            returnYear = 0
    return returnYear

def CalculateMatch(video,vid_title,vid_description,oldsong = False):
    try:
        list = ""
        conlist = ""
        if(oldsong == False):
            artistName = video.artist
            ftArtistName = video.ftArtist
            connectorList = video.connectors
            songName = video.name
        else:
            artistName = video.artist.artistName[0]
            ftArtistName = video.ftArtistList.ftArtistName
            connectorList = video.connPhraseList.connPhrase
            songName = video.songName

        fList = ""
        albumname = ""
        error_str = ""
        decision = "Incorrect"
        stemwords = [ 'screen','m/v','artist','ft','featuring','live','hd','1080P','video','mix','feat','official','lyrics','music','cover','version','original','\
hq','band','audio','album','world','instrumental','intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod','piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single','official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','song','digital','hour','nature','motion','sounds','sound','ballad','unplugged','singers','singer','legend','legends', 'french','strings','string','classic','cast','act','full','screen','radio','remix','song','edit','tracks','remaster','reissue','review','re-issue','trailer','studio','improvization','solo','download','tour','dvd','festival','remastered']
        '''stemwords = ['video','mix','feat','official','lyrics','music','cover','version','original','hq','band','audio','album','world','instrumental', 'intro','house','acoustic','sound','orchestra','vs','track','project','records','extended','01','02','03','04','05','06','07','08','09','2008','prod', 'piano','town','disco','solo','festival','vivo','vocal','featuring','name','london','1995','soundtrack','tv','em','ti','quartet','system','single', 'official','top','low','special','trance','circle','stereo','videoclip','lp','quality','underground','espanol','vinyl','tribute','master','step','uk','eu','voice','promo','choir','outro','au','anthem','songs','digital','hour','nature','motion','sounds','ballad','unplugged','singers','legend', 'french','strings','classic','cast','act','full','screen','radio','remix','song','edit','tracks']'''
        stemcharacters = ['[',']','(',')','\'','"','.','’']
        youtubematch = vid_title.lower()
        descriptionmatch = vid_description.lower()
        diffset = []
        substring_album = "false"
        #remove the characters form songname and youtubename
        #for c in stemcharacters:
        #    youtubematch = youtubematch.replace(c,' ').strip()
        #    songName = songName.replace(c,' ').strip()
        artist_order = {}
        #remvoe the stemwords form youtube name
        songnameset = re.findall("\w+",songName.lower(),re.U)
        for word in stemwords:
            if(word not in songnameset):
                diffset.append(word)
                pattern = '\\b'+word+'\\b'
                youtubematch = re.sub(pattern,'', youtubematch)
                descriptionmatch = re.sub(pattern,'',descriptionmatch)

        for c in connectorList:
            if(c != None):
                conlist = conlist+" "+c
        #Find positions of artist ,song and feat artists
        songpos = youtubematch.lower().find(songName.lower())
        songpos_desc = descriptionmatch.lower().find(songName.lower())
        artpos = youtubematch.lower().find(artistName.lower())
        artpos_desc = descriptionmatch.lower().find(artistName.lower())
        if(oldsong == False):
            for l in video.album:
                albumpos = youtubematch.lower().find(l['albumname'].lower())
                if(albumpos != -1):
                    substring_album = "true"
                    artist_order[albumpos] = l['albumname']
                    albumname = l['albumname']
                    break
        ftart_substring = []
        ftartpos = []
        ftartistmatchdesc = []
        ftartistmatch = []
        comparestringartist = artistName;
        if(songpos != -1):
            substring_song = "true"
            artist_order[songpos] = songName
        else:
            substring_song = "false"
        if(songpos_desc != -1):
            substring_song_desc = "true"
        else:
            substring_song_desc = "false"
        if(artpos_desc != -1):
            substring_artist_desc = "true"
        else:
            substring_artist_desc = "false"
        if(artpos != -1):
            substring_artist = "true"
            artist_order[artpos] = artistName
        else:
            substring_artist = "false"
        for f in ftArtistName:
            fList = fList+" "+f
            comparestringartist = comparestringartist + " " + f
            currpos = youtubematch.lower().find(f.lower())
            currpos_desc = descriptionmatch.lower().find(f.lower())

            ftartpos.append(currpos)
            if(currpos != -1):
                ftartistmatch.append(True)
                artist_order[artpos] = f
            else:
                ftartistmatch.append(False)
            if(currpos_desc != -1):
                ftartistmatchdesc.append(True)
            else:
                ftartistmatchdesc.append(False)

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

        ########
        # partial set and fuzzy ratios
        #######
        partial_songmatch = fuzz.partial_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(songName.lower()))
        partial_artist = fuzz.partial_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(artistName.lower()))
        partial_totalmatch = fuzz.token_sort_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(songName.lower()+ " " +comparestringartist.lower()))
        setratio_songmatch = fuzz.token_set_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(songName.lower()))
        setratio_artistmatch = fuzz.token_set_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(artistName.lower()))
        setratio_totalmatch = fuzz.token_set_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(songName.lower()+ " " +comparestringartist.lower()))
        ftartist_partial = []
        ftartist_setratio = []
        for f in ftArtistName:
            ftartist_partial.append(fuzz.partial_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(f.lower())))
            ftartist_setratio.append(fuzz.token_set_ratio(RemoveStemCharactersforComparison(youtubematch.lower()),RemoveStemCharactersforComparison(f.lower())))




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
        elif(substring_artist == "true" and substring_song == "true"  and (percentMatch > 80.0 or (leftMatch == 100.0 and bhiphen and len(artistName.strip().split()) > 1) and percentMatch > 30.0)):
            decision = "correct"
            condition = 4
        #no hiphen , song match shd be 100 and left or right should be 100
        elif(substring_artist == "true" and substring_song == "true" and not bhiphen and songMatch == 100.0 and (leftMatch == 100.0 or rightMatch == 100.0) and percentMatch > 60.0 and ( len(artistName.strip().split()) > 1)):
            decision = "correct"
            condition = 5
        if(bhiphen == "true" and (songMatch == 0  or (leftMatch == 0.0 and rightMatch == 0.0))):
            decision = "Incorrect"
        '''if(len(vid_title) > len(video.name)):
            soundexmax = fuzzy.Soundex(len(vid_title))
            soundexmin = fuzzy.Soundex(len(video.name))
        else:
            soundexmax = fuzzy.Soundex(len(video.name))
            soundexmin = fuzzy.Soundex(len(vid_title))'''
        error_str += "##decision:"
        error_str += str(decision)
        error_str += "##condition:"
        error_str += str(condition)
        error_str += "##match:"
        error_str += str(match)
        error_str += "##artistName:"
        error_str += str(artistName)
        error_str += "##songName:"
        error_str += str(songName)
        error_str += "##ftArtistName:"
        error_str += str(ftArtistName)
        error_str += "##substring_album:"
        error_str += str(substring_album)
        error_str += "##vid_title:"
        error_str += str(vid_title)
        error_str += "##vid_description:"
        error_str += str(vid_description)
        error_str += "##comparestringleft:"
        error_str += str(RemoveStemCharactersforComparison(songName.lower()+ " " +comparestringartist.lower()))
        error_str += "##substring_song:"
        error_str += str(substring_song)
        error_str += "##substring_artist:"
        error_str += str(substring_artist)
        error_str += "##ftartistmatch:"
        error_str += str(ftartistmatch)
        error_str += "##substring_song_desc:"
        error_str += str(substring_song_desc)
        error_str += "##substring_artist_desc:"
        error_str += str(substring_artist_desc)
        error_str += "##ftartistmatch_desc:"
        error_str += str(ftartistmatchdesc)
        error_str += "##percentMatch:"
        error_str += str(percentMatch)
        error_str += "##youtubematch:"
        error_str += str(youtubematch)
        error_str += "##partial songmatch:"
        error_str +=str(partial_songmatch)
        error_str += "##partial artist:"
        error_str += str(partial_artist)
        error_str += "##partial ft artist:"
        for f in ftartist_partial:
            error_str += ','
            error_str += str(f)
        error_str += "##partial total match"
        error_str += str(partial_totalmatch)
        error_str += "##setratio songmatch:"
        error_str += str(setratio_songmatch)
        error_str += "##setratio artist:"
        error_str += str(setratio_artistmatch)
        error_str += "##setratio ft artist:"
        for f in ftartist_setratio:
            error_str += ','
            error_str += str(f)
        error_str += "##setratio total match:"
        error_str += str(setratio_totalmatch)
        logger_decisions.error(error_str)
        '''logger_decisions.error(decision)
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
        logger_decisions.error("phonetic distance : ")'''
        #logger_decisions.error(fuzz.ratio(soundex(youtubematch.lower()),soundex(comparestring.lower())))
        #logger_decisions.error('-----------------')
    except Exception, e:
            logger_error.exception(e)
    #print error_str
    return decision,match,tm,sm,am,error_str

def GetKeyFromSong(song):
    keySong = song['name'].lower()
    keySong = keySong + "," + song['artistName']
    if(len(song['featArtists'])!= 0):
        temp_str = ','.join(song['featArtists'])
        keySong = keySong + "," +temp_str.lower()
    KeySong = keySong.strip()
    return KeySong

''' Incremental Functions '''




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
            artist_id = artist['artist_id']
        else:
            ftArtistList.append(artName)
        if(artist['join_relation'] != None):
            connList.append(artist['join_relation'])
    return artistName,artist_id,ftArtistList,connList








def getYoutubeUrl(video,flag,mostpopular):
    global request_count
    bret = False
    try:
        flist = ""
        yearfromName = 0
        #yearfromName = GetYearFromTitle(video.name)
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
            searchResult = simplejson.load(urlopen(searchUrl),"utf-8")
            request_count = request_count + 100
            #print searchResult
        except HTTPError as e:
            request_count = request_count + 100
            logger_error.exception(e.read())
            return video,bret
        except URLError as e:
            request_count = request_count + 100
            logger_error.exception(e.reason)
            return video,bret
        except Exception as e:
            request_count = request_count + 100
            logger_error.exception(e)
            #print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
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
                    [currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch,error_str] = CalculateMatch(video,searchEntry['snippet']['title'],searchEntry['snippet']['description'])
                    video.errorstr = error_str
                    if(currentVideoDecision == "correct"):# || currentVideoDecision == "Incorrect"):
                        currentVideoYear = GetYearFromTitle(searchEntry['snippet']['title'])
                        youtubeVideoId = searchEntry['id']['videoId']
                        videoUrl = "https://www.googleapis.com/youtube/v3/videos?id="+str(youtubeVideoId)+"&key=AIzaSyBE5nUPdQ7J_hlc3345_Z-I4IG-Po1ItPU&part=statistics,contentDetails,status"
                        try:
                            videoResult = simplejson.load(urlopen(videoUrl),"utf-8")
                            request_count = request_count + 7
                        except HTTPError as e:
                            request_count = request_count + 7
                            #erro_message = e.read()
                            logger_error.exception(e.read())
                            continue
                        except URLError as e:
                            request_count = request_count + 7
                            logger_error.exception(e.reason)
                            continue
                        except Exception as e:
                            request_count = request_count + 7
                            logger_error.exception(e)
                            #logger_error.exception("Error %d --- %s"% (e.resp.status, e.content))
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
                            if (mostpopular == 1):
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
                                break
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
                        #check if the earliest year present in the name of the song from youtube
                        if(selectedVideoYear != 0):
                            video.videoYear = selectedVideoYear
                            if(str(video.year).split('-')[0] != ''):
                                curr_year = int(str(video.year).split('-')[0])
                            else:
                                curr_year = 1001
                            if(curr_year == 1001 or (curr_year > int(video.videoYear))):
                                video.year = video.videoYear
                        #check if the earliest year present in the title of the song from discogs
                        if(yearfromName != 0):
                            video.videoYearName = yearfromName
                            if(str(video.year).split('-')[0] != ''):
                                curr_year = int(str(video.year).split('-')[0])
                            else:
                                curr_year = 1001
                            if(curr_year == 1001 or (curr_year > int(video.videoYearName))):
                                video.year = video.videoYearName
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

def checkpreviousfull(directory):
    retVal = 0
    if(os.path.exists(directory+'/last_full_part2.txt')):
        retVal = 1
    #elif(os.path.exists(directory+'/dump')):
    #    retVal = 1
    return retVal


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    filenameList = []
    t1 = datetime.now()
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
            #t1=time.time()
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
            n = len(foldlist)

            logger_error.debug("Starting Processes:")
            songs_pool = Pool()
            songs_pool =Pool(processes=m1)
            songs_pool.map_async(crawlArtist,foldlist)
            songs_pool.close()
            songs_pool.join()
            print datetime.now()-t1
            logger_error.debug("completed for split : "+','.join(map(str,split)))
            fwrite.write(str(split[0]))
            fwrite.write("\n")
            if(split[-1]!= None):
                fwrite.write(str(split[-1]))
                fwrite.write("\n")
        fwrite.close()
    except Exception as e:
        logger_error.exception(e)
    t2=datetime.now()
    print "time=" +str(t2-t1)
