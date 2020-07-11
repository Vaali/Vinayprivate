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
import loggingmodule
import random
from functools import partial
import managekeys
from songsutils import is_songname_same_artistname, CalculateMatch, GetYearFromTitle
import soundcloud
from config import IsYoutudeApi,IsSoundCloud
from config import DiscogsDataDirectory, NumberOfProcesses, NumberofFolders, IsIncremental, IsCrawlingYoutube
from youtubeapis import youtubecalls,youtubedlcalls


reload(sys)
sys.setdefaultencoding('utf8')
'''
Initialising the loggers

'''

logger_decisions = loggingmodule.initialize_logger1('decisions','decisions_new.log')
logger_error = loggingmodule.initialize_logger('errors','errors_getVideos.log')


stemwords_uniquelist = ["(Edited Short Version)","(Alternate Early Version)","(Alternate Version)","(Mono)","(Radio Edit)","(Original Album Version)","(Different Mix)","(Music Film)","(Stereo)","(Single Version)","Stereo","Mono","(Album Version)","Demo","(Demo Version)"]
solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_artists')

#class
#proj_keys =['AIzaSyBX-WCpgMHu_9OGpfkdQJD3SMsJTcDCscE']
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
        
        '''
        This change is to replace the user error of adding hip hop in genres instead of hip-hop.
        '''
        #genre = genre.lower().replace("hip hop","Hip-hop")
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
        #print song['masterGenres'],song['masterStyles']
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
    if((IsIncremental == 1 or IsIncremental == 3) and retVal == 1):
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
                        if(curr_year >2020):
                            curr_year = 1001#print 'year greater than 2020'
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
                if( IsSoundCloud == 1 or CheckifSongsExistsinSolr(song['name'],song['artistName'],song['featArtists']) == False):
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
        print directory
        print "------------"
    return songs_list,final_song_list,full_country_list,aliases,ear_count,ear_year,ear_rel

def get_song_list_master(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list):
    releases_list = []
    global prev_time
    global IsIncremental
    retVal = checkpreviousfull(directory)
    if((IsIncremental == 1 or IsIncremental == 3) and retVal == 1):
        master_list = glob.glob(directory+"/master*.json")
        for fileName in master_list:
            if( os.path.getmtime(fileName) > prev_time):
                '''with codecs.open(fileName,"r","utf-8") as input1:
                    curr_master = json.load(input1)
                    releases_list.append(curr_master)'''
                releases_list.append(fileName)
        #print prev_time
        #print releases_list
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
                    print song['year']
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
                    if( IsSoundCloud == 1 or CheckifSongsExistsinSolr(song['name'],song['artistName'],song['featArtists']) == False):
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
            print '---Found song---'
            return True
    except Exception as e:
        sys.exc_clear()
        #logger_error.exception(e)
    return False


def crawlArtist(directory):
    start_time = datetime.now()
    logger_decisions.error(directory + " ---- started ---")
    #print os.path.basename(directory)
    
    if(os.path.basename(directory) == '194' or os.path.basename(directory) == '355'):
        logger_decisions.error('skipping ' +directory)
        return
    try:
        curr_artist_dir = os.path.basename(directory)
        songs_list = list()
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

        if((IsIncremental == 1 or IsIncremental == 3) and retVal == 1):
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
        if(len(sorted_list) == 0):
            return
    except Exception, e:
        logger_error.exception(e)
        return
    logger_decisions.error(" -- Completed with time -- ")
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
    logger_decisions.error(directory + " -- Completed with time -- " + str(datetime.now() - start_time))
    #print parallel_songs_list
    if(IsIncremental == 0 or IsIncremental == 2):
        with open(directory + '/getyoutubelist.txt', 'wb') as f:
			pickle.dump(parallel_songs_list, f)
    else:
        with open(directory + '/getyoutubelist_incr.txt', 'wb') as f:
			pickle.dump(parallel_songs_list, f)
    
    #runYoutubeApi(directory)



def runYoutubeApi(directory):
    try:
        #print fl
        start_time = datetime.now()
        logger_decisions.error(directory + " ---- runYoutubeApi started ---")
        global IsIncremental
        global request_count
        global misses
        global hits
        request_count = 0
        curr_artist_dir = os.path.basename(directory)
        if(IsIncremental == 0 or IsIncremental ==2):
            lastrunfile = directory + '/lastrun.txt'
        else:
            lastrunfile = directory + '/lastrun_incr.txt'
        lasttime =0
        try:
            fread = open(lastrunfile,'r')
            lasttime = pickle.load(fread)
            fread.close()
        except Exception as e:
            sys.exc_clear()
        print str(lasttime)+'----lttt'
        currtime =0
        if(IsIncremental == 0 or IsIncremental == 2):
            infile = directory + '/getyoutubelist.txt'
        else:
            infile = directory + '/getyoutubelist_incr.txt'
        '''if(os.path.exists(infile)):
            currtime = int(os.path.getmtime(infile))
            if(currtime < lasttime):
                logger_decisions.error(directory + " -- runYoutubeApi Completed with time -- " + str(datetime.now() - start_time))
                return'''
        print str(currtime)+'------'
        try:
            fread = open(infile,'r')
        except IOError as e:
            logger_error.error(e)
            return
        parallel_songs_list = pickle.load(fread)
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
                    continue
            if(ret_val[0] == None ):
                misses = misses+1
            else:
                for rv in ret_val[0]:
                    if(rv == None):
                        continue
                    if('url' not in rv.__dict__):
                        misses = misses + 1
                    else:
                        hits = hits + 1
                        if(rv.__dict__['artist_id'] == curr_artist_dir):
                            rv.__dict__['same_artist'] = True
                        else:
                            rv.__dict__['same_artist'] = False

                        #print rv.__dict__
                        #tv = collections.OrderedDict(rv.__dict__)
                        vid.append(rv.__dict__)
        print "Hits:"+str(hits)+" Misses:"+str(misses) + " Found : "+ str(found)
        logger_decisions.error(directory +  "Hits:"+str(hits)+" Misses:"+str(misses) + " Found : "+ str(found))
        if(IsIncremental == 0 or IsIncremental ==2):
            write(vid,directory+"/dump")
            with open(directory + '/last_full_part2.txt', 'wb') as f1:
                f1.write(str(int(time.time())))
                f1.close()
        else:
            if(os.path.exists(directory+"/dump_incr")):
                os.remove(directory+"/dump_incr")
            write(vid,directory+"/dump_incr")
            with open(directory + '/last_incr_part2.txt', 'wb') as f1:
                f1.write(str(int(time.time())))
                f1.close()
        with open(lastrunfile, 'wb') as f2:
            print 'dumping '+str(int(time.time()))
            pickle.dump(str(int(time.time())),f2)
        logger_decisions.error(directory + " -- runYoutubeApi Completed with time -- " + str(datetime.now() - start_time))
    except Exception as e:
        print e
        logger_error.exception(e)
    

def checkIfSongExists(curr_song,songs_list):
    retVal = False
    matched_song = ""
    song_name = curr_song['name']
    phonectic_distance = 0
    try:
     for s in songs_list:
        #print song
        song = songs_list[s]['name']
        if(len(song) > len(song_name)):
            soundex = fuzzy.Soundex(len(song))
        else:
            soundex = fuzzy.Soundex(len(song_name))
        try:
            phonectic_distance = fuzz.ratio(soundex(song),soundex(song_name))
        except Exception as ex:
            #logger_error.error("fix soundex error")
            #print 'soundex error'
            sys.exc_clear()
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
    except Exception as ex:
        #logger_error.exception(source)
        #logger_error.exception(destination)
        logger_error.exception(ex)
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
        try:
            part1_dist = fuzz.ratio(slist[0].lower(),dlist[0].lower())
        except Exception, e:
            return False,0
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
    #if(is_songname_same_artistname(curr_elem['name'],curr_elem['artistName'])):
    #    return None,False,False
    #print '---------------------'
    print artname
    #print '-----------------'

    if(IsIncremental >=2):#both for full and incremental 2,3
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
                    if(rv == None):
                        continue
                    rv.artist = artname
        if(retvid == None):
            curr_elem['artistName'] = artname
            retvid,bret = getVideo(curr_elem,1)
        else:
            emptyvid = 0
            for rv in retvid:
                if(rv == None):
                    continue
                if('url' in rv.__dict__):
                    emptyvid = 1
            if(emptyvid == 0):
                curr_elem['artistName'] = artname
                retvid,bret = getVideo(curr_elem,1)
    except Exception as e:
        logger_error.exception('getVideoFromYoutube')
    '''if(retvid != None):
        tempDictionary = retvid[0].__dict__
        if('errorstr' in tempDictionary):
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
        #if(len(alist)==0):
        #    print curr_elem
        video12.album = alist
        video12.year = curr_elem['year']
        video12.language = 'English'
        video12.songcountry = curr_elem['songcountry']
        flist = ""
        #Apostolos
        try:
            video1 = Video()
            #video2 = Video()
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
            #print curr_elem['release_Id']
            if('anv' in curr_elem):
                video1.anv = curr_elem['anv']
            if('artistalias' in curr_elem):
                video1.artistalias = curr_elem['artistalias']
            video1.genres = curr_elem['genres']
            video1.styles = curr_elem['styles']
            if( IsSoundCloud == 1):
                video1,bret = getsoundcloudId(video1,flag,0)
            else:
                video1,bret = getYoutubeUrl(video1,flag,0)
            if(video1 != None):
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
    return [video1],bret

def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)

class Video():
	pass


class Album_Data():
	pass

class Audio(object):
	pass



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


def getsoundcloudId(video,flag,mostpopular):
    global request_count
    bret = False
    try:
        flist = ""
        yearfromName = 0
        for f in video.ftArtist:
            ttt=f.strip("-")
            flist = flist+" "+ttt
        ftartists = flist
        allArtists = video.artist.strip("-")+" "+ftartists
        key = "MMajjeox7VxGUdf7Audm3eQuwx1oPhGY"
        client = soundcloud.Client(client_id=key)
        searchUrl = urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))
        print searchUrl
        try:
            tracks = client.get('/tracks', q=searchUrl , limit = 6);
        except HTTPError as e:            
            if( e.response.status_code == 403 ):
                logger_error.error("Daily Limit Exceeded")
            else:
                logger_error.exception(e.message)
            return video,bret
        except URLError as e:
            logger_error.exception(e.message)
            return video,bret
        except Exception as e:
            logger_error.exception(e)
            return video,bret
        now = datetime.now()
        try:
            if len(tracks)!= 0:
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
                selectedVideoid = "";
                video.sclist=[]
                for track in tracks:
                    if(track.description == None):
                        description = ""
                    else:
                        description = track.description
                    [currentVideoDecision,currentVideoMatch,currentVideoTotalMatch,currentVideoSongMatch,currentVideoArtistMatch,error_str] = CalculateMatch(video,track.title,description,logger_error)
                    if( currentVideoDecision == "Incorrect" ):
                        [ currentVideoDecision, currentVideoMatch, currentVideoTotalMatch, currentVideoSongMatch, currentVideoArtistMatch, error_str ] = CalculateMatch(video,track.title + " "+ track.user["username"], description, logger_error )
                    #print track.title
                    #print currentVideoDecision
                    video.errorstr = error_str
                    if(currentVideoDecision == "correct"):# || currentVideoDecision == "Incorrect"):
                        
                        currentVideoYear = GetYearFromTitle(track.title,video.name)
                        currentVideoid = track.id
                        currentVideoViewCount = track.playback_count
                        currentVideolikes = track.favoritings_count
                        currentVideodislikes = 0
                        currentVideoEmbedded = track.embeddable_by
                        currentVideoStatus = track.streamable
                        currentVideoTags = track.track_type
                        currentVideoHashTags = track.tag_list
                        currentVideoGenres = track.genre
                        print currentVideoHashTags
                        if(currentVideoEmbedded != 'all' or currentVideoStatus == False):
                            continue
                        
                        currsc = {}
                        selectedVideoViewCount = currentVideoViewCount
                        selectedVideoMatch = currentVideoMatch
                        selectedVideoTotalMatch = currentVideoTotalMatch
                        selectedVideoSongMatch = currentVideoSongMatch
                        selectedVideoArtistMatch = currentVideoArtistMatch
                        selectedVideoTitle = track.title
                        selectedVideoYear = currentVideoYear
                        selectedVideoUrl = track.permalink_url
                        selectedVideoPublishedDate = track.created_at.replace('/','-')
                        selectedVideoDuration = track.duration
                        selectedVideolikes = currentVideolikes
                        selectedVideodislikes = currentVideodislikes
                        selectedVideoId = currentVideoid

                        currsc['ViewCount'] = selectedVideoViewCount
                        currsc['url'] = selectedVideoUrl
                        currsc['title'] = selectedVideoTitle
                        currsc['id'] = selectedVideoId
                        currsc['published'] = selectedVideoPublishedDate
                        currsc['match'] = selectedVideoMatch
                        currsc['tm'] = selectedVideoTotalMatch
                        currsc['sm'] = selectedVideoSongMatch
                        currsc['am'] = selectedVideoArtistMatch
                        currsc['tags'] = currentVideoTags
                        currsc['hash_tags'] = currentVideoHashTags
                        currsc['genre'] = currentVideoGenres
                        video.sclist.append(currsc)
                        if (int(selectedVideoViewCount) <= int(currentVideoViewCount) and (mostpopular == 0)):
                            iindex=i
                        if (mostpopular == 1):
                            iindex=i
                            break
                        if (selectedVideoTotalMatch == currentVideoTotalMatch and (mostpopular == 1) and int(selectedVideoViewCount) <= int(currentVideoViewCount)):
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
                        video.id = selectedVideoId
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
                        print ydate
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
            logger_error.exception('getsoundcloudId')
            logger_error.exception(e)
    except Exception as e:
        logger_error.exception('getsoundcloudId')
        logger_error.exception(e)
    return video,bret


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
        oldsongdetails = {}
        oldsongdetails['artist'] = video.artist
        oldsongdetails['ftArtistList'] = video.ftArtist
        oldsongdetails['connPhraseList'] = video.connectors
        oldsongdetails['songName'] = video.name
        oldsongdetails['album'] = video.album
        if(IsYoutudeApi == 1):
		    ytapi = youtubecalls(manager)
        else: 
		    ytapi = youtubedlcalls()
        
        #key = "AIzaSyBEM6ijEuRqrGREP8lxZU8XzEufEMVToO0"
        now = datetime.now()
        try:
            #get the videos
            selectedVideo = ytapi.crawlyoutube(allArtists, video.name, flag,mostpopular, oldsongdetails)
            if(selectedVideo != None):
                    bret = True
                    if(int(selectedVideo['likes']) !=0 and int(selectedVideo['dislikes'])!=0):
                        video.rating = (float(selectedVideo['likes'])*5)/(float(selectedVideo['likes'])+float(selectedVideo['dislikes']))
                    video.url = selectedVideo['Url']
                    video.match = selectedVideo['Match']
                    video.tm = selectedVideo['TotalMatch']
                    video.sm = selectedVideo['SongMatch']
                    video.am = selectedVideo['ArtistMatch']
                    video.title = selectedVideo['Title']
                    video.id = selectedVideo['VideoId']
                    if('youtubedldata' in selectedVideo):
                        video.youtubedldata = selectedVideo['youtubedldata']
                    #check if the earliest year present in the name of the song from youtube
                    if(selectedVideo['Year'] != 0):
                        video.videoYear = selectedVideo['Year']
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
                    video.published = selectedVideo['PublishedDate']
                    video.viewcount = selectedVideo['ViewCount']
                    if( IsYoutudeApi == 1):
                        m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),video.published)
                        n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),video.published)
                        ydate = m.group()+n.group()
                        dd = ydate
                    else:
                        dd = video.published
                    yy = int(str(dd)[0:4])
                    mm = int(str(dd)[4:6])
                    total = (now.year-yy)*12+(now.month-mm)
                    if total < 1:
                        total = 1
                    if(total != 0):
                        video.viewcountRate = float(video.viewcount)/total
                    video.length = selectedVideo['Duration']
                    if(now.month<10):
                        mm = '0'+str(now.month)
                    else:
                        mm = str(now.month)
                    if(now.day<10):
                        dd = '0'+str(now.day)
                    else:
                        dd = str(now.day)
                    video.crawldate = str(now.year)+"-"+mm+"-"+dd
                    
                    
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
    manager = managekeys.ManageKeys()
    manager.reset_projkeys()
    
    try:
        lastdirectory = 0
        logger_error.debug("Discogs Main Program Starting")
        directory = DiscogsDataDirectory
        m1 = NumberOfProcesses
        folders = NumberofFolders
        folders = int(folders)
        m1=int(m1)
        crawlyoutube = IsCrawlingYoutube
        crawlyoutube = int(crawlyoutube)
        incr = IsIncremental
        incr = int(incr)
        IsIncremental = incr
        prev_time = 0
        timeFile = directory + "/timelog.txt"
        if(IsIncremental == 1 or IsIncremental == 3):
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
            if(crawlyoutube == 0):
                songs_pool.imap(crawlArtist,foldlist)
            else:
                songs_pool.imap(runYoutubeApi,foldlist)
                
            songs_pool.close()
            songs_pool.join()
            print datetime.now()-t1
            logger_error.debug("completed for split : "+','.join(map(str,split)))
            fwrite.write(str(split[0]))
            fwrite.write("\n")
            if(split[-1]!= None):
                fwrite.write(str(split[-1]))
                logger_decisions.error(str(split[-1]))
                fwrite.write("\n")
        fwrite.close()
    except Exception as e:
        logger_error.exception(e)
    t2=datetime.now()
    print "time=" +str(t2-t1)
