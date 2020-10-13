import sys
import os
import codecs
import re
import loggingmodule
import itertools
import time
import glob
import fuzzy
import json
import pickle
from solr import SolrConnection
from solr.core import SolrException
from datetime import datetime, date, timedelta
from fuzzywuzzy import fuzz
from multiprocessing.pool import ThreadPool
import concurrent.futures
from multiprocessing import Pool
import operator
from itertools import repeat
from config import DiscogsDataDirectory, NumberOfProcesses, NumberofFolders, IsIncremental, SkipRecentlyCrawledDirectories
from config import IsSoundCloud,NumberofThreads
from songsutils import GetSize,stemwords_uniquelist




reload(sys)
sys.setdefaultencoding('utf8')
logger_decisions = loggingmodule.initialize_logger1('decisions_songs','decisions_songs.log')
logger_error = loggingmodule.initialize_logger('errors','errors_getsongs.log')


solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_artists')




def GetSongsFromFullList(songs_details):
    full_songs_list = {}
    for song in songs_details:
        keySong = GetKeyFromSong(song)
        full_songs_list[keySong] = song
    return full_songs_list

def GetSongsFromIncrList(songs_details,full_songs_list):
    for song in songs_details:
        keySong = GetKeyFromSong(song)
        if(keySong not in full_songs_list):
            full_songs_list[keySong] = song
    return full_songs_list


def checkpreviousfull(directory):
    retVal = 0
    if(os.path.exists(directory+'/last_full_part1.txt')):
        retVal = 1
    #elif(os.path.exists(directory+'/dump')):
    #    retVal = 1
    return retVal


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


def GetKeyFromSong(song):
    keySong = song['name'].lower()
    keySong = keySong + "," + song['artistName']
    if(len(song['featArtists'])!= 0):
        temp_str = ','.join(song['featArtists'])
        keySong = keySong + "," +temp_str.lower()
    KeySong = keySong.strip()
    return KeySong

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
        except Exception as e:
            return False,0
        part2_dist = 0
        if(len(slist) >1 and len(dlist) > 1):
            fuzz.ratio(slist[1].lower(),dlist[1].lower())

        if(part1_dist >= 85 and part2_dist >=85):
            return True,0
        else:
            return False,0
    except Exception as e:
        logger_error.exception(source)
        logger_error.exception(destination)
        logger_error.exception(e)
        return False,0

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
    except Exception as e:
            logger_error.exception(e)
    return final_artist_alias_list


def changeName(artName):
    artNamewords = artName.split()
    retNamewords = []
    for artword in artNamewords:
         retNamewords.append(artword[0].upper()+ artword[1:])
    return " ".join(retNamewords)


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
        
        #if(intersect > 0):
        #    print '---Found song---'
        #    return True
    except Exception as e:
        sys.exc_clear()
        #logger_error.exception(e)
    return False

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
            artist_id = artist['artist_id']
        else:
            ftArtistList.append(artName)
        if(artist['join_relation'] != None):
            connList.append(artist['join_relation'])
    return artistName,artist_id,ftArtistList,connList




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


def get_song_list_normal_new(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,final_song_list,full_songs_list):
    releases_list = []
    start_time = datetime.now()
    global prev_time
    global IsIncremental
    retVal = checkpreviousfull(directory)
    try:
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
        print len(releases_list)
        with concurrent.futures.ThreadPoolExecutor(max_workers=NumberofThreads) as executor:
            return_pool = executor.map(get_list_from_normal_release,zip(releases_list,repeat(ear_count),repeat(ear_year),repeat(ear_rel)))
        #print len(releases_list)
        
        logger_decisions.error('before merging normal  files '+str(datetime.now() - start_time))

        for result in return_pool:
            songs_list = result[0]
            final_song_list = GetUniquesongs(result[0],final_song_list,False,False,result[1],full_songs_list)
            print len(songs_list)
            print len(final_song_list)
        print directory
        
        logger_decisions.error('completed normal files '+str(datetime.now() - start_time))#directory+"------------ completed----normal"
    except Exception as e:
            logger_error.exception(e)
    return songs_list,final_song_list,full_country_list,aliases,ear_count,ear_year,ear_rel


def get_list_from_normal_release((release,ear_count,ear_year,ear_rel)):
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
            #print len(curr_album['tracks'])
            for track in curr_album['tracks']:
                if(track == None):
                    continue
                if(track['position'] == "" and track['duration'] == ""):
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
                    print 'skipping4'
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
                #if(is_songname_same_artistname(song['name'],song['artistName'])):
                #    continue
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
                if(CheckifSongsExistsinSolr(song['name'],song['artistName'],song['featArtists']) == False):
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
        except Exception as e:
            logger_error.exception(e)
        print len(songs_list)
        return songs_list,ear_count


def get_song_list_master_new(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list):
    releases_list = []
    start_time = datetime.now()
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
    final_song_list = {}
    releases_list = sorted(releases_list)
    #release_pool = Pool()
    #songs_pool =Pool(processes=15)
    #results = songs_pool.imap(get_list_from_song,releases_list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        return_pool = executor.map(get_list_from_release,zip(releases_list,repeat(ear_count),repeat(ear_year),repeat(ear_rel)))        
    #songs_pool.close()
    #songs_pool.join()
    #get_list_from_song(releases_list[0])
    combined_songs_list = []
    #release_song_list,curr_song_list,full_song_list,ear_count
    logger_decisions.error('before merging master files '+str(datetime.now() - start_time))
    #logger_decisions.error(len(list(return_pool)))
    for result in return_pool:
        combined_songs_list = combined_songs_list + result[3]
        final_song_list = GetUniquesongs(result[0],final_song_list,False,False,result[2],full_song_list)
        final_song_list = GetUniquesongs(result[1],final_song_list,False,True,result[2],full_song_list)
    logger_decisions.error('completed master files '+str(datetime.now() - start_time))
    return combined_songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict,final_song_list


def get_list_from_release((release,ear_count,ear_year,ear_rel)):
    release_song_list = []
    curr_song_list =[]
    combined_songs_list = []
    full_country_list = {}
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
                    #if(is_songname_same_artistname(song['name'],song['artistName'])):
                    #    continue
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
                    if(CheckifSongsExistsinSolr(song['name'],song['artistName'],song['featArtists']) == False):
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
    #print release_song_list
    return release_song_list,curr_song_list,ear_count,combined_songs_list



def checkIfSongExists(curr_song,songs_list):
    retVal = False
    matched_song = ""
    song_name = curr_song['name']
    phonectic_distance = 0
    try:
     for s in songs_list:
        #print song
        song = songs_list[s]['name']
        print song
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



def GetUniquesongs(songs_list,final_song_list,isMaster,same_album,ear_count,full_songs_list = {}):
    #artist_country = None
    for song in songs_list:
        keySong = song['name'].lower()
        #keySong = removeStemCharacters(keySong)
        keySong = keySong + "," + song['artistName']
        if(len(song['featArtists'])!= 0):
            temp_str = ','.join(song['featArtists'])
            keySong = keySong + "," +temp_str.lower()
        keySong.strip()
        #logger_decisions.error(keySong)
        AddedSong = False
        isPresentSong = False
        song['genres'],song['styles'] = getGenresAndStyles(song['genres'],song['styles'])
        song['masterGenres'],song['masterStyles'] = getGenresAndStyles(song['masterGenres'],song['masterStyles'])
        #print song['masterGenres'],song['masterStyles']
        if(keySong not in final_song_list):
            ''' First check in the full songlist. '''
            #isPresentSong,matchedsong = checkIfSongExists(song,full_songs_list)
            if(keySong in full_songs_list):
                print 'Skipping'
                continue
            #if(isPresentSong == True):
            #    continue
            #isPresentSong,matchedsong = checkIfSongExists(song,final_song_list)
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





def crawlArtist(directorylist):
    start_time = datetime.now()
    directory,count = directorylist
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
            infile = directory + '/songslist_incr.txt'
            try:
                fread = open(infile,'r')
                parallel_songs_list = pickle.load(fread)
                full_song_list = GetSongsFromIncrList(parallel_songs_list,full_song_list)
            except IOError as e:
                logger_error.exception(e)
        logger_decisions.error(directory + " -- Full songs list  -- "+str(len(full_song_list)))
        songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,ear_conflict,final_song_list = get_song_list_master_new(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,full_song_list)
        print ear_count
        print ear_year
        if(len(songs_list) != 0):
            master_ear_count = ear_count
            master_ear_year = ear_year
            bskipflag = 1
        songs_list,final_song_list,full_country_list,aliases,ear_count,ear_year,ear_rel = get_song_list_normal_new(directory,songs_list,full_country_list,aliases,ear_count,ear_year,ear_rel,final_song_list,full_song_list)
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
        #if(len(sorted_list) == 0):
        #    return
    except Exception as e:
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
        print 'parallel_songs_list'
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
    except Exception as e:
        logger_error.exception(e)
    logger_decisions.error(directory + " -- Completed with time -- " + str(datetime.now() - start_time))
    #print parallel_songs_list
    if(IsIncremental == 0 or IsIncremental == 2):
        with open(directory + '/getyoutubelist.txt', 'wb') as f:
			pickle.dump(parallel_songs_list, f)
    else:
        if(os.path.exists(directory + '/getyoutubelist_incr.txt')):
            os.remove(directory + '/getyoutubelist_incr.txt')
        with open(directory + '/getyoutubelist_incr.txt', 'wb') as f:
			pickle.dump(parallel_songs_list, f)
    #runYoutubeApi(directory)



if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    t1 = datetime.now()
    try:
        lastdirectory = 0
        logger_error.debug("Discogs Main Program Starting")
        directory = DiscogsDataDirectory
        m1 = NumberOfProcesses
        folders = NumberofFolders
        folders = int(folders)
        m1=int(m1)
        incr = IsIncremental
        incr = int(incr)
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
        print splitlist
        for split in splitlist:
            foldlist = {}
            foldercompletelist = {}
            folderstartedlist = {}
            logger_error.debug("Getting the Folders List")
            for dirs in split:
                if(dirs == None):
                    continue
                for curr_dir, sub_dir, filenames in os.walk(directory+'/'+str(dirs)):
                            strg = curr_dir
                            foldlist[strg] = GetSize(strg)
            sortedfolders = sorted(foldlist.iteritems(), key=lambda (k,v): (v,k),reverse = True)
            print sortedfolders
            logger_error.debug("Folders List:")
            n = len(sortedfolders)
            logger_error.debug("Starting Processes:")
            songs_pool = Pool()
            songs_pool =Pool(processes=m1)
            songs_pool.imap(crawlArtist,sortedfolders)
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
        print e
    t2=datetime.now()
    print "time=" +str(t2-t1)
