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
import operator
config = ConfigParser.ConfigParser()
reload(sys)
sys.setdefaultencoding('utf8')
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger_genre = logging.getLogger('simple_logger')
hdlr_1 = logging.FileHandler('genres.log')
hdlr_1.setFormatter(formatter)
logger_genre.addHandler(hdlr_1)
logger_genre = logging.getLogger('simple_logger')
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG, filename='errors_part2.log')
# second file logger
logger_finished = logging.getLogger('simple_logger_2')
hdlr_2 = logging.FileHandler('finishedpart2.log')    
hdlr_2.setFormatter(formatter)
logger_finished.addHandler(hdlr_2)

# 0 - 10K - 50K - 100 K - 500K - 1M - 5M - 10M - 20M - 40M - 60M - 80M - 100M - 400M - 600M 
def CalculateScale(viewcount):
	if(viewcount > 600000000):
		return 15
	if(viewcount > 400000000):
		return 14
	if(viewcount > 100000000):
		return 13
	if(viewcount > 80000000):
		return 12
	if(viewcount > 60000000):
		return 11
	if(viewcount > 40000000):
		return 10
	if(viewcount > 20000000):
		return 9
	if(viewcount > 10000000):
		return 8
	if(viewcount > 5000000):
		return 7
	if(viewcount > 1000000):
		return 6
	if(viewcount > 500000):
		return 5
	if(viewcount > 100000):
		return 4
	if(viewcount > 50000):
		return 3
	if(viewcount > 10000):
		return 2
	if(viewcount > 0):
		return 1




def encodexml(s):
    s = s.replace("&", "0000")
    s = s.replace(" ", "_")
    s = s.replace("/", "1111")
    s = s.replace("-", "2222")
    s = s.replace("\\", "3333")
    s = s.replace(".", "4444")
    s = s.replace("'", "5555")
    s = s.replace("(", "6666")
    s = s.replace(")", "7777")
    s = s.replace("!", "8888")
    s = s.replace(",","")
    return s

	
def decodexml(s):
 	s = s.replace("0000", "&")
	s = s.replace("_", " ")	   
	s = s.replace("1111", "/")
	s = s.replace("2222", "-")
	s = s.replace("3333", "\\")	   
	s = s.replace("4444", ".")
	s = s.replace("5555", "'")
	s = s.replace("6666", "(")	   
	s = s.replace("7777", ")")
	s = s.replace("8888", "!")
   	return s

def setGenre(genre):
	gr = api.genre()
	for g in genre:
		gr.add_level1Genres(g)
	return gr

def setSubgenre(sgen):
	sgr = api.subgenre()
	for g in sgen:
		sgr.add_level2Genres(g)
	return sgr

def setStyle(sty):
	st = api.styles()
	for s in sty:
		st.add_level3Genres(s)
	return st

def getMatch(strg,matchstring):
	strg = strg + ","
	ms = matchstring+":"
	n = strg.find(ms)
	m = strg.find(",",n)
	st = strg[n+len(ms):m]
	return float(st)
def changeName(artName):
    artNamewords = artName.split()
    retNamewords = []
    for artword in artNamewords:
         retNamewords.append(artword[0].upper()+ artword[1:])
    return " ".join(retNamewords)
     
    
def genXML(vid,avgcnt,avgcntrece,artistId,genreCountList):
    xmlpath = ""
    try:
        global opdir
        global doc
        artistMatch = 0.0
        percentMatch = 0.0
        albumsList = []
        yearsList = []
        aliases = []
        xmlsng = "</songs>\n"
        flist = ""
        conlist = ""
        mysong = api.songs()
        artistName = vid['artist']
        curr_artist_id = vid['artist_id']
        
        artistName.strip("-")
        ftArtistName = vid['ftArtist']
        try:
			connectorList = vid['connectors']
        except IOError as e:
			connectorList = list()
        try:
			albumsList = vid['album']
        except IOError as e:
			albumsList = list()
        try:
			yearsList = vid['year']
        except IOError as e:
			yearsList = list()
        songName = vid['name']
        albumName = vid['album']
		#print "=============================================TOTAL COUNT+++++++++++++++++++++++++++++++++++++++++++++++++++++++"
		#print totalCount
        for f in ftArtistName:
			flist = flist+" "+f
        fw = open("match2.txt",'a')
        ftartists = flist[0:]
        allArtists = artistName+" "+ftartists
        for c in connectorList:
            if(c != None):
                conlist = conlist+" "+c 
		#i = vid['url'].rfind('&')
        if('url' not in vid):
            return
        url = vid['url'].replace('https','http',1)
        #print url[-11:]
        if vid.has_key('published'):
			m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),vid['published'])
			n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),vid['published'])
			ydate = m.group()+" "+n.group()
        else:
			ydate = '0001-01-01 00:00:00'
        mysong.set_youtubeId(url[-11:])
        mysong.set_artistId(int(curr_artist_id))
        mysong.set_songName(songName)	
        mysong.set_youtubeName(vid['title'])
        vid['title'] = vid['title'].replace(',','')
        if('anv' in vid):
            print vid['anv']
        songName = songName.replace(',','')
        if(vid['title'].lower().find(songName.lower())!= -1):
			substring_song = "true"
			mysong.set_substring_song("true")
        else:
			mysong.set_substring_song("false")
			substring_song = "false"
        if(vid['title'].lower().find(artistName.lower())!= -1):
			mysong.set_substring_artist("true")
			substring_artist = "true"
        else:
			mysong.set_substring_artist("false")
			substring_artist = "false"
        yname = vid['title']
        bhiphen = False
		#Remove the unwanted words
        yname = yname.lower().replace("full version","")
        yname = yname.lower().replace("lyrics on screen","")
        yname = yname.lower().replace("official music video","")
        yname = yname.lower().replace("with lyrics","")
        yname = yname.lower().replace("full album","")
        yname = yname.lower().replace("official song","")
        
        ftArtistSet = re.findall("\w+",ftartists.lower(),re.U)
        ftAMatch = 0
        ftMatch = 0
        for artist in ftArtistSet:
			if(yname.find(artist)!= -1):
				ftAMatch = ftAMatch + 1
        if(len(ftArtistSet)!=0):
			ftMatch = ftAMatch*100/len(ftArtistSet)
        mysong.set_substring_ftartist(ftMatch);
        remove = "lyrics official video hd hq edit music lyric audio acoustic videoclip featuring ft feat"
        diffset = re.findall("\w+",remove.lower(),re.U)
        yfullset = re.findall("\w+",yname.lower(),re.U)
        ydiffset = set(yfullset) - set(diffset)
        yresultset = [o for o in yfullset if o in ydiffset]
        
        if "feat" in yresultset:
			totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower(),re.U)
        else:
			totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
        common = list(set(yresultset).intersection(set(totalset)))
        if float(len(yresultset)) !=0:
			percentMatch = len(common)*100/float(len(yresultset))
        fw.write(str(percentMatch)+'\n')
        fw.close()
        for f in ftArtistSet:
			yname.replace(f,"")
        yname = yname.lower().replace("feat.","")
        yname = yname.lower().replace("ft.","")
        yname = yname.lower().replace("featuring","")
        y1 = yname.find("-")
        y2 = yname.find(":")
        
        decision = "Incorrect"
        mysong.set_totalMatch(vid['tm'])
        mysong.set_songMatch(vid['sm'])
        mysong.set_artistMatch(vid['am'])
        mysong.set_overLap(vid['match'])
        mysong.set_decision(decision)
        mysong.set_genresCountList(genreCountList)
		###adding audio details 
        '''audioList = api.soundcloudList()
		audioDetails = api.audio()
		soundcloudDetails = vid['audio']
		aflag = 0
		if('url' in soundcloudDetails):
			aflag = 1
			audioDetails.set_soundcloudUrl(soundcloudDetails['url'])
		if('listenCount' in soundcloudDetails):
			aflag = 1			
			audioDetails.set_soundcloudViewcount(soundcloudDetails['listenCount'])
		if('likeCount' in soundcloudDetails):
			aflag = 1
			audioDetails.set_soundcloudLikes(soundcloudDetails['likeCount'])
		if('genres' in soundcloudDetails):
			aflag = 1
			audioGenres = api.soundcloudGenres()
			audioGenres.add_genreName(soundcloudDetails['genres'])
			audioDetails.set_soundcloudGenres(audioGenres)
		if(aflag == 1):
			audioList.add_audio(audioDetails)
			mysong.set_soundcloudList(audioList)'''
		#since artist is a list
        ar = api.artist()
        aliases = GetAlias(directory)
        ar.set_artistPopularityAll(avgcnt)
        ar.set_artistPopularityRecent(avgcntrece)
        artistName = changeName(artistName)
        ar.add_artistName(artistName)
        iAliaslist = api.indexedArtistAliasList()
        if(len(aliases) != 0):
			ar.set_artistAlias(aliases)
			for alias in aliases:
				iAliaslist.add_indexedArtistAliasName(alias)
        if('artistalias' in vid):
            aliases = vid['artistalias']
            if(len(aliases) != 0):
                for alias in aliases:
                    iAliaslist.add_indexedArtistAliasName(alias)
                    ar.add_artistAlias(alias)
        mysong.set_indexedArtistAliasList(iAliaslist)
        mysong.set_artist(ar)
        
        #featuring artist is a list too
        fAr = api.ftArtistList()
        fIAr = api.indexedftArtistList()
        for f in ftArtistName:
            f = changeName(f)
            fAr.add_ftArtistName(f)
            fIAr.add_indexedftArtistName(f)
        mysong.set_ftArtistList(fAr)
        mysong.set_indexedftArtistList(fIAr)
		#connector of artists is a list
        cr = api.connPhraseList()
        for c in connectorList:
            if(c != None):
                cr.add_connPhrase(c)
        mysong.set_connPhraseList(cr)
        #index artist is a list
        iar = api.indexedArtist()
        iar.add_indexedArtistName(artistName)
        mysong.set_indexedArtist(iar)
        mysong.set_url(url)
        
        albList = api.albumList()
        album = api.album()
        releaseyear = 1001
		#for x in range(len(albumsList)):
        for x in albumsList:
			curr_album = x	
			album.set_albumName(curr_album['albumname'])
			#album.set_albumReleasedate((curr_album['year']))
			if('country' in curr_album):
				album.set_country(curr_album['country'])
			if('language' in curr_album):
				album.set_language(curr_album['language'])
			if('barcode' in curr_album):
				album.set_barCode(curr_album['barcode'])
			albList.add_album(album)
			album = api.album()
        mysong.set_songLanguage(vid['language'])
		#print vid['language']
        releaseyear = str(vid['year']).split('-')[0]
        mysong.set_releaseDate(int(releaseyear))
        '''if('videoYear' in vid):
            if(int(releaseyear) != int(vid['videoYear'])):
                print 'mis-match of years'
                print releaseyear
                print vid['name']
                print vid['videoYear']
                print vid['title']
                print '----------------------------------' '''
        mysong.set_decade(int(releaseyear)/10)
        mysong.set_earliestDate(vid['year'])
        if('songcountry' in vid):
			mysong.set_songCountry(vid['songcountry'])
        else:
			mysong.set_songCountry('Unknown')
        mysong.set_albumList(albList)
        ydate = datetime.strptime(ydate,'%Y-%m-%d %H:%M:%S')
        mysong.set_youtubeDate(ydate)
        if vid.has_key('crawldate'):
			crdt = vid['crawldate']+" 00:00:00"
			crdt = datetime.strptime(crdt,'%Y-%m-%d %H:%M:%S')
			mysong.set_crawlDate(crdt)
        if vid.has_key('viewcount'):
			vc = int(vid['viewcount'])
        else:
            vc=0
        #master
        if(vid.has_key('release_Id')):
            mysong.set_releaseId(int(vid['release_Id']))
        if(vid.has_key('masterRelease')):
            mysong.set_masterRelease(int(vid['masterRelease']))
        if(vid.has_key('isCompilation')):
            #print vid['isCompilation']
            #print vid['url']
            mysong.set_isCompilation(vid['isCompilation'])
        mysong.set_viewcount(vc)
        mysong.set_viewCountGroup(CalculateScale(vc))
        if vid.has_key('rating'):
			rating = vid['rating']
        else:
			rating = 0.0
        mysong.set_rating(rating)
        genres_levels = {0:[],1:[]}
        genre = vid['genres']
        #print genre
        curr_genres_list = []
        if(genre != None):
            #genre = genre.replace("{","")
            #genre = genre.replace("}","")
            #genre = genre.split(',')
            for g in genre:
                '''g = g.replace("\"","")
                if(g != "Folk, World, & Country"):
                    g = g.replace(" ","_")
                else:
                    g = "Folk_World_Country"
                g = g.lower()
                g = g[0].upper()+g[1:]
                if(g == 'Rock_&_roll'):
                    g = 'Rock_and_roll'
                else:
                    g = g.replace('&','and')
                    g.strip() '''
                g = g.replace("\"","")
                g = g.lower()
                g = g[0].upper()+g[1:]
                if(g.strip() == ""):
                    continue
                g = encodexml(g)
                xmlpath = "//"+str(g)
                genre_paths = []
                try:
                    genre_paths = doc.xpath(xmlpath)
                except Exception as ex:
                    logger_genre.error(xmlpath)
                    logging.exception("Error in path for "+xmlpath)
                if(len(genre_paths) == 0):
                    xmlpath = "//"+str(g+"_music")
                    try:
                        genre_paths = doc.xpath(xmlpath)
                    except Exception as ex:
                        logger_genre.error(xmlpath)
                        logging.exception("Error in path for "+xmlpath)
                    if(len(genre_paths) == 0):
                        logger_genre.error(g)
                    else:
                        curr_genres_list.append(g+"_music")
                else:
                    curr_genres_list.append(g)
                #print genre_paths    
                for gp in genre_paths:
                    sAbsolutePath = doc.getpath(gp)
                    pathList = sAbsolutePath.split('/')
                    pathlength = len(pathList)
                    for k,l in enumerate(pathList[2:]):
                        if k in genres_levels:
                            if l not in genres_levels[k]:
                                genres_levels[k].append(l)
                        else:
                            genres_levels[k] = [l]
                
        style = vid['styles']
        #print style
        Curr_Genres_List =[]
        if(style != None):
            #style = style.replace("{","")
            #style = style.replace("}","")
            #style = style.split(',')
            for g in style:
                g = g.replace("\"","")
                g = g.replace(" ","_")
                g = g.lower()
                g = g[0].upper()+g[1:]
                if(g == 'Rock_&_roll'):
                    g = 'Rock_and_roll'
                else:
                    g = g.replace('&','and')
                    g.strip()
                if(g.strip() == ""):
                    continue
                '''g = g.replace("\"","")
                g = g.lower()
                g = g[0].upper()+g[1:]'''
                g = encodexml(g)
                xmlpath = "//"+str(g)
                #print "xmla styles :" +g
                genre_paths = []
                try:
                    #print xmlpath
                    genre_paths = doc.xpath(xmlpath)
                except Exception as ex:
                    logger_genre.error(xmlpath)
                    logging.exception("Error in path for "+xmlpath)
                if(len(genre_paths) == 0):
                    xmlpath = "//"+str(g+"_music")
                    try:
                        genre_paths = doc.xpath(xmlpath)
                    except Exception as ex:
                        logger_genre.error(xmlpath)
                        logging.exception("Error in path for "+xmlpath)
                    '''if(len(genre_paths) == 0):
                        logger_genre.error(g)
                    else:
                        genres_levels[1].append(g+"_music")'''
                '''else:
                    print 'notfound'+g'''
                for gp in genre_paths:
                    sAbsolutePath = doc.getpath(gp)
                    pathList = sAbsolutePath.split('/')
                    pathlength = len(pathList)
                    found = 0
                    #print gp
                    #added change for discogs genres. only adding the genres from second level.
                    for k,l in enumerate(pathList[3:]):
                        if k+1 in genres_levels:
                            if l not in genres_levels[k+1]:
                                genres_levels[k+1].append(l)
                        else:
                            genres_levels[k+1] = [l]
                    '''if(found == 1):
                        k = len(pathList) - 3
                        print k
                        l = pathList[len(pathList)-1]
                        print l
                        if k in genres_levels:
                            if l not in genres_levels[k]:
                                genres_levels[k].append(l)
                        else:
                            genres_levels[k] = [l]'''
        #print genres_levels
        masterGenres = api.masterGenres()
        masterStyles = api.masterStyles()
        genreMatch = []
        if('masterGenres' in vid and vid['masterGenres'] != None):
            g = ','.join(vid['masterGenres'])
            g = g.lower()
            masterGenres.add_genreName(g)
        if('masterStyles' in vid and vid['masterStyles'] != None):
            g = ','.join(vid['masterStyles'])
            g = g.lower()
            masterStyles.add_genreName(g)
        mysong.set_masterGenres(masterGenres)
        mysong.set_masterStyles(masterStyles)
        #print genres_levels
        genre_tags = []
        for i in genres_levels:
            if(i == 0):
                level1Genres = api.level1Genres()
                for level1 in genres_levels[i]:
                    level1 = decodexml(level1)
                    genreMatch.append(level1.lower())
                    genre_tags.append(level1.lower())
                    level1Genres.add_genreName(level1)
                mysong.set_level1Genres(level1Genres)
                continue
            if(i == 1):
                level2Genres = api.level2Genres()
                for level2 in genres_levels[i]:
                    level2 = decodexml(level2)
                    genreMatch.append(level2.lower())
                    genre_tags.append(level2.lower())
                    level2Genres.add_genreName(level2)
                mysong.set_level2Genres(level2Genres)
                continue
            if(i == 2):
                level3Genres = api.level3Genres()
                for level3 in genres_levels[i]:
		    level3 = decodexml(level3)
                    genreMatch.append(level3.lower())
                    genre_tags.append(level3.lower())
		    level3Genres.add_genreName(level3)
                mysong.set_level3Genres(level3Genres)
                continue
            if(i >= 3):
                level4Genres = api.level4Genres()
                for level4 in genres_levels[i]:
                    level4 = decodexml(level4)
                    genreMatch.append(level4.lower())
                    genre_tags.append(level4.lower())
                    level4Genres.add_genreName(level4)
                mysong.set_level4Genres(level4Genres)
                continue
        genre_tags = sorted(genre_tags)
        combinedgenrestring = '@'.join(genre_tags)
        combinedgenrestring = combinedgenrestring.lower()
        mysong.set_genreTag(combinedgenrestring)
        genreMatch = sorted(genreMatch)
        combinedgenrestring = ' '.join(genreMatch)
        for ch in [' ','/','&','.','-','\\',"'",'(',')','!']:
            combinedgenrestring = combinedgenrestring.replace(ch,'')
        mysong.set_genreMatch(combinedgenrestring)
        mysong.set_duration(timedelta(seconds=int(vid['length'])))
        if vid.has_key('viewcountRate'):
			mysong.set_viewcountRate(vid['viewcountRate'])
        path = opdir
        if not os.path.exists(path):
			os.mkdir(path)
        fname = path + "/0000" +url[-11:] + ".xml"
        
        if os.path.exists(fname):
            fr = codecs.open(fname,'r','utf-8')
            oldsong = api.parse(fname)
            '''if(url[-11:].lower() == "aVIA1n5ng4Y".lower()):
                print 'hit'
                print mysong.overLap
                print mysong.releaseDate
                print mysong.songName
                print mysong.artist.artistPopularityAll
                print oldsong.overLap
                print oldsong.releaseDate
                print oldsong.songName
                print oldsong.artist.artistPopularityAll
                print 'done'
            '''
            if(oldsong.isCompilation == True and mysong.isCompilation == False):
				print "With this :"
				mysong = CombineAlbums(oldsong,mysong)
            elif(round(oldsong.totalMatch) < round(mysong.totalMatch)):
				#print "overwriting :"
				#print oldsong.overLap
				#print oldsong.totalMatch
				print "With this :"
				mysong = CombineAlbums(oldsong,mysong)
				#print mysong.overLap
            elif ((round(oldsong.totalMatch) == round(mysong.totalMatch)) and (round(oldsong.songMatch) < round(mysong.songMatch))):
				#print "overwriting :"
				#print oldsong.totalMatch
				#print oldsong.songMatch
				print "With this :"
				mysong = CombineAlbums(oldsong,mysong)
				#print mysong.match
            elif ((round(oldsong.songMatch) == round(mysong.songMatch)) and (round(oldsong.artistMatch) < round(mysong.artistMatch))):
				#print "overwriting :"
				#print oldsong.totalMatch
				print "With this :"
				mysong = CombineAlbums(oldsong,mysong)
				#print mysong.totalMatch
            elif(round(oldsong.totalMatch) == round(mysong.totalMatch) and round(oldsong.songMatch) == round(mysong.songMatch) and round(oldsong.artistMatch) == round(mysong.artistMatch)):
                #if(artistPopularityAll in oldsong):
                #if(oldsong.artist.artistPopularityAll > mysong.artist.artistPopularityAll):
                #        mysong = oldsong
                
                if(mysong.releaseDate != 1001 and int(oldsong.releaseDate) > int(mysong.releaseDate)):
					print "With this :"
					mysong = CombineAlbums(oldsong,mysong)  
                else:
                    mysong = oldsong
                #print oldsong.totalMatch
                #print "With this :"
                #print mysong.totalMatch
            else:
                mysong = oldsong
        fx = codecs.open(fname,"w","utf-8")
        fx.write('<?xml version="1.0" ?>\n')
        #mysong.export(fx,0) Apostolos changed 0 to 1
        mysong.export(fx,0)
        fx.close()
        fh = codecs.open(fname, "rb","utf-8")
        for line in fh:
            pass
        last = line
        if(xmlsng != last):
            if os.path.exists(fname):
	    	os.remove(fname)
	    if not os.path.exists(failedxmls):
		os.mkdir(failedxmls)
	    fname = failedxmls + "/0000" +url[-11:] + ".xml"
	    fx = codecs.open(fname,"w","utf-8")
	    fx.write('<?xml version="1.0" ?>\n')
	    #mysong.export(fx,0) Apostolos Changed 0 to 1
	    mysong.export(fx,0)   
	    fx.write(artistId) 
	    fx.close()
    except Exception as ex:
        logging.exception(ex)



def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)
def write1(self,filename):
	#print "filename" +filename
	fw=codecs.open(filename,"a","utf-8") 
	#print "Self in write1" +self
	fw.write(self+"\n")
	fw.close()
	#with codecs.open(filename,"a","utf-8") as output:
		#json.dump(self+"\n",output)
def xmlifPresent(vid,avgcnt,avgcntrece):
	genXML(vid,avgcnt,avgcntrece)


def load(filename):
	with codecs.open(filename,"r","utf-8") as input:
		vids = json.load(input)
	return vids

def GetAlias(directory):
	#global aliases
	aliases = []

	if(os.path.exists(directory+'/alias.txt') == False):
		#print "alias not present"
		return []
	fread = codecs.open(directory+'/alias.txt','r','utf-8')
	lines = fread.readlines()
	for l in lines:
		aliases.append(l.strip())
	return aliases

def CombineAlbums(oldsong,mysong):
	print 'CombineAlbums'
	print oldsong.artistId
	destList = mysong.albumList
	sourceList = oldsong.albumList.album
	#print sourceList
	#print len(sourceList)
	#albList = api.albumList()
	#album = api.album()
	try:
            for album in sourceList:
			destList.add_album(album)
			print album.albumName
			print album.country
			print album.language
			print album.barCode
			print '-----'
            mysong.set_albumList(destList)
	except Exception as e:
		print e
	return mysong
	#print len(mysong.albumList.album)
		

def CalculateAverages(directory,topnsongs):
    averageCount = 0
    averageCountRecent = 0
    earlier_year = 10000
    path = directory + "/dump"
    print path
    dindex = directory.rfind("/")
    artistId = directory[dindex+1:]
    viewcountlist = []
    try:
		json_data = codecs.open(path)
    except IOError as e:
		print "File does not exist"
		return 0,0
    songs = json.load(json_data)
    json_data.close()
    count = 0
    ViewcountSum = 0
    ViewCountRateSum = 0
    for s in songs:
        if('isCompilation' in s and s['isCompilation']== True):
            continue
	releaseyear = 2050
        if('year' in s):
            releaseyear = int(str(s['year']).split('-')[0])
        if(earlier_year > releaseyear and releaseyear > 1900):
		earlier_year = releaseyear
        if('viewcount' in s and artistId == s['artist_id']):
            ViewcountSum = ViewcountSum + int(s['viewcount'])
            viewcountlist.append(int(s['viewcount']))
            ViewCountRateSum = ViewCountRateSum + int(s['viewcountRate'])
            count = count + 1
    #check artist id
    if(len(songs) != 0):
		averageCount = ViewcountSum / len(songs)
		#print "Averages: "
		#print averageCount
		averageCountRecent = ViewCountRateSum / len(songs) 
		#print averagerating
    #viewcountlist = filter(lambda x:x>averageCount,viewcountlist)
    viewcountlist = sorted(viewcountlist,reverse = True)
    viewcountlist = viewcountlist[0:topnsongs]
    if(topnsongs >0 ):
        averageCount = sum(viewcountlist)/topnsongs
    print averageCount
    return averageCount,averageCountRecent,earlier_year

def CalculateIdsforGenres(node,currentGenre):
    #genreMat[decodexml(node.tag.lower())] = currentGenre
    current_list = []
    for child in node.iterchildren("*"):
        current_list.append(decodexml(child.tag.lower()))
    current_list.sort()
    #print current_list
    if(current_list == None):
        return
    for child in current_list:
        currentGenre = currentGenre+1
        genreMat[child] = currentGenre
    #print genreMat
    for child in node.iterchildren("*"):
        CalculateIdsforGenres(child,currentGenre)

def CalculateIds():
	doc1 = etree.parse(codecs.open('Rock.xml'))
	currentGenre = 0
	children = doc1.getroot().iterchildren("*")
	#print children
	for child in doc1.getroot().iterchildren("*"):
		genreMat[decodexml(child.tag.lower())] = currentGenre
		CalculateIdsforGenres(child,currentGenre)
		currentGenre = currentGenre + 100

def GetTotalGenresCountForArtist(vids,artistId,directory):
    genreList = api.genresCountList()
    genrecount = {}
    stylecount = {}
    TotalGenreStyleCount = 0
    CalculateIds()
    parseMat = list()
    #print genreMat
    with codecs.open(directory+'/result.json', 'w','utf8') as fp:
        json.dump(genreMat, fp)
	artists_count = {}
        artists_id_mapping = {}
    for v in vids:
        genre = v['genres']
        curr_artist = v['artist'].lower()
        curr_artist_id = v['artist_id']
        #possible bug for feature artists. we need to test for featured artists.
        if(curr_artist_id != artistId):
            print curr_artist_id
            print artistId
            print '----------------'
            continue
        if(curr_artist not in artists_count):
            artists_count[curr_artist] = 1
            artists_id_mapping[curr_artist] = curr_artist_id
        else:
            artists_count[curr_artist] = artists_count[curr_artist] + 1
        if(genre != None):
            for g in genre:
                g = g.replace("\"","")
                g = g.lower()
                if(g.strip() == ""):
                    continue
                TotalGenreStyleCount = TotalGenreStyleCount+1
                if(g not in genrecount):
                    genrecount[g] = 1
                else:
                    genrecount[g] = genrecount[g] + 1
        style = v['styles']
        if(style != None):
            for g in style:
                g = g.replace("\"","")
                g = g.lower()
                if(g.strip() == ""):
                    continue
                TotalGenreStyleCount = TotalGenreStyleCount + 1
                if(g not in stylecount):
                    stylecount[g] = 1
                else:
                    stylecount[g] = stylecount[g] + 1
    #print stylecount
    #print genrecount
    #print TotalGenreStyleCount
    #print artists_count
    sorted_x = sorted(artists_count.items(), key=operator.itemgetter(1),reverse = True)
    print 'here'
    print sorted_x
    if(len(sorted_x) == 0):
	artistName = 'Unknown'
    else:	
        artistName = sorted_x[0][0]
    for genre in genrecount:
        g = api.genresCount()
        g.set_Genre(genre)
        g.set_Count(genrecount[genre])
        curr_parseVal = list()
        percent = 0
        if(TotalGenreStyleCount != 0):
            percent = (genrecount[genre]*100.0)/TotalGenreStyleCount
            g.set_Percentage(percent)
        genreList.add_genresCount(g)
        curr_parseVal.append(changeName(artistName))
        curr_parseVal.append(artistId)
        if(genre in genreMat):
            curr_parseVal.append(genre)
            curr_parseVal.append(genreMat[genre])
        else:
            logger_genre.error(genre)
            continue
        curr_parseVal.append(genrecount[genre])
        print genrecount[genre]
        #curr_parseVal.append(percent)
        parseMat.append(curr_parseVal)
    for style in stylecount:
        g = api.genresCount()
        g.set_Genre(style)
        g.set_Count(stylecount[style])
        percent = 0
        curr_parseVal = list()
        if(TotalGenreStyleCount != 0):
            percent = (stylecount[style]*100.0)/TotalGenreStyleCount
            g.set_Percentage(percent)
        genreList.add_genresCount(g)
        curr_parseVal.append(changeName(artistName))
        curr_parseVal.append(artistId)
        if(style in genreMat):
            curr_parseVal.append(style)
            curr_parseVal.append(genreMat[style])
        else:
            logger_genre.error(style)
            continue
        #curr_parseVal.append(percent)
        curr_parseVal.append(stylecount[style])
        parseMat.append(curr_parseVal)
    return genreList,parseMat

#Main starts here
t1=datetime.now()
#myfile = codecs.open('levels.xml','r','utf8');
#doc_str = myfile.read()
#doc_str = doc_str.encode("UTF-8")
#doc = etree.parse(open('discogs_genres.xml'))
#hparser = etree.HTMLParser(encoding='utf-8')
#htree   = etree.parse(fname, hparser)
doc = etree.parse(codecs.open('Rock.xml'))
#doc = libxml2.parseFile('genres_manual.xml')
#doc = libxml2.xmlReadFile("levels.xml","utf8")
#doc = libxml2.parseDoc(doc_str)
#print "Working on this Artist Now: "
#print artistId
genreMat = {}
genre = list()
style = list()
subgenlist = list()
stylelist = list()
level1Gen = list()
slist = list()
lev1 = list()
config.read('test.ini')
try:
	opdir = config.get('Paths','opdir')
except:
	opdir = "solr_newData11"
try:
	errordir = config.get('Paths','errordir')
except:
	errordir="ERRORS"
try:
	failedxmls = config.get('Paths','failedxmls')
except:
	failedxmls = "failedxml"
#errordir="/Volumes/Secondone/vina/ERRORS"
##failedxmls = "/Volumes/Secondone/vina/solr_newData7/failedxml"
#opdir = "/aurora.cs/local2/apo/solr_newData_output6"
#errordir="/aurora.cs/local2/aposolr_newData_output6/ERRORS"
#failedxmls = "/aurora.cs/local2/apo/solr_newData_output6/failedxml"
#opdir = "solr_newData8"
#errordir="solr_newData8/ERRORS"
#failedxmls = "solr_newData8/failedxml"


#opdir = "/home/navneet/Desktop/june1/mydata"
#failedxmls = "/home/navneet/Desktop/june1/mydata"
#opdir = "solr_newData4"
#failedxmls = "solr_newData4/failedxml"
#opdir = "/home/anudeep/Desktop/newData"
#failedxmls = "/home/anudeep/Desktop/newData/failedxml"
genres_levels = {}
if(os.path.exists(opdir) == False):
	os.mkdir(opdir)
if(os.path.exists(failedxmls) == False):
	os.mkdir(failedxmls)
if(os.path.exists(errordir) == False):
	os.mkdir(errordir)
path = opdir+"/failedurls.txt"
fname = path
fx = codecs.open(fname,"w","utf-8")
fx.write("")
fx.close()
#print "testing"

def generatexmls(dirlist):
    global IsIncremental
    global topnsongs
    try:
        d = dirlist
        #for d in dirlist:
        if(len(d.strip()) == 0):
			#continue
			return
        directory = d.strip()
        dindex = directory.rfind("/")
        avgcnt,avgcntrece,earlier_year =  CalculateAverages(directory,topnsongs)
        print avgcnt,avgcntrece,earlier_year
        vids = []
        try:
            if(IsIncremental == 0):
                vids = load(directory+'/dump')
            else:
                vids = load(directory+'/dump_incr')
        except Exception as e:
			#continue
			return
        artistId = directory[dindex+1:]
        genreCountList,parseMat = GetTotalGenresCountForArtist(vids,artistId,directory)
        #print genreCountList
        #print parseMat
        if(IsIncremental == 1):
                f = codecs.open(directory+'/matrix.txt','a','utf8')
        else:
                f = codecs.open(directory+'/matrix.txt','w','utf8')
        for i in parseMat:
		i.append(avgcnt)
		i.append(earlier_year)
		f.write(':;'.join(str(t) for t in i))
		f.write('\n')
        f.close()
        for v in vids:
            t3=datetime.now()
            if(int(artistId) == 194):
                artistId = v['artist_id']
            genXML(v,avgcnt,avgcntrece,artistId,genreCountList)
        #logger_finished.error('Completed for directory '+ str(directory))
    except Exception as e:
        logging.exception(e)


#main starts here

if __name__ == '__main__':
    t2=datetime.now()
    directory = raw_input("Enter directory: ")
    m = raw_input("Enter m: ")
    m=int(m)
    IsIncremental = raw_input("Isincremental : ")
    IsIncremental = int(IsIncremental)
    topnsongs = raw_input("Enter n for top songs: ")
    topnsongs = int(topnsongs)

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
				#print os.path.join(curr_dir,sd)
				f = re.search(r'[0-9]+',str(sd),0)
				if not f:
					continue
				strg = os.path.join(curr_dir,sd)
				foldlist.append(strg)
    #generatexmls(foldlist)
    try:
        p =Pool(processes=int(m))
        p.map(generatexmls,foldlist)
        p.close()
        p.join()
    except Exception as e:
        logging.exception(e)
    t2=datetime.now()

    print "time=" +str(t2-t1)
