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

def genXML(vid,avgcnt,avgcntrece,artistId):
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
        artistName = artistName[0].upper()+ artistName[1:]
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
        if (artistName == artistName):
			mysong.set_artistId(int(artistId))
        mysong.set_songName(songName)	
        mysong.set_youtubeName(vid['title'])
        vid['title'] = vid['title'].replace(',','')
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
        ar.add_artistName(artistName)
        #print artistName
        #if(artistName not in aliases):
        #	aliases.append(artistName)
        iAliaslist = api.indexedArtistAliasList()
        if(len(aliases) != 0):
			#print aliases
			ar.set_artistAlias(aliases)
			for alias in aliases:
				iAliaslist.add_indexedArtistAliasName(alias)
        if('artistalias' in vid):
            aliases = vid['artistalias']
            if(len(aliases) != 0):
                #ar.set_artistAlias(aliases)
                for alias in aliases:
                    #print alias
                    iAliaslist.add_indexedArtistAliasName(alias)
                    ar.add_artistAlias(alias)
        mysong.set_indexedArtistAliasList(iAliaslist)
        mysong.set_artist(ar)
        
        #featuring artist is a list too
        fAr = api.ftArtistList()
        fIAr = api.indexedftArtistList()
		#print ftArtistName
        for f in ftArtistName:
            f = f[0].upper()+f[1:]
            fAr.add_ftArtistName(f)
            fIAr.add_indexedftArtistName(f)
            #mysong.add_ftArtistName
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
        if(genre != None):
            #genre = genre.replace("{","")
            #genre = genre.replace("}","")
            #genre = genre.split(',')
            for g in genre:
                g = g.replace("\"","")
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
                    g.strip()
                xmlpath = "//"+str(g)
                #print "xmla oath :" +xmlpath
                genre_paths = []
                try:
                    genre_paths = doc.xpath(xmlpath)
                except Exception as ex:
                    #print ex
                    logger_genre.error(xmlpath)
                    logging.exception("Error in path for "+xmlpath)
                #print genre_paths
                if(len(genre_paths) == 0):
                    xmlpath = "//"+str(g+"_music")
                    try:
                        genre_paths = doc.xpath(xmlpath)
                    except Exception as ex:
                        #print ex
                        logger_genre.error(xmlpath)
                        logging.exception("Error in path for "+xmlpath)
                    if(len(genre_paths) == 0):
                        logger_genre.error(g)
                    else:
                        genres_levels[0].append(g+"_music")
                else:
                    genres_levels[0].append(g)
                    
                '''for gp in genre_paths:
                    sAbsolutePath = doc.getpath(gp)
                    pathList = sAbsolutePath.split('/')
                    #print pathList
                    pathlength = len(pathList)
                    for k,l in enumerate(pathList[2:]):
                        if k in genres_levels:
                            if l not in genres_levels[k]:
                                genres_levels[k].append(l)
                        else:
                            genres_levels[k] = [l]'''
        style = []
        style = vid['styles']
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
                xmlpath = "//"+str(g)
                #print "xmla styles :" +g
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
                        genres_levels[1].append(g+"_music")
                else:
                    genres_levels[1].append(g)
                '''for gp in genre_paths:
                    sAbsolutePath = doc.getpath(gp)
                    pathList = sAbsolutePath.split('/')
                    #print pathList
                    pathlength = len(pathList)
                    for k,l in enumerate(pathList[2:]):
                        if k in genres_levels:
                            if l not in genres_levels[k]:
                                genres_levels[k].append(l)
                        else:
                            genres_levels[k] = [l]'''
                
        for i in genres_levels:
			if(i == 0):
				level1Genres = api.level1Genres()

				for level1 in genres_levels[i]:
					level1 = decodexml(level1)
					level1Genres.add_genreName(level1)
				mysong.set_level1Genres(level1Genres)
				continue
			if(i == 1):
				level2Genres = api.level2Genres()

				for level2 in genres_levels[i]:
					level2 = decodexml(level2)
					level2Genres.add_genreName(level2)
				mysong.set_level2Genres(level2Genres)
				continue
			if(i == 2):
				level3Genres = api.level3Genres()

				for level3 in genres_levels[i]:
					level3 = decodexml(level3)
					level3Genres.add_genreName(level3)
				mysong.set_level3Genres(level3Genres)
				continue
			if(i == 3):
				level4Genres = api.level4Genres()

				for level4 in genres_levels[i]:
					level4 = decodexml(level4)
					level4Genres.add_genreName(level4)
				mysong.set_level4Genres(level4Genres)
				continue
			if(i == 4):
				level5Genres = api.level5Genres()

				for level5 in genres_levels[i]:
					level5 = decodexml(level5)
					level5Genres.add_genreName(level5)
				mysong.set_level5Genres(level5Genres)
				continue
			if(i == 5):
				level6Genres = api.level6Genres()

				for level6 in genres_levels[i]:
					level6 = decodexml(level6)
					level6Genres.add_genreName(level6)
				mysong.set_level6Genres(level6Genres)
				continue
			if(i == 6):
				level7Genres = api.level7Genres()

				for level7 in genres_levels[i]:
					level7 = decodexml(level7)
					level1Genres.add_genreName(level7)
				mysong.set_level7Genres(level7Genres)
				continue
			if(i == 7):
				level8Genres = api.level8Genres()

				for level8 in genres_levels[i]:
					level8 = decodexml(level8)
					level8Genres.add_genreName(level8)
				mysong.set_level8Genres(level8Genres)
				continue
			if(i == 8):
				level9Genres = api.level9Genres()

				for level9 in genres_levels[i]:
					level9 = decodexml(level9)
					level9Genres.add_genreName(level9)
				mysong.set_level9Genres(level9Genres)
        #mysong.set_artist(artistName)
        mysong.set_duration(timedelta(seconds=int(vid['length'])))
        if vid.has_key('viewcountRate'):
			mysong.set_viewcountRate(vid['viewcountRate'])
		#idVal = url[-11:]
		#dirtec = idVal[:8]
		#fileN = idVal[8:]
		#path = opdir+'/'+dirtec
        path = opdir
        #path = opdir + '/' + str(random.randint(1, 5))
        if not os.path.exists(path):
			os.mkdir(path)
        fname = path + "/0000" +url[-11:] + ".xml"
        
        if os.path.exists(fname):
            fr = codecs.open(fname,'r','utf-8')
            oldsong = api.parse(fname)
            if(url[-11:] == "kiTL5A4_zWM"):
                print 'hit'
                print mysong.overLap
                print mysong.releaseDate
                print mysong.songName
                print oldsong.overLap
                print oldsong.releaseDate
                print oldsong.songName
                print 'done'
            if(round(oldsong.totalMatch) < round(mysong.totalMatch)): 
				#print "overwriting :"
				#print oldsong.overLap
				#print oldsong.totalMatch
				print "With this :"
				#print mysong.overLap
            elif ((round(oldsong.totalMatch) == round(mysong.totalMatch)) and (round(oldsong.songMatch) < round(mysong.songMatch))):
				#print "overwriting :"
				#print oldsong.totalMatch
				#print oldsong.songMatch
				print "With this :"
				#print mysong.match
            elif ((round(oldsong.songMatch) == round(mysong.songMatch)) and (round(oldsong.artistMatch) < round(mysong.artistMatch))):
				#print "overwriting :"
				#print oldsong.totalMatch
				print "With this :"
				#print mysong.totalMatch
            elif(round(oldsong.totalMatch) == round(mysong.totalMatch) and round(oldsong.songMatch) == round(mysong.songMatch) and round(oldsong.artistMatch) == round(mysong.artistMatch)):
                #if(artistPopularityAll in oldsong):
                if(oldsong.artist.artistPopularityAll > mysong.artist.artistPopularityAll):
                        mysong = oldsong
                if(mysong.releaseDate != 1001 and oldsong.releaseDate < mysong.releaseDate):
                    print "With this :"
                    print mysong.releaseDate
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

def CalculateAverages(directory):
    averageCount = 0
    averageCountRecent = 0
    path = directory + "/dump"
    print path
    try:
		json_data = open(path)
    except IOError as e:
		print "File does not exist"
		return 0,0
    songs = json.load(json_data)
    json_data.close()
    count = 0
    ViewcountSum = 0
    ViewCountRateSum = 0
    for s in songs:
        if('viewcount' in s):
            ViewcountSum = ViewcountSum + int(s['viewcount'])
            ViewCountRateSum = ViewCountRateSum + int(s['viewcountRate'])
            count = count + 1
    if(len(songs) != 0):
		averageCount = ViewcountSum / len(songs)
		#print "Averages: "
		#print averageCount
		averageCountRecent = ViewCountRateSum / len(songs) 
		#print averagerating
    return averageCount,averageCountRecent

#Main starts here
t1=datetime.now()
#myfile = codecs.open('levels.xml','r','utf8');
#doc_str = myfile.read()
#doc_str = doc_str.encode("UTF-8")
doc = etree.parse(open('new_genres_list.xml'))
#doc = libxml2.parseFile('genres_manual.xml')
#doc = libxml2.xmlReadFile("levels.xml","utf8")
#doc = libxml2.parseDoc(doc_str)
#print "Working on this Artist Now: "
#print artistId
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
    try:
        d = dirlist
        #for d in dirlist:
        if(len(d.strip()) == 0):
			#continue
			return
        directory = d.strip()
        dindex = directory.rfind("/")
        avgcnt,avgcntrece =  CalculateAverages(directory)
        print avgcnt,avgcntrece
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
        #logger_finished.error('Completed for directory '+ str(directory))
    except Exception as e:
        logging.exception(e)

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
