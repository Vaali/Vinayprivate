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
import libxml2
from datetime import datetime, date, timedelta
import songs_api as api
reload(sys)
sys.setdefaultencoding('utf8')
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
"""
def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
           key = key.encode('utf-8')
        if isinstance(value, unicode):
           value = value.encode('utf-8')
        elif isinstance(value, list):
           value = _decode_list(value)
        elif isinstance(value, dict):
           value = _decode_dict(value)
        rv[key] = value
    return rv
"""

def genXML(vid,avgcnt,avgcntrece):
	global genre
	global stylelist
	global opdir
	global subgenlist
	global totalCount
	global hitCount
	global failCount
	global absoluteCount
	global flist
	global songMatch 
	global artistMatch 
	global tempArMatch
	global leftMatch 
	global rightMatch
	global percentMatch
	global albumsList
	global yearsList
	xmlsng = "</songs>\n"
	flist = ""
	conlist = ""
	mysong = api.songs()
	artistName = vid['artist']
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
	#print (line)
	albumName = vid['album']
	totalCount = totalCount + 1
	#print "=============================================TOTAL COUNT+++++++++++++++++++++++++++++++++++++++++++++++++++++++"
	#print totalCount
	for f in ftArtistName:
		flist = flist+" "+f
	fw = open("match2.txt",'a')

	ftartists = flist[0:]
	allArtists = artistName+" "+ftartists
	for c in connectorList:
		conlist = conlist+" "+c
	i = vid['url'].rfind('&')				
	url = vid['url'][:i].replace('https','http',1)
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

	'''if((y1 != -1) or (y2 != -1)):
		bhiphen = True
		if(y1 != -1):
			aname = yname[0:y1]
			sname = yname[y1+1:]
		else:
			aname = yname[0:y2]
			sname = yname[y2+1:]
		aname.strip()
		sname.strip()
		snameset1 = set(re.findall("\w+",sname.lower(),re.U))-set(diffset)
		snameset = set(snameset1)
		sreadset = re.findall("\w+",songName.lower(),re.U)
		common1 = list(set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		#if "feat" in anameset:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower(),re.U)
		#else:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = list(set(anameset).intersection(set(areadset)))
		if float(len(areadset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
		tempArMatch = artistMatch
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = list(set(leftset).intersection(set(arnameset)))
		rightIntersection = list(set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset))
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
		mysong.set_overLap(vid['match'])
		#if (leftMatch == 100.0):
		#	mysong.set_decision("Correct")
	if(((y1 != -1) or (y2 != -1)) and leftMatch != 100.0): # right match if left match is zero.
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
		snameset1 = set(re.findall("\w+",sname.lower(),re.U))-set(diffset)
		snameset = set(snameset1)-set(ftArtistSet)
		sreadset = re.findall("\w+",songName.lower(),re.U)	
		common1 = list(set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = list(set(anameset).intersection(set(areadset)))
		if float(len(anameset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = list(set(leftset).intersection(set(arnameset)))
		rightIntersection = list(set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset))
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		#print			
		#if artistMatch > tempArMatch:
		match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
		#else:
		#	artistMatch = tempArMatch
		#	match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)			
		mysong.set_overLap(vid['match'])
	if((y1 == -1) and (y2 == -1)):	
		#print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = list(set(leftset).intersection(set(arnameset)))
		rightIntersection = list(set(rightset).intersection(set(arnameset)))
		if float(len(arnameset))  !=0:
			leftMatch = len(leftIntersection)*100/float(len(arnameset)) 
			rightMatch = len(rightIntersection)*100/float(len(arnameset))
		#songreadset = re.findall("\w+",songName.lower(),re.U)		
		#common_set = list(set(yresultset).intersection(set(songreadset)))
		#yresultset = list(set(yresultset) - set(arnameset))
		#if float(len(yresultset)) !=0:
		#	songMatch = len(common_set)*100/float(len(yresultset))
		if(leftMatch > rightMatch):
			songreadset = re.findall("\w+",songName.lower(),re.U)		
			common_set = list(set(yresultset[-len(songreadset):]).intersection(set(songreadset)))
			yresultset = list(set(yresultset) - set(arnameset))
			if float(len(yresultset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
		else:
			songreadset = re.findall("\w+",songName.lower(),re.U)		
			common_set = list(set(yresultset[:len(songreadset)]).intersection(set(songreadset)))
			yresultset = list(set(yresultset) - set(arnameset))
			if float(len(yresultset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)	
		mysong.set_overLap(vid['match'])
		#print arnameset
		#print leftset
		#print rightset
	#rules'''
	decision = "Incorrect"
	mysong.set_totalMatch(vid['tm'])
	mysong.set_songMatch(vid['sm'])
	mysong.set_artistMatch(vid['am'])
	mysong.set_overLap(vid['match'])
	'''
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
	elif(substring_artist == "true" and substring_song == "true" and (len(songName.strip().split()) > 1 or len(artistName.strip().split()) > 1) and (percentMatch > 80.0 or (leftMatch == 100.0 and bhiphen))):
		decision = "correct"
	#no hiphen , song match shd be 100 and left or right should be 100 
	elif(substring_artist == "true" and substring_song == "true" and not bhiphen and songMatch == 100.0 and (leftMatch == 100.0 or rightMatch == 100.0) and percentMatch > 60.0):
		decision = "correct"

	if(bhiphen == "true" and (songMatch == 0  or (leftMatch == 0.0 and rightMatch == 0.0))):
		decision = "Incorrect" '''

	mysong.set_decision(decision)	
	#print "songMatch: "
	#print songMatch
	#print songName
	#print yname
	#since artist is a list
	ar = api.artist()
	
	ar.set_artistPopularityAll(avgcnt)
	ar.set_artistPopularityRecent(avgcntrece)
	if(artistName not in aliases):
		aliases.append(artistName)

	ar.add_artistName(artistNameFromArtist)
	if(len(aliases) != 0):
		ar.set_artistAlias(aliases)

	mysong.set_artist(ar)

	#featuring artist is a list too
	fAr = api.ftArtistList()
	#print ftArtistName
	for f in ftArtistName:
		fAr.add_ftArtistName(f)
		#mysong.add_ftArtistName(f)
	mysong.set_ftArtistList(fAr)
	#connector of artists is a list
	cr = api.connPhraseList()
	for c in connectorList:
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
	releaseyear = vid['year'].split('-')[0]
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
	#except:
	#	pass
	gr = api.genre()
	genrenew = list()
	for g in genre:
		g = decodexml(g)
		genrenew.append(g)
		
	genrenew = list(set(genrenew))
	#print genrenew
	for g in genrenew:
		gr.add_level1Genres(g)
	mysong.set_genre(gr)
	sGr = api.subgenre()

	subgenlistnew = list()
	for g in subgenlist:
		g = decodexml(g)
		subgenlistnew.append(g)
		
	subgenlistnew = list(set(subgenlistnew))
	for sg in subgenlistnew:
		sGr.add_level2Genres(sg)
	mysong.set_subgenre(sGr)
	sty = api.styles()
	stylelistnew = list()
	for g in stylelist:
		g = decodexml(g)
		stylelistnew.append(g)

	stylelistnew = list(set(stylelistnew))
	for s in stylelistnew:
		sty.add_level3Genres(s)
	mysong.set_styles(sty)
	#mysong.set_artist(artistName)
	mysong.set_duration(timedelta(seconds=int(vid['length'])))
	if vid.has_key('viewcountRate'):
		mysong.set_viewcountRate(vid['viewcountRate'])
	idVal = url[-11:]
	dirtec = idVal[:8]
	fileN = idVal[8:]
	#path = opdir+'/'+dirtec
	path = opdir
	if not os.path.exists(path):
		os.mkdir(path)
	fname = path + "/0000" +url[-11:] + ".xml"
	if os.path.exists(fname):
		fr = codecs.open(fname,'r','utf-8')
		oldsong = api.parse(fname)
		if(round(oldsong.totalMatch) <= round(mysong.totalMatch)): 
			print "overwriting :"
			print oldsong.overLap
			print oldsong.totalMatch
			print "With this :"
			print mysong.overLap
			#print mysong.songName
			#print mysong.artist.artistName
			print mysong.totalMatch
			print fname
			#mysong = oldsong
			print mysong.overLap
		elif ((round(oldsong.totalMatch) == round(mysong.totalMatch)) and (round(oldsong.songMatch) <= round(mysong.songMatch))):
			print "overwriting :"
			print oldsong.overLap
			print oldsong.songMatch
			print "With this :"
			print mysong.overLap
			#print mysong.songName
			#print mysong.artist.artistName
			print mysong.songMatch
			#mysong = oldsong
			print fname
			print mysong.overLap
		elif ((round(oldsong.songMatch) == round(mysong.songMatch)) and (round(oldsong.artistMatch) <= round(mysong.artistMatch))):
			print "overwriting :"
			#print oldsong.overLap
			print oldsong.artistMatch
			print "With this :"
			#print mysong.overLap
			#print mysong.songName
			#print mysong.artist.artistName
			print mysong.artistMatch
			#mysong = oldsong
			print fname
			#print mysong.overLap
		elif(round(oldsong.totalMatch) == round(mysong.totalMatch) and round(oldsong.songMatch) == round(mysong.songMatch) and round(oldsong.artistMatch) == round(mysong.artistMatch)):
			if(oldsong.releaseDate < mysong.releaseDate):
				mysong.releaseDate = oldsong.releaseDate
			print "overwriting :"
			print oldsong.overLap
			print oldsong.artistMatch
			print "With this :"
			print mysong.overLap
			#print mysong.songName
			#print mysong.artist.artistName
			print mysong.artistMatch
			#mysong = oldsong
			print fname
			#print mysong.overLap

		else:
			#print "testing"
			#print mysong.overLap
			#print oldsong.overLap
			#print fname
			mysong = oldsong

	fx = codecs.open(fname,"w","utf-8")
	fx.write('<?xml version="1.0" ?>\n')
	#absoluteCount = absoluteCount + 1
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


'''def updateartist(vid):
	global artistName
	global indexedArtist
	global genre
	global subgenlist
	global stylelist
	global songMatch 
	global artistMatch 
	global tempArMatch
	global leftMatch 
	global rightMatch
	global percentMatch
	artist = api.artist()
	oldartistNames = ""
	artistName = vid['artist']
	ftArtistName = vid['ftArtist']
	connectorList = vid['connectors']	
	songName = vid['name']
	albumName = vid['album']
	#vid = getYouTubeDetails(artistName,ftArtistName,songName)
	i = vid['url'].rfind('&')
	url = vid['url'][:i].replace('https','http',1)
	#print "reached update"
	fname = opdir + "/0000" +url[-11:] + ".xml"
	#print fname
	flag = 0
	mysong = api.parse(fname)
	#print mysong
	artist = mysong.get_artist()
	indexedartist = mysong.get_indexedArtist()
	arlist = artist.get_artistName()
	#print str(len(arlist))
	for ar in arlist:
		ar = ar.strip()
		if ar == artistName:
			ar = artistName #doing nothin actually
			flag = 1
			#return
		oldartistNames = oldartistNames + ar + " "
	if len(arlist) > 1:
		flag = 0
	#oldName = oldartistNames+ mysong.get_songName()
	#Apostolos Change 9/8/2012
    #ArtistId update LOGIC is defined here
	if mysong.get_songName() is None:
		oldName = oldartistNames
	else:
		oldName = oldartistNames + mysong.get_songName()
	newName = artistName + " " + songName
	#print oldName +" - " +newName
	wcOldName = oldName.count(' ') + 1
	wcNewName = newName.count(' ') + 1
	#get viewcount
	if vid.has_key('viewcount'):
		vc = int(vid['viewcount'])
        else:
		vc=0
	oldvc = mysong.get_viewcount()
	#Apostolos: changed != to >
        #if vc != oldvc: 
	if vc > oldvc:
		return 1
	#get release date
	#NEW GET RELEASE DATE ADDED ON 05/26/2013
	try:
		yearsList = vid['year']
	except IOError as e:
		yearsList = list()

	ryear = 1001
	for x in range(len(yearsList)):	
		curryear = int(yearsList[x])
		if(curryear != 1001):
			if(ryear == 1001):
				ryear = curryear
			elif(curryear < ryear):
				ryear = curryear

	#ADDITION OF NEW RELASE DATE LOGIC ENDS HERE
	#COMMENTING THE FOLLOWING ryear :( to achieve the above mentioned functionality 	
	#ryear = int(vid['year'])
	oldyear = mysong.get_releaseDate()
	#print oldyear, ryear
	if flag == 1 and oldyear < ryear:
		#print "return 1"
		return 1
	elif flag ==1 and oldyear > ryear:
		return 0 
	addflag = 0
	youtitle = vid['title']
	youtitle = youtitle.lower()
	overlap = mysong.get_overLap()
	amp = getMatch(overlap,"am")
	smp = getMatch(overlap,"sm")
	lmp = getMatch(overlap,"lam")
	rmp = getMatch(overlap,"ram")
	tmp = getMatch(overlap,"tm")


	if  not (((amp < artistMatch) or (amp == artistMatch and smp < songMatch) or (smp == songMatch and tmp < percentMatch) or (leftMatch > amp or rightMatch > amp))):
		return

	newartist = api.artist()
	#newartistalias = api.
	getArtistalias()
	newartist.add_artistName(artistName)
	newindexedArtist = api.indexedArtist()
	newindexedArtist.add_indexedArtistName(artistName)
	mysong.set_artist(newartist)
	if(artistName == artistNameFromArtist):
		mysong.set_artistId(int(artistId))
	mysong.set_releaseDate(ryear)
	mysong.set_indexedArtist(newindexedArtist)
	if (genre != None):	
		mysong.set_genre(setGenre(genre))
	#adding subgenre
	if (subgenlist != None):
		mysong.set_subgenre(setSubgenre(subgenlist))
	if (stylelist != None):	
		print "before mysong.set_styles(setStyle(stylelist))"
		mysong.set_styles(setStyle(stylelist))
		print "after mysong.set_styles(setStyle(stylelist))"

	#UPDATING ALBUMS INFO IN HERE UPDATE DATE 05/26/2013
	if vid.has_key('album'):
		try:
			albumsList = vid['album']
		except IOError as e:
			albumsList = list()
		albList = api.albumList()
		album = api.album()
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
		mysong.set_releaseDate(ryear)
		mysong.set_decade(ryear/10)			
	#BYE BYE OLD ALBUMS :(
	
	match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
	mysong.set_overLap(match)

	#get old crawl date
	#print mysong.get_crawlDate()
	ocrDate = datetime.strptime(mysong.get_crawlDate(),'%Y-%m-%d %H:%M:%S')
	#print ocrDate
	ncrDate = datetime.strptime(vid['crawldate'],'%Y-%m-%d')
	#print (ocrDate - ncrDate)
	if not ocrDate < ncrDate:
		fname = opdir + "/0000" + url[-11:] + ".xml"
		fx = codecs.open(fname,"w","utf-8")
		fx.write('<?xml version="1.0" ?>\n')
		#Apostolos Chnaged 0 to 1
		
		mysong.export(fx,0)
		fx.close()
		#print "3"
		return 0
	if vid.has_key('published'):
		m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),vid['published'])
		n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),vid['published'])
		ydate = m.group()+" "+n.group()
	else:
		ydate = '0001-01-01 00:00:00'
	#get rating
	if vid.has_key('rating'):
		rating = vid['rating']
	else:
		rating = 0.0
	#get viewcount
	if vid.has_key('viewcount'):
		vc = int(vid['viewcount'])
        else:
		vc=0
	#get crawl date
	if vid.has_key('crawldate'):
		crdt = vid['crawldate']+" 00:00:00"
	#calculate viewcount rate
	now = datetime.now()
	dd = ydate
	yy = int(str(dd)[0:4])
	mm = int(str(dd)[5:7])
	total = (now.year-yy)*12+(now.month-mm)
	if total < 1:
	       total = 1
	rate = float(vc)/total
	fname = opdir + "/0000" +url[-11:] + ".xml"
	fx = codecs.open(fname,"w","utf-8")
	fx.write('<?xml version="1.0" ?>\n')
	mysong.export(fx,1)
	fx.close()
	#print "reached here"
		#mysong.export(sys.stdout,0)'''

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

"""def xmlifPresent(vid):
	global absoluteCount
	i = vid['url'].rfind('&')				
	url = vid['url'][:i].replace('https','http',1)
	idVal = url[-11:]
	path = opdir
	fname = path + "/0000" + idVal + ".xml"
	try:
		fx = codecs.open(fname,"r","utf-8")
	except IOError as e:		
		genXML(vid)
		return	
	#print "This is "+vid
	result = updateartist(vid)
	if result == 1:
		genXML(vid)
"""
def load(filename):
	global vids
	with codecs.open(filename,"r","utf-8") as input:
		vids = json.load(input)

def GetAlias(directory):
	global aliases
	aliases = []

	if(os.path.exists(directory+'/alias.txt') == False):
		print "mistake"
		return
	fread = codecs.open(directory+'/alias.txt','r','utf-8')
	lines = fread.readlines()
	for l in lines:
		aliases.append(l.strip())
	print lines

def CalculateAverages(directory):
	averageCount = 0
	averageCountRecent = 0
	path = directory + "/dump"
	try:
		json_data = open(path)
	except IOError as e:
		print "File does not exist"
		exit()
	songs = json.load(json_data)
	json_data.close()
	count = 0
	ViewcountSum = 0
	ViewCountRateSum = 0
	#now = datetime.datetime.now()
	for s in songs:
		ViewcountSum = ViewcountSum + int(s['viewcount'])
		ViewCountRateSum = ViewCountRateSum + int(s['viewcountRate'])
		count = count + 1
	#print "ratingnumber:"
	#print "length : "
	#print len(songs)
	#print count
	if(len(songs) != 0):
		averageCount = ViewcountSum / len(songs)
		#print "Averages: "
		#print averageCount
		averageCountRecent = ViewCountRateSum / len(songs) 
		#print averagerating
	return averageCount,averageCountRecent

#Main starts here
t1=datetime.now()
songMatch = 0.0
artistMatch = 0.0
tempArMatch = 0.0
leftMatch = 0.0
rightMatch = 0.0
percentMatch = 0.0
doc = libxml2.parseFile('genres_manual.xml')
directory = str(sys.argv[1])
dindex = directory.rfind("/")

GetAlias(directory)
avgcnt,avgcntrece =  CalculateAverages(directory)
"""
try:
	json_data = codecs.open(directory+'/dump',"r","utf-8")
	#strg = json_data.read()
	vids = json.load(json_data)
	#vids = json.loads(strg, object_hook=_decode_dict)
except IOError as e:
	print "Missing dump file"
	exit()
"""
vids = []
try:
	load(directory+'/dump')
except Exception as e:
	exit()
#print vids
#write(vids,directory+"/dumps")

#print dindex
artistId = directory[dindex+1:]
useBelow = directory[0:dindex]
uindex = useBelow.rfind("/")
dirt = useBelow[uindex+1:]
#print "Working on this Artist Now: "
#print artistId
flagstyle = 1
styleHandleString = ""
ttt=0
totalCount = 0
hitCount = 0
failCount = 0
absoluteCount = 0
flist = ""
genre = list()
style = list()
subgenlist = list()
stylelist = list()
level1Gen = list()
slist = list()
lev1 = list()
opdir = "/Volumes/Secondone/vina/solr_newData7"
errordir="/Volumes/Secondone/vina/ERRORS"
failedxmls = "/Volumes/Secondone/vina/solr_newData7/failedxml"
#opdir = "/aurora.cs/local2/apo/solr_newData8"
#errordir="/aurora.cs/local2/apo/ERRORS"
#failedxmls = "/aurora.cs/local2/apo/solr_newData8/failedxml"
#opdir = "/venti/local2/apo/solr_newData8"
#errordir="/venti/local2/apo/solr_newData8/ERRORS"
#failedxmls = "/venti/local2/apo/solr_newData8/failedxml"


#opdir = "/home/navneet/Desktop/june1/mydata"
#failedxmls = "/home/navneet/Desktop/june1/mydata"
#opdir = "solr_newData4"
#failedxmls = "solr_newData4/failedxml"
#opdir = "/home/anudeep/Desktop/newData"
#failedxmls = "/home/anudeep/Desktop/newData/failedxml"

if(os.path.exists(opdir) == False):
	os.mkdir(opdir)
if(os.path.exists(failedxmls) == False):
	os.mkdir(failedxmls)
path = opdir+"/failedurls.txt"
fname = path
fx = codecs.open(fname,"w","utf-8")
fx.write("")
fx.close()
path = directory + "/artist.txt"
try:
	fa = codecs.open(path,"r","utf-8")
except IOError as e:
#	print "Missing artist file!!"
	exit()
line = fa.readline()
if not line:
	print "artist name not found"
	fa.close()
	exit()
fa.close()
global artistNameFromArtist
artistNameFromArtist = str(line)
artistNameFromArtist = artistNameFromArtist.replace("&amp;", "&")
artistNameFromArtist = artistNameFromArtist.replace('\n','',1)
artistNameFromArtist = artistNameFromArtist.strip()
path = directory + "/genres.txt"
aliases.append(artistNameFromArtist)

try:
	fg = codecs.open(path,"r","utf-8")
	while 1:
		line = fg.readline()
		if not line:
			break
		line=str(line).replace("&amp;", "&")
		line = line.replace('\n','')
		if line not in genre:
			genre.append(line)
	fg.close()
except IOError as e:
	print "Missing genres for Artist:"+artistNameFromArtist+" Folder:"+directory+"\n"
path = directory + "/styles.txt"
length = 0
try:
	fs = codecs.open(path,"r","utf-8")
	while 1:
		line = fs.readline()
		if not line:
			break
		line=str(line).replace("&amp;", "&")
		#styleHandleString = styleHandleString + line
		length = length + len(line.strip())
		style.append(line.replace('\n',''))
	fs.close()
except IOError as e:
	print "Missing styles for Artist:"+artistNameFromArtist+" Folder:"+directory+"\n"
	flagstyle = 0
genl1 = doc.xpathEval("/categories/*")
for l in genl1:
	#print l.name
	lev1.append(l.name)
"""for g in genre:
	#print g
	if g in lev1:
		if g not in level1g:
			level1g.append(g)
	else:
		wronggen.append(g)"""
print "Before styles loop"		
if length > 0:
	for st in style:
		st = st.strip()
		if(st == ''):
			continue;
		st = str(st).replace("&amp;", "&")
		if st in lev1 and st not in genre:
			genre.append(st)
		stE = encodexml(st)
		print "stE   " +str(stE)
		xmlpath = "//"+stE+"/.."
		print "xmlpath  " +str(xmlpath)
		try:
			parentnode = doc.xpathEval(xmlpath)
			for node in parentnode:
				parent = node.name
				if parent in lev1: 
					if st not in subgenlist:
						subgenlist.append(st)
				elif parent == "categories":
					if st not in genre:
						genre.append(st)
				elif parent not in subgenlist:
					subgenlist.append(parent)
					if st not in stylelist:
						stylelist.append(st)
		except:
			print "Calling write1"
			write1("Error in styles    xmlpath   " +str(xmlpath) +str(directory),errordir+"/StyleError")
			print "Error in styles    xmlpath   " +str(xmlpath)
for l2 in subgenlist:
	stE = encodexml(l2)
	xmlpath = "//"+stE+"/.."
	try:
		parentnode = doc.xpathEval(xmlpath)
		for node in parentnode:
			parent = node.name
			if parent in lev1 and parent not in genre:
				genre.append(parent)
	except:
		print "Error in styles    xmlpath   " +str(xmlpath)
		
	


for v in vids:
	ttt=ttt+1
	t3=datetime.now()
	#print "v in vid============================================================================"
	#print v
	#print "ttt in vids=========================================================================================="
	#print ttt
	genXML(v,avgcnt,avgcntrece)
	#print "time for xmlifpresent= " +str(datetime.now()-t3)
#print "len"+str(length)
t2=datetime.now()
print "time=" +str(t2-t1)
