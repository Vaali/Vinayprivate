'''
Vinay Kumar pamarthi
Program to get the songs from the songs.txt and then crawl youtube to get the appropriate video for the song if present.
The song is selected based on the match percent of the artist,song and total match.



'''



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
import collections
from datetime import datetime, date, timedelta
import logging
import songs_api as api
reload(sys)
sys.setdefaultencoding('utf8')
#fwritetext = codecs.open("id2.txt",'a','utf8')
hits = 0
full_lang_list = {}
full_country_list = {}
full_country_list_sort = {}

misses = 0
class Album_Data():
	pass

class Video(object):
	pass

def getArtistName(line):
	lindex1 = line.find("||~||")
	mainArtist = line[0:lindex1]
	return mainArtist

def getSongName(line):
	lindex2 = line.find("|~:~")
	lindex3 = line.find("|#||#|")
	delen = len("|#||#|")
        songName = line[lindex3+delen:lindex2]
	return songName

def getYear(line):
	lindex1 = line.find("|~~|")
	delen = len("|~~|")
	lindex2 = line.find("|:~:|")
	albumYear = line[lindex1+delen:lindex2]
	return albumYear

def getFeatArtists(line):
	fList = list()
	lindex2 = line.find("||~||")
	temp = line[lindex2+len("||~||"):len(line)]
	if  (temp.find("||~||") != -1):
		lindex1 = temp.find("||~|||~||#|")
		artistline = temp[0:lindex1]
		fList = artistline.split("||~||")
	return fList

def getAlbumInfo(line):
	albumInfo = ""
	lindex1 = line.find("|~:~")
	lindex2 = line.find("|~~|")
	albumInfo = line[lindex1+len("|~:~"):lindex2]
	return albumInfo

def getConnectors(line):
	cList = list()
	if(line.find("||~|||~||#|") != -1):
		lindex1 = line.find("|#|")+3
		lindex2 = line.find("|#||#|")
		cLine = line[lindex1:lindex2]
		cList = cLine.split("|#|")
	return cList

def getCountry(line):
	lindex1 = line.find('|:~:|')
	lindex2 = line.find('|~:|')
	countryName = line[lindex1+len('|:~:|'):lindex2]
	countryName = countryName.replace('[','')
	countryName = countryName.replace(']','')
	if(countryName.strip() == ''):
		return ''
	if(countryName.strip() not in full_country_list):
		full_country_list[countryName] = 1
	else:
		full_country_list[countryName] = full_country_list[countryName] + 1
	return countryName

def getLanguage(line):
	lindex1 = line.find('|~:|')
	lindex2 = line.find('|:~|')
	language = line[lindex1+len('|~:|'):lindex2]
	language = language.replace("[","")
	language = language.replace("]","")
	if(language.strip() == ''):
		return ''
	if(language.strip() not in full_lang_list):
		full_lang_list[language] = 1
	else:
		full_lang_list[language] = full_lang_list[language] + 1
	return language

def getBarcode(line):
	lindex1 = line.find('|:~|')
	#lindex2 = line.find('')
	barcode = line[lindex1+len('|:~|'):len(line)]
	return barcode


#Calculates the match of the original song with the  youtube video 

def CalculateMatch(curr_elem,vid_title):
	list = ""
	conlist = ""
	artistName = curr_elem['artistName']
	ftArtistName = curr_elem['featArtists']
	connectorList = curr_elem['connectors']
	songName = curr_elem['name']
	#fw = open("match.txt",'a')
	tm = 0
	sm = 0
	am = 0
	#print (line)
	#print "=============================================TOTAL COUNT+++++++++++++++++++++++++++++++++++++++++++++++++++++++"
	#print totalCount
	fList = ""
	for f in ftArtistName:
		fList = fList+" "+f
	ftartists = ""
	if(len(fList)!=0):
		ftartists = fList[0:]
	allArtists = artistName+" "+ftartists
	for c in connectorList:
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
		#mysong.set_substring_song("false")
		substring_song = "false"
	if(vid_title.lower().find(artistName.lower())!= -1):
		#mysong.set_substring_artist("true")
		substring_artist = "true"
	else:
		#mysong.set_substring_artist("false")
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

	ftArtistSet = re.findall("\w+",ftartists.lower(),re.U)
	ftAMatch = 0
	ftMatch = 0
	songMatch = 0
	for artist in ftArtistSet:
		if(yname.find(artist)!= -1):
			ftAMatch = ftAMatch + 1
	if(len(ftArtistSet)!=0):
		ftMatch = ftAMatch*100/len(ftArtistSet)
	#mysong.set_substring_ftartist(ftMatch);
	remove = "lyrics official video hd hq edit music lyric audio acoustic videoclip featuring ft feat radio remix "
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
		snameset1 = set(re.findall("\w+",sname.lower(),re.U))-set(diffset)
		snameset = set(snameset1)
		sreadset = re.findall("\w+",songName.lower(),re.U)
		common1 = (set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		#if "feat" in anameset:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower(),re.U)
		#else:
		#	areadset = re.findall("\w+",allArtists.lower()+"."+conlist.lower().replace("feat","ft"),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = (set(anameset).intersection(set(areadset)))
		if float(len(anameset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
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
		#mysong.set_overLap(match)
		#if (leftMatch == 100.0):
		#	mysong.set_decision("Correct")
		#print match
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
		common1 = (set(snameset).intersection(set(sreadset)))
		if float(len(snameset)) !=0:
			songMatch = len(common1)*100/float(len(snameset))
		anameset = re.findall("\w+",aname.lower(),re.U)
		areadset = re.findall("\w+",artistName.lower(),re.U)
		common2 = (set(anameset).intersection(set(areadset)))
		if float(len(anameset)) !=0:
			artistMatch = len(common2)*100/float(len(anameset))
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
		#else:
		#	artistMatch = tempArMatch
		#	match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", am:"+str(artistMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)			
		#mysong.set_overLap(match)
	if((y1 == -1) and (y2 == -1)):	
		#print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
		arnameset = re.findall("\w+",artistName.lower(),re.U)
		leftset = yresultset[:len(arnameset)]
		rightset = yresultset[-len(arnameset):]
		leftIntersection = (set(leftset).intersection(set(arnameset)))
		rightIntersection = (set(rightset).intersection(set(arnameset)))
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
			common_set = (set(yresultset[-len(songreadset):]).intersection(set(songreadset)))
			yresultset = (set(yresultset) - set(arnameset))
			if float(len(songreadset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
			tm = percentMatch
			sm = songMatch
			am = leftMatch
		else:
			songreadset = re.findall("\w+",songName.lower(),re.U)		
			common_set = (set(yresultset[:len(songreadset)]).intersection(set(songreadset)))
			yresultset = (set(yresultset) - set(arnameset))
			if float(len(songreadset)) !=0:
				songMatch = len(common_set)*100/float(len(songreadset))
			match = "tm:"+str(percentMatch)+", sm:"+str(songMatch)+", lam:"+str(leftMatch)+", ram:"+str(rightMatch)
			tm = percentMatch
			sm = songMatch
			am = leftMatch	
		#mysong.set_overLap(match)
		#print arnameset
		#print leftset
		#print rightset
	#rules
	#fw.write(curr_elem['name']+'\t' + vid_title + '\n')
	#fw.write(str(match)+'\n')
	#fw.close()

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
	#fwritetext.write(match + "\t" + artistName + "\t" + songName + "\t" + vid_title + "\t" + decision + "\n")
	
	#if(decision == "Incorrect"):
	#	fwritetext.write(match + "\t" + artistName + "\t" + songName + "\t" + vid_title + "\t" + decision + "\n")
	return decision,match,tm,sm,am


def getVideo(curr_elem,v):
	global hits
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
		if(len(l['country'])> 0):
			album_details.country = l['country']
		else:
			album_details.country = 'No Country'			
		if(len(l['language'])> 0):
			album_details.language = l['language']
		else:
			album_details.country = 'No Language'

		if(len(l['barCode'])> 0):
			album_details.barcode = l['barCode']
		alist.append(album_details.__dict__)
	if(len(alist)==0):
		print curr_elem
	video.album = alist
	video.year = curr_elem['year']
	video.language = curr_elem['language']
	video.songcountry = curr_elem['songcountry']
	flist = ""
	"""
	for f in video.ftArtist:
		flist = flist+" "+f
	"""
	#Apostolos
	for f in video.ftArtist:
		ttt=f.strip("-")
		flist = flist+" "+ttt
		
	ftartists = flist[1:]
	#Apostolos allArtists = video.artist+" "+ftartists
	allArtists = video.artist.strip("-")+" "+ftartists
	url = "https://gdata.youtube.com/feeds/api/videos/-/Music?q=allintitle%3A"+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&max-results=5&key=AI39si508le_IQuKYfcn6ypItfVuy02D0k7yTR4OOYb1Bcs2bN97vRJcHXkEu3sHc-VtYxFfk0vuMfq79AxiEE_RJ9ZCSCPE4A"
	#print url  
	#Youtube Key AI39si508le_IQuKYfcn6ypItfVuy02D0k7yTR4OOYb1Bcs2bN97vRJcHXkEu3sHc-VtYxFfk0vuMfq79AxiEE_RJ9ZCSCPE4A
	try:
		result = simplejson.load(urllib2.urlopen(url),"utf-8")
	except Exception as e:
		#print curr_elem['name']
		#print e
		misses = misses + 1
		return
		#logging.warning('No results from google',e)
	now = datetime.now()
	if result['feed'].has_key('entry'):
		i = 0
		vcount=0
		newvcount=0
		iindex=-1	
		flag = 0
		vmatch = ""
		vtm = 0
		vsm = 0
		vam = 0
		for videoresult in result['feed']['entry']:
			entry = result['feed']['entry'][i]
			[decision,match,tm,sm,am] = CalculateMatch(curr_elem,entry['title']['$t'])
			#fwritetext.write( entry['link'][0]['href'] + "\n")
			#fwritetext.write(entry['link'][0]['href'] + "\n")
			if entry.has_key('yt$statistics'):
				newvcount = entry['yt$statistics']['viewCount']
				if (decision == "correct" and int(vcount) < int(newvcount)):
					vcount = newvcount
					vmatch = match
					vtm = tm
					vsm = sm
					vam = am
					iindex=i
			i = i + 1
		if(iindex == -1):
			#print len(result['feed']['entry'])
			misses = misses + 1

			#print curr_elem['name']
			#fwritetext.write(curr_elem['artistName']+ "\t" + curr_elem['name']+"\n")

			return						

		'''	entry = result['feed']['entry'][i]	
			video1.url = entry['link'][0]['href']
			print video1.url
			print entry['title']['$t']
			video1.title = entry['title']['$t']
			video1.author = entry['author']	
			video1.img = entry['media$group']['media$thumbnail'][0]['url']
			video1.published = entry['published']['$t']
			video1.length = entry['media$group']['yt$duration']['seconds']
			m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),video1.published)
			n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),video1.published)
			ydate = m.group()+" "+n.group()
			dd = ydate
			yy = int(str(dd)[0:4])
			mm = int(str(dd)[5:7])
			total = (now.year-yy)*12+(now.month-mm)
			if total < 1:
				total = 1	
			video1.length = entry['media$group']['yt$duration']['seconds']
			if(now.month<10):
				mm = '0'+str(now.month)
			else:
				mm = str(now.month)
			if(now.day<10):
				dd = '0'+str(now.day)
			else:
				dd = str(now.day)
			video1.crawldate = str(now.year)+"-"+mm+"-"+dd
			if entry.has_key('yt$statistics'):
				video1.viewcount = entry['yt$statistics']['viewCount']
				video1.viewcountRate = float(video1.viewcount)/total
			if entry.has_key('gd$rating'):
				video1.rating = entry['gd$rating']['average']
		#fwritetext.write (line)
		#fwritetext.write(':')
			fwritetext.write(video1.url)
			i = i + 1
			#print v
			v.append(video1.__dict__)
			video1 = None
			fwritetext.write('\n')'''
		video1 = Video()
		video1.artist = curr_elem['artistName']
		video1.ftArtist = curr_elem['featArtists']
		video1.name = curr_elem['name']
		video1.connectors = curr_elem['connectors']
		video1.album = alist
		video1.year = curr_elem['year']
		video1.language = curr_elem['language']
		video1.songcountry = curr_elem['songcountry']
		entry = result['feed']['entry'][iindex]	
		video1.url = entry['link'][0]['href']
		video1.lang_count = curr_elem['lang_count']
		video1.match = vmatch
		video1.tm = vtm
		video1.sm = vsm
		video1.am = vam
		#print video1.url
		#print entry['title']['$t']
		video1.title = entry['title']['$t']
		video1.author = entry['author']	
		video1.img = entry['media$group']['media$thumbnail'][0]['url']
		video1.published = entry['published']['$t']
		video1.length = entry['media$group']['yt$duration']['seconds']
		m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),video1.published)
		n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),video1.published)
		ydate = m.group()+" "+n.group()
		dd = ydate
		yy = int(str(dd)[0:4])
		mm = int(str(dd)[5:7])
		total = (now.year-yy)*12+(now.month-mm)
		if total < 1:
			total = 1	
		video1.length = entry['media$group']['yt$duration']['seconds']
		if(now.month<10):
			mm = '0'+str(now.month)
		else:
			mm = str(now.month)
		if(now.day<10):
			dd = '0'+str(now.day)
		else:
			dd = str(now.day)
		video1.crawldate = str(now.year)+"-"+mm+"-"+dd
		if entry.has_key('yt$statistics'):
			video1.viewcount = entry['yt$statistics']['viewCount']
			if(total != 0):
				video1.viewcountRate = float(video1.viewcount)/total
		if entry.has_key('gd$rating'):
			video1.rating = entry['gd$rating']['average']
		#fwritetext.write (line)
		#fwritetext.write(':')
			#print v
		v.append(video1.__dict__)
		video1 = None
		hits = hits + 1
	else:
		misses = misses + 1
		#print curr_elem['name']
		#print "no Entry"
	
def getArtistAliasList(sorted_list):
	artist_alias_list = {}
	final_artist_alias_list = []
	for song in sorted_list:
		if(song['artistName'].lower() in artist_alias_list):
			artist_alias_list[song['artistName'].lower()] = artist_alias_list[song['artistName'].lower()] + 1
		else:
			artist_alias_list[song['artistName'].lower()] = 1
	for i in artist_alias_list:
		artist_alias_list[i] = (artist_alias_list[i]*100)/len(sorted_list)
	artist_alias_list = sorted(artist_alias_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
	print artist_alias_list
	for artist in artist_alias_list:
		if(artist[1] > 4.0):
			final_artist_alias_list.append(artist[0])
	return final_artist_alias_list


def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)

def check(date1,date2):
	list1 = date1.split('-')
	list2 = date2.split('-')
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



#Main starts here
directory = str(sys.argv[1])

print "slist========================================" 


########################################## sort the songs to eliminate duplicates ###################################
try:
	fread = codecs.open(directory+'/songs.txt','r','utf8')

	
	fwrite_country = codecs.open(directory+'/country_percentage.txt','w','utf8')
	fchange_country = codecs.open(directory+'/changecountry.txt','w','utf8')
	fchange_language = codecs.open(directory+'/changelanguage.txt','w','utf8')

	try:
		freadcountry = codecs.open(directory+'/origin.txt','r','utf8')
		artist_country = freadcountry.readline().strip()
		print artist_country
	except:
		artist_country = 'Unknown'
	if(artist_country == ''):
		artist_country = 'Unknown'
	curr_line = fread.readline()
	songs_list = list()

#parse each line to extract data for the song in that line
	while curr_line:
		curr_line = curr_line.strip()
		song = {}
		song['artistName'] = getArtistName(curr_line)
		song['featArtists'] = getFeatArtists(curr_line)
		song['name'] = getSongName(curr_line)
		song['connectors'] = getConnectors(curr_line)
		albumInfo = {}
		albumInfo['albumName'] = getAlbumInfo(curr_line)
		albumInfo['year'] = getYear(curr_line)
		albumInfo['country'] = getCountry(curr_line)
		albumInfo['language'] = getLanguage(curr_line)
		albumInfo['barCode'] = getBarcode(curr_line)
		song['albumInfo'] = [albumInfo]
		songs_list.append(song)
		curr_line = fread.readline()

	#write(songs_list,'songs_json.txt')
	#with codecs.open('songs_json.txt',"r","utf-8") as input:
	#	vids = json.load(input)
#sort the songs list 
	sorted_list = sorted(songs_list,key = lambda x:x['name'].lower()) 
	#write(sorted_list,'sorted.txt')
	final_song_list = {}

	############################################# Add the repetitive songs year and album info together ############################

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
				if(stemp['year'] == '1001'):
					stemp['year'] = (s['year'])
				elif (s['year'] != '1001'):
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
		#print final_song_list[Item_id]['songcountry']
		#print Item_id
	#for s in final_song_list:

	#fsl = collections.OrderedDict(sorted(final_song_list.items()))
	#for k in fsl.keys():
	#	print k 
	#	print fsl[k]['year']
	#write(fsl,'songs_json_final.txt')
	#print final_song_list
	total_count = 0
	for i in full_lang_list:
		total_count = total_count + full_lang_list[i]
	percent_lang = {}
	for i in full_lang_list:
		percent_lang[i] = (full_lang_list[i]*100.0)/total_count

	for i in percent_lang:
			fwrite_country.write(str(i) +'\t')
			fwrite_country.write(str(percent_lang[i]))
			fwrite_country.write('\n')
	percent_lang = sorted(percent_lang.iteritems(), key=lambda (k,v): (v,k),reverse = True)
	#print percent_lang
	change_language = ''
	
	if(len(percent_lang) != 0 and percent_lang[0][1] > 97.0):
		change_language = percent_lang[0][0]
	else:
		change_language = ''

	full_lang_list = sorted(full_lang_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
	#full_country_list = sorted(full_country_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
	#print full_country_list

	fwrite_country.write('\n'+ 'Countries : \n')
	for i in full_country_list:
			fwrite_country.write(str(i) +'\t')
			fwrite_country.write(str(full_country_list[i]))
			fwrite_country.write('\n')

	fwrite_country.close()
	full_country_list_sort = sorted(full_country_list.iteritems(), key=lambda (k,v): (v,k),reverse = True)
	if(len(full_country_list) != 0):
		if(artist_country not in full_country_list):
			artist_country = full_country_list_sort[0][0]
	print artist_country
	vid = list()

	for s in final_song_list.values():
		lang_dict = s['lang_count']
		if(artist_country != 'Unknown' and artist_country != s['songcountry'] ):
			#print "Changing"
			fchange_country.write(s['name']+ '\t'+s['songcountry'] + '\t' + artist_country+'\n')
			#s['songcountry'] = artist_country
		s['songcountry'] = artist_country

		temp_lang_list = sorted(lang_dict.iteritems(), key=lambda (k,v): (v,k),reverse = True)
		#print temp_lang_list[0][0]
		if( len(temp_lang_list) >1 and temp_lang_list[0][1] == temp_lang_list[1][1]):
			s['language'] = full_lang_list[0][0]
		elif(len(temp_lang_list)!=0):
			s['language'] = temp_lang_list[0][0]
		if(change_language != '' and s['language']!= change_language):
			fchange_language.write(s['name']+ '\t'+s['language'] + '\t' + change_language+'\n')
			s['language'] = change_language


		#print s['language'] 
		#print s['lang_count']
		if(not s.has_key('artistName')):
			print s
			continue
		if(s['artistName'] in artist_alias_list):
			for art_alias in  artist_alias_list:
				s['artistName'] = art_alias
				getVideo(s,vid)
		else:
			getVideo(s,vid)

	print "Hits:"+str(hits)+" Misses:"+str(misses)

	write(vid,directory+"/dump")

	#fwritetext.write(','.join(songSetList))
	#fwritetext.close()
	fchange_country.close()
	fchange_language.close()
except Exception as e:
	print "Unexpected error:", e