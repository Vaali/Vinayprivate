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
import time
import logging
logging.basicConfig(filename='example.log',level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

import songs_api as api
reload(sys)
sys.setdefaultencoding('utf8')
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
	index2 = barcode.find('|:~|')
	barcode = line[lindex1+len('|:~|'):index2]
	return barcode

def getTimes(line):
	#remove the first one which is barcode
	times = line.split('|:~|')
	times = times[2:]
	if(times[2]!=''):
		atime = round(int(times[2])/1000)
	else:
		atime = 0
	if(times[3]!=''):
		stime = round(int(times[3])/1000)
	else:
		stime = 0
	if(times[1]!=''):
		sduration = round(int(times[1])/1000)
	else:
		sduration = 0
	return atime,stime,sduration
#########################################################
def CalculateMatch(curr_elem,vid_title):
	list = ""
	#conlist = ""
	artistName = curr_elem['artistName']
	#ftArtistName = curr_elem['featArtists']
	#connectorList = curr_elem['connectors']
	songName = curr_elem['albumname'].lower() + " full album"
	songName = songName.replace('(',"")
	songName = songName.replace(')','')
	fw = open("match.txt",'a')
	tm = 0
	sm = 0
	am = 0
	#print (line)
	#print "=============================================TOTAL COUNT+++++++++++++++++++++++++++++++++++++++++++++++++++++++"
	#print totalCount
	fList = ""
	#for f in ftArtistName:
	#	fList = fList+" "+f
	ftartists = ""
	if(len(fList)!=0):
		ftartists = fList[0:]
	allArtists = artistName+" "+ftartists
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
	yname = yname.lower().replace("(","")
	yname = yname.lower().replace(")","")
	yname = yname.lower().replace("full version","")
	yname = yname.lower().replace("lyrics on screen","")
	yname = yname.lower().replace("official music video","")
	yname = yname.lower().replace("with lyrics","")
	#yname = yname.lower().replace("full album","")
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
	remove = "lyrics official video hd hq edit music lyric audio acoustic videoclip featuring ft feat radio remix"
	diffset = re.findall("\w+",remove.lower(),re.U)
	yfullset = re.findall("\w+",yname.lower(),re.U)
	ydiffset = set(yfullset) - set(diffset) 
	yresultset = [o for o in yfullset if o in ydiffset]
	if "feat" in yresultset:
		totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower(),re.U)
	else:
		totalset = re.findall("\w+",allArtists.lower()+"."+songName.lower(),re.U)
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
	fw.write(str(match)+'\n')
	fw.close()

	decision = "correct"

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
	#fwritetext.write(vid_title + "\t" + decision + "\n")
	if(decision == "Incorrect"):
		logging.warning(yname+ "----" + songName + "------"+match)
	#if(decision == "correct"):
	#	print vid_title
	if(curr_elem['albumname'].lower() == 'Young Americans'):
		logging.warning(decision)
	return decision,match,tm,sm,am



def getYoutubeVideoforAlbum(album_name,artist_name,album_time):
	#album_name = album_name.replace('-')
	url = "https://gdata.youtube.com/feeds/api/videos/-/Music?q=allintitle%3A"+urllib.quote_plus(str(album_name))+"+"+urllib.quote_plus(str(artist_name))+"+"+urllib.quote_plus(str("full album"))+"&alt=json&max-results=5&key=AI39si533lh1K1CnVTpmF6G6FI2kdWzSdjYE2jnsxo5-Nx-5VKkMgqBCB0-fd2sCxavZPOJQ8XQUEwqsUvBHRhFnzWNDQTM8FA"
	try:
		result = simplejson.load(urllib2.urlopen(url),"utf-8")
	except Exception as e:
		logging.warning(e)
		return
	now = datetime.now()
	curr_elem = {}
	curr_elem['albumname'] = album_name
	curr_elem['artistName'] = artist_name
	#print url
	#print result
	album_link_list = {}
	try:
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
			duration = 0
			for videoresult in result['feed']['entry']:
				entry = result['feed']['entry'][i]
				tempduration = entry['media$group']['yt$duration']['seconds']
				if entry.has_key('yt$statistics'):
					newvcount = entry['yt$statistics']['viewCount']
					newtime = entry['yt$statistics']
					tempduration = round(int(tempduration))
					video1 = {}
					match =0
					logging.warning(album_time)
					for time in album_time:
						#print album_time
						time = round(time)
						if(int(tempduration) in range(int(time) - 30,int(time) + 30)):
					  		logging.warning("times match ---tempduration "+ str(tempduration) +" --- time" + str(time))
					  		#print entry['link'][0]['href']
					  		#print album_link_list
					  		#fwritetext.write(entry['link'][0]['href']+"\n")
					  		match = 1
					  		if(entry['link'][0]['href'] not in album_link_list):
					  			album_link_list[entry['link'][0]['href']] ={}
					  			album_link_list[entry['link'][0]['href']]['times']= [time]
					  		else:
					  			album_link_list[entry['link'][0]['href']]['times'].append(time)
					  		#print album_link_list
					if(match == 1):
						album_link_list[entry['link'][0]['href']]['title'] = entry['title']['$t']
						album_link_list[entry['link'][0]['href']]['author'] = entry['author']	
						album_link_list[entry['link'][0]['href']]['img'] = entry['media$group']['media$thumbnail'][0]['url']
						album_link_list[entry['link'][0]['href']]['published'] = entry['published']['$t']
						album_link_list[entry['link'][0]['href']]['length'] = entry['media$group']['yt$duration']['seconds']
						album_link_list[entry['link'][0]['href']]['name'] = album_name
						m = re.search(re.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}"),album_link_list[entry['link'][0]['href']]['published'])
						n = re.search(re.compile("[0-9]{2}[:][0-9]{2}[:][0-9]{2}"),album_link_list[entry['link'][0]['href']]['published'])
						ydate = m.group()+" "+n.group()
						dd = ydate
						yy = int(str(dd)[0:4])
						mm = int(str(dd)[5:7])
						total = (now.year-yy)*12+(now.month-mm)
						if total < 1:
							total = 1	
						album_link_list[entry['link'][0]['href']]['length'] = entry['media$group']['yt$duration']['seconds']
						if(now.month<10):
							mm = '0'+str(now.month)
						else:
							mm = str(now.month)
						if(now.day<10):
							dd = '0'+str(now.day)
						else:
							dd = str(now.day)
						album_link_list[entry['link'][0]['href']]['crawldate'] = str(now.year)+"-"+mm+"-"+dd
						if entry.has_key('yt$statistics'):
							album_link_list[entry['link'][0]['href']]['viewcount'] = entry['yt$statistics']['viewCount']
							if(total != 0):
								album_link_list[entry['link'][0]['href']]['viewcountRate'] = float(album_link_list[entry['link'][0]['href']]['viewcount'])/total
						if entry.has_key('gd$rating'):
							album_link_list[entry['link'][0]['href']]['rating'] = entry['gd$rating']['average']
				i = i + 1

			return album_link_list
	except Exception as e:
		logging.warning(e)
		return

def write(self,filename):
	with codecs.open(filename,"w","utf-8") as output:
		json.dump(self,output)

##gets the album time whihc is  closer to the original time mapping.
def getalbumtime(alinks_list,atime_list):
	time_link_mapping ={}
	for link in alinks_list:
		if('http' in link): #check to see the link or the total counts 
			times = alinks_list[link]['times']
			for t in atime_list:
				if(t in times):
					time_link_mapping[t] = link

	return time_link_mapping

def getalbumIds(album_list,album_id):
	#album_id=[]
	for x in album_list:
		for t in album_list[x]:
			tempstr = album_list[x][t][:album_list[x][t].find('&feature=youtube_gdata')]
			if(tempstr[-11:] not in album_id):
				album_id.append(tempstr[-11:])


	return album_id

def getMostFrequentTime(songTimeList):
	#print songs_list_temp
	song_times = {}
	for s in songTimeList:
		tempstr = s[:s.find('&feature=youtube_gdata')]
		#print tempstr[-11:]
		tid = tempstr[-11:]
		stime = int(s[s.find('&t=')+3:s.find('&d=')])
		if(stime not in song_times):
			song_times[stime] = 1
		else:
			song_times[stime] += 1
	ret_list = sorted(song_times, key=song_times.get, reverse=True)
	highest_occurrence = ret_list[0]

	for s in songTimeList:
		if(str(highest_occurrence)  in s):
			return s
	return ""

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
t1=time.time()

directory = str(sys.argv[1])

logging.warning("slist========================================")
fread = codecs.open(directory+'/songs.txt','r','utf8')
curr_line = fread.readline()
crawled_albums = []
total_albums = {}
missed_albums = []
songs_list = []
full_lang_list = {}
full_country_list = {}
full_country_list_sort = {}

fread = codecs.open(directory+'/songs.txt','r','utf8')

path = directory + "/artist.txt"
try:
	fa = codecs.open(path,"r","utf-8")
except IOError as e:
	logging.warning("Missing artist file!!")
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
	
fwrite_country = codecs.open(directory+'/country_percentage.txt','w','utf8')
fchange_country = codecs.open(directory+'/changecountry.txt','w','utf8')
fchange_language = codecs.open(directory+'/changelanguage.txt','w','utf8')

try:
	freadcountry = codecs.open(directory+'/origin.txt','r','utf8')
	artist_country = freadcountry.readline().strip()
	#print artist_country
except:
	artist_country = 'Unknown'
if(artist_country == ''):
	artist_country = 'Unknown'
times ={}
while curr_line:
		curr_line= curr_line.strip()
		song = {}
		song['artistName'] = getArtistName(curr_line)
		song['featArtists'] = getFeatArtists(curr_line)
		song['name'] = getSongName(curr_line)
		song['connectors'] = getConnectors(curr_line)
		albumInfo = {}
		albumInfo['albumname'] = getAlbumInfo(curr_line)
		albumInfo['year'] = getYear(curr_line)
		albumInfo['country'] = getCountry(curr_line)
		albumInfo['language'] = getLanguage(curr_line)
		albumInfo['barCode'] = getBarcode(curr_line)
		song['albumInfo'] = [albumInfo]

		curr_album_name = albumInfo['albumname'].lower()
		albumInfo['albumtime'],albumInfo['songtime'],albumInfo['songduration'] = getTimes(curr_line)
		#times[albumInfo['albumtime']] = albumInfo['songtime']
		if('times' not in albumInfo):
			albumInfo['times'] = {}
			albumInfo['times'][albumInfo['albumtime']] = [albumInfo['songtime'],albumInfo['songduration']]
		else:
			albumInfo['times'][albumInfo['albumtime']] = [albumInfo['songtime'],albumInfo['songduration']]

		if(curr_album_name not in total_albums):
		#total_albums.append(curr_album_name)
			total_albums[curr_album_name]= []
			total_albums[curr_album_name].append(int(albumInfo['albumtime']))
		else:
			times_list = total_albums[curr_album_name]
			if(albumInfo['albumtime'] not in times_list):
				total_albums[curr_album_name].append(int(albumInfo['albumtime']))
		songs_list.append(song)
		curr_line = fread.readline()

#print sorted_list
fwritetext = codecs.open("id2.txt",'w','utf8')


#print total_albums
album_list = {}
for album in total_albums:
	youtubelink_list = getYoutubeVideoforAlbum(album,artistNameFromArtist,total_albums[album])
	
	if(not youtubelink_list):
		continue
	if(album.lower() not in album_list):
		album_list[album.lower()] ={}
	#print youtubelink_list
	for link in youtubelink_list:
		logging.warning(link)
		if(youtubelink_list[link]['name'].lower() in album_list):
			album_list[album.lower()][link] = youtubelink_list[link]
for album in album_list:
	tempviewcount = 0
	tempviewcountrate = 0
	temprating = 0
	for x in album_list[album]:
		tempviewcount += int(album_list[album][x]['viewcount'])
		tempviewcountrate += int(album_list[album][x]['viewcountRate'])
		if('rating' in album_list[album][x]):
			temprating += float(album_list[album][x]['rating'])
	totallen = len(album_list[album].keys())
	if( totallen != 0):
		tempviewcount /= totallen
		tempviewcountrate /= totallen
		temprating /= totallen
	album_list[album]['viewcount'] = tempviewcount
	album_list[album]['viewcountRate'] = tempviewcountrate
	album_list[album]['rating'] = temprating
	#print album_list[album]


		
#print album_list
curr_time = "2020-14-33"
curr_language = ""
curr_song = {}
artist_alias_list = []
#artist_alias_list = getArtistAliasList(sorted_list)
sorted_list = sorted(songs_list,key = lambda x:x['name'].lower()) 
final_song_list = {}

for song in sorted_list:
	Item_id = song['name'].lower()
	Item_id = Item_id + " "
	if(len(song['featArtists'])!= 0):
		temp_str = ','.join(song['featArtists'])
		Item_id = Item_id + temp_str.lower()
	Item_id.strip()
	k = 0
	temp_turl_list = []

	if(Item_id not in final_song_list):
		final_song_list[Item_id] = song
		final_song_list[Item_id]['urllist'] =[]

		for album in song['albumInfo']:
			if(album['albumname'].lower() in album_list):
				album['matched_youtube_links'] = album_list[album['albumname'].lower()]
				link_mapping = getalbumtime(album['matched_youtube_links'],album['times'])
				final_song_list[Item_id]['viewcount'] = album_list[album['albumname'].lower()]['viewcount']
				final_song_list[Item_id]['viewcountRate'] = album_list[album['albumname'].lower()]['viewcountRate']
				final_song_list[Item_id]['rating'] = album_list[album['albumname'].lower()]['rating']	
				for link in link_mapping:
					final_song_list[Item_id]['urllist'].append(link_mapping[link]+'&t='+str(int(album['times'][link][0]))+'&d='+str(int(album['times'][link][1])))
				#print album['times']
		final_song_list[Item_id]['year'] = song['albumInfo'][0]['year']
		final_song_list[Item_id]['language'] = song['albumInfo'][0]['language']
		final_song_list[Item_id]['songcountry'] = song['albumInfo'][0]['country']
		lang_count = {}
		lang_count[final_song_list[Item_id]['language']] = 1
		final_song_list[Item_id]['lang_count'] = lang_count
		final_song_list[Item_id]['album'] = song['albumInfo']
			#final_song_list[Item_id]['url'] = album_list[song['albumInfo'][0]['albumname'].lower()]#+'&t='+ str(int(song['albumInfo'][0]['songtime']))
	else:
		stemp = final_song_list[Item_id]

		for s in song['albumInfo']:
			if(s['albumname'].lower() in album_list):
				s['matched_youtube_links'] = album_list[s['albumname'].lower()]
				#print s['matched_youtube_links']
				link_mapping = getalbumtime(s['matched_youtube_links'],s['times'])
				for link in link_mapping:
					final_song_list[Item_id]['urllist'].append(link_mapping[link]+'&t='+str(int(s['times'][link][0]))+'&d='+str(int(s['times'][link][1])))

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
		stemp['album'].append(s)
	tempviewcount = 0
	tempviewcountrate = 0
	temprating = 0
	final_song_list[Item_id]['artist'] = song['artistName']
	final_song_list[Item_id]['title'] = song['artistName']
	#final_song_list[Item_id]['viewcount'] = song['albumInfo'][0]['viewcount']
	for album in song['albumInfo']:
		if(album['albumname'].lower() in album_list):
			tempviewcount += album_list[album['albumname'].lower()]['viewcount']
			tempviewcountrate += album_list[album['albumname'].lower()]['viewcountRate']
	final_song_list[Item_id]['viewcountRate'] = tempviewcountrate
	final_song_list[Item_id]['viewcount'] = tempviewcount

	final_song_list[Item_id]['ftArtist'] = song['featArtists']
	final_song_list[Item_id]['name'] = song['name']
	final_song_list[Item_id]['connectors'] = song['connectors']
song_album_time_mapping = {}
album_id = []
song_list_dump = []
#album_id = getalbumIds(album_list,album_id)
for s in final_song_list:
	
	if('urllist' in final_song_list[s] and len(final_song_list[s]['urllist']) !=0):
		final_song_list[s]['url'] = getMostFrequentTime(final_song_list[s]['urllist'])
		song_list_dump.append(final_song_list[s])

write(final_song_list,directory+"/dump_data")
write(song_list_dump,directory+"/dump")

fwritetext.close()
#print final_song_list"""
print len(album_list)
logging.warning("album crawl time=" + str(time.time()-t1))

