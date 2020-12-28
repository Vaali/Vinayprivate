# -*- coding: utf-8 -*-
import sys
import codecs
import os
import shutil
import time
import re
from fuzzywuzzy import fuzz
from config import IsSoundCloud
#import songs_api as api


reload(sys)
sys.setdefaultencoding('utf8')

stemwords_uniquelist = ["(Edited Short Version)","(Alternate Early Version)","(Alternate Version)","(Mono)","(Radio Edit)","(Original Album Version)","(Different Mix)","(Music Film)","(Stereo)","(Single Version)","Stereo","Mono","(Album Version)","Demo","(Demo Version)"]


def is_songname_same_artistname(songname,artistname):
    return (songname.lower() == artistname.lower())

def get_artistid(directory):
    directory = directory.strip()
    dindex = directory.rfind("/")
    artistId = directory[dindex+1:]
    return artistId

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

def moveFiles(filename,movetype):
    fname = filename[filename.rfind('/')+1:]
    foldername = filename[:filename.rfind('/')]
    output_directory = foldername+'/'+movetype
    if(not os.path.exists(output_directory)):
      os.makedirs(output_directory)
    if(os.path.exists(os.path.join(output_directory, fname))):
      os.remove(os.path.join(output_directory, fname))
    dest = output_directory+'/'+fname
    shutil.move(filename,dest)

def movefilestodeleted(filename):
    moveFiles(filename,'deletedvideos')

def movefilestofailed(filename):
    moveFiles(filename,'failedvideos')

def movefilestowrong(filename):
    moveFiles(filename,'wrongvideos')

def movefilestocompleted(filename):
    moveFiles(filename,'completedvideos')

def GetYearFromTitle(vid_title,song_name):
    returnYear = 0
    yearList = re.findall(r'\d\d\d\d+',vid_title)
    #print yearList
    if(len(yearList) != 0):
        returnYear = int(yearList[0])
        if(returnYear > 2020):
            returnYear = 0
            #print 'the year is greater than 2020'
        if(vid_title == yearList[0] or (song_name.isdigit() and int(song_name) == yearList[0])):
            returnYear = 0

    return returnYear

def resetZeroTagsFix(oldsong):
    if( oldsong.rating == None ):
        oldsong.rating = 0
    if( oldsong.viewcountRate == None ):
        oldsong.viewcountRate = 0
    if( oldsong.totalMatch == None ):
        oldsong.totalMatch = 0
    if( oldsong.songMatch == None ):
        oldsong.songMatch = 0
    if( oldsong.artistMatch == None ):
        oldsong.artistMatch = 0
    return oldsong


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

def CalculateMatch(video,vid_title,vid_description, logger_error, oldsong = False):
    try:
        list = ""
        conlist = ""
        fList = ""
        albumname = ""
        error_str = ""
        match =""
        tm=0.0
        sm=0.0
        am=0.0
        decision = "Incorrect"
        if(oldsong == False):
            artistName = video['artist']
            ftArtistName = video['ftArtistList']
            connectorList = video['connPhraseList']
            songName = video['songName']
        else:
            if(IsSoundCloud == 0):
                artistName = video['artist'].artistName[0]
                ftArtistName = video['ftArtistList'].ftArtistName
                connectorList = video['connPhraseList'].connPhrase
                songName = video['songName']
            else:
                artistName = video.artist.artistName[0]
                ftArtistName = video.ftArtistList.ftArtistName
                connectorList = video.connPhraseList.connPhrase
                songName = video.songName

        
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
            for l in video['album']:
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
        ftAMatch = 0.0
        ftMatch = 0.0
        songMatch = 0.0
        leftMatch = 0.0
        rightMatch = 0.0

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
        ram = 0
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
        else:
            if(ram == 100):
                ram =0.0
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
    except Exception, e:
            logger_error.exception(e)
    #print error_str
    return decision,match,tm,sm,am,error_str

def GetSize(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

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