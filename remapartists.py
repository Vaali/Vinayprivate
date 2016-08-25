# -*- coding: utf-8 -*-
""" 
Vinay Kumar Pamarthi
This program will generate the genres based on the popular genres from artists and the categories from the heirachy.py"""

import os
import sys
import re
import codecs
reload(sys)
sys.setdefaultencoding('utf8')

def getmatrixdata(filename):
        artist_dict = {}
        artist_count = 0
        genres_dict = {}
        genre_count = 0
        try:
	    fileopen = codecs.open(filename,"r","utf-8")
        except Exception as e:
            return
	lines = []
	remapped_lines = []
	lines = fileopen.readlines()
	'''for line in fileopen:
		line = line.replace('\n','')
		if(line not in lines):
			lines.append(line)'''
	lines = filter(lambda x: x.replace('\n','') != '',lines)
	lines = map(lambda x:x.replace('\n',''),lines)
	print len(lines)
	for line in lines:
		if(line != ''):
			try:
				words = line.split(':;')
                                if(len(words) < 4):
                                    words.append('1001')
				if(int(words[1]) not in artist_dict):
					artist_dict[int(words[1])] = artist_count
					artist_count = artist_count + 1	
				line = line.replace(':;'+words[1]+':;',':;'+str(artist_dict[int(words[1])])+':;',1)
				if(int(words[3]) not in genres_dict):
					genres_dict[int(words[3])] = genre_count
					genre_count = genre_count + 1
				line = line.replace(':;'+words[3]+':;',':;'+str(genres_dict[int(words[3])])+':;')
				line = line + ':;' + words[1]
				remapped_lines.append(line)
			except Exception as e:
				print e
				print line
        print 'artist'
	print len(artist_dict)
        print 'genres'
	print len(genres_dict)
	fileopen.close()
	filewrite = codecs.open('remapped_artist_file.txt',"w","utf-8")	
	for line in remapped_lines:
		filewrite.write(line)
		filewrite.write('\n')
				
def getsongsdata(filename):
    #songname:;youtubeId:;artistid:;artistname:;popularity:;year:;genre:;songid:;genreid
    songs_dict = {}
    songs_count = 0
    genres_dict = {}
    genre_count = 0
    fileopen = codecs.open(filename,"r","utf-8")
    lines = []
    remapped_lines = []
    lines = fileopen.readlines()
    lines = filter(lambda x: x.replace('\n','') != '',lines)
    lines = map(lambda x:x.replace('\n',''),lines)
    print len(lines)
    for line in lines:
        if(line != ''):
            try:
                #remapped_lines.append(line)
                words = line.split(':;')
                if(words[1] not in songs_dict):
                    songs_dict[words[1]] = songs_count
                    songs_count = songs_count + 1
                    #line = line.replace(':;'+words[1]+':;',':;'+str(songs_dict[words[1]])+':;',1)
                line = line +':;'+str(songs_dict[words[1]])
                if(words[6] not in genres_dict):
                    genres_dict[words[6]] = genre_count
                    #line = line +':;'+str(genre_count)
                    genre_count = genre_count + 1
                    #line = line.replace(':;'+words[6]+':;',':;'+str(genres_dict[words[6]])+':;')
                line = line +':;'+str(genres_dict[words[6]])
                remapped_lines.append(line)
	    except Exception as e:
                print e
                print line
    print 'songs'
    print len(songs_dict)
    print 'genres'
    print len(genres_dict)
    fileopen.close()
    filewrite = codecs.open('remapped_songs_file.txt',"w","utf-8")
    for line in remapped_lines:
        filewrite.write(line)
        filewrite.write('\n')		

getmatrixdata('logdir/combined.txt')
getsongsdata('songsdir/combinedsongs.txt')
#print artist_dict
#print genres_dict
