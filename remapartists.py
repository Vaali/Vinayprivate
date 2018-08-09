# -*- coding: utf-8 -*-
""" 
Vinay Kumar Pamarthi
This program will generate the genres based on the popular genres from artists and the categories from the heirachy.py"""

import os
import sys
import re
import codecs
import json
import loggingmodule

reload(sys)
sys.setdefaultencoding('utf8')

def get_genres_matrix():
    genres_list=[]
    with codecs.open('genresmatrix.json', 'r') as f:
        genres_list = json.load(f)
    genres_dict = dict(map(reversed, genres_list.items()))

    return genres_dict


def getmatrixdata(filename):
        artist_dict = {}
        artist_count = 0
        genres_dict = get_genres_matrix()
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
	prevline = ""
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
				'''if(int(words[3]) not in genres_dict):
					genres_dict[int(words[3])] = genre_count
					genre_count = genre_count + 1'''
				line = line.replace(':;'+words[3]+':;',':;'+str(genres_dict[words[2].lower()])+':;')
				line = line + ':;' + words[1]
				remapped_lines.append(line)
				prevline = line
			except Exception as e:
				print e
				print line
				print prevline
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
    genres_dict = get_genres_matrix()
    #genre_count = 0
    fileopen = codecs.open(filename,"r","utf-8")
    lines = []
    remapped_lines = []
    lines = fileopen.readlines()
    #lines = filter(lambda x: x.replace('\n','') != '',lines)
    #lines = map(lambda x:x.replace('\n',''),lines)
    print len(lines)
    '''with codecs.open('genresmatrix.json', 'r') as f:
            genres_list = json.load(f)
        genres_dict = {v: k for k, v in genres_list.iteritems()}'''
    genre_count = len(genres_dict)
    prevline = ''
    for line in lines:
        line = line[:-1]
        #print line
        if(line != ''):
            try:
                #remapped_lines.append(line)
                words = line.split(':;')
                if(words[1] not in songs_dict):
                    songs_dict[words[1]] = songs_count
                    songs_count = songs_count + 1
                    #print words[1]
                    #print songs_count
                    #line = line.replace(':;'+words[1]+':;',':;'+str(songs_dict[words[1]])+':;',1)
                line = line +':;'+str(songs_dict[words[1]])
                if(words[6].lower() not in genres_dict):
                    genres_dict[words[6].lower()] = genre_count
                    #line = line +':;'+str(genre_count)
                    genre_count = genre_count + 1
                    #line = line.replace(':;'+words[6]+':;',':;'+str(genres_dict[words[6]])+':;')
                line = line +':;'+str(genres_dict[words[6].lower()])
                remapped_lines.append(line)
                prevline = line
            except Exception as e:
                logger_errors.exception(e)
                logger_errors.exception(line)
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
logger_errors = loggingmodule.initialize_logger('remapartists.log')
getmatrixdata('logs/combined.txt')
getsongsdata('logs/combinedsongs.txt')
#print artist_dict
#print genres_dict
