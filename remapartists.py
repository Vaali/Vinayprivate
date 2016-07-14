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
artist_dict = {}
artist_count = 0
genres_dict = {}
genre_count = 0

def getmatrixdata(filename):
	global artist_count
	global genre_count
	fileopen = codecs.open(filename,"r","utf-8")
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
				line = line.replace(':;'+words[1]+':;',':;'+str(artist_dict[int(words[1])])+':;')
				if(int(words[3]) not in genres_dict):
					genres_dict[int(words[3])] = genre_count
					genre_count = genre_count + 1
				line = line.replace(':;'+words[3]+':;',':;'+str(genres_dict[int(words[3])])+':;')
				line = line + ':;' + words[1]
				remapped_lines.append(line)
			except Exception as e:
				print e
				print line
	print len(remapped_lines)
	print len(lines)
	fileopen.close()
	filewrite = codecs.open('remapped_artist_file.txt',"w","utf-8")	
	for line in remapped_lines:
		filewrite.write(line)
		filewrite.write('\n')
				

		

getmatrixdata('logdir/combined.txt')
#print artist_dict
#print genres_dict
