import sys
import codecs
import os
import shutil

reload(sys)
sys.setdefaultencoding('utf8')

def is_songname_same_artistname(songname,artistname):
    return (songname.lower() == artistname.lower())

def get_artistid(directory):
    directory = directory.strip()
    dindex = directory.rfind("/")
    artistId = directory[dindex+1:]
    return artistId


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